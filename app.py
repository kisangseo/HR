from __future__ import annotations

import csv
import io
import imaplib
import json
import os
import re
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from email import message_from_bytes
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None

ROOT = Path(__file__).resolve().parent
APP_VERSION = "2026-04-27.ingest-diagnostics-v3"
SQL_CONNECTION_STRING = os.getenv("HR_SQL_CONNECTION_STRING", "").strip()
EMAIL_POLL_ENABLED = os.getenv("HR_EMAIL_POLL_ENABLED", "false").lower() == "true"
EMAIL_IMAP_HOST = os.getenv("HR_IMAP_HOST", "outlook.office365.com")
EMAIL_IMAP_PORT = int(os.getenv("HR_IMAP_PORT", "993"))
EMAIL_IMAP_USER = os.getenv("HR_IMAP_USER", "noreply@baltimorecitysheriff.gov")
EMAIL_IMAP_PASSWORD = os.getenv("HR_IMAP_PASSWORD", "")
EMAIL_IMAP_MAILBOX = os.getenv("HR_IMAP_MAILBOX", "INBOX")
EMAIL_IMAP_PROCESSED_MAILBOX = os.getenv("HR_IMAP_PROCESSED_MAILBOX", "Processed")
EMAIL_SUBJECT_KEYWORD = os.getenv("HR_EMAIL_SUBJECT_KEYWORD", "Job Application Form")
EMAIL_POLL_SECONDS = int(os.getenv("HR_EMAIL_POLL_SECONDS", "60"))
GRAPH_TENANT_ID = os.getenv("HR_GRAPH_TENANT_ID", "").strip()
GRAPH_CLIENT_ID = os.getenv("HR_GRAPH_CLIENT_ID", "").strip()
GRAPH_CLIENT_SECRET = os.getenv("HR_GRAPH_CLIENT_SECRET", "").strip()
GRAPH_MAILBOX = os.getenv("HR_GRAPH_MAILBOX", "noreply@baltimorecitysheriff.gov").strip()
GRAPH_PROCESSED_FOLDER = os.getenv("HR_GRAPH_PROCESSED_FOLDER", "Processed").strip()

INDEX_HTML = ROOT / "index.html"
STATIC_JS = ROOT / "app.js"
STATIC_CSS = ROOT / "styles.css"

ALIASES = {
    "first_name": ["first name", "first_name", "firstname", "name first"],
    "last_name": ["last name", "last_name", "lastname", "name last"],
    "full_name": ["name", "full name", "full_name", "applicant name"],
    "email": ["email", "email address"],
    "phone": ["phone", "phone number", "mobile"],
    "submitted_at": ["submission date", "submitted at", "date", "created at", "timestamp"],
    "primary_position": [
        "primary position",
        "primary position you are applying for",
        "position applied for",
        "job title",
        "position",
    ],
}

def get_sql_connection():
    if pyodbc is None:
        raise RuntimeError(
            "pyodbc is not installed. Install it and set HR_SQL_CONNECTION_STRING to connect to SQL Server."
        )
    if not SQL_CONNECTION_STRING:
        raise RuntimeError("HR_SQL_CONNECTION_STRING is not set.")
    return pyodbc.connect(SQL_CONNECTION_STRING)


def normalize_key(value: str) -> str:
    normalized = " ".join(value.strip().lower().split())
    return normalized.lstrip("\ufeff")


def split_multi_value(value: str) -> list[str]:
    normalized = value.replace("|", ",").replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def make_unique_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique: list[str] = []
    for header in headers:
        base = header or ""
        count = seen.get(base, 0) + 1
        seen[base] = count
        if count == 1:
            unique.append(base)
        else:
            unique.append(f"{base}__dup{count}")
    return unique


def pick_first(row: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        value = row.get(key, "").strip()
        if value:
            return value
    return ""


def pick_first_by_substring(row: dict[str, str], fragments: list[str]) -> str:
    for key, value in row.items():
        key_lower = key.lower()
        if any(fragment in key_lower for fragment in fragments):
            text = (value or "").strip()
            if text:
                return text
    return ""


def parse_submitted_at(raw_value: str) -> str | None:
    raw_value = raw_value.strip()
    if not raw_value:
        return None

    accepted_formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%b-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
    ]

    for fmt in accepted_formats:
        try:
            dt = datetime.strptime(raw_value, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(raw_value)
        return dt.date().isoformat()
    except ValueError:
        return None


def normalize_phone(raw_phone: str) -> str:
    text = (raw_phone or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits


def contains_test_name(full_name: str) -> bool:
    return "test" in (full_name or "").lower()


def canonical_positions(primary_position: str, other_positions: list[str]) -> tuple[str, tuple[str, ...]]:
    primary = (primary_position or "").strip()
    cleaned_other = sorted({(value or "").strip() for value in other_positions if (value or "").strip()})
    return primary.lower(), tuple(value.lower() for value in cleaned_other)


def extract_other_positions(row: dict[str, str], primary_position: str) -> list[str]:
    keys = []
    for key in row.keys():
        key_lower = key.lower()
        if (
            "other interested positions" in key_lower
            or "other positions" in key_lower
            or key_lower.startswith("other inte")
        ):
            keys.append(key)
    values = [row[key].strip() for key in keys if row[key].strip()]

    if not values:
        return []

    selected: list[str] = []
    for value in values:
        selected.extend(split_multi_value(value))
    primary_normalized = primary_position.strip().lower()

    deduped: list[str] = []
    seen = set()
    for value in selected:
        lower = value.lower()
        if lower == primary_normalized:
            continue
        if lower in seen:
            continue
        seen.add(lower)
        deduped.append(value)

    return deduped


def map_row(raw_row: dict[str, str]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    row = {normalize_key(k): (v or "") for k, v in raw_row.items()}

    first_name = pick_first(row, ALIASES["first_name"]) or pick_first_by_substring(
        row, ["name: fi", "first"]
    )
    last_name = pick_first(row, ALIASES["last_name"]) or pick_first_by_substring(
        row, ["name: la", "last"]
    )
    full_name = pick_first(row, ALIASES["full_name"])

    name_parts = [first_name, last_name]
    combined_name = " ".join([part for part in name_parts if part]).strip()
    final_name = combined_name or full_name or "Unknown Applicant"
    if final_name == "Unknown Applicant":
        errors.append("No name fields were detected.")

    submitted_at_raw = pick_first(row, ALIASES["submitted_at"]) or pick_first_by_substring(
        row, ["entry date", "entry d", "submission", "created", "timestamp", " date"]
    )
    submitted_at = parse_submitted_at(submitted_at_raw)
    if not submitted_at:
        if submitted_at_raw.strip():
            errors.append(f"Unrecognized submission date format: {submitted_at_raw!r}.")
        else:
            errors.append("No submission date field found; using ingest timestamp.")
        submitted_at = datetime.now(timezone.utc).date().isoformat()

    primary_position = pick_first(row, ALIASES["primary_position"]) or pick_first_by_substring(
        row, ["primary", "position", "job title"]
    )
    if not primary_position:
        errors.append("Primary position column/value not found.")
    other_positions = extract_other_positions(row, primary_position)
    email = pick_first(row, ALIASES["email"]) or pick_first_by_substring(row, ["email"])
    phone = normalize_phone(
        pick_first(row, ALIASES["phone"]) or pick_first_by_substring(row, ["phone", "mobile"])
    )

    if not email:
        errors.append("Email field missing.")

    if not primary_position:
        return None, errors

    return {
        "submitted_at": submitted_at,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": final_name,
        "email": email,
        "phone": phone,
        "primary_position": primary_position,
        "other_positions": other_positions,
        "status": "interest_submitted",
        "source": "csv",
        "raw_payload": raw_row,
    }, errors


def extract_email_fields(email_text: str) -> dict[str, str]:
    lines = [line.strip() for line in email_text.splitlines()]
    lines = [line for line in lines if line]
    labels = {
        "name": "Name",
        "email": "Email",
        "phone": "Phone Number",
        "primary": "Primary Position You Are Applying For",
        "other": "Other Interested Positions",
    }
    output: dict[str, str] = {}
    for idx, line in enumerate(lines):
        for key, label in labels.items():
            if line.lower() == label.lower():
                # take next non-empty line as value
                for next_idx in range(idx + 1, len(lines)):
                    candidate = lines[next_idx].strip()
                    if candidate and candidate.lower() not in {v.lower() for v in labels.values()}:
                        output[key] = candidate
                        break
    return output


def get_message_body(msg: Message) -> str:
    if msg.is_multipart():
        plain_part = None
        html_part = None
        for part in msg.walk():
            content_type = part.get_content_type()
            if part.get_content_maintype() == "multipart":
                continue
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except LookupError:
                text = payload.decode("utf-8", errors="replace")
            if content_type == "text/plain" and not plain_part:
                plain_part = text
            elif content_type == "text/html" and not html_part:
                html_part = text
        body = plain_part or html_part or ""
    else:
        payload = msg.get_payload(decode=True) or b""
        charset = msg.get_content_charset() or "utf-8"
        try:
            body = payload.decode(charset, errors="replace")
        except LookupError:
            body = payload.decode("utf-8", errors="replace")

    if "<" in body and ">" in body:
        body = re.sub(r"<br\\s*/?>", "\n", body, flags=re.IGNORECASE)
        body = re.sub(r"</p\\s*>", "\n", body, flags=re.IGNORECASE)
        body = re.sub(r"<[^>]+>", " ", body)
    return body


def build_record_from_email(fields: dict[str, str], submitted_at: str, raw_payload: dict[str, Any]) -> dict[str, Any] | None:
    full_name = fields.get("name", "").strip()
    if not full_name:
        return None

    name_parts = full_name.split(maxsplit=1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ""
    primary = fields.get("primary", "").strip()
    other_values = split_multi_value(fields.get("other", ""))
    other_values = [value for value in other_values if value.lower() != primary.lower()]

    return {
        "submitted_at": submitted_at,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": fields.get("email", "").strip(),
        "phone": normalize_phone(fields.get("phone", "")),
        "primary_position": primary,
        "other_positions": other_values,
        "status": "interest_submitted",
        "source": "email_interest_form",
        "raw_payload": raw_payload,
    }


def insert_mapped_record(cursor, mapped: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO job_applications (
            submitted_at, first_name, last_name, email, phone,
            primary_position, other_positions, status, source, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mapped["submitted_at"],
            mapped["first_name"],
            mapped["last_name"],
            mapped["email"],
            mapped["phone"],
            mapped["primary_position"],
            json.dumps(mapped["other_positions"]),
            mapped["status"],
            mapped["source"],
            json.dumps(mapped["raw_payload"]),
        ),
    )

def ingest_csv(csv_text: str) -> dict[str, Any]:
    clean_text = csv_text.replace("\x00", "")
    sample = clean_text[:4096]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel

    base_reader = csv.reader(io.StringIO(clean_text), dialect=dialect)
    rows = list(base_reader)
    if not rows:
        rows = [[]]
    original_headers = rows[0] if rows else []
    unique_headers = make_unique_headers(original_headers)
    inserted = 0
    skipped = 0
    parsed_rows = 0
    errors: list[dict[str, Any]] = []
    fieldnames = [normalize_key(name or "") for name in original_headers]
    delimiter = getattr(dialect, "delimiter", ",")
    seen_row_fingerprints: set[tuple[str, str, tuple[str, ...]]] = set()

    with get_sql_connection() as conn:
        cursor = conn.cursor()
        for index, row_values in enumerate(rows[1:], start=2):
            if row_values is None:
                skipped += 1
                errors.append({"row": index, "reason": "Empty row object from parser."})
                continue
            raw_row = {}
            for col_index, unique_header in enumerate(unique_headers):
                raw_row[unique_header] = row_values[col_index] if col_index < len(row_values) else ""
            parsed_rows += 1
            mapped, row_errors = map_row(raw_row)
            if not mapped:
                skipped += 1
                errors.append(
                    {
                        "row": index,
                        "reason": "Record not ingested.",
                        "details": row_errors or ["Unknown mapping failure."],
                    }
                )
                continue

            if contains_test_name(mapped["full_name"]):
                skipped += 1
                errors.append(
                    {
                        "row": index,
                        "reason": "Record not ingested.",
                        "details": ["Name contains 'test' and was excluded."],
                    }
                )
                continue

            fingerprint = (
                mapped["full_name"].strip().lower(),
                *canonical_positions(mapped["primary_position"], mapped["other_positions"]),
            )
            if fingerprint in seen_row_fingerprints:
                skipped += 1
                errors.append(
                    {
                        "row": index,
                        "reason": "Record not ingested.",
                        "details": ["Exact duplicate in CSV batch was excluded."],
                    }
                )
                continue
            seen_row_fingerprints.add(fingerprint)

            insert_mapped_record(cursor, mapped)
            inserted += 1
            if row_errors:
                errors.append(
                    {
                        "row": index,
                        "reason": "Record ingested with warnings.",
                        "details": row_errors,
                    }
                )
        conn.commit()

    if parsed_rows == 0:
        errors.append(
            {
                "row": 0,
                "reason": "No data rows parsed from file.",
                "details": [
                    "The file may be XLS/XLSX instead of CSV/TSV, or line delimiters are not recognized.",
                    "Try 'Save As CSV UTF-8' and upload again.",
                ],
            }
        )
    elif skipped > 0 and not errors:
        errors.append(
            {
                "row": 0,
                "reason": "Rows were skipped but no row-level diagnostics were captured.",
                "details": [
                    "This usually indicates an old server process is running older code.",
                    "Stop and restart python3 app.py, then try ingest again.",
                ],
            }
        )

    issue_counter: Counter[str] = Counter()
    for issue in errors:
        if issue.get("details"):
            for detail in issue["details"]:
                issue_counter[detail] += 1
        else:
            issue_counter[issue.get("reason", "Unknown issue")] += 1

    return {
        "app_version": APP_VERSION,
        "inserted": inserted,
        "skipped": skipped,
        "parsed_rows": parsed_rows,
        "detected_delimiter": delimiter,
        "detected_headers": fieldnames[:20],
        "issue_count": len(errors),
        "issue_summary": dict(issue_counter.most_common(10)),
        "issues": errors[:200],
    }


def query_applicants(filters: dict[str, str]) -> list[dict[str, Any]]:
    sql = """
        SELECT
            id, submitted_at, full_name, email, phone,
            primary_position, other_positions, status, source
        FROM job_applications
        WHERE 1 = 1
    """

    params: list[str] = []

    if filters.get("name"):
        sql += " AND LOWER(full_name) LIKE ?"
        params.append(f"%{filters['name'].lower()}%")

    if filters.get("job_title"):
        sql += " AND LOWER(primary_position) LIKE ?"
        params.append(f"%{filters['job_title'].lower()}%")

    if filters.get("date_from"):
        sql += " AND CAST(submitted_at AS date) >= ?"
        params.append(filters["date_from"])

    if filters.get("date_to"):
        sql += " AND CAST(submitted_at AS date) <= ?"
        params.append(filters["date_to"])

    sql += " ORDER BY submitted_at DESC"

    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(sql, params).fetchall()

    raw_output: list[dict[str, Any]] = []
    for row in rows:
        submitted_value = row[1]
        if hasattr(submitted_value, "date"):
            submitted_text = submitted_value.date().isoformat()
        else:
            submitted_text = str(submitted_value)[:10]
        raw_output.append(
            {
                "id": row[0],
                "submittedAt": submitted_text,
                "name": row[2],
                "email": row[3],
                "phone": row[4],
                "primaryPosition": row[5],
                "otherPositions": json.loads(row[6] or "[]"),
                "status": row[7],
                "source": row[8],
            }
        )
    # Smart presentation layer:
    # - remove names containing "test"
    # - combine same-name applicants into one row, merging positions
    grouped: dict[str, dict[str, Any]] = {}
    for item in raw_output:
        if contains_test_name(item["name"]):
            continue
        key = item["name"].strip().lower()
        if key not in grouped:
            grouped[key] = {
                **item,
                "allPositions": set([item["primaryPosition"]]) | set(item["otherPositions"]),
            }
            continue

        existing = grouped[key]
        existing["allPositions"].update([item["primaryPosition"]])
        existing["allPositions"].update(item["otherPositions"])
        # Keep latest submission date row as base
        if item["submittedAt"] > existing["submittedAt"]:
            existing["submittedAt"] = item["submittedAt"]
            existing["primaryPosition"] = item["primaryPosition"]
            existing["email"] = item["email"] or existing["email"]
            existing["phone"] = item["phone"] or existing["phone"]

    output: list[dict[str, Any]] = []
    for merged in grouped.values():
        all_positions = {p for p in merged["allPositions"] if p}
        primary = merged["primaryPosition"]
        if primary in all_positions:
            all_positions.remove(primary)
        merged["otherPositions"] = sorted(all_positions)
        merged.pop("allPositions", None)
        output.append(merged)

    output.sort(key=lambda item: item["submittedAt"], reverse=True)
    return output


def poll_interest_form_emails_forever() -> None:
    if not EMAIL_IMAP_PASSWORD:
        print("Email poller disabled: HR_IMAP_PASSWORD is not set.")
        return

    print(f"Email poller started for mailbox {EMAIL_IMAP_USER} ({EMAIL_IMAP_MAILBOX}).")
    while True:
        try:
            with imaplib.IMAP4_SSL(EMAIL_IMAP_HOST, EMAIL_IMAP_PORT) as client:
                client.login(EMAIL_IMAP_USER, EMAIL_IMAP_PASSWORD)
                client.select(EMAIL_IMAP_MAILBOX)
                status, data = client.search(None, f'(UNSEEN SUBJECT "{EMAIL_SUBJECT_KEYWORD}")')
                if status != "OK":
                    time.sleep(EMAIL_POLL_SECONDS)
                    continue

                message_ids = data[0].split()
                if not message_ids:
                    time.sleep(EMAIL_POLL_SECONDS)
                    continue

                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    for message_id in message_ids:
                        fetch_status, payload = client.fetch(message_id, "(RFC822)")
                        if fetch_status != "OK" or not payload or not payload[0]:
                            continue
                        msg = message_from_bytes(payload[0][1])
                        body = get_message_body(msg)
                        fields = extract_email_fields(body)
                        if not fields:
                            client.store(message_id, "+FLAGS", "(\\Seen)")
                            continue

                        date_header = msg.get("Date", "")
                        parsed_date = None
                        if date_header:
                            try:
                                parsed_date = parsedate_to_datetime(date_header)
                            except Exception:
                                parsed_date = None
                        submitted_at = (
                            parsed_date.date().isoformat()
                            if parsed_date else datetime.now(timezone.utc).date().isoformat()
                        )

                        mapped = build_record_from_email(
                            fields,
                            submitted_at=submitted_at,
                            raw_payload={"subject": msg.get("Subject", ""), "fields": fields},
                        )
                        if not mapped or contains_test_name(mapped["full_name"]):
                            client.store(message_id, "+FLAGS", "(\\Seen)")
                            if EMAIL_IMAP_PROCESSED_MAILBOX:
                                client.copy(message_id, EMAIL_IMAP_PROCESSED_MAILBOX)
                                client.store(message_id, "+FLAGS", "(\\Deleted)")
                            continue

                        insert_mapped_record(cursor, mapped)
                        client.store(message_id, "+FLAGS", "(\\Seen)")
                        if EMAIL_IMAP_PROCESSED_MAILBOX:
                            client.copy(message_id, EMAIL_IMAP_PROCESSED_MAILBOX)
                            client.store(message_id, "+FLAGS", "(\\Deleted)")
                    if EMAIL_IMAP_PROCESSED_MAILBOX:
                        client.expunge()
                    conn.commit()

        except Exception as exc:
            print(f"Email poller error: {exc}")
        time.sleep(EMAIL_POLL_SECONDS)


def graph_request(
    method: str,
    url: str,
    access_token: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        text = response.read().decode("utf-8")
        return json.loads(text) if text else {}


def graph_get_access_token() -> str:
    if not GRAPH_TENANT_ID or not GRAPH_CLIENT_ID or not GRAPH_CLIENT_SECRET:
        raise RuntimeError("Graph poller not configured: HR_GRAPH_TENANT_ID/CLIENT_ID/CLIENT_SECRET are required.")

    token_url = f"https://login.microsoftonline.com/{quote(GRAPH_TENANT_ID)}/oauth2/v2.0/token"
    form = (
        "client_id=" + quote(GRAPH_CLIENT_ID)
        + "&client_secret=" + quote(GRAPH_CLIENT_SECRET)
        + "&scope=" + quote("https://graph.microsoft.com/.default")
        + "&grant_type=client_credentials"
    ).encode("utf-8")
    request = Request(
        token_url,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Could not fetch Graph access token: {payload}")
    return token


def graph_get_or_create_folder_id(access_token: str, mailbox: str, folder_name: str) -> str:
    mailbox_q = quote(mailbox)
    folder_q = folder_name.replace("'", "''")
    list_url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/mailFolders"
        f"?$filter=displayName eq '{folder_q}'&$select=id,displayName"
    )
    listed = graph_request("GET", list_url, access_token)
    values = listed.get("value", [])
    if values:
        return values[0]["id"]

    created = graph_request(
        "POST",
        f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/mailFolders",
        access_token,
        payload={"displayName": folder_name},
    )
    folder_id = created.get("id")
    if not folder_id:
        raise RuntimeError(f"Unable to create/find folder {folder_name}")
    return folder_id


def poll_interest_form_graph_forever() -> None:
    print(f"Graph email poller started for mailbox {GRAPH_MAILBOX} (subject: {EMAIL_SUBJECT_KEYWORD}).")
    processed_folder_id = ""
    while True:
        try:
            access_token = graph_get_access_token()
            if not processed_folder_id and GRAPH_PROCESSED_FOLDER:
                processed_folder_id = graph_get_or_create_folder_id(access_token, GRAPH_MAILBOX, GRAPH_PROCESSED_FOLDER)

            mailbox_q = quote(GRAPH_MAILBOX)
            subject_q = EMAIL_SUBJECT_KEYWORD.replace("'", "''")
            messages_url = (
                f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/mailFolders/inbox/messages"
                f"?$select=id,subject,receivedDateTime,body,isRead"
                f"&$top=50"
                f"&$filter=isRead eq false and contains(subject,'{subject_q}')"
                f"&$orderby=receivedDateTime asc"
            )
            result = graph_request("GET", messages_url, access_token)
            messages = result.get("value", [])
            if not messages:
                time.sleep(EMAIL_POLL_SECONDS)
                continue

            with get_sql_connection() as conn:
                cursor = conn.cursor()
                for msg in messages:
                    message_id = msg.get("id")
                    body = (msg.get("body") or {}).get("content", "")
                    fields = extract_email_fields(body)
                    if not fields:
                        graph_request(
                            "PATCH",
                            f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/messages/{quote(message_id)}",
                            access_token,
                            payload={"isRead": True},
                        )
                        continue

                    received = msg.get("receivedDateTime", "")
                    submitted_at = datetime.now(timezone.utc).date().isoformat()
                    if received:
                        try:
                            submitted_at = datetime.fromisoformat(received.replace("Z", "+00:00")).date().isoformat()
                        except ValueError:
                            submitted_at = datetime.now(timezone.utc).date().isoformat()

                    mapped = build_record_from_email(
                        fields,
                        submitted_at=submitted_at,
                        raw_payload={"subject": msg.get("subject", ""), "fields": fields},
                    )
                    if mapped and not contains_test_name(mapped["full_name"]):
                        insert_mapped_record(cursor, mapped)

                    if processed_folder_id:
                        graph_request(
                            "POST",
                            f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/messages/{quote(message_id)}/move",
                            access_token,
                            payload={"destinationId": processed_folder_id},
                        )
                    else:
                        graph_request(
                            "PATCH",
                            f"https://graph.microsoft.com/v1.0/users/{mailbox_q}/messages/{quote(message_id)}",
                            access_token,
                            payload={"isRead": True},
                        )
                conn.commit()
        except Exception as exc:
            print(f"Graph email poller error: {exc}")
        time.sleep(EMAIL_POLL_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Any, code: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, content_type: str, code: int = 200) -> None:
        if not path.exists():
            self.send_error(404)
            return
        data = path.read_bytes()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path == "/":
            self._send_file(INDEX_HTML, "text/html; charset=utf-8")
            return

        if parsed.path == "/app.js":
            self._send_file(STATIC_JS, "text/javascript; charset=utf-8")
            return

        if parsed.path == "/styles.css":
            self._send_file(STATIC_CSS, "text/css; charset=utf-8")
            return

        if parsed.path == "/api/applicants":
            query = parse_qs(parsed.query)
            filters = {
                "name": (query.get("name") or [""])[0],
                "job_title": (query.get("job_title") or [""])[0],
                "date_from": (query.get("date_from") or [""])[0],
                "date_to": (query.get("date_to") or [""])[0],
            }
            try:
                data = query_applicants(filters)
                self._send_json({"applicants": data})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/version":
            self._send_json({"app_version": APP_VERSION, "db_backend": "sqlserver"})
            return

        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/ingest-csv":
            self.send_error(404)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length).decode("utf-8")

        if not body.strip():
            self._send_json({"error": "CSV payload is empty."}, 400)
            return

        try:
            result = ingest_csv(body)
            self._send_json(result)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)


def run() -> None:
    if EMAIL_POLL_ENABLED:
        poller_target = poll_interest_form_graph_forever if GRAPH_TENANT_ID and GRAPH_CLIENT_ID and GRAPH_CLIENT_SECRET else poll_interest_form_emails_forever
        thread = threading.Thread(target=poller_target, daemon=True)
        thread.start()
    server = ThreadingHTTPServer(("127.0.0.1", 8000), Handler)
    print("HR app running at http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
