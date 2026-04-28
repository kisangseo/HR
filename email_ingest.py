from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import msal
import pyodbc
import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
PROCESSED_FOLDER_NAME = "processed"
APPLICATIONS_TABLE = "dbo.job_applications"

MAILBOX_EMAIL = (os.getenv("MAILBOX_EMAIL") or "").strip()
TARGET_SENDER = (os.getenv("JOB_APP_SENDER", "noreply@baltimorecitysheriff.gov") or "").strip().lower()
SENDER_MATCH_MODE = (os.getenv("JOB_APP_SENDER_MATCH_MODE", "exact") or "exact").strip().lower()
SUBJECT_CONTAINS = (os.getenv("JOB_APP_SUBJECT_CONTAINS", "Job Application") or "").strip().lower()
INBOX_SCAN_LIMIT = int(os.getenv("INBOX_SCAN_LIMIT", "500"))
SQL_CONNECTION_STRING = (os.getenv("HR_SQL_CONNECTION_STRING") or "").strip()
INGEST_SOURCE = (os.getenv("JOB_APP_INGEST_SOURCE", "csv") or "csv").strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class ParsedApplication(dict):
    name: str
    email: str
    phone: str
    primary_position: str
    other_positions: list[str]


def get_access_token() -> str:
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    tenant_id = os.getenv("TENANT_ID")

    if not all([client_id, client_secret, tenant_id]):
        raise RuntimeError("CLIENT_ID, CLIENT_SECRET, and TENANT_ID must be set")

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret,
    )

    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    token = result.get("access_token")
    if not token:
        raise RuntimeError(f"Failed to obtain token: {result}")
    return token


def get_sql_connection() -> pyodbc.Connection:
    if not SQL_CONNECTION_STRING:
        raise RuntimeError("HR_SQL_CONNECTION_STRING is not set")
    return pyodbc.connect(SQL_CONNECTION_STRING)


def get_processed_folder_id(token: str, mailbox_email: str) -> str:
    endpoint = f"{GRAPH_BASE}/users/{mailbox_email}/mailFolders/inbox/childFolders"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {"$select": "id,displayName", "$top": "200"}

    next_url: str | None = endpoint
    next_params: dict[str, str] | None = params

    while next_url:
        response = requests.get(next_url, params=next_params, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()

        for folder in payload.get("value", []):
            if (folder.get("displayName") or "").strip().lower() == PROCESSED_FOLDER_NAME:
                folder_id = folder.get("id")
                if folder_id:
                    return folder_id

        next_url = payload.get("@odata.nextLink")
        next_params = None

    raise RuntimeError(f"Unable to locate '{PROCESSED_FOLDER_NAME}' folder under Inbox")


def fetch_folder_messages(token: str, mailbox_email: str, folder_id: str, scan_limit: int) -> list[dict[str, Any]]:
    endpoint = f"{GRAPH_BASE}/users/{mailbox_email}/mailFolders/{folder_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": 'outlook.body-content-type="html"',
    }
    params = {
        "$orderby": "receivedDateTime desc",
        "$select": "id,subject,body,from,sender,receivedDateTime,sentDateTime",
        "$top": "50",
    }

    messages: list[dict[str, Any]] = []
    next_url: str | None = endpoint
    next_params: dict[str, str] | None = params

    while next_url and len(messages) < scan_limit:
        response = requests.get(next_url, params=next_params, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()

        messages.extend(payload.get("value", []))
        if len(messages) >= scan_limit:
            break

        next_url = payload.get("@odata.nextLink")
        next_params = None

    return messages[:scan_limit]


def move_email_to_processed_folder(token: str, mailbox_email: str, message_id: str, destination_folder_id: str) -> None:
    endpoint = f"{GRAPH_BASE}/users/{mailbox_email}/messages/{message_id}/move"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"destinationId": destination_folder_id}

    response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
    response.raise_for_status()


def extract_sender_address(message: dict[str, Any]) -> str:
    sender = ((message.get("from") or {}).get("emailAddress") or {}).get("address")
    if not sender:
        sender = ((message.get("sender") or {}).get("emailAddress") or {}).get("address")
    return (sender or "").strip().lower()


def strip_html_to_text(body_html: str) -> str:
    body_html = body_html or ""
    body_html = re.sub(r"<br\\s*/?>", "\n", body_html, flags=re.IGNORECASE)
    body_html = re.sub(r"</p\\s*>", "\n", body_html, flags=re.IGNORECASE)
    body_html = re.sub(r"</div\\s*>", "\n", body_html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", body_html)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = text.replace("\r", "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_job_application_email(body_html: str) -> ParsedApplication:
    text = strip_html_to_text(body_html)
    label_map = {
        "name": "name",
        "email": "email",
        "phone number": "phone",
        "primary position you are applying for": "primary_position",
        "other interested positions": "other_positions",
    }
    known_labels = set(label_map.keys()) | {"sent from"}
    collected: dict[str, list[str]] = {key: [] for key in label_map.values()}
    current_key: str | None = None

    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    for line in lines:
        if not line:
            continue
        normalized_line = line.lower().rstrip(":")
        if normalized_line in known_labels:
            if normalized_line == "sent from":
                current_key = None
                continue
            current_key = label_map[normalized_line]
            continue
        if current_key:
            collected[current_key].append(line)

    name = " ".join(collected["name"]).strip()
    email_value = " ".join(collected["email"]).strip()
    phone = " ".join(collected["phone"]).strip()
    primary_position = " ".join(collected["primary_position"]).strip()
    other_raw = "\n".join(collected["other_positions"]).strip()

    other_parts = [
        part.strip()
        for chunk in re.split(r"\n+", other_raw)
        for part in chunk.split(",")
        if part.strip()
    ]

    return ParsedApplication(
        name=name,
        email=email_value,
        phone=phone,
        primary_position=primary_position,
        other_positions=other_parts,
        raw_text=text,
    )


def parse_name_from_subject(subject: str) -> str:
    text = (subject or "").strip()
    if not text:
        return ""
    match = re.search(r"job application(?: form)?\s+(.+)$", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def split_name(full_name: str) -> tuple[str, str]:
    value = (full_name or "").strip()
    if not value:
        return "", ""
    parts = value.split(maxsplit=1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""
    return first_name, last_name


def insert_application(cursor: pyodbc.Cursor, parsed: ParsedApplication, message: dict[str, Any]) -> None:
    subject = message.get("subject") or ""
    parsed_name = (parsed.get("name") or "").strip()
    if not parsed_name:
        parsed_name = parse_name_from_subject(subject)
    first_name, last_name = split_name(parsed_name)

    submitted_at_raw = message.get("receivedDateTime") or message.get("sentDateTime")
    submitted_at_dt = datetime.now(timezone.utc)
    if submitted_at_raw:
        try:
            submitted_at_dt = datetime.fromisoformat(submitted_at_raw.replace("Z", "+00:00"))
        except ValueError:
            pass

    raw_payload = {
        "message_id": message.get("id"),
        "subject": message.get("subject") or "",
        "sender": extract_sender_address(message),
        "parsed": {
            "name": parsed_name,
            "email": parsed.get("email", ""),
            "phone": parsed.get("phone", ""),
            "primary_position": parsed.get("primary_position", ""),
            "other_positions": parsed.get("other_positions", []),
        },
    }

    logging.info(
        "DB insert target=%s message_id=%s name=%s email=%s phone=%s primary_position=%s",
        APPLICATIONS_TABLE,
        message.get("id") or "",
        parsed_name,
        (parsed.get("email") or "").strip(),
        (parsed.get("phone") or "").strip(),
        (parsed.get("primary_position") or "").strip(),
    )

    cursor.execute(
        """
        INSERT INTO dbo.job_applications (
            submitted_at, first_name, last_name, email, phone,
            primary_position, other_positions, status, source, raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            submitted_at_dt,
            first_name,
            last_name,
            (parsed.get("email") or "").strip(),
            (parsed.get("phone") or "").strip(),
            (parsed.get("primary_position") or "").strip(),
            json.dumps(parsed.get("other_positions") or []),
            "interest_submitted",
            INGEST_SOURCE,
            json.dumps(raw_payload),
        ),
    )


def is_target_job_application(message: dict[str, Any]) -> bool:
    sender_address = extract_sender_address(message)
    subject = (message.get("subject") or "").strip().lower()
    if SENDER_MATCH_MODE == "contains":
        sender_matches = TARGET_SENDER in sender_address
    else:
        sender_matches = sender_address == TARGET_SENDER
    return sender_matches and SUBJECT_CONTAINS in subject


def run_ingest(scan_limit: int, source_folder: str = "inbox") -> dict[str, int]:
    if not MAILBOX_EMAIL:
        raise RuntimeError("MAILBOX_EMAIL is not set")

    token = get_access_token()
    processed_folder_id = get_processed_folder_id(token, MAILBOX_EMAIL)
    source_folder_normalized = (source_folder or "inbox").strip().lower()
    source_folder_id = "inbox" if source_folder_normalized == "inbox" else processed_folder_id

    logging.info(
        "Email ingest config: mailbox=%s target_sender=%s sender_match_mode=%s subject_contains=%s scan_limit=%s",
        MAILBOX_EMAIL,
        TARGET_SENDER,
        SENDER_MATCH_MODE,
        SUBJECT_CONTAINS,
        max(scan_limit, 1),
    )
    logging.info("Scanning source folder: %s", source_folder_normalized)
    logging.info("DB table target: %s", APPLICATIONS_TABLE)

    messages = fetch_folder_messages(token, MAILBOX_EMAIL, source_folder_id, max(scan_limit, 1))
    logging.info("Fetched %d inbox messages", len(messages))

    if messages:
        sender_counts: dict[str, int] = {}
        for message in messages:
            sender = extract_sender_address(message)
            sender_counts[sender] = sender_counts.get(sender, 0) + 1
        top_senders = sorted(sender_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        logging.info("Top senders in scanned inbox messages: %s", top_senders)

    inserted = 0
    moved = 0
    matched = 0

    with get_sql_connection() as conn:
        cursor = conn.cursor()

        for message in messages:
            message_id = (message.get("id") or "").strip()
            if not message_id:
                continue

            if not is_target_job_application(message):
                continue

            matched += 1
            body_html = ((message.get("body") or {}).get("content") or "")
            parsed = parse_job_application_email(body_html)

            try:
                insert_application(cursor, parsed, message)
                inserted += 1
            except Exception:
                logging.exception("Insert failed for message_id=%s", message_id)
                continue

            if source_folder_normalized == "inbox":
                move_email_to_processed_folder(token, MAILBOX_EMAIL, message_id, processed_folder_id)
                moved += 1

        conn.commit()

    logging.info(
        "Email ingest complete: scanned=%d matched=%d inserted=%d moved=%d",
        len(messages),
        matched,
        inserted,
        moved,
    )
    return {"scanned": len(messages), "matched": matched, "inserted": inserted, "moved": moved}


def main() -> None:
    cli = argparse.ArgumentParser(description="Ingest Baltimore Sheriff job-application emails into SQL Server.")
    cli.add_argument("--scan-limit", type=int, default=INBOX_SCAN_LIMIT, help="Maximum inbox emails to inspect")
    cli.add_argument(
        "--source-folder",
        choices=["inbox", "processed"],
        default="inbox",
        help="Mailbox folder to scan. Use 'processed' to recover previously moved emails.",
    )
    args = cli.parse_args()
    run_ingest(max(args.scan_limit, 1), source_folder=args.source_folder)


if __name__ == "__main__":
    main()
