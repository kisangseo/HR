from __future__ import annotations

import csv
import io
import json
import os
from collections import Counter
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None

ROOT = Path(__file__).resolve().parent
APP_VERSION = "2026-04-27.ingest-diagnostics-v3"
SQL_CONNECTION_STRING = os.getenv("HR_SQL_CONNECTION_STRING", "").strip()
MAKE_WEBHOOK_TOKEN = os.getenv("HR_MAKE_WEBHOOK_TOKEN", "").strip()
RUN_INGEST_TOKEN = os.getenv("HR_RUN_INGEST_TOKEN", "").strip()
SERVER_HOST = os.getenv("HR_HOST", "127.0.0.1").strip() or "127.0.0.1"
SERVER_PORT = int(os.getenv("PORT") or os.getenv("HR_PORT") or "8000")

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


def insert_mapped_record(cursor, mapped: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO dbo.job_applications (
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


def parse_json_body(raw_body: str) -> dict[str, Any]:
    payload = json.loads(raw_body)
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")
    return payload


def build_record_from_make(payload: dict[str, Any]) -> dict[str, Any] | None:
    full_name = str(
        payload.get("name")
        or payload.get("full_name")
        or payload.get("applicant_name")
        or ""
    ).strip()
    first_name = str(payload.get("first_name") or "").strip()
    last_name = str(payload.get("last_name") or "").strip()
    if not full_name:
        full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    if not full_name:
        return None

    if not first_name and not last_name:
        parts = full_name.split(maxsplit=1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

    submitted_raw = str(
        payload.get("submission_date")
        or payload.get("submitted_at")
        or payload.get("date")
        or ""
    ).strip()
    submitted_at = parse_submitted_at(submitted_raw) if submitted_raw else None
    if not submitted_at:
        submitted_at = datetime.now(timezone.utc).date().isoformat()

    primary_position = str(
        payload.get("primary_position")
        or payload.get("job_title")
        or payload.get("primary")
        or ""
    ).strip()

    other_raw = payload.get("other_positions") or payload.get("other_interested_positions") or []
    if isinstance(other_raw, list):
        other_positions = [str(value).strip() for value in other_raw if str(value).strip()]
    else:
        other_positions = split_multi_value(str(other_raw))
    other_positions = [value for value in other_positions if value.lower() != primary_position.lower()]

    return {
        "submitted_at": submitted_at,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": str(payload.get("email") or "").strip(),
        "phone": normalize_phone(str(payload.get("phone") or payload.get("phone_number") or "")),
        "primary_position": primary_position,
        "other_positions": other_positions,
        "status": "interest_submitted",
        "source": "make_webhook",
        "raw_payload": payload,
    }

# Legacy CSV ingest helper retained for possible future re-enable.
# API routes for /api/ingest-csv are currently disabled.
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
        FROM dbo.job_applications
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


def query_job_titles() -> list[str]:
    sql = """
        SELECT DISTINCT primary_position
        FROM dbo.job_applications
        WHERE primary_position IS NOT NULL
          AND LTRIM(RTRIM(primary_position)) <> ''
        ORDER BY primary_position ASC
    """
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(sql).fetchall()
    return [str(row[0]).strip() for row in rows if str(row[0]).strip()]


def run_email_ingest(scan_limit: int, source_folder: str = "inbox") -> dict[str, Any]:
    from email_ingest import run_ingest

    return run_ingest(scan_limit=max(scan_limit, 1), source_folder=source_folder)


def _http_status(code: int) -> str:
    phrases = {
        200: "OK",
        400: "Bad Request",
        401: "Unauthorized",
        404: "Not Found",
        405: "Method Not Allowed",
        500: "Internal Server Error",
    }
    return f"{code} {phrases.get(code, 'OK')}"


def _wsgi_json(start_response, payload: Any, code: int = 200):
    body = json.dumps(payload).encode("utf-8")
    start_response(
        _http_status(code),
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _wsgi_file(start_response, path: Path, content_type: str):
    if not path.exists():
        return _wsgi_json(start_response, {"error": "Not Found"}, 404)
    data = path.read_bytes()
    start_response(
        _http_status(200),
        [
            ("Content-Type", content_type),
            ("Content-Length", str(len(data))),
        ],
    )
    return [data]


def app(environ, start_response):
    method = (environ.get("REQUEST_METHOD") or "GET").upper()
    path = environ.get("PATH_INFO") or "/"
    query = parse_qs(environ.get("QUERY_STRING") or "")

    content_length_raw = environ.get("CONTENT_LENGTH", "0")
    try:
        content_length = int(content_length_raw or "0")
    except ValueError:
        content_length = 0
    body_text = ""
    if content_length > 0:
        body_text = (environ.get("wsgi.input") or BytesIO()).read(content_length).decode("utf-8")

    if method == "GET":
        if path == "/":
            return _wsgi_file(start_response, INDEX_HTML, "text/html; charset=utf-8")
        if path == "/app.js":
            return _wsgi_file(start_response, STATIC_JS, "text/javascript; charset=utf-8")
        if path == "/styles.css":
            return _wsgi_file(start_response, STATIC_CSS, "text/css; charset=utf-8")
        if path == "/api/version":
            return _wsgi_json(start_response, {"app_version": APP_VERSION, "db_backend": "sqlserver"})
        if path == "/run-ingest":
            provided_token = environ.get("HTTP_X_RUN_TOKEN", "") or (query.get("token") or [""])[0]
            if RUN_INGEST_TOKEN and provided_token != RUN_INGEST_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized run token."}, 401)
            try:
                scan_limit = int((query.get("scan_limit") or ["500"])[0] or "500")
            except ValueError:
                scan_limit = 500
            source_folder = ((query.get("source_folder") or ["inbox"])[0] or "inbox").strip().lower()
            if source_folder not in {"inbox", "processed"}:
                source_folder = "inbox"
            try:
                result = run_email_ingest(scan_limit=scan_limit, source_folder=source_folder)
                return _wsgi_json(start_response, {"ok": True, **result})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/applicants":
            filters = {
                "name": (query.get("name") or [""])[0],
                "job_title": (query.get("job_title") or [""])[0],
                "date_from": (query.get("date_from") or [""])[0],
                "date_to": (query.get("date_to") or [""])[0],
            }
            try:
                data = query_applicants(filters)
                return _wsgi_json(start_response, {"applicants": data})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/job-titles":
            try:
                titles = query_job_titles()
                return _wsgi_json(start_response, {"job_titles": titles})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        return _wsgi_json(start_response, {"error": "Not Found"}, 404)

    if method == "POST":
        if path == "/api/ingest-interest-form":
            if not body_text.strip():
                return _wsgi_json(start_response, {"error": "JSON payload is empty."}, 400)

            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)

            try:
                payload = parse_json_body(body)

                if "body" in payload:
                    fields = extract_email_fields(str(payload.get("body") or ""))
                    submitted_at = parse_submitted_at(str(payload.get("received") or "")) or datetime.now(timezone.utc).date().isoformat()

                    mapped = build_record_from_email(
                        fields,
                        submitted_at=submitted_at,
                        raw_payload=payload,
                    )
                else:
                    mapped = build_record_from_make(payload)
                if not mapped:
                    return _wsgi_json(start_response, {"error": "Could not parse applicant name from payload."}, 400)
                if contains_test_name(mapped["full_name"]):
                    return _wsgi_json(start_response, {"inserted": 0, "skipped": 1, "reason": "Name contains 'test'."})
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    insert_mapped_record(cursor, mapped)
                    conn.commit()
                return _wsgi_json(start_response, {"inserted": 1, "source": "make_webhook"})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/ingest-csv":
            # CSV ingest is intentionally disabled for now to avoid manual user uploads.
            # Legacy handler kept commented for quick restore:
            # if not body_text.strip():
            #     return _wsgi_json(start_response, {"error": "CSV payload is empty."}, 400)
            # try:
            #     result = ingest_csv(body_text)
            #     return _wsgi_json(start_response, result)
            # except Exception as exc:
            #     return _wsgi_json(start_response, {"error": str(exc)}, 500)
            return _wsgi_json(start_response, {"error": "CSV ingest is disabled."}, 410)

        return _wsgi_json(start_response, {"error": "Not Found"}, 404)

    return _wsgi_json(start_response, {"error": "Method Not Allowed"}, 405)


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

        if parsed.path == "/api/job-titles":
            try:
                self._send_json({"job_titles": query_job_titles()})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/version":
            self._send_json({"app_version": APP_VERSION, "db_backend": "sqlserver"})
            return

        if parsed.path == "/run-ingest":
            query = parse_qs(parsed.query)
            provided_token = self.headers.get("X-Run-Token", "") or (query.get("token") or [""])[0]
            if RUN_INGEST_TOKEN and provided_token != RUN_INGEST_TOKEN:
                self._send_json({"error": "Unauthorized run token."}, 401)
                return
            try:
                scan_limit = int((query.get("scan_limit") or ["500"])[0] or "500")
            except ValueError:
                scan_limit = 500
            source_folder = ((query.get("source_folder") or ["inbox"])[0] or "inbox").strip().lower()
            if source_folder not in {"inbox", "processed"}:
                source_folder = "inbox"
            try:
                result = run_email_ingest(scan_limit=scan_limit, source_folder=source_folder)
                self._send_json({"ok": True, **result})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return

        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/ingest-interest-form":
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            if not body.strip():
                self._send_json({"error": "JSON payload is empty."}, 400)
                return

            provided_token = self.headers.get("X-Webhook-Token", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                self._send_json({"error": "Unauthorized webhook token."}, 401)
                return

            try:
                payload = parse_json_body(body)

                # NEW: handle raw email from MAKE
                if "body" in payload:
                    email_text = payload.get("body", "")
                    fields = extract_email_fields(email_text)

                    mapped = build_record_from_email(
                        fields,
                        submitted_at=payload.get("received"),
                        raw_payload=payload
                    )
                else:
                    mapped = build_record_from_make(payload)
                if not mapped:
                    self._send_json({"error": "Could not parse applicant name from payload."}, 400)
                    return
                if contains_test_name(mapped["full_name"]):
                    self._send_json({"inserted": 0, "skipped": 1, "reason": "Name contains 'test'."})
                    return
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    insert_mapped_record(cursor, mapped)
                    conn.commit()
                self._send_json({"inserted": 1, "source": "make_webhook"})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path != "/api/ingest-csv":
            self.send_error(404)
            return

        # CSV ingest is intentionally disabled for now to avoid manual user uploads.
        # Legacy handler kept commented for quick restore:
        # content_length = int(self.headers.get("Content-Length", "0"))
        # body = self.rfile.read(content_length).decode("utf-8")
        #
        # if not body.strip():
        #     self._send_json({"error": "CSV payload is empty."}, 400)
        #     return
        #
        # try:
        #     result = ingest_csv(body)
        #     self._send_json(result)
        # except Exception as exc:
        #     self._send_json({"error": str(exc)}, 500)
        self._send_json({"error": "CSV ingest is disabled."}, 410)


def run() -> None:
    server = ThreadingHTTPServer((SERVER_HOST, SERVER_PORT), Handler)
    print(f"HR app running at http://{SERVER_HOST}:{SERVER_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    run()
