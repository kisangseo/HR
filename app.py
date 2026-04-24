from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "hr.db"

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


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submitted_at TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                full_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                primary_position TEXT NOT NULL,
                other_positions TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'interest_submitted',
                source TEXT NOT NULL DEFAULT 'csv',
                raw_payload TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_applications_submitted_at ON job_applications(submitted_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_applications_name ON job_applications(full_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_applications_primary_position ON job_applications(primary_position)"
        )


def normalize_key(value: str) -> str:
    normalized = " ".join(value.strip().lower().split())
    return normalized.lstrip("\ufeff")


def split_multi_value(value: str) -> list[str]:
    normalized = value.replace("|", ",").replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


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
    ]

    for fmt in accepted_formats:
        try:
            dt = datetime.strptime(raw_value, fmt)
            return dt.isoformat(timespec="seconds")
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(raw_value)
        return dt.isoformat(timespec="seconds")
    except ValueError:
        return None


def extract_other_positions(row: dict[str, str], primary_position: str) -> list[str]:
    keys = [
        key
        for key in row.keys()
        if "other interested positions" in key or "other positions" in key
    ]
    values = [row[key].strip() for key in keys if row[key].strip()]

    if not values:
        return []

    selected = split_multi_value(values[0])
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
        submitted_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

    primary_position = pick_first(row, ALIASES["primary_position"]) or pick_first_by_substring(
        row, ["primary", "position", "job title"]
    )
    if not primary_position:
        errors.append("Primary position column/value not found.")
    other_positions = extract_other_positions(row, primary_position)
    email = pick_first(row, ALIASES["email"]) or pick_first_by_substring(row, ["email"])
    phone = pick_first(row, ALIASES["phone"]) or pick_first_by_substring(row, ["phone", "mobile"])

    if not email:
        errors.append("Email field missing.")
    if not phone:
        errors.append("Phone field missing.")

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


def ingest_csv(csv_text: str) -> dict[str, Any]:
    clean_text = csv_text.replace("\x00", "")
    sample = clean_text[:4096]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(clean_text), dialect=dialect)
    inserted = 0
    skipped = 0
    parsed_rows = 0
    errors: list[dict[str, Any]] = []
    fieldnames = [normalize_key(name or "") for name in (reader.fieldnames or [])]
    delimiter = getattr(dialect, "delimiter", ",")

    with sqlite3.connect(DB_PATH) as conn:
        for index, raw_row in enumerate(reader, start=2):
            if raw_row is None:
                skipped += 1
                errors.append({"row": index, "reason": "Empty row object from parser."})
                continue
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

            conn.execute(
                """
                INSERT INTO job_applications (
                    submitted_at, first_name, last_name, full_name, email, phone,
                    primary_position, other_positions, status, source, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mapped["submitted_at"],
                    mapped["first_name"],
                    mapped["last_name"],
                    mapped["full_name"],
                    mapped["email"],
                    mapped["phone"],
                    mapped["primary_position"],
                    json.dumps(mapped["other_positions"]),
                    mapped["status"],
                    mapped["source"],
                    json.dumps(mapped["raw_payload"]),
                ),
            )
            inserted += 1
            if row_errors:
                errors.append(
                    {
                        "row": index,
                        "reason": "Record ingested with warnings.",
                        "details": row_errors,
                    }
                )

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

    return {
        "inserted": inserted,
        "skipped": skipped,
        "parsed_rows": parsed_rows,
        "detected_delimiter": delimiter,
        "detected_headers": fieldnames[:20],
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
        sql += " AND lower(full_name) LIKE ?"
        params.append(f"%{filters['name'].lower()}%")

    if filters.get("job_title"):
        sql += " AND lower(primary_position) LIKE ?"
        params.append(f"%{filters['job_title'].lower()}%")

    if filters.get("date_from"):
        sql += " AND submitted_at >= ?"
        params.append(f"{filters['date_from']}T00:00:00")

    if filters.get("date_to"):
        sql += " AND submitted_at <= ?"
        params.append(f"{filters['date_to']}T23:59:59")

    sql += " ORDER BY submitted_at DESC"

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "id": row["id"],
                "submittedAt": row["submitted_at"],
                "name": row["full_name"],
                "email": row["email"],
                "phone": row["phone"],
                "primaryPosition": row["primary_position"],
                "otherPositions": json.loads(row["other_positions"] or "[]"),
                "status": row["status"],
                "source": row["source"],
            }
        )
    return output


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
            data = query_applicants(filters)
            self._send_json({"applicants": data})
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

        result = ingest_csv(body)
        self._send_json(result)


def run() -> None:
    init_db()
    server = ThreadingHTTPServer(("127.0.0.1", 8000), Handler)
    print("HR app running at http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
