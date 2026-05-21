from __future__ import annotations

import csv
import io
import json
import logging
import mimetypes
import os
import re

import msal
import requests
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, ContentSettings, generate_blob_sas
from collections import Counter
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None
try:
    from azure.storage.blob import BlobSasPermissions, BlobServiceClient, ContentSettings, generate_blob_sas
except ImportError:  # pragma: no cover
    BlobSasPermissions = None
    BlobServiceClient = None
    ContentSettings = None
    generate_blob_sas = None

ROOT = Path(__file__).resolve().parent
APP_VERSION = "2026-04-29.cognito-upsert-v1"
SQL_CONNECTION_STRING = os.getenv("HR_SQL_CONNECTION_STRING", "").strip()
MAKE_WEBHOOK_TOKEN = os.getenv("HR_MAKE_WEBHOOK_TOKEN", "").strip()
RUN_INGEST_TOKEN = os.getenv("HR_RUN_INGEST_TOKEN", "").strip()
SERVER_HOST = os.getenv("HR_HOST", "127.0.0.1").strip() or "127.0.0.1"
SERVER_PORT = int(os.getenv("PORT") or os.getenv("HR_PORT") or "8000")
CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "").strip()
TENANT_ID = os.getenv("TENANT_ID", "").strip()
MAILBOX_EMAIL = os.getenv("MAILBOX_EMAIL", "").strip()
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "hrjobapp").strip() or "hrjobapp"
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "").strip()
AZURE_STORAGE_SAS_TTL_MINUTES = int(os.getenv("AZURE_STORAGE_SAS_TTL_MINUTES", "10"))
AZURE_STORAGE_BLOB_PREFIX = os.getenv("AZURE_STORAGE_BLOB_PREFIX", "applications").strip() or "applications"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]

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

POSITION_CANONICAL = {
    "court security officer": "Court Security Officer",
    "deputy sheriff": "Deputy Sheriff",
    "radio dispatcher": "Radio Dispatcher",
    "information technology": "Information Technology",
    "communications": "Communications",
    "social worker": "Social Worker",
    "other": "Other",
}
POSITION_SPLIT_PATTERN = re.compile(
    r"(court security officer|deputy sheriff|radio dispatcher|information technology|communications|social worker|other)",
    flags=re.IGNORECASE,
)


def _blob_service_client() -> BlobServiceClient | None:
    if BlobServiceClient is None:
        return None
    if AZURE_STORAGE_CONNECTION_STRING:
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    if AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY:
        return BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=AZURE_STORAGE_ACCOUNT_KEY,
        )
    return None


def _is_azure_blob_url(url: str) -> bool:
    return ".blob.core.windows.net/" in (url or "").lower()


def _parse_blob_url(blob_url: str) -> tuple[str, str] | None:
    parsed = urlparse(blob_url or "")
    parts = parsed.path.lstrip("/").split("/", 1)
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def _sanitize_filename_part(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", (value or "").strip()).strip("-").lower()
    return text[:80] if text else "unknown-applicant"


def _build_blob_name(app_id: int, document_type: str, original_url: str, applicant_name: str | None = None, content_type: str | None = None) -> str:
    ext = ""
    parsed = urlparse(original_url or "")
    tail = (Path(parsed.path).name or "").strip()
    if "." in tail:
        ext = "." + tail.rsplit(".", 1)[-1].lower()
    if not ext and content_type:
        guessed = mimetypes.guess_extension((content_type or "").split(";")[0].strip().lower())
        if guessed:
            ext = guessed
    if not ext:
        ext = ".bin"
    safe_doc = re.sub(r"[^a-z0-9_-]+", "-", document_type.lower()).strip("-") or "document"
    safe_name = _sanitize_filename_part(applicant_name or "")
    return f"{AZURE_STORAGE_BLOB_PREFIX}/{app_id}/{safe_name}_{safe_doc}_{int(datetime.now(timezone.utc).timestamp())}{ext}"


def copy_url_to_azure_blob(app_id: int, document_type: str, source_url: str, applicant_name: str | None = None) -> tuple[str | None, str | None, str | None]:
    source = (source_url or "").strip().strip('"').strip("'").strip("{}")
    if not source:
        return None, None, "empty_source"
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"}:
        return None, None, "invalid_source_url"
    client = _blob_service_client()
    if client is None:
        if BlobServiceClient is None:
            return None, None, "azure_sdk_not_installed"
        return None, None, "azure_storage_not_configured"
    safe_name = _sanitize_filename_part(applicant_name or "")
    if _is_azure_blob_url(source):
        parsed_parts = _parse_blob_url(source)
        if not parsed_parts:
            return source, source, None
        container_name, existing_blob_name = parsed_parts
        existing_base = os.path.basename(existing_blob_name).lower()
        if safe_name and safe_name in existing_base:
            return existing_blob_name, source, None
        source_blob_client = client.get_blob_client(container=container_name, blob=existing_blob_name)
        props = source_blob_client.get_blob_properties()
        content_type = (props.content_settings.content_type if props and props.content_settings else None) or "application/octet-stream"
        content_bytes = source_blob_client.download_blob().readall()
        new_blob_name = _build_blob_name(app_id, document_type, source, applicant_name=applicant_name, content_type=content_type)
        target_blob_client = client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=new_blob_name)
        upload_kwargs: dict[str, Any] = {"overwrite": True}
        if ContentSettings is not None:
            upload_kwargs["content_settings"] = ContentSettings(content_type=content_type)
        target_blob_client.upload_blob(content_bytes, **upload_kwargs)
        return new_blob_name, target_blob_client.url, None
    resp = requests.get(source, timeout=45)
    if resp.status_code >= 400:
        if resp.status_code in {401, 403, 404}:
            return None, None, f"source_expired_or_denied_{resp.status_code}"
        return None, None, f"source_download_failed_{resp.status_code}"
    ctype = (resp.headers.get("Content-Type") or "application/octet-stream").split(";")[0].strip()
    blob_name = _build_blob_name(app_id, document_type, source, applicant_name=applicant_name, content_type=ctype)
    blob_client = client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=blob_name)
    upload_kwargs: dict[str, Any] = {"overwrite": True}
    if ContentSettings is not None:
        upload_kwargs["content_settings"] = ContentSettings(content_type=ctype)
    blob_client.upload_blob(resp.content, **upload_kwargs)
    return blob_name, blob_client.url, None


def build_read_sas_url(blob_url: str) -> str:
    text = (blob_url or "").strip()
    if not text or not _is_azure_blob_url(text):
        return text
    if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY or generate_blob_sas is None or BlobSasPermissions is None:
        return text
    parsed = urlparse(text)
    path_parts = parsed.path.lstrip("/").split("/", 1)
    if len(path_parts) != 2:
        return text
    container, blob_name = path_parts
    download_name = os.path.basename(blob_name) or "document.pdf"
    try:
        token = generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT_NAME,
            container_name=container,
            blob_name=blob_name,
            account_key=AZURE_STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(minutes=AZURE_STORAGE_SAS_TTL_MINUTES),
            content_disposition=f'attachment; filename="{download_name}"',
        )
        return f"{parsed.scheme}://{parsed.netloc}/{container}/{blob_name}?{token}"
    except Exception:
        logging.exception("build_read_sas_url failed for blob_url=%s", text)
        return text

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


def strip_sent_from_suffix(value: str) -> str:
    return re.sub(
        r"(?is)\s*sent from the baltimore city sheriff[’']?s office.*$",
        "",
        (value or "").strip(),
    ).strip(" ,;-")


def split_positions_text(value: str) -> list[str]:
    text = strip_sent_from_suffix(value)
    text = re.sub(r"\s*-\s*\$?\d+(?:\.\d{1,2})?\s*", " ", text)
    text = " ".join(text.split())
    if text in {"—", "-", "--"}:
        return []
    if not text:
        return []
    if "," in text or ";" in text or "|" in text:
        base_parts = split_multi_value(text)
    else:
        matches = POSITION_SPLIT_PATTERN.findall(text)
        base_parts = matches if len(matches) > 1 else [text]

    normalized: list[str] = []
    for part in base_parts:
        key = " ".join((part or "").strip().lower().split())
        if not key:
            continue
        if key in {"—", "-", "--"}:
            continue
        normalized.append(POSITION_CANONICAL.get(key, part.strip()))
    return normalized


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


def normalize_phone_us(raw_phone: str) -> str:
    digits = normalize_phone(raw_phone)
    if len(digits) == 11 and digits.startswith("1"):
        return digits[-10:]
    return digits


def normalize_name(raw_value: str) -> str:
    return " ".join((raw_value or "").strip().lower().split())


def normalize_email(raw_email: str) -> str:
    return (raw_email or "").strip().lower()


def parse_bool(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on", "checked"}:
        return 1
    if text in {"0", "false", "f", "no", "n", "off", "unchecked"}:
        return 0
    return None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_status_label(value: Any) -> str:
    text = str(value or '').strip()
    lowered = text.lower()
    if lowered in {'interest_submitted', 'interest form submitted', 'interest submitted'}:
        return 'Interest Submitted'
    if lowered in {'approval needed for background check', 'needs approval', 'needs attention'}:
        return 'Needs Attention'

    if lowered in {'needs approval - approved', 'needs attention - approved'}:
        return 'Needs Attention - Approved'
    if lowered in {'needs approval - denied', 'needs attention - denied'}:
        return 'Needs Attention - Denied'
    if lowered in {'application/consent to background submitted', 'approved - background check sent', 'background check sent'}:
        return 'Background Check Sent'
    return text


def extract_first_email(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if not text:
        return ""
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0).strip() if match else ""


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
        "status": "Interest Submitted",
        "source": "csv",
        "raw_payload": raw_row,
    }, errors


def insert_mapped_record(cursor, mapped: dict[str, Any]) -> int:
    first_norm = normalize_name(str(mapped.get("first_name") or ""))
    last_norm = normalize_name(str(mapped.get("last_name") or ""))
    email_norm = normalize_email(str(mapped.get("email") or ""))
    phone_norm = normalize_phone_us(str(mapped.get("phone") or ""))
    existing = cursor.execute(
        """
        SELECT TOP 1 id
        FROM dbo.job_applications
        WHERE
          (
            (? <> '' AND ? <> '' AND LOWER(LTRIM(RTRIM(COALESCE(first_name_norm, '')))) = ? AND LOWER(LTRIM(RTRIM(COALESCE(last_name_norm, '')))) = ?)
            OR
            (? <> '' AND LOWER(LTRIM(RTRIM(COALESCE(email_norm, '')))) = ?)
            OR
            (? <> '' AND LTRIM(RTRIM(COALESCE(phone_norm, ''))) = ?)
          )
        ORDER BY COALESCE(updated_at, created_at) DESC
        """,
        (first_norm, last_norm, first_norm, last_norm, email_norm, email_norm, phone_norm, phone_norm),
    ).fetchone()
    if existing:
        app_id = int(existing[0])
        cursor.execute(
            """
            UPDATE dbo.job_applications
            SET submitted_at = ?,
                first_name = ?,
                last_name = ?,
                email = ?,
                phone = ?,
                primary_position = ?,
                other_positions = ?,
                status = COALESCE(NULLIF(?, ''), status),
                source = COALESCE(NULLIF(?, ''), source),
                raw_payload = ?,
                first_name_norm = ?,
                last_name_norm = ?,
                email_norm = NULLIF(?, ''),
                phone_norm = NULLIF(?, '')
            WHERE id = ?
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
                first_norm,
                last_norm,
                email_norm,
                phone_norm,
                app_id,
            ),
        )
        return app_id

    cursor.execute(
        """
        INSERT INTO dbo.job_applications (
            submitted_at, first_name, last_name, email, phone,
            primary_position, other_positions, status, source, raw_payload,
            first_name_norm, last_name_norm, email_norm, phone_norm
        ) OUTPUT INSERTED.id
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))
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
            first_norm,
            last_norm,
            email_norm,
            phone_norm,
        ),
    )
    inserted = cursor.fetchone()
    return int(inserted[0]) if inserted else 0


def upsert_cognito_record(cursor, mapped: dict[str, Any], payload: dict[str, Any]) -> int:
    first_name = str(mapped.get("first_name") or "").strip()
    last_name = str(mapped.get("last_name") or "").strip()
    email = str(mapped.get("email") or "").strip()
    phone = str(mapped.get("phone") or "").strip()

    first_norm = normalize_name(first_name)
    last_norm = normalize_name(last_name)
    email_norm = normalize_email(email)
    phone_norm = normalize_phone_us(phone)

    candidates = cursor.execute(
        """
        SELECT TOP 1 id
        FROM dbo.job_applications
        WHERE
          (
            (LOWER(LTRIM(RTRIM(COALESCE(first_name_norm, '')))) = ? AND LOWER(LTRIM(RTRIM(COALESCE(last_name_norm, '')))) = ?)
            OR
            (LOWER(LTRIM(RTRIM(COALESCE(first_name_norm, '')))) = ? AND LOWER(LTRIM(RTRIM(COALESCE(last_name_norm, '')))) = ?)
          )
          AND (
            (? <> '' AND LOWER(LTRIM(RTRIM(COALESCE(email_norm, '')))) = ?)
            OR
            (? <> '' AND LTRIM(RTRIM(COALESCE(phone_norm, ''))) = ?)
          )
        ORDER BY COALESCE(cognito_date_updated, updated_at, created_at) DESC
        """,
        (first_norm, last_norm, last_norm, first_norm, email_norm, email_norm, phone_norm, phone_norm),
    ).fetchone()

    cognito_form_id = payload.get("cognito_form_id")
    cognito_entry_number = payload.get("cognito_entry_number")
    cognito_entry_id = payload.get("cognito_entry_id")
    cognito_document_candidates = extract_file_urls(payload.get("cognito_document_link"))
    cognito_document_link = clean_text(cognito_document_candidates[0] if cognito_document_candidates else payload.get("cognito_document_link"))
    cognito_pdf_candidates = extract_file_urls(payload.get("cognito_pdf_url"))
    cognito_pdf_url = clean_text(cognito_pdf_candidates[0] if cognito_pdf_candidates else payload.get("cognito_pdf_url"))

    middle_name = clean_text(payload.get("middle_name"))
    address_line1 = clean_text(payload.get("address_line1"))
    address_line2 = clean_text(payload.get("address_line2"))
    city = clean_text(payload.get("city"))
    state = clean_text(payload.get("state"))
    postal_code = clean_text(payload.get("postal_code"))
    country = clean_text(payload.get("country"))
    country_code = clean_text(payload.get("country_code"))
    full_address = clean_text(payload.get("full_address"))
    drivers_license_number = clean_text(payload.get("drivers_license_number"))
    drivers_license_state = clean_text(payload.get("drivers_license_state"))
    resume_file_name = clean_text(payload.get("resume_file_name"))
    resume_file_url = clean_text(payload.get("resume_file_url"))
    resume_content_type = clean_text(payload.get("resume_content_type"))
    signature_png_url = clean_text(payload.get("signature_png_url"))
    signature_svg_url = clean_text(payload.get("signature_svg_url"))
    signature_typed_text = clean_text(payload.get("signature_typed_text"))

    consent_background_investigation = parse_bool(payload.get("consent_background_investigation"))
    has_valid_drivers_license = parse_bool(payload.get("has_valid_drivers_license"))
    felony_conviction = parse_bool(payload.get("felony_conviction"))
    domestic_violence_misdemeanor = parse_bool(payload.get("domestic_violence_misdemeanor"))
    protective_order = parse_bool(payload.get("protective_order"))
    currently_under_charges = parse_bool(payload.get("currently_under_charges"))
    unlawful_drug_use_last_3y = parse_bool(payload.get("unlawful_drug_use_last_3y"))
    prior_police_service = parse_bool(payload.get("prior_police_service"))

    flagged_responses = (
        felony_conviction,
        domestic_violence_misdemeanor,
        protective_order,
        currently_under_charges,
        unlawful_drug_use_last_3y,
    )
    status = "Needs Attention" if any(value == 1 for value in flagged_responses) else "Background Check Sent"

    if candidates:
        app_id = int(candidates[0])
        if cognito_document_link:
            cognito_document_link = persist_document_record(cursor, app_id, "initial_application", cognito_document_link)
        cursor.execute(
            """
            UPDATE dbo.job_applications
            SET
              submitted_at = ?,
              first_name = ?,
              last_name = ?,
              middle_name = COALESCE(?, middle_name),
              email = COALESCE(NULLIF(?, ''), email),
              phone = COALESCE(NULLIF(?, ''), phone),
              primary_position = ?,
              other_positions = ?,
              status = ?,
              source = 'cognito',
              raw_payload = ?,
              first_name_norm = ?,
              last_name_norm = ?,
              email_norm = NULLIF(?, ''),
              phone_norm = NULLIF(?, ''),
              cognito_form_id = ?,
              cognito_entry_number = ?,
              cognito_entry_id = ?,
              cognito_internal_link = COALESCE(?, cognito_internal_link),
              cognito_public_link = COALESCE(?, cognito_public_link),
              cognito_admin_link = COALESCE(?, cognito_admin_link),
              cognito_document_link = COALESCE(?, cognito_document_link),
              cognito_date_created = COALESCE(TRY_CAST(? AS DATETIME2), cognito_date_created),
              cognito_date_submitted = COALESCE(TRY_CAST(? AS DATETIME2), cognito_date_submitted),
              cognito_date_updated = COALESCE(TRY_CAST(? AS DATETIME2), cognito_date_updated),
              address_line1 = COALESCE(?, address_line1),
              address_line2 = COALESCE(?, address_line2),
              city = COALESCE(?, city),
              state = COALESCE(?, state),
              postal_code = COALESCE(?, postal_code),
              country = COALESCE(?, country),
              country_code = COALESCE(?, country_code),
              full_address = COALESCE(?, full_address),
              consent_background_investigation = COALESCE(?, consent_background_investigation),
              has_valid_drivers_license = COALESCE(?, has_valid_drivers_license),
              drivers_license_number = COALESCE(?, drivers_license_number),
              drivers_license_state = COALESCE(?, drivers_license_state),
              felony_conviction = COALESCE(?, felony_conviction),
              domestic_violence_misdemeanor = COALESCE(?, domestic_violence_misdemeanor),
              protective_order = COALESCE(?, protective_order),
              currently_under_charges = COALESCE(?, currently_under_charges),
              unlawful_drug_use_last_3y = COALESCE(?, unlawful_drug_use_last_3y),
              prior_police_service = COALESCE(?, prior_police_service),
              resume_file_name = COALESCE(?, resume_file_name),
              resume_file_url = COALESCE(?, resume_file_url),
              resume_content_type = COALESCE(?, resume_content_type),
              signature_png_url = COALESCE(?, signature_png_url),
              signature_svg_url = COALESCE(?, signature_svg_url),
              signature_typed_text = COALESCE(?, signature_typed_text),
              cognito_pdf_url = COALESCE(NULLIF(?, ''), cognito_pdf_url),
              cognito_pdf_generated_at = CASE WHEN NULLIF(?, '') IS NOT NULL THEN SYSUTCDATETIME() ELSE cognito_pdf_generated_at END,
              last_cognito_sync_at = SYSUTCDATETIME()
            WHERE id = ?
            """,
            (
                mapped["submitted_at"], first_name, last_name, middle_name, email, phone, mapped["primary_position"], json.dumps(mapped["other_positions"]), status, json.dumps(payload),
                first_norm, last_norm, email_norm, phone_norm, cognito_form_id, cognito_entry_number, cognito_entry_id,
                clean_text(payload.get("cognito_internal_link")), clean_text(payload.get("cognito_public_link")), clean_text(payload.get("cognito_admin_link")), cognito_document_link,
                payload.get("cognito_date_created"), payload.get("cognito_date_submitted"), payload.get("cognito_date_updated"),
                address_line1, address_line2, city, state, postal_code, country, country_code, full_address,
                consent_background_investigation, has_valid_drivers_license, drivers_license_number, drivers_license_state, felony_conviction,
                domestic_violence_misdemeanor, protective_order, currently_under_charges, unlawful_drug_use_last_3y, prior_police_service,
                resume_file_name, resume_file_url, resume_content_type, signature_png_url, signature_svg_url, signature_typed_text,
                cognito_pdf_url, cognito_pdf_url, app_id
            ),
        )
    else:
        inserted_row = cursor.execute(
            """
            INSERT INTO dbo.job_applications (
              submitted_at, first_name, last_name, middle_name, email, phone,
              primary_position, other_positions, status, source, raw_payload,
              first_name_norm, last_name_norm, email_norm, phone_norm,
              cognito_form_id, cognito_entry_number, cognito_entry_id,
              cognito_internal_link, cognito_public_link, cognito_admin_link, cognito_document_link,
              cognito_date_created, cognito_date_submitted, cognito_date_updated,
              address_line1, address_line2, city, state, postal_code, country, country_code, full_address,
              consent_background_investigation, has_valid_drivers_license, drivers_license_number, drivers_license_state,
              felony_conviction, domestic_violence_misdemeanor, protective_order, currently_under_charges, unlawful_drug_use_last_3y, prior_police_service,
              resume_file_name, resume_file_url, resume_content_type, signature_png_url, signature_svg_url, signature_typed_text,
              cognito_pdf_url, cognito_pdf_generated_at, last_cognito_sync_at
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'cognito', ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?,
                      TRY_CAST(? AS DATETIME2), TRY_CAST(? AS DATETIME2), TRY_CAST(? AS DATETIME2),
                      ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?,
                      NULLIF(?, ''), CASE WHEN NULLIF(?, '') IS NOT NULL THEN SYSUTCDATETIME() ELSE NULL END, SYSUTCDATETIME())
            """,
            (
                mapped["submitted_at"], first_name, last_name, middle_name, email, phone, mapped["primary_position"], json.dumps(mapped["other_positions"]), status, json.dumps(payload),
                first_norm, last_norm, email_norm, phone_norm, cognito_form_id, cognito_entry_number, cognito_entry_id,
                clean_text(payload.get("cognito_internal_link")), clean_text(payload.get("cognito_public_link")), clean_text(payload.get("cognito_admin_link")), clean_text(payload.get("cognito_document_link")),
                payload.get("cognito_date_created"), payload.get("cognito_date_submitted"), payload.get("cognito_date_updated"),
                address_line1, address_line2, city, state, postal_code, country, country_code, full_address,
                consent_background_investigation, has_valid_drivers_license, drivers_license_number, drivers_license_state,
                felony_conviction, domestic_violence_misdemeanor, protective_order, currently_under_charges, unlawful_drug_use_last_3y, prior_police_service,
                resume_file_name, resume_file_url, resume_content_type, signature_png_url, signature_svg_url, signature_typed_text,
                cognito_pdf_url, cognito_pdf_url
            ),
        )
        app_id = int(inserted_row.fetchone()[0])
        if cognito_document_link:
            cognito_document_link = persist_document_record(cursor, app_id, "initial_application", cognito_document_link)
            cursor.execute("UPDATE dbo.job_applications SET cognito_document_link = ? WHERE id = ?", (cognito_document_link, app_id))
        elif cognito_pdf_url:
            logging.info("initial_application_missing_cognito_document_link app_id=%s using_cognito_pdf_url_fallback", app_id)
            cognito_pdf_url = persist_document_record(cursor, app_id, "initial_application", cognito_pdf_url)
            cursor.execute("UPDATE dbo.job_applications SET cognito_document_link = ? WHERE id = ?", (cognito_pdf_url, app_id))

    cursor.execute(
        """
        INSERT INTO dbo.cognito_submission_history (
          job_application_id, cognito_form_id, cognito_entry_number, cognito_entry_id, submitted_at, source, raw_payload
        ) VALUES (?, ?, ?, ?, ?, 'cognito', ?)
        """,
        (app_id, cognito_form_id, cognito_entry_number, cognito_entry_id, mapped["submitted_at"], json.dumps(payload)),
    )
    return app_id




def upsert_background_record(cursor, mapped: dict[str, Any], payload: dict[str, Any]) -> int:
    email_norm = normalize_email(str(mapped.get("email") or ""))
    phone_norm = normalize_phone_us(str(mapped.get("phone") or ""))
    row = cursor.execute(
        """
        SELECT TOP 1 id
        FROM dbo.job_applications
        WHERE (? <> '' AND LOWER(LTRIM(RTRIM(COALESCE(email_norm, '')))) = ?)
           OR (? <> '' AND LTRIM(RTRIM(COALESCE(phone_norm, ''))) = ?)
        ORDER BY COALESCE(updated_at, created_at) DESC
        """,
        (email_norm, email_norm, phone_norm, phone_norm),
    ).fetchone()
    app_id = upsert_cognito_record(cursor, mapped, payload) if not row else int(row[0])
    background_pdf_url = clean_text(payload.get("background_pdf_url"))
    if background_pdf_url:
        background_pdf_url = persist_document_record(cursor, app_id, "background_check_form", background_pdf_url)
    background_document_url = clean_text(payload.get("background_document_url"))
    if background_document_url:
        background_document_url = persist_document_record(cursor, app_id, "background_check_document", background_document_url)
    def persist_latest(document_type: str, value: Any) -> list[str]:
        urls = extract_file_urls(value)
        latest_url = ""
        for url in urls:
            latest_url = persist_document_record(cursor, app_id, document_type, url)
        return [latest_url] if latest_url else []

    drivers_license_urls = persist_latest("drivers_license", payload.get("drivers_license_files") or payload.get("drivers_license_urls") or payload.get("drivers_license_document_urls") or payload.get("drivers_license_document_url"))
    dd214_urls = persist_latest("dd214", payload.get("dd214_files") or payload.get("dd214_urls") or payload.get("dd214_document_urls") or payload.get("dd214_document_url"))
    diploma_urls = persist_latest("diploma", payload.get("diploma_files") or payload.get("diploma_urls") or payload.get("diploma_document_urls") or payload.get("diploma_document_url"))
    social_security_front_urls = persist_latest("social_security_front", payload.get("social_security_front") or payload.get("social_security_front_file") or payload.get("ss_front"))
    social_security_back_urls = persist_latest("social_security_back", payload.get("social_security_back") or payload.get("social_security_back_file") or payload.get("ss_back"))
    credit_report_urls = persist_latest("credit_report", payload.get("credit_report") or payload.get("credit_report_pdf") or payload.get("credit_report_file"))
    birth_certificate_urls = persist_latest("birth_certificate", payload.get("birth_cert") or payload.get("birth_certificate") or payload.get("birth_certificate_file"))
    passport_urls = persist_latest("passport", payload.get("passport") or payload.get("passport_file"))
    references = payload.get("references")
    if isinstance(references, list):
        cursor.execute("DELETE FROM dbo.job_application_references WHERE job_application_id = ?", (app_id,))
        for idx, reference in enumerate(references, start=1):
            if not isinstance(reference, dict):
                continue
            ref_type = clean_text(reference.get("reference_type"))
            ref_name = clean_text(reference.get("name"))
            ref_phone = clean_text(reference.get("phone"))
            ref_email = clean_text(reference.get("email"))
            if not any([ref_type, ref_name, ref_phone, ref_email]):
                continue
            cursor.execute(
                """
                INSERT INTO dbo.job_application_references (
                    job_application_id, reference_order, reference_type, name, phone, email
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (app_id, idx, ref_type, ref_name, ref_phone, ref_email),
            )
    cursor.execute(
        """
        UPDATE dbo.job_applications
        SET status = 'Background Check Submitted',
            raw_payload = ?,
            background_pdf_url = COALESCE(NULLIF(?, ''), background_pdf_url),
            background_document_url = COALESCE(NULLIF(?, ''), background_document_url),
            drivers_license_document_urls = COALESCE(NULLIF(?, ''), drivers_license_document_urls),
            dd214_document_urls = COALESCE(NULLIF(?, ''), dd214_document_urls),
            diploma_document_urls = COALESCE(NULLIF(?, ''), diploma_document_urls),
            social_security_front_document_urls = COALESCE(NULLIF(?, ''), social_security_front_document_urls),
            social_security_back_document_urls = COALESCE(NULLIF(?, ''), social_security_back_document_urls),
            credit_report_document_urls = COALESCE(NULLIF(?, ''), credit_report_document_urls),
            birth_certificate_document_urls = COALESCE(NULLIF(?, ''), birth_certificate_document_urls),
            passport_document_urls = COALESCE(NULLIF(?, ''), passport_document_urls),
            background_submitted_at = COALESCE(TRY_CAST(? AS DATETIME2), background_submitted_at),
            last_cognito_sync_at = SYSUTCDATETIME()
        WHERE id = ?
        """,
        (
            json.dumps(payload),
            background_pdf_url,
            background_document_url,
            json.dumps(drivers_license_urls),
            json.dumps(dd214_urls),
            json.dumps(diploma_urls),
            json.dumps(social_security_front_urls),
            json.dumps(social_security_back_urls),
            json.dumps(credit_report_urls),
            json.dumps(birth_certificate_urls),
            json.dumps(passport_urls),
            payload.get("cognito_date_submitted"),
            app_id,
        ),
    )
    return app_id


def upsert_job_app_docs(cursor, payload: dict[str, Any]) -> dict[str, Any]:
    email_norm = normalize_email(str(payload.get("email") or ""))
    if not email_norm:
        raise ValueError("email is required.")
    full_name = str(payload.get("name") or payload.get("full_name") or "").strip()
    full_name_norm = normalize_name(full_name)
    require_name_match = bool(full_name_norm)

    row = cursor.execute(
        """
        SELECT TOP 1 id,
               social_security_front_document_urls,
               social_security_back_document_urls,
               credit_report_document_urls,
               birth_certificate_document_urls,
               passport_document_urls
        FROM dbo.job_applications
        WHERE
          (? <> '' AND (
            LOWER(LTRIM(RTRIM(COALESCE(email_norm, '')))) = ?
            OR LOWER(LTRIM(RTRIM(COALESCE(email, '')))) = ?
          ))
          AND (
            ? = 0
            OR (
              LOWER(LTRIM(RTRIM(COALESCE(full_name, '')))) = ?
            )
          )
        ORDER BY COALESCE(updated_at, created_at) DESC
        """,
        (
            email_norm,
            email_norm,
            email_norm,
            1 if require_name_match else 0,
            full_name_norm,
        ),
    ).fetchone()
    if not row:
        if require_name_match:
            raise LookupError("No matching applicant found for provided email + full name.")
        raise LookupError("No matching applicant found for provided email.")

    def resolve_latest(existing_value: Any, incoming_values: list[Any], document_type: str) -> list[str]:
        existing = extract_file_urls(existing_value)
        latest_url = existing[-1] if existing else ""
        for incoming_value in incoming_values:
            for url in extract_file_urls(incoming_value):
                latest_url = persist_document_record(cursor, app_id, document_type, url)
        return [latest_url] if latest_url else []

    def pick_doc_values(*keys: str) -> list[Any]:
        return [payload.get(key) for key in keys if key in payload]

    app_id = int(row[0])
    ss_front = resolve_latest(row[1], pick_doc_values("social_security_front", "social_security_front_file", "ss_front"), "social_security_front")
    ss_back = resolve_latest(row[2], pick_doc_values("social_security_back", "social_security_back_file", "ss_back"), "social_security_back")
    credit_report = resolve_latest(row[3], pick_doc_values("credit_report_pdf", "credit_report", "credit_report_file"), "credit_report")
    birth_certificate = resolve_latest(row[4], pick_doc_values("birth_certificate", "birth_certificate_file"), "birth_certificate")
    passport = resolve_latest(row[5], pick_doc_values("passport", "passport_file"), "passport")
    cursor.execute(
        """
        UPDATE dbo.job_applications
        SET social_security_front_document_urls = ?,
            social_security_back_document_urls = ?,
            credit_report_document_urls = ?,
            birth_certificate_document_urls = ?,
            passport_document_urls = ?,
            raw_payload = ?,
            source = COALESCE(NULLIF(?, ''), source),
            email_norm = NULLIF(?, ''),
            last_cognito_sync_at = SYSUTCDATETIME()
        WHERE id = ?
        """,
        (
            json.dumps(ss_front),
            json.dumps(ss_back),
            json.dumps(credit_report),
            json.dumps(birth_certificate),
            json.dumps(passport),
            json.dumps(payload),
            "job-app-docs",
            email_norm,
            app_id,
        ),
    )
    return {
        "job_application_id": app_id,
        "updated": 1,
        "email": email_norm,
        "document_counts": {
            "social_security_front": len(ss_front),
            "social_security_back": len(ss_back),
            "credit_report_pdf": len(credit_report),
            "birth_certificate": len(birth_certificate),
            "passport": len(passport),
        },
    }


def parse_json_body(raw_body: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        preview = (raw_body or "")[:200].replace("\n", "\\n")
        raise ValueError(
            f"Invalid JSON body at line {exc.lineno}, column {exc.colno}: {exc.msg}. "
            f"Body preview: {preview}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")
    return payload


def extract_file_urls(value: Any) -> list[str]:
    urls: list[str] = []
    if value is None:
        return urls
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return urls
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                return extract_file_urls(parsed)
            except Exception:
                pass
        matches = re.findall(r"https?://[^\s,\]\}\"']+", text)
        if matches:
            for match in matches:
                cleaned = clean_text(match)
                if cleaned:
                    urls.append(cleaned)
            return urls
        urls.append(text)
        return urls
    if isinstance(value, dict):
        candidate = clean_text(value.get("file") or value.get("url"))
        if candidate:
            urls.append(candidate)
        return urls
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                candidate = clean_text(item.get("file") or item.get("url"))
            else:
                candidate = clean_text(item)
            if candidate:
                urls.append(candidate)
    return urls


def persist_document_record(cursor, app_id: int, document_type: str, source_url: str) -> str:
    applicant_row = cursor.execute("SELECT TOP 1 full_name FROM dbo.job_applications WHERE id = ?", (app_id,)).fetchone()
    applicant_name = clean_text(applicant_row[0]) if applicant_row else None
    blob_name, blob_url, error = copy_url_to_azure_blob(app_id, document_type, source_url, applicant_name=applicant_name)
    status = "uploaded" if blob_url else ("manual_required" if error and "expired_or_denied" in error else "failed")
    needs_manual = 1 if status == "manual_required" else 0
    cursor.execute(
        """
        INSERT INTO dbo.job_application_documents (
            job_application_id, document_type, source_url, azure_blob_name, azure_blob_url,
            status, error_message, needs_manual, uploaded_at, last_attempt_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 'uploaded' THEN SYSUTCDATETIME() ELSE NULL END, SYSUTCDATETIME(), SYSUTCDATETIME())
        """,
        (app_id, document_type, source_url, blob_name, blob_url, status, error, needs_manual, status),
    )
    logging.info(
        "document_persist app_id=%s document_type=%s status=%s needs_manual=%s blob_name=%s error=%s",
        app_id,
        document_type,
        status,
        needs_manual,
        blob_name,
        error,
    )
    return blob_url or source_url


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
        "status": "Application/Consent to Background Submitted",
        "source": "cognito",
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




def parse_json_array_text(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def build_document_links(cognito_pdf_url: Any, cognito_document_link: Any, background_pdf_url: Any, background_document_url: Any, resume_file_url: Any, drivers_license_document_urls: Any, dd214_document_urls: Any, diploma_document_urls: Any, social_security_front_document_urls: Any, social_security_back_document_urls: Any, credit_report_document_urls: Any, birth_certificate_document_urls: Any, passport_document_urls: Any) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []

    def add(label: str, url: Any):
        text = str(url or "").strip()
        if not text:
            return
        if any(item["url"] == text for item in links):
            return
        links.append({"label": label, "url": build_read_sas_url(text)})

    add("Initial Application", cognito_document_link)
    add("Background Check Form", background_pdf_url)
    add("Resume", resume_file_url)
    for url in parse_json_array_text(drivers_license_document_urls):
        add("Driver License", url)
    for url in parse_json_array_text(dd214_document_urls):
        add("DD214", url)
    for url in parse_json_array_text(diploma_document_urls):
        add("Diploma", url)
    for url in parse_json_array_text(social_security_front_document_urls):
        add("Social Security - Front", url)
    for url in parse_json_array_text(social_security_back_document_urls):
        add("Social Security - Back", url)
    for url in parse_json_array_text(credit_report_document_urls):
        add("Credit Report", url)
    for url in parse_json_array_text(birth_certificate_document_urls):
        add("Birth Certificate", url)
    for url in parse_json_array_text(passport_document_urls):
        add("Passport", url)

    return links


def build_document_links_safe(*args: Any) -> list[dict[str, str]]:
    try:
        return build_document_links(*args)
    except Exception:
        logging.exception("build_document_links failed")
        return []


def _is_cognito_link(url: Any) -> bool:
    return "cognitoforms.com" in str(url or "").lower()


def query_applicants(filters: dict[str, str]) -> list[dict[str, Any]]:
    sql = """
        SELECT
            id, submitted_at, full_name, email, phone,
            primary_position, other_positions, status, source, cognito_pdf_url, cognito_document_link, background_pdf_url, background_document_url, resume_file_url, drivers_license_document_urls, dd214_document_urls, diploma_document_urls, social_security_front_document_urls, social_security_back_document_urls, credit_report_document_urls, birth_certificate_document_urls, passport_document_urls, contacted, denied
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

    status_value = (filters.get("status") or "").strip().lower()
    if status_value == "denied":
        sql += " AND denied = 1"
    else:
        sql += " AND ISNULL(denied, 0) = 0"

    if filters.get("status") and status_value != "denied":
        sql += " AND (LOWER(status) LIKE ? OR LOWER(REPLACE(status, '_', ' ')) LIKE ?)"
        params.append(f"%{status_value}%")
        params.append(f"%{status_value}%")

    if filters.get("date_from"):
        sql += " AND CAST(submitted_at AS date) >= ?"
        params.append(filters["date_from"])

    if filters.get("date_to"):
        sql += " AND CAST(submitted_at AS date) <= ?"
        params.append(filters["date_to"])

    sql += """
        ORDER BY
            CASE
                WHEN status = 'Background Check Submitted' THEN 1
                WHEN status = 'Needs Attention' THEN 2
                ELSE 3
            END,
            submitted_at ASC
        """

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
        raw_other_positions = json.loads(row[6] or "[]")
        if not isinstance(raw_other_positions, list):
            raw_other_positions = []
        primary_parts = split_positions_text(row[5] or "")
        primary_clean = primary_parts[0] if primary_parts else (strip_sent_from_suffix(str(row[5] or "")) or "—")
        other_clean: list[str] = []
        for value in raw_other_positions:
            other_clean.extend(split_positions_text(str(value)))
        other_clean = [value for value in other_clean if value and value.lower() != primary_clean.lower()]
        if _is_cognito_link(row[10]):
            logging.warning(
                "applicants_initial_link_still_cognito id=%s name=%s cognito_document_link=%s",
                row[0],
                row[2],
                row[10],
            )
        if _is_cognito_link(row[13]):
            logging.warning(
                "applicants_resume_still_cognito id=%s name=%s resume_file_url=%s",
                row[0],
                row[2],
                row[13],
            )
        raw_output.append(
            {
                "id": row[0],
                "submittedAt": submitted_text,
                "name": row[2],
                "email": extract_first_email(str(row[3] or "")),
                "phone": normalize_phone(str(row[4] or "")),
                "primaryPosition": primary_clean,
                "otherPositions": list(dict.fromkeys(other_clean)),
                "status": "Denied" if bool(row[23]) else normalize_status_label(row[7]),
                "source": row[8],
                "cognitoPdfUrl": row[9],
                "cognitoDocumentLink": row[10],
                "documents": build_document_links_safe(row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21]),
                "contacted": bool(row[22]) if row[22] is not None else False,
            }
        )
    # Smart presentation layer:
    # - remove names containing "test"
    # - combine same-name applicants into one row, merging positions
    grouped: dict[str, dict[str, Any]] = {}
    for item in raw_output:
        if not (item.get("name") or "").strip():
            continue
        if contains_test_name(item["name"]):
            continue
        key = item["name"].strip().lower()
        if key not in grouped:
            initial_positions = {p for p in [item["primaryPosition"], *item["otherPositions"]] if p and p != "—"}
            grouped[key] = {
                **item,
                "allPositions": initial_positions,
                "documentsByUrl": {doc.get("url"): doc for doc in item.get("documents", []) if doc.get("url")},
            }
            continue

        existing = grouped[key]
        existing["allPositions"].update([p for p in [item["primaryPosition"]] if p and p != "—"])
        existing["allPositions"].update([p for p in item["otherPositions"] if p and p != "—"])
        for doc in item.get("documents", []):
            url = doc.get("url")
            if not url:
                continue
            existing["documentsByUrl"][url] = doc
        # Keep latest submission date row as base
        if item["submittedAt"] > existing["submittedAt"]:
            existing["submittedAt"] = item["submittedAt"]
            existing["primaryPosition"] = item["primaryPosition"]
            existing["status"] = item.get("status") or existing.get("status")
            existing["email"] = item["email"] or existing["email"]
            existing["phone"] = item["phone"] or existing["phone"]

    output: list[dict[str, Any]] = []
    for merged in grouped.values():
        all_positions = {p for p in merged["allPositions"] if p}
        primary = merged["primaryPosition"]
        if primary in all_positions:
            all_positions.remove(primary)
        merged["otherPositions"] = sorted(all_positions)
        merged["documents"] = list(merged.get("documentsByUrl", {}).values())
        merged.pop("documentsByUrl", None)
        merged.pop("allPositions", None)
        output.append(merged)

    output.sort(key=lambda item: item["submittedAt"], reverse=True)
    return output


def run_blob_backfill(limit: int = 200) -> dict[str, int]:
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT TOP (?) id, cognito_pdf_url, cognito_document_link, background_pdf_url, resume_file_url,
                   drivers_license_document_urls, dd214_document_urls, diploma_document_urls,
                   social_security_front_document_urls, social_security_back_document_urls,
                   credit_report_document_urls, birth_certificate_document_urls, passport_document_urls
            FROM dbo.job_applications
            ORDER BY id DESC
            """,
            (limit,),
        ).fetchall()
        migrated = 0
        for row in rows:
            app_id = int(row[0])
            for doc_type, col_idx, col_name in [("initial_application", 2, "cognito_document_link"), ("background_check_form", 3, "background_pdf_url"), ("resume", 4, "resume_file_url")]:
                original = clean_text(row[col_idx])
                if not original:
                    continue
                try:
                    updated = persist_document_record(cursor, app_id, doc_type, original)
                except Exception:
                    logging.exception("backfill persist failed app_id=%s doc_type=%s", app_id, doc_type)
                    continue
                if doc_type == "initial_application":
                    logging.info(
                        "backfill_initial_application_result app_id=%s source=%s updated=%s changed=%s",
                        app_id,
                        original,
                        updated,
                        updated != original,
                    )
                if updated != original:
                    cursor.execute(f"UPDATE dbo.job_applications SET {col_name} = ? WHERE id = ?", (updated, app_id))
                    migrated += 1
            for doc_type, idx, col_name in [("drivers_license", 5, "drivers_license_document_urls"), ("dd214", 6, "dd214_document_urls"), ("diploma", 7, "diploma_document_urls"), ("social_security_front", 8, "social_security_front_document_urls"), ("social_security_back", 9, "social_security_back_document_urls"), ("credit_report", 10, "credit_report_document_urls"), ("birth_certificate", 11, "birth_certificate_document_urls"), ("passport", 12, "passport_document_urls")]:
                current = parse_json_array_text(row[idx])
                replaced: list[str] = []
                for url in current:
                    try:
                        replaced.append(persist_document_record(cursor, app_id, doc_type, url))
                    except Exception:
                        logging.exception("backfill persist failed app_id=%s doc_type=%s", app_id, doc_type)
                        replaced.append(url)
                if replaced != current:
                    cursor.execute(f"UPDATE dbo.job_applications SET {col_name} = ? WHERE id = ?", (json.dumps(replaced), app_id))
                    migrated += 1
        conn.commit()
        manual_count_row = cursor.execute("SELECT COUNT(*) FROM dbo.job_application_documents WHERE needs_manual = 1").fetchone()
        manual_count = int(manual_count_row[0]) if manual_count_row else 0
    return {"processed": len(rows), "migrated": migrated, "manual_required": manual_count}


def get_blob_backfill_status() -> dict[str, Any]:
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT status, COUNT(*)
            FROM dbo.job_application_documents
            GROUP BY status
            """
        ).fetchall()
        by_status = {str(row[0] or "unknown"): int(row[1]) for row in rows}
        manual_row = cursor.execute("SELECT COUNT(*) FROM dbo.job_application_documents WHERE needs_manual = 1").fetchone()
        recent_manual = cursor.execute(
            """
            SELECT TOP 20 id, job_application_id, document_type, source_url, error_message, last_attempt_at
            FROM dbo.job_application_documents
            WHERE needs_manual = 1
            ORDER BY COALESCE(last_attempt_at, created_at) DESC
            """
        ).fetchall()
    return {
        "status_counts": by_status,
        "manual_required_count": int(manual_row[0]) if manual_row else 0,
        "manual_required_recent": [
            {
                "id": int(row[0]),
                "job_application_id": int(row[1]),
                "document_type": str(row[2] or ""),
                "source_url": str(row[3] or ""),
                "error_message": str(row[4] or ""),
                "last_attempt_at": str(row[5] or ""),
            }
            for row in recent_manual
        ],
    }


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
    cleaned: list[str] = []
    for row in rows:
        for value in split_positions_text(str(row[0] or "")):
            if value:
                cleaned.append(value)
    return sorted(set(cleaned), key=lambda item: item.lower())




def query_statuses() -> list[str]:
    sql = """
        SELECT DISTINCT status
        FROM dbo.job_applications
        WHERE status IS NOT NULL AND LTRIM(RTRIM(status)) <> ''
        ORDER BY status ASC
    """
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(sql).fetchall()
    seen = set()
    output: list[str] = []
    for row in rows:
        normalized = normalize_status_label(row[0])
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    if "denied" not in {value.lower() for value in output}:
        output.append("Denied")
    return sorted(output, key=lambda value: value.lower())

def _approve_or_deny_application(application_id: int, action: str) -> None:
    action_value = (action or "").strip().lower()
    if action_value == "approve":
        new_status = "Needs Attention - Approved"
        sql = """
            UPDATE dbo.job_applications
            SET status = ?,
                denied = 0
            WHERE id = ?
        """
    elif action_value == "deny":
        new_status = None
        sql = """
            UPDATE dbo.job_applications
            SET denied = 1
            WHERE id = ?
        """
    else:
        raise ValueError("Unsupported action.")

    with get_sql_connection() as conn:
        cursor = conn.cursor()
        if action_value == "approve":
            cursor.execute(sql, (new_status, application_id))
        else:
            cursor.execute(sql, (application_id,))
        conn.commit()


def _get_graph_access_token() -> str:
    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID]):
        raise RuntimeError("CLIENT_ID, CLIENT_SECRET, and TENANT_ID must be set.")
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    oauth_app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET,
    )
    result = oauth_app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    token = result.get("access_token")
    if not token:
        raise RuntimeError(f"Failed to obtain Graph access token: {result}")
    return token


def _build_denial_email_body(applicant_name: str, denial_type: str) -> str:
    safe_name = (applicant_name or "Applicant").strip()
    if denial_type == "permanent":
        return (
            f"Dear {safe_name},\n\n"
            "Thank you for your interest in employment with the Baltimore City Sheriff’s Office and for the time and effort you invested in the application process.\n\n"
            "After review of your background application and supporting materials, we regret to inform you that you will not be moving forward in the hiring process with the Baltimore City Sheriff’s Office.\n\n"
            "We appreciate your interest in serving the citizens of Baltimore City and thank you for considering our agency for employment.\n\n"
            "We wish you the best in your future professional endeavors.\n\n"
            "Respectfully,\nBaltimore City Sheriff's Office"
        )
    return (
        f"Dear {safe_name},\n\n"
        "Thank you for your interest in employment with the Baltimore City Sheriff’s Office and for the time and effort you invested throughout the application and selection process.\n\n"
        "After careful review and consideration of all applicants, we regret to inform you that you were not selected to move forward in the hiring process at this time. This decision was made after evaluating a highly competitive pool of candidates and was not an easy one.\n\n"
        "Please know that your interest in serving the citizens of Baltimore City and pursuing a career in law enforcement is sincerely appreciated. We recognize the commitment required to seek a position in public safety, and we thank you for your willingness to serve.\n\n"
        "Your application will remain on file for three (3) months, should additional opportunities become available that align with your qualifications and experience. We also encourage you to apply for future openings with the Baltimore City Sheriff’s Office.\n\n"
        "We appreciate your interest in our agency and wish you continued success in your professional and personal endeavors.\n\n"
        "Respectfully,\n\nHuman Resources"
    )


def _send_denial_email(to_email: str, applicant_name: str, denial_type: str) -> None:
    if not MAILBOX_EMAIL:
        raise RuntimeError("MAILBOX_EMAIL is not set.")
    token = _get_graph_access_token()
    subject = "Baltimore City Sheriff's Office Application Status"
    content = _build_denial_email_body(applicant_name, denial_type)
    endpoint = f"{GRAPH_BASE}/users/{MAILBOX_EMAIL}/sendMail"
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": content},
            "toRecipients": [{"emailAddress": {"address": to_email}}],
        },
        "saveToSentItems": True,
    }
    response = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if response.status_code >= 300:
        raise RuntimeError(f"Failed to send denial email: {response.status_code} {response.text}")


def _deny_applications(application_ids: list[int], denial_type: str) -> int:
    ids = sorted({int(value) for value in application_ids if int(value) > 0})
    if not ids:
        return 0
    if denial_type not in {"permanent", "soft"}:
        raise ValueError("Invalid denial type.")
    placeholders = ",".join("?" for _ in ids)
    status_value = "Needs Attention - Denied (Permanent)" if denial_type == "permanent" else "Needs Attention - Denied (Soft)"
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        rows = cursor.execute(
            f"SELECT id, full_name, email FROM dbo.job_applications WHERE id IN ({placeholders})",
            ids,
        ).fetchall()
        for row in rows:
            to_email = extract_first_email(str(row[2] or ""))
            if not to_email:
                raise RuntimeError(f"Applicant id {row[0]} is missing a valid email.")
            _send_denial_email(to_email, str(row[1] or "Applicant"), denial_type)
        cursor.execute(
            f"""UPDATE dbo.job_applications
            SET denied = 1,
                status = ?,
                denial_type = ?,
                denial_sent_at = SYSUTCDATETIME(),
                denial_sent_to = COALESCE(NULLIF(email, ''), denial_sent_to)
            WHERE id IN ({placeholders})""",
            [status_value, denial_type, *ids],
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def _undo_denial(application_id: int) -> None:
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE dbo.job_applications
            SET denied = 0
            WHERE id = ?
            """,
            (application_id,),
        )
        conn.commit()


def _undo_denials(application_ids: list[int]) -> int:
    ids = sorted({int(value) for value in application_ids if int(value) > 0})
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE dbo.job_applications SET denied = 0 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def _set_contacted(application_id: int, contacted: bool) -> None:
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE dbo.job_applications
            SET contacted = ?
            WHERE id = ?
            """,
            1 if contacted else 0,
            application_id,
        )
        conn.commit()


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
    raw_path = environ.get("PATH_INFO") or "/"
    path = raw_path.rstrip("/") or "/"
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
            if source_folder not in {"all", "inbox", "processed"}:
                source_folder = "inbox"
            try:
                result = run_email_ingest(scan_limit=scan_limit, source_folder=source_folder)
                logging.info("/run-ingest completed source_folder=%s scan_limit=%s result=%s", source_folder, scan_limit, result)
                return _wsgi_json(start_response, {"ok": True, **result})
            except Exception as exc:
                logging.exception("/run-ingest failed source_folder=%s scan_limit=%s", source_folder, scan_limit)
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/applicants":
            filters = {
                "name": (query.get("name") or [""])[0],
                "job_title": (query.get("job_title") or [""])[0],
                "status": (query.get("status") or [""])[0],
                "date_from": (query.get("date_from") or [""])[0],
                "date_to": (query.get("date_to") or [""])[0],
            }
            try:
                data = query_applicants(filters)
                return _wsgi_json(start_response, {"applicants": data})
            except Exception as exc:
                logging.exception("/api/applicants failed")
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/job-titles":
            try:
                titles = query_job_titles()
                return _wsgi_json(start_response, {"job_titles": titles})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/statuses":
            try:
                statuses = query_statuses()
                return _wsgi_json(start_response, {"statuses": statuses})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        if path == "/api/admin/backfill-azure-status":
            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)
            try:
                return _wsgi_json(start_response, {"source": "backfill-azure-status", **get_blob_backfill_status()})
            except Exception as exc:
                logging.exception("/api/admin/backfill-azure-status failed")
                return _wsgi_json(start_response, {"error": str(exc)}, 500)
        return _wsgi_json(start_response, {"error": "Not Found"}, 404)

    if method == "POST":
        if path == "/api/applicants/undo-denial":
            try:
                payload = parse_json_body(body_text or "{}")
            except Exception:
                return _wsgi_json(start_response, {"error": "Invalid JSON payload."}, 400)
            ids = payload.get("ids") if isinstance(payload, dict) else []
            if not isinstance(ids, list):
                return _wsgi_json(start_response, {"error": "Expected 'ids' array."}, 400)
            try:
                restored_count = _undo_denials([int(value) for value in ids])
                return _wsgi_json(start_response, {"ok": True, "restored_count": restored_count})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/applicants/deny":
            try:
                payload = parse_json_body(body_text or "{}")
            except Exception:
                return _wsgi_json(start_response, {"error": "Invalid JSON payload."}, 400)
            ids = payload.get("ids") if isinstance(payload, dict) else []
            denial_type = str(payload.get("denial_type") or "").strip().lower()
            if not isinstance(ids, list):
                return _wsgi_json(start_response, {"error": "Expected 'ids' array."}, 400)
            try:
                denied_count = _deny_applications([int(value) for value in ids], denial_type=denial_type)
                return _wsgi_json(start_response, {"ok": True, "denied_count": denied_count})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        undo_denial_match = re.fullmatch(r"/api/applicants/(\d+)/undo-denial", path or "")
        if undo_denial_match:
            app_id = int(undo_denial_match.group(1))
            try:
                _undo_denial(app_id)
                return _wsgi_json(start_response, {"ok": True, "id": app_id, "action": "undo-denial"})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        applicant_action_match = re.fullmatch(r"/api/applicants/(\d+)/(approve|deny|undo-denial)", path or "")
        if applicant_action_match:
            app_id = int(applicant_action_match.group(1))
            action = applicant_action_match.group(2)
            try:
                if action == "undo-denial":
                    _undo_denial(app_id)
                elif action == "deny":
                    payload = parse_json_body(body_text or "{}") if body_text else {}
                    denial_type = str(payload.get("denial_type") or "").strip().lower()
                    _deny_applications([app_id], denial_type=denial_type)
                else:
                    _approve_or_deny_application(app_id, action)
                return _wsgi_json(start_response, {"ok": True, "id": app_id, "action": action})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        applicant_contacted_match = re.fullmatch(r"/api/applicants/(\d+)/contacted", path or "")
        if applicant_contacted_match:
            app_id = int(applicant_contacted_match.group(1))
            try:
                payload = parse_json_body(body_text or "{}")
            except Exception:
                return _wsgi_json(start_response, {"error": "Invalid JSON payload."}, 400)
            contacted = bool(payload.get("contacted"))
            try:
                _set_contacted(app_id, contacted)
                return _wsgi_json(start_response, {"ok": True, "id": app_id, "contacted": contacted})
            except Exception as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/ingest-interest-form":
            if not body_text.strip():
                return _wsgi_json(start_response, {"error": "JSON payload is empty."}, 400)

            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)

            try:
                payload = parse_json_body(body_text)
                if "body" in payload:
                    fields = extract_email_fields(str(payload.get("body") or ""))
                    submitted_at = parse_submitted_at(str(payload.get("received") or "")) or datetime.now(timezone.utc).date().isoformat()
                    mapped = build_record_from_email(fields, submitted_at=submitted_at, raw_payload=payload)
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

        if path == "/api/ingest-background-form":
            if not body_text.strip():
                return _wsgi_json(start_response, {"error": "JSON payload is empty."}, 400)
            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)
            try:
                payload = parse_json_body(body_text)
                mapped = build_record_from_make(payload)
                if not mapped:
                    return _wsgi_json(start_response, {"error": "Could not parse applicant name from payload."}, 400)
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    app_id = upsert_background_record(cursor, mapped, payload)
                    conn.commit()
                return _wsgi_json(start_response, {"inserted": 1, "source": "background_check", "job_application_id": app_id})
            except Exception as exc:
                logging.exception("/api/ingest-background-form failed")
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/ingest-cognito-form":
            if not body_text.strip():
                return _wsgi_json(start_response, {"error": "JSON payload is empty."}, 400)
            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)
            try:
                payload = parse_json_body(body_text)
                mapped = build_record_from_make(payload)
                if not mapped:
                    return _wsgi_json(start_response, {"error": "Could not parse applicant name from payload."}, 400)
                if contains_test_name(mapped["full_name"]):
                    return _wsgi_json(start_response, {"inserted": 0, "skipped": 1, "reason": "Name contains 'test'."})
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    app_id = upsert_cognito_record(cursor, mapped, payload)
                    conn.commit()
                return _wsgi_json(start_response, {"inserted": 1, "source": "cognito", "job_application_id": app_id})
            except Exception as exc:
                logging.exception("/api/ingest-cognito-form failed")
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/job-app-docs":
            if not body_text.strip():
                return _wsgi_json(start_response, {"error": "JSON payload is empty."}, 400)
            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)
            try:
                payload = parse_json_body(body_text)
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    result = upsert_job_app_docs(cursor, payload)
                    conn.commit()
                return _wsgi_json(start_response, {"source": "job-app-docs", **result})
            except LookupError as exc:
                return _wsgi_json(start_response, {"error": str(exc)}, 404)
            except Exception as exc:
                logging.exception("/api/job-app-docs failed")
                return _wsgi_json(start_response, {"error": str(exc)}, 500)

        if path == "/api/admin/backfill-azure-blobs":
            provided_token = environ.get("HTTP_X_WEBHOOK_TOKEN", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                return _wsgi_json(start_response, {"error": "Unauthorized webhook token."}, 401)
            try:
                payload = parse_json_body(body_text or "{}")
                limit = int(payload.get("limit") or 200)
                result = run_blob_backfill(limit=limit)
                return _wsgi_json(start_response, {"source": "backfill-azure-blobs", **result})
            except Exception as exc:
                logging.exception("/api/admin/backfill-azure-blobs failed")
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
                "status": (query.get("status") or [""])[0],
                "date_from": (query.get("date_from") or [""])[0],
                "date_to": (query.get("date_to") or [""])[0],
            }
            try:
                data = query_applicants(filters)
                self._send_json({"applicants": data})
            except Exception as exc:
                logging.exception("/api/applicants failed")
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/job-titles":
            try:
                self._send_json({"job_titles": query_job_titles()})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/statuses":
            try:
                self._send_json({"statuses": query_statuses()})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return
        if parsed.path == "/api/admin/backfill-azure-status":
            provided_token = self.headers.get("X-Webhook-Token", "")
            if MAKE_WEBHOOK_TOKEN and provided_token != MAKE_WEBHOOK_TOKEN:
                self._send_json({"error": "Unauthorized webhook token."}, 401)
                return
            try:
                self._send_json({"source": "backfill-azure-status", **get_blob_backfill_status()})
            except Exception as exc:
                logging.exception("/api/admin/backfill-azure-status failed")
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
            if source_folder not in {"all", "inbox", "processed"}:
                source_folder = "inbox"
            try:
                result = run_email_ingest(scan_limit=scan_limit, source_folder=source_folder)
                logging.info("/run-ingest completed source_folder=%s scan_limit=%s result=%s", source_folder, scan_limit, result)
                self._send_json({"ok": True, **result})
            except Exception as exc:
                logging.exception("/run-ingest failed source_folder=%s scan_limit=%s", source_folder, scan_limit)
                self._send_json({"error": str(exc)}, 500)
            return

        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        normalized_path = (parsed.path or "/").rstrip("/") or "/"
        if normalized_path == "/api/applicants/deny":
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                payload = parse_json_body(body or "{}")
            except Exception:
                self._send_json({"error": "Invalid JSON payload."}, 400)
                return
            ids = payload.get("ids") if isinstance(payload, dict) else []
            denial_type = str(payload.get("denial_type") or "").strip().lower()
            if not isinstance(ids, list):
                self._send_json({"error": "Expected 'ids' array."}, 400)
                return
            try:
                denied_count = _deny_applications([int(value) for value in ids], denial_type=denial_type)
                self._send_json({"ok": True, "denied_count": denied_count})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return
        if normalized_path == "/api/applicants/undo-denial":
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                payload = parse_json_body(body or "{}")
            except Exception:
                self._send_json({"error": "Invalid JSON payload."}, 400)
                return
            ids = payload.get("ids") if isinstance(payload, dict) else []
            if not isinstance(ids, list):
                self._send_json({"error": "Expected 'ids' array."}, 400)
                return
            try:
                restored_count = _undo_denials([int(value) for value in ids])
                self._send_json({"ok": True, "restored_count": restored_count})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return
        applicant_action_match = re.fullmatch(r"/api/applicants/(\d+)/(approve|deny|undo-denial)", normalized_path or "")
        if applicant_action_match:
            app_id = int(applicant_action_match.group(1))
            action = applicant_action_match.group(2)
            try:
                if action == "undo-denial":
                    _undo_denial(app_id)
                elif action == "deny":
                    content_length = int(self.headers.get("Content-Length", "0"))
                    body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
                    payload = parse_json_body(body or "{}")
                    denial_type = str(payload.get("denial_type") or "").strip().lower()
                    _deny_applications([app_id], denial_type=denial_type)
                else:
                    _approve_or_deny_application(app_id, action)
                self._send_json({"ok": True, "id": app_id, "action": action})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return
        applicant_contacted_match = re.fullmatch(r"/api/applicants/(\d+)/contacted", parsed.path or "")
        if applicant_contacted_match:
            app_id = int(applicant_contacted_match.group(1))
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length).decode("utf-8")
            try:
                payload = parse_json_body(body or "{}")
            except Exception:
                self._send_json({"error": "Invalid JSON payload."}, 400)
                return
            contacted = bool(payload.get("contacted"))
            try:
                _set_contacted(app_id, contacted)
                self._send_json({"ok": True, "id": app_id, "contacted": contacted})
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)
            return
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
                if "body" in payload:
                    email_text = payload.get("body", "")
                    fields = extract_email_fields(email_text)
                    mapped = build_record_from_email(fields, submitted_at=payload.get("received"), raw_payload=payload)
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

        if parsed.path == "/api/ingest-background-form":
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
                mapped = build_record_from_make(payload)
                if not mapped:
                    self._send_json({"error": "Could not parse applicant name from payload."}, 400)
                    return
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    app_id = upsert_background_record(cursor, mapped, payload)
                    conn.commit()
                self._send_json({"inserted": 1, "source": "background_check", "job_application_id": app_id})
            except Exception as exc:
                logging.exception("/api/ingest-background-form failed")
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/ingest-cognito-form":
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
                mapped = build_record_from_make(payload)
                if not mapped:
                    self._send_json({"error": "Could not parse applicant name from payload."}, 400)
                    return
                if contains_test_name(mapped["full_name"]):
                    self._send_json({"inserted": 0, "skipped": 1, "reason": "Name contains 'test'."})
                    return
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    app_id = upsert_cognito_record(cursor, mapped, payload)
                    conn.commit()
                self._send_json({"inserted": 1, "source": "cognito", "job_application_id": app_id})
            except Exception as exc:
                logging.exception("/api/ingest-cognito-form failed")
                self._send_json({"error": str(exc)}, 500)
            return

        if parsed.path == "/api/job-app-docs":
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
                with get_sql_connection() as conn:
                    cursor = conn.cursor()
                    result = upsert_job_app_docs(cursor, payload)
                    conn.commit()
                self._send_json({"source": "job-app-docs", **result})
            except LookupError as exc:
                self._send_json({"error": str(exc)}, 404)
            except Exception as exc:
                logging.exception("/api/job-app-docs failed")
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
