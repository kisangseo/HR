# HR Job Applicants MVP

This version runs ingest/query directly against SQL Server so the web app and your SQL database stay in sync.

## Stack

- `app.py`: Python HTTP server + API + SQL Server persistence (via `pyodbc`)
- `index.html`, `styles.css`, `app.js`: UI for upload, search, and table rendering
- `schema.sql`: SQL Server / Azure Data Studio-ready schema for `job_applications`

## Features

- Ingest CSV submissions via `POST /api/ingest-csv`
- Auto-detect delimiter for CSV-like exports (comma, tab, semicolon, pipe)
- Normalize duplicate/conditional "Other Interested Positions" source columns
- Persist applicant records to SQL Server (`job_applications`)
- `full_name` is computed by SQL Server schema from `first_name` + `last_name` (not inserted directly)
- Normalize phone values to digits only (leading `+` removed)
- Store and display submission date as date-only (`YYYY-MM-DD`)
- Return ingest diagnostics (detected delimiter, detected headers, row-level warnings/skips)
- Search applicants via `GET /api/applicants` filters:
  - `name`
  - `date_from`
  - `date_to`
  - `job_title`
- Debug server build with `GET /api/version`
- UI uses short labels for selected positions (`Court Security Officer` -> `CSO`, `Deputy Sheriff` -> `Deputy`, `Information Technology` -> `IT`)
- Records with names containing `test` are excluded from ingest/display
- Same-name applicants are merged in API response and positions are unioned for display
- Optional IMAP poller can auto-ingest emails sent to `noreply@baltimorecitysheriff.gov` when subject includes `Job Application Form`

## Run

```bash
export HR_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=tcp:YOUR_SERVER.database.windows.net,1433;Database=YOUR_DB;Uid=YOUR_USER;Pwd=YOUR_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
# optional email auto-ingest
export HR_EMAIL_POLL_ENABLED=true
export HR_IMAP_HOST=outlook.office365.com
export HR_IMAP_PORT=993
export HR_IMAP_USER=noreply@baltimorecitysheriff.gov
export HR_IMAP_PASSWORD=YOUR_EMAIL_PASSWORD
export HR_IMAP_MAILBOX=INBOX
export HR_IMAP_PROCESSED_MAILBOX=Processed
export HR_EMAIL_SUBJECT_KEYWORD="Job Application Form"
export HR_EMAIL_POLL_SECONDS=60
python3 app.py
```

Then open `http://127.0.0.1:8000`.

## Notes about your conditional position fields

When multiple duplicate "Other Interested Positions" columns exist in a CSV export,
all non-empty values across those columns are collected, deduplicated, and primary position is excluded.

If a submission date is malformed/missing (for example spreadsheet-export placeholders like `########`),
ingest falls back to the current timestamp so records are not dropped.

If rows are skipped, the API/UI now shows row-level reasons so you can see exactly which field failed mapping.
The ingest panel now also shows a compact issue summary and truncates very long issue lists.
Phone is treated as optional during ingest and will not produce a warning when blank.
