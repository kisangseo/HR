# HR Job Applicants MVP

This version runs ingest/query directly against SQL Server so the web app and your SQL database stay in sync.

## Stack

- `app.py`: Python HTTP server + API + SQL Server persistence (via `pyodbc`)
- `index.html`, `styles.css`, `app.js`: UI for search and table rendering
- `schema.sql`: SQL Server / Azure Data Studio-ready schema for `job_applications`

## Features

- CSV ingest endpoint exists in code as legacy logic but is currently disabled in the API/UI
- Normalize duplicate/conditional "Other Interested Positions" source columns
- Persist applicant records to SQL Server (`job_applications`)
- `full_name` is computed by SQL Server schema from `first_name` + `last_name` (not inserted directly)
- Normalize phone values to digits only (leading `+` removed)
- Store and display submission date as date-only (`YYYY-MM-DD`)
- Return ingest diagnostics (detected delimiter, detected headers, row-level warnings/skips)
- Search applicants via `GET /api/applicants` filters:
  - `name`
  - `date_from` / `date_to` (UI uses one combined date-range picker)
  - `job_title` (UI uses full-name dropdown)
- Debug server build with `GET /api/version`
- UI uses short labels for selected positions (`Court Security Officer` -> `CSO`, `Deputy Sheriff` -> `Deputy`, `Information Technology` -> `IT`)
- Records with names containing `test` are excluded from ingest/display
- Same-name applicants are merged in API response and positions are unioned for display
- Optional MAKE webhook endpoint (`POST /api/ingest-interest-form`) to ingest parsed interest forms directly
- Optional Microsoft Graph email ingest script (`email_ingest.py`) for inbox-based job applications

## Run

```bash
python3 -m pip install -r requirements.txt
export HR_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=tcp:YOUR_SERVER.database.windows.net,1433;Database=YOUR_DB;Uid=YOUR_USER;Pwd=YOUR_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
# optional webhook auth for MAKE
export HR_MAKE_WEBHOOK_TOKEN=YOUR_SHARED_TOKEN
# optional server bind overrides
export HR_HOST=127.0.0.1
export HR_PORT=8000
# cloud platforms can provide PORT; app will respect it automatically
python3 app.py
```

Then open `http://127.0.0.1:8000` for local development.

## Email ingest (Microsoft Graph)

Use this when job applications arrive by email and should be inserted into `job_applications`.

```bash
export CLIENT_ID=...
export CLIENT_SECRET=...
export TENANT_ID=...
export MAILBOX_EMAIL=shared-mailbox@yourdomain.org
export HR_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};..."

# optional overrides
export JOB_APP_SENDER=noreply@baltimorecitysheriff.gov
export JOB_APP_SENDER_MATCH_MODE=exact  # exact | contains
export JOB_APP_SUBJECT_CONTAINS="Job Application"
export INBOX_SCAN_LIMIT=500
# defaults to csv so existing source='csv' queries continue to work
export JOB_APP_INGEST_SOURCE=csv

python3 email_ingest.py
```

Behavior:
- Scans the main Inbox.
- Processes messages where sender equals `JOB_APP_SENDER` and subject contains `JOB_APP_SUBJECT_CONTAINS` (case-insensitive).
- Parses fixed labels: Name, Email, Phone Number, Primary Position You Are Applying For, Other Interested Positions.
- Stores `other_positions` as JSON array split by comma/newline.
- Inserts every matching email (no dedupe), with source defaulting to `csv`.
- Moves every matching/processed job-application email into Inbox child folder `processed`.

Recovery tip:
- If emails were moved to `processed` but not inserted correctly, re-run against processed folder:
  - CLI: `python3 email_ingest.py --source-folder processed --scan-limit 500`
  - Endpoint: `/run-ingest?source_folder=processed&scan_limit=500`

Troubleshooting:
- The script reads environment variables from the shell/session where `python3 email_ingest.py` runs.
- Azure App Service environment variables are only used by code running inside App Service (not your local terminal run).
- Startup logs now print mailbox/sender config and top senders scanned so you can verify filtering.

## Run email ingest from GitHub Actions (simple endpoint trigger)

This repo includes `.github/workflows/email_ingest_job.yml` that just calls a hosted endpoint every 5 minutes (or manually), similar to:

```yaml
run: curl https://YOUR_APP/run-ingest
```

Endpoint is currently hardcoded in workflow to:
- `https://jobapplications-badhh8h5afdpcqe8.centralus-01.azurewebsites.net/run-ingest`

Optional (if you later decide to protect `/run-ingest`):
- `HR_RUN_INGEST_TOKEN`

Then run:
1. GitHub → **Actions** → **Email ingest trigger**.
2. Click **Run workflow**.

### Endpoint security

Set `HR_RUN_INGEST_TOKEN` in your app settings to require auth for `/run-ingest`.
If set, callers must send the same value as `X-Run-Token` (or `?token=` query param).

### Azure App Service note

This repo now exposes a WSGI callable named `app` in `app.py`, so Oryx/Gunicorn startup (`gunicorn app:app`) works without additional startup command overrides.

## MAKE webhook setup

1. In MAKE, create scenario with trigger (email or form source).
2. Add an HTTP module:
   - Method: `POST`
   - URL: `http://YOUR_HOST:8000/api/ingest-interest-form`
   - Headers: `Content-Type: application/json`
   - Optional auth header: `X-Webhook-Token: <HR_MAKE_WEBHOOK_TOKEN>`
3. Send JSON body with fields like:
   - `name`
   - `email`
   - `phone` (or `phone_number`)
   - `primary_position` (or `job_title`)
   - `other_positions` (array or comma-separated string)
   - `submission_date` (optional; fallback is current UTC date)

## Notes about your conditional position fields

When multiple duplicate "Other Interested Positions" columns exist in a CSV export,
all non-empty values across those columns are collected, deduplicated, and primary position is excluded.

If a submission date is malformed/missing (for example spreadsheet-export placeholders like `########`),
ingest falls back to the current timestamp so records are not dropped.

If rows are skipped, the API/UI now shows row-level reasons so you can see exactly which field failed mapping.
The ingest panel now also shows a compact issue summary and truncates very long issue lists.
Phone is treated as optional during ingest and will not produce a warning when blank.
