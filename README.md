# HR Job Applicants MVP

This version moves the ingest logic into Python so future workflow functions can be added server-side.

## Stack

- `app.py`: Python HTTP server + API + SQLite persistence
- `index.html`, `styles.css`, `app.js`: UI for upload, search, and table rendering
- `schema.sql`: SQL Server / Azure Data Studio-ready schema for `job_applications`

## Features

- Ingest CSV submissions via `POST /api/ingest-csv`
- Auto-detect delimiter for CSV-like exports (comma, tab, semicolon, pipe)
- Normalize duplicate/conditional "Other Interested Positions" source columns
- Persist applicant records to local SQLite (`hr.db`)
- Return ingest diagnostics (detected delimiter, detected headers, row-level warnings/skips)
- Search applicants via `GET /api/applicants` filters:
  - `name`
  - `date_from`
  - `date_to`
  - `job_title`
- Debug server build with `GET /api/version`

## Run

```bash
python3 app.py
```

Then open `http://127.0.0.1:8000`.

## Notes about your conditional position fields

When multiple duplicate "Other Interested Positions" columns exist in a CSV export,
only the first non-empty value is used during normalization, and empty duplicate columns are ignored.

If a submission date is malformed/missing (for example spreadsheet-export placeholders like `########`),
ingest falls back to the current timestamp so records are not dropped.

If rows are skipped, the API/UI now shows row-level reasons so you can see exactly which field failed mapping.
The ingest panel now also shows a compact issue summary and truncates very long issue lists.
Phone is treated as optional during ingest and will not produce a warning when blank.
