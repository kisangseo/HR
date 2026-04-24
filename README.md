# HR Job Applicants MVP

This version moves the ingest logic into Python so future workflow functions can be added server-side.

## Stack

- `app.py`: Python HTTP server + API + SQLite persistence
- `index.html`, `styles.css`, `app.js`: UI for upload, search, and table rendering
- `schema.sql`: PostgreSQL schema for future production DB alignment

## Features

- Ingest CSV submissions via `POST /api/ingest-csv`
- Normalize duplicate/conditional "Other Interested Positions" source columns
- Persist applicant records to local SQLite (`hr.db`)
- Search applicants via `GET /api/applicants` filters:
  - `name`
  - `date_from`
  - `date_to`
  - `job_title`

## Run

```bash
python3 app.py
```

Then open `http://127.0.0.1:8000`.

## Notes about your conditional position fields

When multiple duplicate "Other Interested Positions" columns exist in a CSV export,
only the first non-empty value is used during normalization, and empty duplicate columns are ignored.
