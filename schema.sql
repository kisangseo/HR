-- PostgreSQL schema for HR job applications

CREATE TABLE IF NOT EXISTS job_applications (
  id BIGSERIAL PRIMARY KEY,
  submitted_at TIMESTAMPTZ NOT NULL,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  full_name TEXT GENERATED ALWAYS AS (trim(first_name || ' ' || last_name)) STORED,
  email TEXT NOT NULL,
  phone TEXT,
  primary_position TEXT NOT NULL,
  other_positions JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'interest_submitted',
  source TEXT NOT NULL DEFAULT 'csv',
  raw_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_applications_submitted_at
  ON job_applications (submitted_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_applications_name
  ON job_applications (last_name, first_name);

CREATE INDEX IF NOT EXISTS idx_job_applications_primary_position
  ON job_applications (primary_position);

CREATE INDEX IF NOT EXISTS idx_job_applications_email
  ON job_applications (email);

CREATE INDEX IF NOT EXISTS idx_job_applications_other_positions_gin
  ON job_applications USING GIN (other_positions);

-- Optional helper trigger for updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_job_applications_set_updated_at ON job_applications;
CREATE TRIGGER trg_job_applications_set_updated_at
BEFORE UPDATE ON job_applications
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
