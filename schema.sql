-- SQL Server / Azure Data Studio schema for HR job applications
-- Safe to run multiple times.

IF OBJECT_ID('dbo.job_applications', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.job_applications (
    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    submitted_at DATETIME2(0) NOT NULL,
    first_name NVARCHAR(200) NULL,
    last_name NVARCHAR(200) NULL,
    full_name AS LTRIM(RTRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')))) PERSISTED,
    email NVARCHAR(320) NULL,
    phone NVARCHAR(50) NULL,
    primary_position NVARCHAR(255) NOT NULL,
    other_positions NVARCHAR(MAX) NOT NULL CONSTRAINT DF_job_applications_other_positions DEFAULT (N'[]'),
    status NVARCHAR(100) NOT NULL CONSTRAINT DF_job_applications_status DEFAULT (N'interest_submitted'),
    denied BIT NOT NULL CONSTRAINT DF_job_applications_denied DEFAULT ((0)),
    source NVARCHAR(100) NOT NULL CONSTRAINT DF_job_applications_source DEFAULT (N'csv'),
    raw_payload NVARCHAR(MAX) NULL,
    created_at DATETIME2(0) NOT NULL CONSTRAINT DF_job_applications_created_at DEFAULT (SYSUTCDATETIME()),
    updated_at DATETIME2(0) NOT NULL CONSTRAINT DF_job_applications_updated_at DEFAULT (SYSUTCDATETIME())
  );
END;
GO

IF COL_LENGTH('dbo.job_applications', 'denied') IS NULL
BEGIN
  ALTER TABLE dbo.job_applications
    ADD denied BIT NOT NULL CONSTRAINT DF_job_applications_denied DEFAULT ((0));
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_job_applications_other_positions_json'
)
BEGIN
  ALTER TABLE dbo.job_applications
    ADD CONSTRAINT CK_job_applications_other_positions_json
    CHECK (ISJSON(other_positions) = 1);
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.check_constraints
  WHERE name = 'CK_job_applications_raw_payload_json'
)
BEGIN
  ALTER TABLE dbo.job_applications
    ADD CONSTRAINT CK_job_applications_raw_payload_json
    CHECK (raw_payload IS NULL OR ISJSON(raw_payload) = 1);
END;
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name = 'idx_job_applications_submitted_at'
    AND object_id = OBJECT_ID('dbo.job_applications')
)
BEGIN
  CREATE INDEX idx_job_applications_submitted_at
    ON dbo.job_applications (submitted_at DESC);
END;
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name = 'idx_job_applications_name'
    AND object_id = OBJECT_ID('dbo.job_applications')
)
BEGIN
  CREATE INDEX idx_job_applications_name
    ON dbo.job_applications (last_name, first_name);
END;
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name = 'idx_job_applications_primary_position'
    AND object_id = OBJECT_ID('dbo.job_applications')
)
BEGIN
  CREATE INDEX idx_job_applications_primary_position
    ON dbo.job_applications (primary_position);
END;
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name = 'idx_job_applications_email'
    AND object_id = OBJECT_ID('dbo.job_applications')
)
BEGIN
  CREATE INDEX idx_job_applications_email
    ON dbo.job_applications (email);
END;
GO

IF OBJECT_ID('dbo.trg_job_applications_set_updated_at', 'TR') IS NOT NULL
  DROP TRIGGER dbo.trg_job_applications_set_updated_at;
GO

CREATE TRIGGER dbo.trg_job_applications_set_updated_at
ON dbo.job_applications
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;

  UPDATE ja
  SET updated_at = SYSUTCDATETIME()
  FROM dbo.job_applications AS ja
  INNER JOIN inserted AS i
    ON ja.id = i.id;
END;
GO
