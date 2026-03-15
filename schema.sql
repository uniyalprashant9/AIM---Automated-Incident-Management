-- ============================================================
-- Azure SQL Schema for AIOps Command Center
-- Run this against your Azure SQL Database before first use.
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'incidents')
BEGIN
    CREATE TABLE incidents (
        id              UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
        incident_id     NVARCHAR(100)   NOT NULL,
        event_type      NVARCHAR(200),
        resource_id     NVARCHAR(500),
        severity        NVARCHAR(10),
        root_cause      NVARCHAR(MAX),
        incident_type   NVARCHAR(100),
        remediation     NVARCHAR(MAX),
        devops_commit   NVARCHAR(500),
        status          NVARCHAR(50)    DEFAULT 'open',
        detected_at     DATETIME2,
        diagnosed_at    DATETIME2,
        remediated_at   DATETIME2,
        documented_at   DATETIME2,
        summary         NVARCHAR(MAX),
        created_at      DATETIME2       DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_incidents_incident_id ON incidents (incident_id);
    CREATE INDEX IX_incidents_status ON incidents (status);
    CREATE INDEX IX_incidents_severity ON incidents (severity);
END;
