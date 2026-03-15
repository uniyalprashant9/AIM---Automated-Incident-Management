"""
SQL Client
==========
Thin wrapper around pyodbc for incident record operations.
All writes target the existing ``incidents`` table without schema changes.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import pyodbc

from services.config import Settings

logger = logging.getLogger(__name__)

_SQL_RETRY_ATTEMPTS = 3
_SQL_RETRY_DELAY_SECONDS = 2  # doubles each attempt: 2s, 4s, 8s

# Error codes that are transient and safe to retry
_RETRYABLE_ERRORS = {
    "08S01",  # Communication link failure
    "08001",  # Unable to connect
    "HYT00",  # Timeout expired
    "HYT01",  # Connection timeout
    "40613",  # Database unavailable (Azure SQL pause/resume)
    "40197",  # Service error processing request
    "40501",  # Service busy
    "49918",  # Cannot process request — not enough resources
    "28000",  # Login failed — may be transient on cold start (MSI token delay)
}


def _masked_conn_str(conn_str: str) -> str:
    """Return connection string with password value replaced by ***."""
    import re
    return re.sub(r'(Pwd\s*=)[^;]+', r'\1***', conn_str, flags=re.IGNORECASE)


def _is_retryable(sqlstate: str, msg: str) -> bool:
    """Return True if the pyodbc error should be retried.

    pyodbc often surfaces Azure SQL transient errors (e.g. 40613) under the
    generic ODBC state ``HY000`` with the actual SQL error number embedded in
    the message text as ``(40613)``.  We extract those numbers and check them
    against the retryable set so the retry loop actually fires.
    """
    if sqlstate in _RETRYABLE_ERRORS:
        return True
    if sqlstate == "HY000":
        for code in re.findall(r'\((\d+)\)', msg):
            if code in _RETRYABLE_ERRORS:
                return True
    return False


def _connect(settings: Settings) -> pyodbc.Connection:
    """Connect with retry for cold-start and transient failures."""
    last_exc: Exception | None = None
    delay = _SQL_RETRY_DELAY_SECONDS
    masked = _masked_conn_str(settings.sql_connection_string)
    logger.debug("SQL connecting — conn_str=%s", masked)
    for attempt in range(1, _SQL_RETRY_ATTEMPTS + 1):
        try:
            t0 = time.monotonic()
            conn = pyodbc.connect(settings.sql_connection_string, timeout=30)
            logger.debug("SQL connected in %.2fs (attempt %d)",
                         time.monotonic() - t0, attempt)
            return conn
        except pyodbc.Error as exc:
            sqlstate = exc.args[0] if exc.args else ""
            msg = exc.args[1] if len(exc.args) > 1 else str(exc)
            if _is_retryable(sqlstate, msg):
                last_exc = exc
                logger.warning(
                    "SQL connection attempt %d/%d failed (state=%s) — retrying in %ds | detail: %s",
                    attempt, _SQL_RETRY_ATTEMPTS, sqlstate, delay, msg,
                )
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(
                    "SQL connection failed with non-retryable error (state=%s) | detail: %s | conn_str=%s",
                    sqlstate, msg, masked,
                )
                raise  # Non-retryable — fail immediately
    logger.error("SQL connection exhausted all %d attempts | last_state=%s | conn_str=%s",
                 _SQL_RETRY_ATTEMPTS,
                 last_exc.args[0] if last_exc and last_exc.args else "unknown",
                 masked)
    raise last_exc  # type: ignore[misc]


# ── Deduplication / idempotency ──────────────────────────────────────────

def find_active_incident(settings: Settings, incident_id: str, dedup_window_minutes: int) -> dict | None:
    """
    Check whether an in-flight or recently-resolved record exists for *incident_id*
    within the dedup window.  Returns the row as a dict, or ``None``.
    """
    if not settings.sql_connection_string:
        logger.warning(
            "SQL not configured (SQL_CONNECTION_STRING is empty) — skipping dedup check")
        return None
    try:
        with _connect(settings) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT TOP 1 id, incident_id, status, severity, root_cause,
                       remediation, devops_commit, detected_at, summary
                FROM incidents
                WHERE incident_id = ?
                  AND created_at >= DATEADD(MINUTE, -?, SYSUTCDATETIME())
                ORDER BY created_at DESC
                """,
                (incident_id, dedup_window_minutes),
            )
            row = cursor.fetchone()
            if row:
                columns = [col[0] for col in cursor.description]
                result = dict(zip(columns, row))
                logger.debug("SQL dedup hit for incident_id=%s — existing status=%s",
                             incident_id, result.get("status"))
                return result
            logger.debug("SQL dedup: no existing record for incident_id=%s within %d min window",
                         incident_id, dedup_window_minutes)
    except pyodbc.Error as exc:
        sqlstate = exc.args[0] if exc.args else "unknown"
        msg = exc.args[1] if len(exc.args) > 1 else str(exc)
        logger.exception(
            "SQL dedup check failed (state=%s) | detail: %s", sqlstate, msg)
    return None


# ── Create / upsert ─────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO incidents (
    incident_id, event_type, resource_id, severity, root_cause,
    incident_type, remediation, devops_commit, status,
    detected_at, diagnosed_at, remediated_at, documented_at, summary
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_UPDATE_SQL = """
UPDATE incidents SET
    severity      = COALESCE(?, severity),
    root_cause    = COALESCE(?, root_cause),
    incident_type = COALESCE(?, incident_type),
    remediation   = COALESCE(?, remediation),
    devops_commit = COALESCE(?, devops_commit),
    status        = COALESCE(?, status),
    detected_at   = COALESCE(?, detected_at),
    diagnosed_at  = COALESCE(?, diagnosed_at),
    remediated_at = COALESCE(?, remediated_at),
    documented_at = COALESCE(?, documented_at),
    summary       = COALESCE(?, summary)
WHERE incident_id = ?
  AND id = (
        SELECT TOP 1 id FROM incidents
        WHERE incident_id = ?
        ORDER BY created_at DESC
  )
"""


def insert_incident(settings: Settings, record: dict) -> str | None:
    """Insert a new incident row. Returns the generated ``id`` (UUID) or ``None``."""
    if not settings.sql_connection_string:
        logger.warning(
            "SQL not configured (SQL_CONNECTION_STRING is empty) — skipping insert")
        return None
    try:
        with _connect(settings) as conn:
            cursor = conn.cursor()
            cursor.execute(
                _INSERT_SQL,
                (
                    record.get("incident_id"),
                    record.get("event_type"),
                    record.get("resource_id"),
                    record.get("severity"),
                    record.get("root_cause"),
                    record.get("incident_type"),
                    _json_or_str(record.get("remediation")),
                    record.get("devops_commit"),
                    record.get("status", "open"),
                    record.get("detected_at"),
                    record.get("diagnosed_at"),
                    record.get("remediated_at"),
                    record.get("documented_at"),
                    record.get("summary"),
                ),
            )
            conn.commit()
            cursor.execute(
                "SELECT TOP 1 id FROM incidents WHERE incident_id = ? ORDER BY created_at DESC",
                (record.get("incident_id"),),
            )
            row = cursor.fetchone()
            new_id = str(row[0]) if row else None
            logger.info("SQL insert succeeded for incident_id=%s — db_id=%s", record.get(
                "incident_id"), new_id)
            return new_id
    except pyodbc.Error as exc:
        sqlstate = exc.args[0] if exc.args else "unknown"
        msg = exc.args[1] if len(exc.args) > 1 else str(exc)
        logger.exception("SQL insert failed for incident_id=%s (state=%s) | detail: %s",
                         record.get("incident_id"), sqlstate, msg)
        return None


def update_incident(settings: Settings, incident_id: str, fields: dict) -> bool:
    """Update the most recent row for *incident_id* with the supplied field values."""
    if not settings.sql_connection_string:
        logger.warning(
            "SQL not configured (SQL_CONNECTION_STRING is empty) — skipping update")
        return False
    try:
        with _connect(settings) as conn:
            cursor = conn.cursor()
            cursor.execute(
                _UPDATE_SQL,
                (
                    fields.get("severity"),
                    fields.get("root_cause"),
                    fields.get("incident_type"),
                    _json_or_str(fields.get("remediation")),
                    fields.get("devops_commit"),
                    fields.get("status"),
                    fields.get("detected_at"),
                    fields.get("diagnosed_at"),
                    fields.get("remediated_at"),
                    fields.get("documented_at"),
                    fields.get("summary"),
                    incident_id,
                    incident_id,
                ),
            )
            conn.commit()
            updated = cursor.rowcount > 0
            logger.info("SQL update for incident_id=%s — rows_affected=%d fields=%s",
                        incident_id, cursor.rowcount, list(fields.keys()))
            return updated
    except pyodbc.Error as exc:
        sqlstate = exc.args[0] if exc.args else "unknown"
        msg = exc.args[1] if len(exc.args) > 1 else str(exc)
        logger.exception("SQL update failed for incident_id=%s (state=%s) | detail: %s",
                         incident_id, sqlstate, msg)
        return False


# ── Helpers ──────────────────────────────────────────────────────────────

def _json_or_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)
