"""
Diagnosis Agent
===============
Uses the Azure OpenAI chat model to classify an incoming signal as
"Normal Log" or "Incident", determine root cause/severity, and retrieve
similar historical incidents from Azure AI Search (RAG).

No hardcoded thresholds — all reasoning is performed by the LLM grounded
on telemetry evidence and retrieved knowledge.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from orchestrator.session import SessionState
from services import azure_openai, azure_search, sql_client

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are the Diagnosis Agent in an AIOps incident-management system.

You will receive:
1. An Azure Monitor alert payload (Common Alert Schema) with resource identifiers, timestamps, severity, alert rule name, signal type, and alert context.
2. Detailed telemetry from Application Insights:
   - exceptions: error type, message, innermost message, full stack trace, operation name
   - failed_requests: HTTP method, URL, response code (4xx vs 5xx), duration, custom dimensions (input parameters)
   - traces: surrounding warning/error log messages
3. Azure Monitor alerts and metrics for the affected resource.
4. Optionally, similar historical incidents retrieved from an Azure AI Search knowledge base.

Your tasks:
A) Classify the signal as ONE of:
   - "normal_log": a routine operational signal that does NOT require incident tracking or remediation.
   - "invalid_api_call": the error was caused by the CALLER sending a malformed, unauthorized, or
     parameter-invalid request (e.g. 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found
     due to wrong path, 422 Unprocessable Entity). The application code is functioning correctly —
     DO NOT recommend code changes for these.
   - "incident": an abnormal signal caused by the APPLICATION or INFRASTRUCTURE itself
     (e.g. 500 Internal Server Error, unhandled exceptions, dependency failures, resource exhaustion,
     timeouts, memory leaks, crashes). Requires tracking and potential remediation.

B) If "incident":
   - Determine severity: P1 (critical), P2 (high), P3 (medium), P4 (low).
   - Identify the most likely root cause. Reference specific error messages, stack traces, or
     metric values from the evidence. Do not fabricate.
   - Identify incident type (e.g., ApplicationFailure, DependencyFailure, ResourceExhaustion,
     SecurityAnomaly, ConfigurationDrift, UnhandledException, TimeoutFailure).
   - Note if this is a recurrence and whether prior remediations apply.
   - Suggest a remediation direction (the Remediation Agent will execute).

C) If "invalid_api_call":
   - Identify WHAT was wrong with the request: missing parameters, invalid values, wrong endpoint,
     missing authentication, schema violations, etc. Use the request URL, resultCode, customDimensions,
     and error messages from the evidence.
   - State this clearly so it can be documented and the API caller can be informed.
   - Severity is always P4. No code remediation should be triggered.

D) If "normal_log":
   - Provide a brief explanation of why this is not an incident.

CRITICAL RULES:
- Distinguish INBOUND errors (caller → function app) from OUTBOUND errors (function app → external
  service). Use the `_structural_analysis` block in the evidence as your primary signal:
    * If `outbound_dependency_failures` or `dependency_exception_types` are non-empty → classify as
      "incident" with type "DependencyFailure", regardless of what HTTP status code that external
      service returned. An OpenAI 429, a SQL timeout, or an Azure Search 503 are all infrastructure
      incidents, NOT invalid_api_call.
    * Exception type names such as openai.BadRequestError, azure.core.HttpResponseError,
      requests.exceptions.ConnectionError, pyodbc.Error, ServiceRequestError, RateLimitError, or
      APIConnectionError are ALWAYS dependency failures → incident, never invalid_api_call.
    * Only classify as "invalid_api_call" when ALL failures are inbound 4xx (400/401/403/404/422)
      with NO outbound dependency failures and NO dependency exception types.
- Distinguish carefully between 4xx errors (caller's fault → invalid_api_call) and 5xx errors
  (application's fault → incident). A 500 caused by bad input is still an "incident" because
  the application should handle it gracefully instead of crashing.
- The `_structural_analysis.conclusion` field in the evidence is a deterministic pre-computed hint
  — treat it as strong evidence, but cross-check against all other telemetry before deciding.
- Do NOT hardcode thresholds. Assess evidence holistically.
- If evidence is ambiguous, default to "incident" with lower severity (P3/P4).
- Base severity on actual business impact, not arbitrary numeric cutoffs.

Respond ONLY with valid JSON matching this schema:
{
  "classification": "incident" | "normal_log" | "invalid_api_call",
  "explanation": "<brief reasoning referencing specific evidence>",
  "severity": "P1" | "P2" | "P3" | "P4" | null,
  "root_cause": "<root cause with evidence references>" | null,
  "incident_type": "<type>" | null,
  "error_message": "<key error message extracted from exceptions or traces>" | null,
  "affected_operation": "<operation_Name or request URL that failed>" | null,
  "invalid_request_details": "<for invalid_api_call: what was wrong with the request>" | null,
  "confidence": "high" | "medium" | "low",
  "is_recurrence": true | false,
  "prior_remediation_applicable": true | false,
  "suggested_remediation_direction": "<brief suggestion>" | null
}
"""


# ---------------------------------------------------------------------------
# Patterns in exception type names that indicate the function app itself made
# a failing outbound call — these are NEVER the inbound caller's fault.
# ---------------------------------------------------------------------------
_DEPENDENCY_EXCEPTION_PATTERNS: tuple[str, ...] = (
    "openai.",
    "azure.core.",
    "azure.identity.",
    "requests.exceptions.",
    "httpx.",
    "aiohttp.",
    "connectionerror",
    "timeouterror",
    "timeoutexception",
    "sockettimeout",
    "sqlexception",
    "pyodbc.",
    "pymssql.",
    "servicerequesterror",
    "httpresponseerror",
    "clientconnectorerror",
    "ratelimiterror",
    "apiconnectionerror",
    "apistatuserror",
    "autherror",
    "servicebuserror",
)


def _extract_ai_rows(data: dict) -> list[dict]:
    """Parse an App Insights KQL result (tables → columns/rows) into row dicts."""
    tables = data.get("tables", [])
    if not tables:
        return []
    table = tables[0]
    cols = [c["name"] for c in table.get("columns", [])]
    return [dict(zip(cols, row)) for row in table.get("rows", [])]


def _build_structural_hints(state: SessionState) -> dict:
    """
    Deterministically analyse the collected telemetry to determine whether
    failures originate from:
      - Inbound bad requests (caller → function app)  → hints at invalid_api_call
      - Outbound dependency failures (function app → external service) → hints at incident

    The returned dict is embedded into the evidence payload so the LLM has an
    unambiguous structural signal alongside the raw telemetry.
    """
    hints: dict = {
        "inbound_4xx_failures": [],
        "inbound_5xx_failures": [],
        "outbound_dependency_failures": [],
        "dependency_exception_types": [],
        "conclusion": "",
    }

    # ── Inbound failed requests ──────────────────────────────────────────
    for row in _extract_ai_rows(state.telemetry.get("failed_requests", {})):
        code = str(row.get("resultCode", ""))
        entry = {"url": row.get("url", ""), "resultCode": code,
                 "name": row.get("name", "")}
        if code.startswith("4"):
            hints["inbound_4xx_failures"].append(entry)
        elif code.startswith("5"):
            hints["inbound_5xx_failures"].append(entry)

    # ── Outbound dependency failures ─────────────────────────────────────
    for row in _extract_ai_rows(state.telemetry.get("dependencies", {})):
        hints["outbound_dependency_failures"].append({
            "target": row.get("target", ""),
            "type": row.get("type", ""),
            "resultCode": str(row.get("resultCode", "")),
            "name": row.get("name", ""),
            "operation_Name": row.get("operation_Name", ""),
        })

    # ── Exception type analysis ──────────────────────────────────────────
    for row in _extract_ai_rows(state.telemetry.get("exceptions", {})):
        exc_type = str(row.get("type", "")).lower()
        if any(pat in exc_type for pat in _DEPENDENCY_EXCEPTION_PATTERNS):
            hints["dependency_exception_types"].append(row.get("type", ""))

    # ── Derive conclusion ────────────────────────────────────────────────
    has_outbound = bool(
        hints["outbound_dependency_failures"] or hints["dependency_exception_types"]
    )
    has_5xx = bool(hints["inbound_5xx_failures"])
    has_4xx_only = (
        bool(hints["inbound_4xx_failures"])
        and not hints["inbound_5xx_failures"]
        and not has_outbound
    )

    if has_outbound:
        targets = ", ".join(
            d["target"] for d in hints["outbound_dependency_failures"] if d["target"]
        ) or "unknown external service"
        hints["conclusion"] = (
            f"OUTBOUND_DEPENDENCY_FAILURE: The function app made outbound calls to "
            f"[{targets}] that failed. This MUST be classified as incident "
            f"(DependencyFailure) — do NOT classify as invalid_api_call even if the "
            f"exception type contains words like 'BadRequest' or 'InvalidRequest', "
            f"because those describe what the external service rejected, not the "
            f"inbound caller's request to this function app."
        )
    elif has_4xx_only:
        hints["conclusion"] = (
            "INBOUND_CALLER_ERROR: All inbound request failures are 4xx with no outbound "
            "dependency failures and no dependency-related exception types detected. "
            "This strongly suggests invalid_api_call."
        )
    elif has_5xx and not has_outbound:
        hints["conclusion"] = (
            "INBOUND_SERVER_ERROR: Inbound requests failed with 5xx and no outbound "
            "dependency failures were detected. This suggests an application-level "
            "incident (unhandled exception or bug in business logic)."
        )
    else:
        hints["conclusion"] = (
            "INCONCLUSIVE: Could not determine failure direction from structural signals. "
            "Use full telemetry (exceptions, traces, stack traces) to decide."
        )

    logger.debug(
        "[diagnosis_agent] structural_hints conclusion=%s outbound=%d dep_exc=%d",
        hints["conclusion"][:80],
        len(hints["outbound_dependency_failures"]),
        len(hints["dependency_exception_types"]),
    )
    return hints


class DiagnosisAgent:
    name: str = "diagnosis_agent"

    def run(self, state: SessionState) -> None:
        settings = state.settings
        if settings is None:
            state.add_message("agent", self.name,
                              "ERROR: settings not available")
            return

        logger.info("[%s] Starting diagnosis for incident_id=%s",
                    self.name, state.incident_id)
        state.add_message("agent", self.name,
                          "Starting signal classification and diagnosis")

        # Build the evidence payload for the LLM
        structural_hints = _build_structural_hints(state)
        evidence: dict = {
            "event": state.event_data,
            # Pre-computed structural analysis — read this FIRST before
            # interpreting raw telemetry. It unambiguously identifies whether
            # failures are inbound (caller error) or outbound (dependency failure).
            "_structural_analysis": structural_hints,
            "telemetry": {
                "alerts": state.telemetry.get("alerts", []),
                "metrics": state.telemetry.get("metrics", {}),
                "exceptions": state.telemetry.get("exceptions", {}),
                "failed_requests": state.telemetry.get("failed_requests", {}),
                "traces": state.telemetry.get("traces", {}),
                # Outbound calls from the function app to external services.
                # Non-empty rows here = DependencyFailure incident.
                "outbound_dependency_failures": state.telemetry.get("dependencies", {}),
            },
        }

        # RAG: retrieve similar historical incidents from Azure AI Search
        query_text = _build_search_query(state)
        similar_incidents = azure_search.search_similar_incidents(
            settings, query_text)
        state.knowledge_snippets = similar_incidents
        evidence["similar_historical_incidents"] = similar_incidents

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(evidence, default=str)},
        ]

        result = azure_openai.chat_completion_json(settings, messages)
        logger.info("[%s] LLM classification: %s",
                    self.name, result.get("classification"))

        state.add_message(
            "agent", self.name, f"Classification result: {json.dumps(result, default=str)}")

        classification = result.get("classification", "incident")
        now_iso = datetime.now(timezone.utc).isoformat()

        if classification == "normal_log":
            _handle_normal_log(state, settings, result, now_iso)
            return

        if classification == "invalid_api_call":
            _handle_invalid_api_call(state, settings, result, now_iso)
            return

        # Incident path
        state.is_incident = True
        state.status = "investigating"
        state.diagnosis = result

        record_fields = {
            "incident_id": state.incident_id,
            "event_type": state.event_data.get("eventType", ""),
            "resource_id": state.event_data.get("subject", ""),
            "severity": result.get("severity"),
            "root_cause": result.get("root_cause"),
            "incident_type": result.get("incident_type"),
            "status": "investigating",
            "detected_at": state.event_data.get("eventTime"),
            "diagnosed_at": now_iso,
            "summary": (
                f"{result.get('explanation', '')} | "
                f"Error: {result.get('error_message', 'N/A')} | "
                f"Operation: {result.get('affected_operation', 'N/A')}"
            ),
        }

        if state.sql_record_id:
            sql_client.update_incident(
                settings, state.incident_id, record_fields)
        else:
            record_id = sql_client.insert_incident(settings, record_fields)
            state.sql_record_id = record_id

        state.add_message(
            "agent", self.name, f"Incident confirmed — severity={result.get('severity')} type={result.get('incident_type')}")
        logger.info(
            "[%s] Incident diagnosed: severity=%s type=%s root_cause=%.120s",
            self.name,
            result.get("severity"),
            result.get("incident_type"),
            result.get("root_cause", ""),
        )


def _handle_invalid_api_call(state: SessionState, settings, result: dict, now_iso: str) -> None:
    """Document an invalid API call and terminate — no code remediation needed."""
    state.is_incident = False
    state.status = "closed"
    state.diagnosis = result

    summary = (
        f"Invalid API call — {result.get('explanation', '')} | "
        f"Request issue: {result.get('invalid_request_details', 'N/A')} | "
        f"Operation: {result.get('affected_operation', 'N/A')}"
    )

    record_fields = {
        "incident_id": state.incident_id,
        "event_type": state.event_data.get("eventType", ""),
        "resource_id": state.event_data.get("subject", ""),
        "severity": "P4",
        "incident_type": "InvalidApiCall",
        "status": "closed",
        "detected_at": state.event_data.get("eventTime"),
        "diagnosed_at": now_iso,
        "summary": summary,
    }

    if state.sql_record_id:
        sql_client.update_incident(settings, state.incident_id, record_fields)
    else:
        record_id = sql_client.insert_incident(settings, record_fields)
        state.sql_record_id = record_id

    state.terminate("invalid_api_call")
    state.add_message(
        "agent", "diagnosis_agent",
        f"Signal classified as invalid API call — no code remediation needed. "
        f"Details: {result.get('invalid_request_details', '')}. Session terminated."
    )
    logger.info(
        "[diagnosis_agent] Invalid API call detected — operation=%s details=%.200s",
        result.get("affected_operation", ""),
        result.get("invalid_request_details", ""),
    )


def _handle_normal_log(state: SessionState, settings, result: dict, now_iso: str) -> None:
    """Mark the signal as a normal log and terminate the session."""
    state.is_incident = False
    state.status = "closed"
    state.diagnosis = result

    record_fields = {
        "incident_id": state.incident_id,
        "event_type": state.event_data.get("eventType", ""),
        "resource_id": state.event_data.get("subject", ""),
        "severity": "P4",
        "incident_type": "NormalLog",
        "status": "closed",
        "detected_at": state.event_data.get("eventTime"),
        "diagnosed_at": now_iso,
        "summary": f"Normal log — {result.get('explanation', 'No incident detected')}",
    }

    if state.sql_record_id:
        sql_client.update_incident(settings, state.incident_id, record_fields)
    else:
        record_id = sql_client.insert_incident(settings, record_fields)
        state.sql_record_id = record_id

    state.terminate("normal_log")
    state.add_message("agent", "diagnosis_agent",
                      f"Signal classified as normal log — session terminated. Reason: {result.get('explanation', '')}")
    logger.info("[diagnosis_agent] Normal log — session terminated")


def _build_search_query(state: SessionState) -> str:
    """Build a concise search query string from the event for Azure AI Search."""
    parts: list[str] = []
    event = state.event_data
    if event.get("eventType"):
        parts.append(str(event["eventType"]))
    if event.get("subject"):
        parts.append(str(event["subject"]))
    data = event.get("data", {})
    if isinstance(data, dict):
        for key in ("alertRule", "resourceUri", "monitorCondition", "severity", "signalType"):
            val = data.get(key)
            if val:
                parts.append(str(val) if not isinstance(
                    val, (list, dict)) else json.dumps(val, default=str))
    telemetry = state.telemetry
    if telemetry.get("alerts"):
        first_alert = telemetry["alerts"][0] if isinstance(
            telemetry["alerts"], list) else {}
        parts.append(str(first_alert.get("name", "")))
    return " ".join(parts)[:2000]
