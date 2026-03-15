"""
Documentation Agent
===================
Uses the Azure OpenAI chat model to produce a comprehensive incident report,
then persists it to the existing Azure SQL record. The documentation is
structured to be suitable for future Azure AI Search indexing.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from orchestrator.session import SessionState
from services import azure_openai, sql_client

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are the Documentation Agent in an AIOps incident-management system.

You will receive the full context of an incident: the original event, telemetry,
diagnosis results, remediation actions taken, and any similar historical incidents
retrieved from the knowledge base.

Your tasks:
1. Summarize WHAT HAPPENED (symptoms observed).
2. State the ROOT CAUSE with evidence references. Do not fabricate evidence.
3. Describe REMEDIATION PERFORMED (or recommended if not yet executed), including
   any DevOps artifacts (commit IDs, PR IDs, pipeline run IDs) if present.
4. Suggest PREVENTION / FOLLOW-UP actions to reduce recurrence risk.
5. Note any SIMILAR HISTORICAL INCIDENTS and whether prior solutions were reused.
6. Provide a one-line SUMMARY suitable for an incident table/dashboard.

IMPORTANT:
- Be factual and concise. Do not invent details not present in the evidence.
- Structure the output so it can be indexed for future knowledge retrieval.
- Use plain text (no markdown). Keep sections clearly labeled.

Respond ONLY with valid JSON:
{
  "summary": "<one-line dashboard summary>",
  "what_happened": "<symptoms description>",
  "root_cause": "<root cause with evidence>",
  "remediation_performed": "<actions taken or recommended>",
  "prevention_followups": "<preventive measures>",
  "similar_incidents": "<references or 'none'>",
  "final_status": "resolved" | "mitigating" | "needs_human_review"
}
"""


# Valid lifecycle statuses — LLM output is validated against this set to
# prevent an unexpected model response from corrupting the incident record.
_VALID_STATUSES = frozenset({
    "resolved", "closed", "mitigating", "needs_human_review",
    "sent_for_human_review", "open", "investigating",
})


class DocumentationAgent:
    name: str = "documentation_agent"

    def run(self, state: SessionState) -> None:
        settings = state.settings
        if settings is None:
            state.add_message("agent", self.name,
                              "ERROR: settings not available")
            return

        if not state.is_incident:
            state.add_message("agent", self.name,
                              "Not an incident — skipping documentation")
            logger.info("[%s] Not an incident — skipping", self.name)
            return

        logger.info("[%s] Compiling incident documentation for %s",
                    self.name, state.incident_id)
        state.add_message("agent", self.name,
                          "Compiling final incident documentation")

        # Build full context for the LLM
        context: dict = {
            "incident_id": state.incident_id,
            "event": state.event_data,
            "telemetry_summary": _compact_telemetry(state.telemetry),
            "diagnosis": state.diagnosis,
            "remediation": state.remediation,
            "similar_historical_incidents": [
                {k: v for k, v in s.items() if k != "search_score"}
                for s in state.knowledge_snippets[:5]
            ],
            "session_messages": [
                {"agent": m["agent"], "content": m["content"][:500]}
                for m in state.messages[-20:]
            ],
        }

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(context, default=str)},
        ]

        doc = azure_openai.chat_completion_json(settings, messages)
        logger.info("[%s] Documentation generated — final_status=%s",
                    self.name, doc.get("final_status"))

        state.documentation = doc
        now_iso = datetime.now(timezone.utc).isoformat()

        # Build the full summary to persist
        full_summary = _build_full_summary(doc, state)

        # Validate the LLM-proposed final_status against the allowlist.
        # Fall back to the current state.status if the model returns an
        # unrecognised value, preventing arbitrary strings reaching the DB.
        raw_final_status = doc.get("final_status", state.status)
        if raw_final_status not in _VALID_STATUSES:
            logger.warning(
                "[%s] LLM returned unrecognised final_status '%s' — keeping current status '%s'",
                self.name, raw_final_status, state.status,
            )
            raw_final_status = state.status

        # Respect statuses that already represent a completed lifecycle —
        # the documentation agent should not downgrade sent_for_human_review
        # back to a generic 'resolved' when a PR is awaiting review.
        _preserve_statuses = {"sent_for_human_review", "needs_human_review"}
        if state.status in _preserve_statuses:
            final_status = state.status
        else:
            final_status = raw_final_status
            if final_status in ("resolved", "closed"):
                state.status = final_status

        # Build remediation text for SQL column
        remediation_text = json.dumps({
            "description": doc.get("remediation_performed", ""),
            "prevention": doc.get("prevention_followups", ""),
            "similar_incidents": doc.get("similar_incidents", ""),
            "plan": state.remediation.get("plan", {}),
            "actions": state.remediation.get("actions", []),
        }, default=str)

        sql_client.update_incident(settings, state.incident_id, {
            "root_cause": doc.get("root_cause"),
            "remediation": remediation_text,
            "status": final_status,
            "documented_at": now_iso,
            "summary": full_summary,
        })

        state.add_message("agent", self.name,
                          f"Documentation persisted — status={final_status}")
        logger.info("[%s] Documentation written to SQL for incident_id=%s",
                    self.name, state.incident_id)

        # If the incident is fully resolved, mark session for closure
        if final_status in ("resolved", "closed"):
            state.status = "closed"
            sql_client.update_incident(
                settings, state.incident_id, {"status": "closed"})
            state.terminate("incident_closed")
            state.add_message("agent", self.name,
                              "Incident closed — session terminating")


def _build_full_summary(doc: dict, state: SessionState) -> str:
    """Build a comprehensive summary string for the SQL summary column."""
    parts = [
        f"Summary: {doc.get('summary', '')}",
        f"What happened: {doc.get('what_happened', '')}",
        f"Root cause: {doc.get('root_cause', '')}",
        f"Remediation: {doc.get('remediation_performed', '')}",
        f"Prevention: {doc.get('prevention_followups', '')}",
    ]
    similar = doc.get("similar_incidents", "")
    if similar and similar.lower() != "none":
        parts.append(f"Similar incidents: {similar}")
    return "\n".join(parts)


def _compact_telemetry(telemetry: dict) -> dict:
    """Produce a size-limited telemetry summary for the LLM context."""
    compact: dict = {}
    alerts = telemetry.get("alerts", [])
    if isinstance(alerts, list):
        compact["alerts"] = alerts[:5]
    metrics = telemetry.get("metrics", {})
    if metrics:
        compact["metrics"] = str(metrics)[:1000]

    def _extract_rows(raw: dict, limit: int) -> list:
        """Extract rows from an App Insights tabular query response."""
        if not raw:
            return []
        tables = raw.get("tables", [])
        if not tables:
            return []
        tbl = tables[0]
        cols = [c["name"] for c in tbl.get("columns", [])]
        return [dict(zip(cols, row)) for row in tbl.get("rows", [])[:limit]]

    exceptions = _extract_rows(telemetry.get("exceptions", {}), 5)
    if exceptions:
        compact["exceptions"] = exceptions

    failed_requests = _extract_rows(telemetry.get("failed_requests", {}), 5)
    if failed_requests:
        compact["failed_requests"] = failed_requests

    traces = _extract_rows(telemetry.get("traces", {}), 10)
    if traces:
        compact["traces"] = traces

    return compact
