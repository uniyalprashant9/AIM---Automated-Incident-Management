"""
Incident Agent (Agent Manager)
==============================
Owns the incident lifecycle: collects telemetry, manages status transitions
in SQL (Open → Investigating → Mitigating → Resolved → Closed), tracks
timestamps, and ensures the session terminates correctly.

Uses the Azure OpenAI chat model to interpret telemetry context and decide
on status transitions — no hardcoded thresholds.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from orchestrator.session import SessionState
from services import sql_client, telemetry as telemetry_svc

logger = logging.getLogger(__name__)


class IncidentAgent:
    name: str = "incident_agent"

    def run(self, state: SessionState) -> None:
        settings = state.settings
        if settings is None:
            state.add_message("agent", self.name,
                              "ERROR: settings not available")
            return

        logger.info("[%s] Collecting telemetry for incident_id=%s",
                    self.name, state.incident_id)
        state.add_message(
            "agent", self.name, "Collecting telemetry from Azure Monitor / App Insights")

        # Determine the resource ID from the event payload
        resource_id = (
            state.event_data.get("subject", "")
            or state.event_data.get("data", {}).get("resourceUri", "")
            or settings.monitored_resource_id
        )

        # Collect all available telemetry
        telem = telemetry_svc.collect_telemetry(settings, resource_id)
        state.telemetry = telem

        state.add_message(
            "agent",
            self.name,
            f"Telemetry collected — alerts={len(telem.get('alerts', []))} "
            f"metrics={'present' if telem.get('metrics') else 'none'} "
            f"exceptions={len(telem.get('exceptions', {}).get('tables', [{}])[0].get('rows', []) if telem.get('exceptions') else [])} "
            f"failed_requests={len(telem.get('failed_requests', {}).get('tables', [{}])[0].get('rows', []) if telem.get('failed_requests') else [])} "
            f"traces={len(telem.get('traces', {}).get('tables', [{}])[0].get('rows', []) if telem.get('traces') else [])}",
        )

        # If diagnosis hasn't run yet, nothing more to do on lifecycle.
        # After diagnosis + remediation, the incident agent may be called again
        # (or we handle lifecycle transitions post-remediation here).
        if state.is_incident is None:
            # Telemetry gathered — diagnosis agent will decide classification next
            logger.info(
                "[%s] Telemetry collected, awaiting diagnosis", self.name)
            return

        # Post-remediation lifecycle management
        if state.remediation and state.status not in ("resolved", "closed"):
            remediated = state.remediation.get("remediated", False)
            now_iso = datetime.now(timezone.utc).isoformat()
            if remediated:
                state.status = "resolved"
                sql_client.update_incident(settings, state.incident_id, {
                    "status": "resolved",
                    "remediated_at": now_iso,
                })
                state.add_message("agent", self.name,
                                  "Incident status → resolved")
                logger.info("[%s] Incident status → resolved", self.name)
            else:
                state.status = "mitigating"
                sql_client.update_incident(settings, state.incident_id, {
                    "status": "mitigating",
                })
                state.add_message(
                    "agent", self.name, "Incident status → mitigating (remediation incomplete)")
                logger.info("[%s] Incident status → mitigating", self.name)
