import azure.functions as func
import json
import logging

from agents.incident_agent import IncidentAgent
from agents.diagnosis_agent import DiagnosisAgent
from agents.remediation_agent import RemediationAgent
from agents.documentation_agent import DocumentationAgent
from orchestrator.session import run_session
from services.config import load_settings

app = func.FunctionApp()
logger = logging.getLogger(__name__)


@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="%ServiceBusQueueName%",
    connection="AzureWebJobsServiceBus",
)
def sb_process_incident(msg: func.ServiceBusMessage) -> None:
    """
    Entry point: Service Bus queue trigger → multi-agent orchestrated session.

    Message body — Azure Monitor Action Group (Common Alert Schema):
        {
            "schemaId": "azureMonitorCommonAlertSchema",
            "data": {
                "essentials": {
                    "alertId": "...",
                    "alertRule": "...",
                    "severity": "Sev2",
                    "signalType": "Metric",
                    "monitorCondition": "Fired",
                    "alertTargetIDs": ["<resource-id>"],
                    "firedDateTime": "..."
                },
                "alertContext": { ... }
            }
        }

    Default agent flow:
        Incident Agent → Diagnosis Agent → Remediation Agent → Documentation Agent
    """
    logger.info("========== AIOps Pipeline START ==========")

    raw_body = msg.get_body().decode("utf-8", errors="replace")
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.error(
            "Failed to parse Service Bus message body as JSON — preview=%.200s", raw_body[:200])
        raise

    logger.debug("Raw message body: %.500s", raw_body[:500])

    # Parse Azure Monitor Action Group Common Alert Schema
    if body.get("schemaId") == "azureMonitorCommonAlertSchema":
        essentials = body.get("data", {}).get("essentials", {})
        targets = essentials.get("alertTargetIDs", [])
        resource_id = targets[0] if targets else ""
        event_data = {
            "id": essentials.get("originAlertId", essentials.get("alertId", "")),
            "subject": resource_id,
            "eventType": "Microsoft.AlertsManagement.AlertFired",
            "eventTime": essentials.get("firedDateTime", ""),
            "data": {
                "resourceUri": resource_id,
                "alertRule": essentials.get("alertRule", ""),
                "severity": essentials.get("severity", ""),
                "signalType": essentials.get("signalType", ""),
                "monitorCondition": essentials.get("monitorCondition", ""),
                "alertContext": body.get("data", {}).get("alertContext", {}),
            },
        }
    else:
        # Fallback: plain object or Event Grid array
        raw = body[0] if isinstance(body, list) and body else body
        event_data = {
            "id": raw.get("id", ""),
            "subject": raw.get("subject", ""),
            "eventType": raw.get("eventType", ""),
            "eventTime": raw.get("eventTime", ""),
            "data": raw.get("data", {}),
        }

    logger.info(
        "Event received: id=%s type=%s subject=%s",
        event_data["id"],
        event_data["eventType"],
        event_data["subject"],
    )

    settings = load_settings()

    # Assemble the agent list (order = default safe flow)
    agents = [
        IncidentAgent(),      # Collects telemetry, manages lifecycle
        DiagnosisAgent(),     # Classifies signal, RAG retrieval, root cause
        RemediationAgent(),   # Plans + executes remediation via DevOps
        DocumentationAgent(),  # Compiles report, updates SQL, closes incident
    ]

    # Run the orchestrated multi-agent session
    state = run_session(
        agents=agents, event_data=event_data, settings=settings)

    logger.info(
        "Pipeline summary — incident=%s status=%s reason=%s session=%s",
        state.incident_id,
        state.status,
        state.termination_reason,
        state.session_id,
    )
    logger.info("========== AIOps Pipeline END ==========")

    logger.info(
        "Pipeline response — incident=%s status=%s session=%s reason=%s",
        state.incident_id,
        state.status,
        state.session_id,
        state.termination_reason,
    )
