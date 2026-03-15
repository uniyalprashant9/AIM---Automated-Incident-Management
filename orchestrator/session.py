"""
Orchestration Session
=====================
Manages a multi-agent group-chat session per Azure Monitor alert event.

Key design:
- Agents are passed as a list — adding/removing agents requires no orchestration changes.
- Shared ``SessionState`` carries event data, knowledge, SQL references, actions, and messages.
- Termination conditions (normal-log exit, resolved/closed, max iterations) are enforced centrally.
- Idempotency: checks for duplicate / in-flight incidents before starting.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from services.config import Settings
from services import sql_client

logger = logging.getLogger(__name__)


# ── Shared session state ─────────────────────────────────────────────────

@dataclass
class SessionState:
    """Mutable state shared across all agents inside one session."""

    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = ""

    # Alert event payload (normalised from Azure Monitor Common Alert Schema)
    event_data: dict = field(default_factory=dict)

    # Configuration
    settings: Settings | None = None

    # Incident identity
    incident_id: str = ""
    sql_record_id: str | None = None

    # Classification
    is_incident: bool | None = None  # None = not yet classified

    # Collected data
    telemetry: dict = field(default_factory=dict)
    knowledge_snippets: list[dict] = field(default_factory=list)

    # Agent outputs
    diagnosis: dict = field(default_factory=dict)
    remediation: dict = field(default_factory=dict)
    documentation: dict = field(default_factory=dict)

    # Lifecycle
    status: str = "open"  # open → investigating → mitigating → resolved → closed
    actions_taken: list[dict] = field(default_factory=list)

    # Group-chat message log (agent-to-agent communication)
    messages: list[dict] = field(default_factory=list)

    # Termination
    terminated: bool = False
    termination_reason: str = ""

    # Timing
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ended_at: str = ""

    def add_message(self, role: str, agent_name: str, content: str) -> None:
        """Append a structured message to the group-chat log."""
        self.messages.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "role": role,
            "agent": agent_name,
            "content": content,
        })

    def terminate(self, reason: str) -> None:
        self.terminated = True
        self.termination_reason = reason
        self.ended_at = datetime.now(timezone.utc).isoformat()


# ── Agent protocol ───────────────────────────────────────────────────────

class Agent(Protocol):
    """Every agent must expose ``name`` and ``run``."""

    name: str

    def run(self, state: SessionState) -> None:
        """Execute agent logic, mutating *state* in place."""
        ...


# ── Orchestrator ─────────────────────────────────────────────────────────

def run_session(
    agents: list[Agent],
    event_data: dict,
    settings: Settings,
) -> SessionState:
    """
    Run a multi-agent session for one alert event.

    Parameters
    ----------
    agents : list[Agent]
        Ordered list of agent instances.  The orchestrator iterates through
        them sequentially (default safe flow) but respects early termination.
    event_data : dict
        Normalised alert event payload (from Azure Monitor Common Alert Schema
        or fallback plain JSON body).
    settings : Settings
        Application configuration.

    Returns
    -------
    SessionState
        Final session state after all agents have run (or early termination).
    """
    incident_id = event_data.get("id", str(uuid.uuid4()))
    state = SessionState(
        correlation_id=incident_id,
        event_data=event_data,
        settings=settings,
        incident_id=incident_id,
    )

    logger.info("[session=%s] Starting multi-agent session for incident_id=%s",
                state.session_id, incident_id)
    state.add_message("system", "orchestrator",
                      f"Session started for event {incident_id}")

    # ── Idempotency / dedup check ────────────────────────────────────
    existing = sql_client.find_active_incident(
        settings, incident_id, settings.dedup_window_minutes)
    if existing:
        existing_status = str(existing.get("status", "")).lower()
        if existing_status in ("resolved", "closed"):
            state.terminate(
                f"Duplicate event — incident already {existing_status}")
            state.sql_record_id = str(existing.get("id", ""))
            state.status = existing_status
            logger.info("[session=%s] Duplicate detected (already %s) — stopping",
                        state.session_id, existing_status)
            return state
        if existing_status in ("open", "investigating", "mitigating"):
            state.terminate(
                f"In-flight session exists (status={existing_status}) — skipping to avoid conflict")
            state.sql_record_id = str(existing.get("id", ""))
            state.status = existing_status
            logger.info(
                "[session=%s] In-flight incident detected — deferring", state.session_id)
            return state

    # ── Run agents ───────────────────────────────────────────────────
    start_time = time.monotonic()
    iteration = 0

    for agent in agents:
        if state.terminated:
            logger.info("[session=%s] Session terminated (%s) — skipping remaining agents",
                        state.session_id, state.termination_reason)
            break

        elapsed = time.monotonic() - start_time
        if elapsed > settings.session_timeout_seconds:
            _timeout_exit(state, settings)
            break

        iteration += 1
        if iteration > settings.max_agent_iterations:
            _timeout_exit(state, settings)
            break

        logger.info("[session=%s] Running agent: %s (iteration %d)",
                    state.session_id, agent.name, iteration)
        state.add_message("system", "orchestrator",
                          f"Invoking agent: {agent.name}")

        try:
            agent.run(state)
        except Exception as exc:
            logger.exception(
                "[session=%s] Agent %s raised an exception | error_type=%s | incident_id=%s | detail: %s",
                state.session_id, agent.name, type(exc).__name__, state.incident_id, exc)
            state.add_message("system", "orchestrator",
                              f"Agent {agent.name} failed: {type(exc).__name__}: {exc}")
            # Continue to next agent rather than crashing the whole session
            continue

    if not state.terminated:
        state.terminate("all_agents_completed")

    logger.info(
        "[session=%s] Session ended — status=%s reason=%s duration=%.1fs",
        state.session_id,
        state.status,
        state.termination_reason,
        time.monotonic() - start_time,
    )
    return state


def _timeout_exit(state: SessionState, settings: Settings) -> None:
    """Handle timeout / max-iteration exit gracefully."""
    state.status = "needs_human_review"
    state.terminate("max_iterations_or_timeout")
    state.add_message("system", "orchestrator",
                      "Session timed out — flagged for human review")
    sql_client.update_incident(settings, state.incident_id, {
                               "status": "needs_human_review"})
    logger.warning(
        "[session=%s] Timeout/max-iteration reached — flagged for human review", state.session_id)
