"""
Remediation Agent
=================
Uses the Azure OpenAI chat model to propose remediation steps based on
diagnosis results and historical knowledge. Executes remediation through
Azure DevOps (commits, PRs, pipelines) when code/config changes are needed.

No hardcoded playbooks — the LLM determines the appropriate action grounded
on the diagnosis, telemetry evidence, and retrieved similar incidents.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from orchestrator.session import SessionState
from services import azure_openai, devops_client, sql_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

# Maps file extension → language metadata used to guide the LLM
_LANGUAGE_MAP: dict[str, dict] = {
    ".py":    {"name": "Python",           "indent": "4 spaces",  "comment": "#",       "block_comment": '"""'},
    ".js":    {"name": "JavaScript",       "indent": "2 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".ts":    {"name": "TypeScript",       "indent": "2 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".jsx":   {"name": "JavaScript (JSX)", "indent": "2 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".tsx":   {"name": "TypeScript (TSX)", "indent": "2 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".java":  {"name": "Java",             "indent": "4 spaces",  "comment": "//",      "block_comment": "/** */"},
    ".cs":    {"name": "C#",               "indent": "4 spaces",  "comment": "//",      "block_comment": "/// (XML doc)"},
    ".go":    {"name": "Go",               "indent": "tab",       "comment": "//",      "block_comment": "/* */"},
    ".rb":    {"name": "Ruby",             "indent": "2 spaces",  "comment": "#",       "block_comment": "=begin/=end"},
    ".rs":    {"name": "Rust",             "indent": "4 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".cpp":   {"name": "C++",              "indent": "4 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".c":     {"name": "C",                "indent": "4 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".h":     {"name": "C/C++ Header",     "indent": "4 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".yaml":  {"name": "YAML",             "indent": "2 spaces",  "comment": "#",       "block_comment": None},
    ".yml":   {"name": "YAML",             "indent": "2 spaces",  "comment": "#",       "block_comment": None},
    ".json":  {"name": "JSON",             "indent": "2 spaces",  "comment": None,      "block_comment": None},
    ".tf":    {"name": "HCL/Terraform",    "indent": "2 spaces",  "comment": "#",       "block_comment": "/* */"},
    ".bicep": {"name": "Bicep",            "indent": "2 spaces",  "comment": "//",      "block_comment": "/* */"},
    ".sh":    {"name": "Bash",             "indent": "2 spaces",  "comment": "#",       "block_comment": ": '...'"},
    ".bash":  {"name": "Bash",             "indent": "2 spaces",  "comment": "#",       "block_comment": ": '...'"},
    ".sql":   {"name": "SQL",              "indent": "2 spaces",  "comment": "--",      "block_comment": "/* */"},
    ".xml":   {"name": "XML",              "indent": "2 spaces",  "comment": "<!-- -->", "block_comment": "<!-- -->"},
    ".html":  {"name": "HTML",             "indent": "2 spaces",  "comment": "<!-- -->", "block_comment": "<!-- -->"},
    ".css":   {"name": "CSS",              "indent": "2 spaces",  "comment": "/* */",   "block_comment": "/* */"},
}


def _detect_language(file_path: str) -> dict:
    """Return language metadata for *file_path* based on its extension."""
    ext = os.path.splitext(file_path)[1].lower()
    return _LANGUAGE_MAP.get(ext, {"name": "Unknown", "indent": "4 spaces",
                                   "comment": "#", "block_comment": None})


def _detect_actual_conventions(content: str) -> dict:
    """
    Inspect the first 200 lines of *content* to determine the actual indentation
    style and size used in the file, rather than relying solely on the extension default.
    """
    lines = content.splitlines()[:200]
    space_lines = [ln for ln in lines if ln and ln[0] == " "]
    tab_lines = [ln for ln in lines if ln and ln[0] == "\t"]

    if tab_lines and len(tab_lines) > len(space_lines):
        return {"indent_char": "tab", "indent_size": "tab"}

    # Determine the smallest indentation unit among space-indented lines
    sizes = []
    for ln in space_lines:
        n = len(ln) - len(ln.lstrip(" "))
        if n > 0:
            sizes.append(n)
    indent_size = min(sizes) if sizes else 4
    return {"indent_char": "space", "indent_size": f"{indent_size} spaces"}


def _fetch_file_contexts(
    settings,
    file_paths: list[str],
    max_lines_per_file: int = 150,
) -> list[dict]:
    """
    Fetch each file from the repo and return a list of context dicts that
    include the file path, detected language, observed conventions, and the
    file content (truncated to *max_lines_per_file* lines to keep token usage
    manageable while still showing the LLM real code).
    """
    contexts: list[dict] = []
    seen: set[str] = set()

    for raw_path in file_paths:
        path = raw_path.strip()
        if not path or path in seen:
            continue
        seen.add(path)

        content = devops_client.get_repo_file(settings, path)
        if content is None:
            logger.warning(
                "[remediation_agent] Could not fetch '%s' for convention analysis — skipped", path)
            continue

        lang = _detect_language(path)
        conventions = _detect_actual_conventions(content)
        # Override the default indent with what we actually observed
        lang = {**lang, "observed_indent": conventions["indent_size"]}

        truncated = content
        lines = content.splitlines()
        if len(lines) > max_lines_per_file:
            truncated = "\n".join(lines[:max_lines_per_file]) + (
                f"\n# ... [{len(lines) - max_lines_per_file} lines truncated]"
            )

        contexts.append({
            "file_path": path,
            "language": lang,
            "existing_content": truncated,
        })
        logger.info(
            "[remediation_agent] Fetched context for '%s' — language=%s indent=%s lines=%d",
            path, lang["name"], lang["observed_indent"], len(lines),
        )

    return contexts


def _extract_candidate_file_paths(state) -> list[str]:
    """
    Collect file paths from similar historical incidents so we can pre-fetch
    their content for the LLM.  Looks inside knowledge_snippets for any
    ``file_path`` or ``file_paths`` fields set by prior remediations.
    """
    paths: list[str] = []
    for snippet in state.knowledge_snippets or []:
        # Snippets may be dicts (structured) or plain strings
        if not isinstance(snippet, dict):
            continue
        remediation = snippet.get("remediation") or {}
        if isinstance(remediation, str):
            try:
                remediation = json.loads(remediation)
            except (json.JSONDecodeError, TypeError):
                remediation = {}
        for fc in remediation.get("file_changes", []) or []:
            p = fc.get("file_path", "")
            if p:
                paths.append(p)
        # Also accept a flat file_paths list on the snippet itself
        for p in snippet.get("file_paths", []) or []:
            if p:
                paths.append(p)
    return paths


SYSTEM_PROMPT = """\
You are the Remediation Agent in an AIOps incident-management system.

You will receive:
1. A diagnosis result containing root cause, severity, incident type, and confidence level.
2. The original event and telemetry evidence.
3. Similar historical incidents (from Azure AI Search) with their past remediations.

Your tasks:
A) Propose concrete remediation steps. Be specific and actionable.
B) Classify your proposed remediation:
   - "code_change": requires a code or configuration file change committed to the repo.
   - "operational": requires an operational action (restart, scale, rollback, config toggle).
   - "escalate": uncertainty is too high for automated action — recommend manual review.
C) If "code_change", provide SURGICAL PATCHES in `file_changes`. A patch targets ONLY the
   broken lines — it MUST NOT rewrite logic that is unrelated to the fix.
   Each file entry is one of:
   - change_type "patch" (edit existing file): provide `patches` — a list of
     {"old_code", "new_code", "description"} hunks. old_code must be the EXACT verbatim
     text currently in the file (including indentation). new_code is the replacement.
     Keep hunks as small as possible — only the lines that must change.
   - change_type "add" (new file): provide the full `content`.
   The system will fetch the live file from the repo, apply only your hunks, and commit
   the result. It will NOT blindly overwrite the entire file.
D) If a similar historical incident has a validated remediation, prefer reusing it.
E) If uncertainty is high, propose safe, minimal-risk steps first.

LANGUAGE & CONVENTION COMPLIANCE (mandatory for code_change):
When file_contexts are provided in the user message, you MUST study each file's
existing_content before writing any patch:
- IDENTIFY the programming language from the file extension (e.g., .py → Python,
  .ts → TypeScript, .go → Go, .yaml → YAML) and the observed_indent reported in
  the language block.
- REPLICATE the exact indentation style (tabs vs. spaces, number of spaces) already
  used in the file. Never mix tabs and spaces.
- FOLLOW the naming conventions already present in the file (snake_case, camelCase,
  PascalCase, SCREAMING_SNAKE for constants, etc.).
- ADD MEANINGFUL COMMENTS to every new or changed block, explaining WHY the change
  was made (not just what it does). Use the comment syntax of the language:
    Python → # or \"\"\"docstrings\"\"\"
    JS/TS/Java/C#/Go/Rust → // or /** */
    SQL → --
    YAML/Bash/Ruby → #
    HTML/XML → <!-- -->
- Keep comment style consistent with the rest of the file (inline vs. block vs. above-line).
- old_code in patches must be character-perfect, including indentation, so the
  patch engine can locate it verbatim. new_code must use the same indentation level.

IMPORTANT:
- NEVER include unrelated code changes. Fix only what caused the incident.
- Do NOT rewrite functions, classes, or modules that are working correctly.
- old_code must be exact — character-perfect including spaces and newlines.
- Do NOT fabricate pipeline IDs, build IDs, or URLs.
- For code_change, ALWAYS set create_pr to true.

Respond ONLY with valid JSON:
{
  "remediation_type": "code_change" | "operational" | "escalate",
  "steps": ["<step 1>", "<step 2>", ...],
  "description": "<summary of remediation>",
  "file_changes": [
    {
      "file_path": "<repo-relative path, e.g. services/my_service.py>",
      "change_type": "patch",
      "patches": [
        {
          "old_code": "<exact verbatim lines to replace>",
          "new_code": "<replacement lines>",
          "description": "<one-line reason>"
        }
      ]
    },
    {
      "file_path": "<new file path>",
      "change_type": "add",
      "content": "<complete file content for new files only>"
    }
  ] | null,
  "commit_message": "<message>" | null,
  "trigger_pipeline": true | false,
  "pipeline_id": <int> | null,
  "create_pr": true | false,
  "pr_title": "<title>" | null,
  "pr_description": "<description>" | null,
  "confidence": "high" | "medium" | "low",
  "risk_level": "low" | "medium" | "high"
}
"""


class RemediationAgent:
    name: str = "remediation_agent"

    def run(self, state: SessionState) -> None:
        settings = state.settings
        if settings is None:
            state.add_message("agent", self.name,
                              "ERROR: settings not available")
            return

        if not state.is_incident:
            state.add_message("agent", self.name,
                              "Not an incident — skipping remediation")
            logger.info("[%s] Not an incident — skipping", self.name)
            return

        logger.info("[%s] Starting remediation planning for incident_id=%s",
                    self.name, state.incident_id)
        state.add_message(
            "agent", self.name, "Planning remediation based on diagnosis and historical knowledge")

        # Pre-fetch file contexts from the repo so the LLM can read real code
        # before proposing patches — enables language detection, convention
        # analysis, meaningful comments, and correct indentation.
        candidate_paths = _extract_candidate_file_paths(state)
        file_contexts = _fetch_file_contexts(
            settings, candidate_paths) if candidate_paths else []
        if file_contexts:
            logger.info(
                "[%s] Providing %d file context(s) to LLM: %s",
                self.name,
                len(file_contexts),
                [fc["file_path"] for fc in file_contexts],
            )

        # Build context for the LLM
        context: dict = {
            "diagnosis": state.diagnosis,
            "event": state.event_data,
            "telemetry_summary": _summarize_telemetry(state.telemetry),
            "similar_historical_incidents": state.knowledge_snippets,
        }
        if file_contexts:
            # Include live file content so the LLM can analyse language and conventions
            context["file_contexts"] = file_contexts

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(context, default=str)},
        ]

        plan = azure_openai.chat_completion_json(settings, messages)
        logger.info("[%s] Remediation plan: type=%s confidence=%s", self.name, plan.get(
            "remediation_type"), plan.get("confidence"))

        state.add_message("agent", self.name,
                          f"Remediation plan: {json.dumps(plan, default=str)}")

        now_iso = datetime.now(timezone.utc).isoformat()
        remediation_result: dict = {
            "remediated": False,
            "remediated_at": now_iso,
            "plan": plan,
            "actions": [],
        }

        rem_type = plan.get("remediation_type", "escalate")

        # Execute DevOps actions for code_change
        if rem_type == "code_change" and plan.get("file_changes"):
            remediation_result = _execute_code_change(
                settings, state, plan, remediation_result)

        elif rem_type == "operational":
            # Log operational steps — actual execution depends on existing automation hooks
            remediation_result["remediated"] = True
            remediation_result["actions"].append({
                "type": "operational",
                "steps": plan.get("steps", []),
                "description": plan.get("description", ""),
            })

        else:
            # Escalation path
            remediation_result["actions"].append({
                "type": "escalate",
                "steps": plan.get("steps", []),
                "description": plan.get("description", "Escalated for manual review"),
            })

        state.remediation = remediation_result

        # Update SQL record
        devops_log = remediation_result.get("devops_log", "")
        file_paths = remediation_result.get("file_paths", [])
        diagnosis = state.diagnosis or {}
        summary = (
            f"{diagnosis.get('explanation', '')} | "
            f"Error: {diagnosis.get('error_message', 'N/A')} | "
            f"Operation: {diagnosis.get('affected_operation', 'N/A')}"
        )
        if file_paths:
            summary += f" | Files changed: {', '.join(file_paths)}"
        sql_client.update_incident(settings, state.incident_id, {
            "remediation": json.dumps({
                "type": rem_type,
                "description": plan.get("description", ""),
                "steps": plan.get("steps", []),
                "confidence": plan.get("confidence", ""),
                "risk_level": plan.get("risk_level", ""),
            }),
            "devops_commit": devops_log[:500] if devops_log else None,
            "summary": summary,
            # For code_change the fix lands on a feature branch and requires a
            # developer to review and approve the PR before it reaches main.
            "status": "sent_for_human_review" if rem_type == "code_change" else "mitigating",
            "remediated_at": now_iso if remediation_result["remediated"] else None,
        })

        state.status = "sent_for_human_review" if rem_type == "code_change" else "mitigating"
        action_summary = (
            "PR opened — awaiting developer review"
            if rem_type == "code_change"
            else ("executed" if remediation_result["remediated"] else "proposed")
        )
        state.add_message(
            "agent",
            self.name,
            f"Remediation {action_summary} — "
            f"type={rem_type} actions={len(remediation_result['actions'])}",
        )
        logger.info("[%s] Remediation complete — remediated=%s",
                    self.name, remediation_result["remediated"])


def _execute_code_change(settings, state: SessionState, plan: dict, result: dict) -> dict:
    """
    Push surgical code patches to a new branch in Azure DevOps and open a
    pull request against the base branch.

    The pipeline stops here — no automatic merge is performed.  A developer
    must review and approve the PR before the fix reaches the main branch.
    """
    devops_actions: dict = {}
    devops_log_lines: list[str] = []

    commit_msg = plan.get(
        "commit_message", f"[AIOps] Remediation for incident {state.incident_id}")
    file_changes = plan["file_changes"]
    file_paths = [fc.get("file_path", "") for fc in file_changes]

    logger.info("[remediation_agent] incident=%s — pushing code changes to DevOps — files: %s",
                state.incident_id, ", ".join(file_paths))

    push_result = devops_client.push_code_changes(
        settings,
        state.incident_id,
        file_changes,
        commit_msg,
    )
    devops_actions["push"] = push_result
    result["actions"].append({"type": "commit", "result": push_result})

    if not push_result.get("success"):
        devops_log_lines.append(
            f"FAILED branch push — files: {', '.join(file_paths)} — "
            f"reason: {push_result.get('reason', '')} {push_result.get('error', '')}")
        logger.error("[remediation_agent] FAILED — branch push — incident=%s reason=%s error=%s",
                     state.incident_id, push_result.get("reason", ""), push_result.get("error", ""))
        result["devops_log"] = " | ".join(devops_log_lines)
        result["devops_actions"] = devops_actions
        return result

    result["remediated"] = True
    branch = push_result.get("branch", "")
    commit_id = push_result.get("commit_id", "")
    devops_log_lines.append(
        f"Branch '{branch}' created | files changed: {', '.join(file_paths)} | commit: {commit_id}")

    # Create PR
    pr_result = devops_client.create_pull_request(
        settings,
        source_branch=branch,
        title=plan.get(
            "pr_title", f"[AIOps] Remediation for {state.incident_id}"),
        description=plan.get("pr_description", ""),
    )
    devops_actions["pull_request"] = pr_result
    result["actions"].append({"type": "pull_request", "result": pr_result})

    if not pr_result.get("success"):
        devops_log_lines.append(
            f"FAILED PR creation — source='{branch}' — "
            f"reason: {pr_result.get('reason', '')} {pr_result.get('error', '')}")
        logger.error("[remediation_agent] FAILED — PR creation — incident=%s branch=%s reason=%s",
                     state.incident_id, branch, pr_result.get("reason", ""))
    else:
        pr_id = pr_result.get("pr_id")
        pr_url = pr_result.get("url", "")
        devops_log_lines.append(
            f"PR #{pr_id} open — '{branch}' → '{settings.azdo_branch}' "
            f"| awaiting developer review and approval"
            + (f" | {pr_url}" if pr_url else ""))
        logger.info(
            "[remediation_agent] PR #%s created — branch '%s' → '%s' — "
            "awaiting developer review (no auto-merge)",
            pr_id, branch, settings.azdo_branch,
        )

    # Trigger pipeline if requested and a pipeline ID is provided
    if plan.get("trigger_pipeline") and plan.get("pipeline_id"):
        pipeline_result = devops_client.trigger_pipeline(
            settings,
            pipeline_id=int(plan["pipeline_id"]),
        )
        devops_actions["pipeline"] = pipeline_result
        result["actions"].append(
            {"type": "pipeline", "result": pipeline_result})
        if pipeline_result.get("success"):
            devops_log_lines.append(
                f"Pipeline #{plan['pipeline_id']} triggered — run_id: {pipeline_result.get('run_id')}")
        else:
            devops_log_lines.append(
                f"FAILED pipeline trigger — pipeline_id: {plan['pipeline_id']} — "
                f"reason: {pipeline_result.get('reason', '')}")

    result["devops_log"] = " | ".join(devops_log_lines)
    result["file_paths"] = file_paths
    result["devops_actions"] = devops_actions
    return result


def _summarize_telemetry(telemetry: dict) -> dict:
    """Produce a compact summary of telemetry for the LLM prompt."""
    summary: dict = {}
    alerts = telemetry.get("alerts", [])
    if alerts:
        summary["alert_count"] = len(alerts)
        summary["alert_names"] = [
            a.get("name", "") for a in alerts[:5]] if isinstance(alerts, list) else []
    if telemetry.get("metrics"):
        summary["metrics_present"] = True

    def _row_count(raw: dict) -> int:
        if not raw:
            return 0
        tables = raw.get("tables", [])
        return len(tables[0].get("rows", [])) if tables else 0

    exc_count = _row_count(telemetry.get("exceptions", {}))
    if exc_count:
        summary["exception_count"] = exc_count

    req_count = _row_count(telemetry.get("failed_requests", {}))
    if req_count:
        summary["failed_request_count"] = req_count

    trace_count = _row_count(telemetry.get("traces", {}))
    if trace_count:
        summary["warning_error_trace_count"] = trace_count

    return summary
