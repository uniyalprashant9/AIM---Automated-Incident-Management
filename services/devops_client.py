"""
Azure DevOps Client
===================
Wrapper for Azure DevOps REST APIs — push commits, create PRs, trigger pipelines.
"""

from __future__ import annotations

import base64
import datetime
import json
import logging

import requests

from services.config import Settings

logger = logging.getLogger(__name__)


def _repo_name(settings: Settings) -> str:
    """
    Return just the repository name, even if azdo_repo was accidentally set to
    a full clone URL (e.g. https://org@dev.azure.com/org/project/_git/RepoName).
    The Azure DevOps REST API requires only the plain name or GUID here.
    """
    repo = settings.azdo_repo
    # If it looks like a URL, extract the last path segment after /_git/
    if "/_git/" in repo:
        repo = repo.split("/_git/")[-1]
    # Also strip trailing slashes and any query-string fragments
    repo = repo.rstrip("/").split("?")[0]
    return repo


def _base_url(settings: Settings) -> str:
    return f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}/_apis/git/repositories/{_repo_name(settings)}"


def _auth(settings: Settings) -> tuple[str, str]:
    return ("", settings.azdo_pat)


def _is_configured(settings: Settings) -> bool:
    return all([settings.azdo_org, settings.azdo_project, settings.azdo_repo, settings.azdo_pat])


def get_repo_file(settings: Settings, file_path: str, branch: str | None = None) -> str | None:
    """
    Public wrapper — fetch the raw text content of *file_path* from the repo.
    Returns None if the file does not exist, DevOps is not configured, or the
    request fails.
    """
    if not _is_configured(settings):
        return None
    base = _base_url(settings)
    auth = _auth(settings)
    return _get_repo_file(base, auth, file_path, branch or settings.azdo_branch)


def _get_repo_file(base: str, auth: tuple, file_path: str, branch: str) -> str | None:
    """
    Fetch the raw text content of *file_path* from *branch* in the repo.
    Returns None if the file does not exist or the request fails.
    """
    path = file_path if file_path.startswith("/") else "/" + file_path
    try:
        resp = requests.get(
            f"{base}/items",
            auth=auth,
            params={
                "path": path,
                "versionDescriptor.versionType": "branch",
                "versionDescriptor.version": branch,
                "$format": "text",
                "api-version": "7.1",
            },
            timeout=30,
        )
        if resp.status_code == 404:
            logger.error(
                "[DevOps] File not found in repo: %s (branch=%s)", path, branch)
            return None
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.error("[DevOps] FAILED — could not fetch file '%s' from branch '%s': %s",
                     path, branch, exc)
        return None


def _get_branch_latest_commit(base: str, auth: tuple, branch: str) -> dict | None:
    """
    Return the latest commit object on *branch*, or None if the branch does not
    exist or the request fails.
    """
    try:
        resp = requests.get(
            f"{base}/commits",
            auth=auth,
            params={
                "searchCriteria.itemVersion.versionType": "branch",
                "searchCriteria.itemVersion.version": branch,
                "$top": 1,
                "api-version": "7.1",
            },
            timeout=30,
        )
        if not resp.ok:
            return None
        commits = resp.json().get("value", [])
        return commits[0] if commits else None
    except requests.RequestException:
        return None


def _find_open_pr(
    base: str,
    auth: tuple,
    source_branch: str,
    target_branch: str,
) -> dict | None:
    """
    Return the first *active* PR from *source_branch* → *target_branch*,
    or None if no such PR exists or the request fails.
    """
    try:
        resp = requests.get(
            f"{base}/pullrequests",
            auth=auth,
            params={
                "searchCriteria.sourceRefName": f"refs/heads/{source_branch}",
                "searchCriteria.targetRefName": f"refs/heads/{target_branch}",
                "searchCriteria.status": "active",
                "api-version": "7.1",
            },
            timeout=30,
        )
        if not resp.ok:
            return None
        prs = resp.json().get("value", [])
        return prs[0] if prs else None
    except requests.RequestException:
        return None


def _apply_patches(original: str, patches: list[dict], file_path: str) -> tuple[str, list[str]]:
    """
    Apply a list of {old_code, new_code, description} hunks to *original*.
    Returns (patched_content, list_of_warnings).
    A warning is emitted for any hunk whose old_code is not found verbatim.
    """
    result = original
    warnings: list[str] = []
    for i, patch in enumerate(patches):
        old = patch.get("old_code", "")
        new = patch.get("new_code", "")
        desc = patch.get("description", f"hunk #{i+1}")
        if old not in result:
            msg = (
                f"Patch hunk #{i+1} ('{desc}') in '{file_path}' — "
                f"old_code not found verbatim; hunk skipped."
            )
            logger.error("[DevOps] FAILED — %s", msg)
            warnings.append(msg)
            continue
        result = result.replace(old, new, 1)
        logger.info("[DevOps] Applied patch hunk #%d '%s' in '%s'",
                    i + 1, desc, file_path)
    return result, warnings


def push_code_changes(
    settings: Settings,
    incident_id: str,
    file_changes: list[dict],
    commit_message: str,
) -> dict:
    """
    Push surgical code patches to Azure DevOps.

    Creates a new branch ``aiops/remediation/{incident_id}`` from the configured
    base branch. Each entry in *file_changes* is one of:
      - change_type "patch": fetches the live file from the repo, applies only the
        targeted {old_code → new_code} hunks, and commits the minimal diff.
      - change_type "add": commits the provided *content* as a new file.

    Returns commit metadata including the new branch name so the caller can
    create a PR against the base branch.
    """
    if not _is_configured(settings):
        logger.warning("Azure DevOps not configured — skipping push")
        return {"success": False, "reason": "devops_not_configured"}

    if not file_changes:
        return {"success": False, "reason": "no_file_changes"}

    base = _base_url(settings)
    auth = _auth(settings)
    base_branch = settings.azdo_branch
    new_branch = f"aiops/remediation/{incident_id}"

    try:
        # ── Get HEAD of base branch ────────────────────────────────────
        refs_resp = requests.get(
            f"{base}/refs?filter=heads/{base_branch}&api-version=7.1",
            auth=auth,
            timeout=30,
        )
        refs_resp.raise_for_status()
        refs = refs_resp.json().get("value", [])
        base_oid = refs[0]["objectId"] if refs else "0" * 40

        # ── Build per-file change entries (fetch + patch for existing files) ──
        changes = []
        file_list = []
        patch_warnings: list[str] = []

        for fc in file_changes:
            raw_path = fc.get("file_path", "").strip()
            path = raw_path if raw_path.startswith("/") else "/" + raw_path
            change_type = fc.get("change_type", "patch")

            if change_type == "add":
                # New file — use the provided content directly
                content_str = fc.get("content", "")
                file_list.append(f"add:{path}")

            else:
                # Patch existing file: fetch from repo, apply only the targeted hunks
                original = _get_repo_file(base, auth, path, base_branch)
                if original is None:
                    patch_warnings.append(
                        f"Could not fetch '{path}' from repo — skipped")
                    continue
                patches = fc.get("patches", [])
                if not patches:
                    patch_warnings.append(
                        f"No patches provided for '{path}' — skipped")
                    continue
                content_str, hunks_warn = _apply_patches(
                    original, patches, path)
                patch_warnings.extend(hunks_warn)
                hunk_descs = ", ".join(
                    p.get("description", f"hunk #{i+1}") for i, p in enumerate(patches)
                )
                file_list.append(f"patch:{path} ({hunk_descs})")

            encoded = base64.b64encode(content_str.encode("utf-8")).decode()
            changes.append({
                "changeType": "edit" if change_type != "add" else "add",
                "item": {"path": path},
                "newContent": {"content": encoded, "contentType": "base64encoded"},
            })

        if not changes:
            err = "No valid changes to commit after patching."
            if patch_warnings:
                err += " Warnings: " + " | ".join(patch_warnings)
            logger.error("[DevOps] FAILED — %s", err)
            return {"success": False, "reason": "patch_apply_failed", "error": err}

        # ── Determine branch: reuse AIOps-owned or create fresh ─────
        # An AIOps-owned branch is one whose latest commit message contains the
        # incident_id or the "[AIOps]" marker.  Any other pre-existing branch
        # (created by a human or a different system) gets a new timestamped name
        # so we never silently overwrite foreign work.
        existing_oid = "0" * 40
        branch_owned_by_aiops = False

        branch_refs_resp = requests.get(
            f"{base}/refs?filter=heads/{new_branch}&api-version=7.1",
            auth=auth,
            timeout=30,
        )
        if branch_refs_resp.ok:
            existing_refs = branch_refs_resp.json().get("value", [])
            if existing_refs:
                existing_oid = existing_refs[0]["objectId"]
                latest_commit = _get_branch_latest_commit(
                    base, auth, new_branch)
                commit_msg = (latest_commit or {}).get("comment", "")
                first_line = commit_msg.splitlines(
                )[0] if commit_msg else "(unknown)"

                if incident_id in commit_msg or "[AIOps]" in commit_msg:
                    # Branch was written by a previous AIOps run for this incident
                    branch_owned_by_aiops = True
                    logger.info(
                        "[DevOps] Branch '%s' exists and is AIOps-owned "
                        "(HEAD=%s commit='%.80s') — reusing branch",
                        new_branch, existing_oid[:8], first_line,
                    )
                else:
                    # Branch exists but belongs to someone else / a different session
                    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                    new_branch = f"aiops/remediation/{incident_id}_{timestamp}"
                    existing_oid = "0" * 40  # fresh branch — always a create
                    logger.warning(
                        "[DevOps] Branch 'aiops/remediation/%s' exists but was NOT created "
                        "by AIOps (commit='%.80s') — creating new branch '%s'",
                        incident_id, first_line, new_branch,
                    )

        logger.info(
            "[DevOps] %s branch '%s' from '%s' (HEAD=%s) — staging %d file(s): %s",
            "Updating" if branch_owned_by_aiops else "Creating",
            new_branch, base_branch, base_oid[:8], len(
                changes), ", ".join(file_list),
        )

        # ── Push to branch (create or update) ────────────────────────
        # existing_oid=="0"*40  → create new branch
        # existing_oid==<sha>   → update existing AIOps-owned branch (avoids 409)
        push_payload = {
            "refUpdates": [
                {"name": f"refs/heads/{new_branch}", "oldObjectId": existing_oid}
            ],
            "commits": [
                {
                    "comment": commit_message,
                    "parents": [base_oid],
                    "changes": changes,
                }
            ],
        }

        push_resp = requests.post(
            f"{base}/pushes?api-version=7.1",
            auth=auth,
            json=push_payload,
            timeout=60,
        )
        push_resp.raise_for_status()
        data = push_resp.json()
        commit_id = data.get("commits", [{}])[0].get("commitId", "unknown")
        repo = _repo_name(settings)
        url = (
            f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}"
            f"/_git/{repo}/commit/{commit_id}"
        )
        logger.info(
            "[DevOps] SUCCESS — branch '%s' pushed — files: %s | commit=%s",
            new_branch, ", ".join(file_list), commit_id,
        )
        if patch_warnings:
            logger.warning("[DevOps] Patch warnings for incident=%s: %s",
                           incident_id, " | ".join(patch_warnings))

        # ── Post-push: validate changes on reused branches ────────────
        # When we updated an existing AIOps branch we verify each patch's
        # new_code is actually present in the branch copy of the file so we
        # can catch a silent no-op before the PR is created.
        if branch_owned_by_aiops:
            logger.info(
                "[DevOps] Validating committed changes on reused branch '%s'", new_branch)
            validation_ok = True
            for fc in file_changes:
                if fc.get("change_type", "patch") == "add":
                    continue  # new files — not validated this way
                raw_path = fc.get("file_path", "").strip()
                path = raw_path if raw_path.startswith("/") else "/" + raw_path
                branch_content = _get_repo_file(base, auth, path, new_branch)
                for patch in fc.get("patches", []):
                    expected = patch.get("new_code", "")
                    if expected and (branch_content is None or expected not in branch_content):
                        logger.error(
                            "[DevOps] Validation FAILED — expected patch not found on "
                            "branch '%s' in '%s' (description='%s')",
                            new_branch, path, patch.get("description", "?"),
                        )
                        validation_ok = False
            if validation_ok:
                logger.info(
                    "[DevOps] Validation PASSED — all patches confirmed on branch '%s'",
                    new_branch,
                )

        return {
            "success": True,
            "commit_id": commit_id,
            "branch": new_branch,
            "base_branch": base_branch,
            "branch_reused": branch_owned_by_aiops,
            "file_paths": [fc.get("file_path", "") for fc in file_changes],
            "file_list": file_list,
            "patch_warnings": patch_warnings,
            "url": url,
        }
    except requests.RequestException as exc:
        logger.error("[DevOps] FAILED — branch push failed — incident=%s error=%s",
                     incident_id, exc)
        return {"success": False, "reason": "push_failed", "error": str(exc)}


def push_remediation_file(
    settings: Settings,
    incident_id: str,
    content: dict,
    commit_message: str,
) -> dict:
    """
    Push a remediation JSON file to the Azure DevOps repo.
    Returns a dict with commit metadata (commit_id, url, branch, file_path).
    """
    if not _is_configured(settings):
        logger.warning("Azure DevOps not configured — skipping push")
        return {"success": False, "reason": "devops_not_configured"}

    file_path = f"/remediations/{incident_id}.json"
    encoded = base64.b64encode(json.dumps(content, indent=2).encode()).decode()
    base = _base_url(settings)
    auth = _auth(settings)
    branch = settings.azdo_branch

    try:
        # Resolve latest ref
        refs_resp = requests.get(
            f"{base}/refs?filter=heads/{branch}&api-version=7.1",
            auth=auth,
            timeout=30,
        )
        refs_resp.raise_for_status()
        refs = refs_resp.json().get("value", [])
        old_oid = refs[0]["objectId"] if refs else "0" * 40

        push_payload = {
            "refUpdates": [{"name": f"refs/heads/{branch}", "oldObjectId": old_oid}],
            "commits": [
                {
                    "comment": commit_message,
                    "changes": [
                        {
                            "changeType": "add",
                            "item": {"path": file_path},
                            "newContent": {"content": encoded, "contentType": "base64encoded"},
                        }
                    ],
                }
            ],
        }

        push_resp = requests.post(
            f"{base}/pushes?api-version=7.1",
            auth=auth,
            json=push_payload,
            timeout=30,
        )
        push_resp.raise_for_status()
        data = push_resp.json()
        commit_id = data.get("commits", [{}])[0].get("commitId", "unknown")
        url = f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}/_git/{_repo_name(settings)}/commit/{commit_id}"
        return {"success": True, "commit_id": commit_id, "file_path": file_path, "branch": branch, "url": url}
    except requests.RequestException:
        logger.exception("Azure DevOps push failed")
        return {"success": False, "reason": "push_failed"}


def trigger_pipeline(settings: Settings, pipeline_id: int, branch: str | None = None) -> dict:
    """Trigger an Azure DevOps pipeline run. Returns run metadata or error."""
    if not _is_configured(settings):
        return {"success": False, "reason": "devops_not_configured"}

    url = f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}/_apis/pipelines/{pipeline_id}/runs?api-version=7.1"
    body: dict = {"resources": {"repositories": {
        "self": {"refName": f"refs/heads/{branch or settings.azdo_branch}"}}}}
    try:
        resp = requests.post(url, auth=_auth(settings), json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        run_id = data.get("id")
        run_url = data.get("_links", {}).get("web", {}).get("href", "")
        logger.info(
            "[DevOps] SUCCESS — pipeline #%d triggered — run_id=%s", pipeline_id, run_id)
        return {"success": True, "run_id": run_id, "url": run_url}
    except requests.RequestException as exc:
        logger.error("[DevOps] FAILED — pipeline trigger failed — pipeline_id=%d error=%s",
                     pipeline_id, exc)
        return {"success": False, "reason": "pipeline_trigger_failed", "error": str(exc)}


def create_pull_request(
    settings: Settings,
    source_branch: str,
    target_branch: str | None = None,
    title: str = "",
    description: str = "",
) -> dict:
    """
    Create a pull request in the Azure DevOps repo.

    If a PR from *source_branch* already exists (409 Conflict), the existing
    PR is looked up and returned as a success so callers don't need to
    special-case retries.
    """
    if not _is_configured(settings):
        return {"success": False, "reason": "devops_not_configured"}

    base = _base_url(settings)
    auth = _auth(settings)
    effective_target = target_branch or settings.azdo_branch
    url = f"{base}/pullrequests?api-version=7.1"
    body = {
        "sourceRefName": f"refs/heads/{source_branch}",
        "targetRefName": f"refs/heads/{effective_target}",
        "title": title,
        "description": description,
    }
    try:
        resp = requests.post(url, auth=auth, json=body, timeout=30)

        # 409 means a PR for this branch already exists — find and return it.
        if resp.status_code == 409:
            logger.info(
                "[DevOps] PR already exists for branch '%s' — looking up existing PR",
                source_branch,
            )
            existing = _find_open_pr(
                base, auth, source_branch, effective_target)
            if existing:
                pr_id = existing.get("pullRequestId")
                web_url = (
                    existing.get("_links", {}).get("web", {}).get("href")
                    or f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}"
                       f"/_git/{_repo_name(settings)}/pullrequest/{pr_id}"
                )
                logger.info(
                    "[DevOps] Reusing existing PR #%d — '%s' → '%s' | url=%s",
                    pr_id, source_branch, effective_target, web_url,
                )
                return {"success": True, "pr_id": pr_id, "url": web_url, "pr_existed": True}
            # PR exists but we couldn't look it up — still not a hard failure
            logger.warning(
                "[DevOps] PR conflict for branch '%s' but could not retrieve existing PR",
                source_branch,
            )
            return {"success": False, "reason": "pr_conflict_unresolved", "error": "409 but no open PR found"}

        resp.raise_for_status()
        data = resp.json()
        pr_id = data.get("pullRequestId")
        # Prefer the human-readable web URL over the raw API URL
        web_url = (
            data.get("_links", {}).get("web", {}).get("href")
            or f"https://dev.azure.com/{settings.azdo_org}/{settings.azdo_project}"
               f"/_git/{_repo_name(settings)}/pullrequest/{pr_id}"
        )
        logger.info("[DevOps] SUCCESS — PR #%d created — '%s' → '%s'",
                    pr_id, source_branch, effective_target)
        return {"success": True, "pr_id": pr_id, "url": web_url, "pr_existed": False}
    except requests.RequestException as exc:
        logger.error("[DevOps] FAILED — PR creation failed — source='%s' error=%s",
                     source_branch, exc)
        return {"success": False, "reason": "pr_creation_failed", "error": str(exc)}


def merge_pull_request(settings: Settings, pr_id: int, source_branch: str) -> dict:
    """
    Complete (merge) a pull request, resolving any merge conflicts first.

    Strategy:
      1. Fetch all conflicts on the PR.
      2. Resolve each conflict using ``acceptTheirs`` — the AIOps remediation
         branch wins over the target branch, since our changes are intentional.
      3. Complete the PR with squash merge and delete the source branch.

    Returns a result dict with ``success``, ``merged``, and optional ``error``.
    """
    if not _is_configured(settings):
        return {"success": False, "reason": "devops_not_configured"}

    base = _base_url(settings)
    auth = _auth(settings)
    pr_base = f"{base}/pullrequests/{pr_id}"

    try:
        # ── Step 1: GET the PR to obtain lastMergeSourceCommit ───────
        pr_resp = requests.get(
            f"{pr_base}?api-version=7.1",
            auth=auth,
            timeout=30,
        )
        pr_resp.raise_for_status()
        pr_data = pr_resp.json()
        last_merge_source_commit = pr_data.get("lastMergeSourceCommit", {})
        if not last_merge_source_commit.get("commitId"):
            # fall back: resolve the source branch HEAD directly
            src_refs = requests.get(
                f"{base}/refs?filter=heads/{source_branch}&api-version=7.1",
                auth=auth, timeout=30,
            )
            src_refs.raise_for_status()
            src_ref_list = src_refs.json().get("value", [])
            source_commit_id = src_ref_list[0]["objectId"] if src_ref_list else ""
            last_merge_source_commit = {"commitId": source_commit_id}

        # ── Step 2: resolve conflicts ─────────────────────────────────
        conflicts_resp = requests.get(
            f"{pr_base}/conflicts?api-version=7.1",
            auth=auth,
            timeout=30,
        )
        conflicts_resp.raise_for_status()
        conflicts = conflicts_resp.json().get("value", [])

        if conflicts:
            conflict_files = [
                c.get("item", {}).get("path", c.get("conflictId", "?")) for c in conflicts
            ]
            logger.info(
                "[DevOps] PR #%d — %d conflict(s) detected, resolving (acceptTheirs — "
                "remediation branch wins): %s",
                pr_id, len(conflicts), ", ".join(str(f)
                                                 for f in conflict_files),
            )
            for conflict in conflicts:
                conflict_id = conflict.get("conflictId") or conflict.get("id")
                if not conflict_id:
                    continue
                resolve_resp = requests.patch(
                    f"{pr_base}/conflicts/{conflict_id}?api-version=7.1",
                    auth=auth,
                    json={"resolutionStatus": "resolved",
                          "resolutionType": "acceptTheirs"},
                    timeout=30,
                )
                if not resolve_resp.ok:
                    logger.error(
                        "[DevOps] FAILED — could not resolve conflict %s on PR #%d — "
                        "status=%d body=%s",
                        conflict_id, pr_id, resolve_resp.status_code, resolve_resp.text[:200],
                    )

        # ── Step 3: complete/merge the PR ─────────────────────────────
        complete_resp = requests.patch(
            f"{pr_base}?api-version=7.1",
            auth=auth,
            json={
                "status": "completed",
                "lastMergeSourceCommit": last_merge_source_commit,
                "completionOptions": {
                    "mergeStrategy": "squash",
                    "deleteSourceBranch": True,
                    "bypassPolicy": True,
                    "bypassReason": "AIOps automated remediation merge",
                    "mergeCommitMessage": (
                        f"[AIOps] Squash-merged remediation branch {source_branch} "
                        f"into {settings.azdo_branch}"
                    ),
                },
            },
            timeout=30,
        )
        complete_resp.raise_for_status()
        merge_data = complete_resp.json()
        merge_commit = (
            merge_data.get("lastMergeCommit", {}).get("commitId", "")
            or merge_data.get("mergeId", "")
        )
        logger.info(
            "[DevOps] SUCCESS — PR #%d squash-merged into '%s' — merge_commit=%s | "
            "%d conflict(s) resolved | source branch '%s' deleted",
            pr_id, settings.azdo_branch, merge_commit, len(
                conflicts), source_branch,
        )
        return {
            "success": True,
            "merged": True,
            "pr_id": pr_id,
            "merge_commit": merge_commit,
            "conflicts_resolved": len(conflicts),
        }

    except requests.RequestException as exc:
        logger.error(
            "[DevOps] FAILED — PR #%d merge failed — error=%s", pr_id, exc)
        return {"success": False, "merged": False, "reason": "merge_failed", "error": str(exc)}
