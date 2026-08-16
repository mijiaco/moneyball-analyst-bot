"""Claim Saturday weekly-report week keys so overlapping Actions runners do not double-post."""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable

import httpx

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


def weekly_report_dedupe_key(week_key: str, report_title: str) -> str:
    return f"WEEKLY_REPORT|{week_key}|{report_title}"


def _github_repo_and_token() -> tuple[str, str, str] | None:
    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if not token or "/" not in repo:
        return None
    owner, name = repo.split("/", 1)
    if not owner or not name:
        return None
    return owner, name, token


def _branch_for_claim() -> str:
    ref = (os.environ.get("GITHUB_REF_NAME") or "").strip()
    if ref:
        return ref
    return "main"


def _parse_reports_state(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def try_claim_weekly_week_key_via_github(
    week_key: str,
    *,
    client: httpx.Client | None = None,
) -> bool | None:
    """Try to set remote ``last_weekly_reports_week_key`` to ``week_key``.

    Returns:
        True — this runner claimed the week (or remote already matched after our write).
        False — another runner already claimed this week.
        None — GitHub claim unavailable (not in Actions / no token); caller should use local claim.
    """
    repo_token = _github_repo_and_token()
    if repo_token is None:
        return None
    owner, repo, token = repo_token
    branch = _branch_for_claim()
    path = "data/reports_state.json"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{path}"

    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=30.0, headers=headers)
    else:
        client.headers.update(headers)

    try:
        for attempt in range(1, 6):
            remote_sha: str | None = None
            remote_data: dict[str, Any] = {}
            get_resp = client.get(url, params={"ref": branch})
            if get_resp.status_code == 200:
                payload = get_resp.json()
                remote_sha = str(payload.get("sha") or "") or None
                content_b64 = str(payload.get("content") or "").replace("\n", "")
                if content_b64:
                    remote_data = _parse_reports_state(
                        base64.b64decode(content_b64).decode("utf-8")
                    )
                if str(remote_data.get("last_weekly_reports_week_key") or "") == week_key:
                    return False
            elif get_resp.status_code != 404:
                logger.warning(
                    "Weekly week-key claim GET failed (%s): %s",
                    get_resp.status_code,
                    get_resp.text[:300],
                )
                return None

            merged = dict(remote_data)
            merged["last_weekly_reports_week_key"] = week_key
            body: dict[str, Any] = {
                "message": f"chore: claim weekly reports {week_key}",
                "content": base64.b64encode(
                    json.dumps(merged, sort_keys=True).encode("utf-8")
                ).decode("ascii"),
                "branch": branch,
            }
            if remote_sha:
                body["sha"] = remote_sha

            put_resp = client.put(url, json=body)
            if put_resp.status_code in (200, 201):
                return True
            if put_resp.status_code in (409, 422) and attempt < 5:
                logger.info(
                    "Weekly week-key claim conflict (%s), retry %s",
                    put_resp.status_code,
                    attempt,
                )
                continue
            logger.warning(
                "Weekly week-key claim PUT failed (%s): %s",
                put_resp.status_code,
                put_resp.text[:300],
            )
            return None
    finally:
        if owns_client:
            client.close()

    return None


def claim_weekly_reports_week(
    state_path: Path,
    week_key: str,
    *,
    load_last: Callable[[Path], str],
    save_last: Callable[[Path, str], None],
    try_github_claim: Callable[[str], bool | None] | None = None,
) -> tuple[bool, bool]:
    """Decide whether this process should post the Saturday weekly batch.

    Returns:
        (should_post, state_changed)
    """
    if load_last(state_path) == week_key:
        return False, False

    github_claim = try_github_claim or try_claim_weekly_week_key_via_github
    claimed = github_claim(week_key)
    if claimed is False:
        save_last(state_path, week_key)
        logger.info(
            "Skipping weekly reports for %s; already claimed by another runner",
            week_key,
        )
        return False, True

    save_last(state_path, week_key)
    if claimed is True:
        logger.info("Claimed weekly reports week %s via GitHub", week_key)
    return True, True
