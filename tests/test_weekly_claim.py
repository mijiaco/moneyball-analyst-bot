"""Tests for Saturday weekly-report week-key claiming."""

from __future__ import annotations

from pathlib import Path
import tempfile

from src.weekly_claim import (
    claim_weekly_reports_week,
    weekly_report_dedupe_key,
)


def _load(path: Path) -> str:
    if not path.is_file():
        return ""
    import json

    raw = json.loads(path.read_text(encoding="utf-8"))
    return str(raw.get("last_weekly_reports_week_key") or "")


def _save(path: Path, week_key: str) -> None:
    import json

    data = {}
    if path.is_file():
        data = json.loads(path.read_text(encoding="utf-8"))
    data["last_weekly_reports_week_key"] = week_key
    path.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")


def test_weekly_report_dedupe_key() -> None:
    assert (
        weekly_report_dedupe_key("2026-W33", "Top Traders This Year")
        == "WEEKLY_REPORT|2026-W33|Top Traders This Year"
    )


def test_claim_skips_when_local_already_has_week() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "reports_state.json"
        _save(path, "2026-W33")
        should_post, changed = claim_weekly_reports_week(
            path,
            "2026-W33",
            load_last=_load,
            save_last=_save,
            try_github_claim=lambda _key: None,
        )
    assert should_post is False
    assert changed is False


def test_claim_posts_when_github_unavailable() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "reports_state.json"
        _save(path, "2026-W32")
        should_post, changed = claim_weekly_reports_week(
            path,
            "2026-W33",
            load_last=_load,
            save_last=_save,
            try_github_claim=lambda _key: None,
        )
        assert _load(path) == "2026-W33"
    assert should_post is True
    assert changed is True


def test_claim_skips_when_github_says_already_claimed() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "reports_state.json"
        _save(path, "2026-W32")
        should_post, changed = claim_weekly_reports_week(
            path,
            "2026-W33",
            load_last=_load,
            save_last=_save,
            try_github_claim=lambda _key: False,
        )
        assert _load(path) == "2026-W33"
    assert should_post is False
    assert changed is True


def test_claim_posts_when_github_claim_succeeds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "reports_state.json"
        should_post, changed = claim_weekly_reports_week(
            path,
            "2026-W33",
            load_last=_load,
            save_last=_save,
            try_github_claim=lambda _key: True,
        )
        assert _load(path) == "2026-W33"
    assert should_post is True
    assert changed is True
