"""Unit tests for scheduled report time gates in run_once."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile

import pytest
from zoneinfo import ZoneInfo

from src import run_once as ro


ET = ZoneInfo("America/New_York")


@pytest.mark.parametrize(
    "when, hour, minute, expected",
    [
        ("2026-04-18", 15, 29, False),  # Sat before window
        ("2026-04-18", 15, 30, True),
        ("2026-04-19", 15, 30, False),  # Sun (weekly batch is Saturday-only)
        ("2026-04-14", 15, 30, False),  # Tue
    ],
)
def test_weekly_reports_due_saturday_after_330(
    when: str, hour: int, minute: int, expected: bool
) -> None:
    y, m, d = (int(p) for p in when.split("-"))
    now_et = datetime(y, m, d, hour, minute, tzinfo=ET)
    assert ro._is_weekly_reports_due(now_et) is expected


@pytest.mark.parametrize(
    "when, hour, minute, expected",
    [
        ("2026-04-19", 12, 59, False),  # Sun before 1:00 PM
        ("2026-04-19", 13, 0, True),
        ("2026-04-19", 18, 0, True),
        ("2026-04-18", 13, 0, False),  # Sat
        ("2026-04-13", 13, 0, False),  # Mon
    ],
)
def test_sunday_unpaid_report_due_sunday_after_1300(
    when: str, hour: int, minute: int, expected: bool
) -> None:
    y, m, d = (int(p) for p in when.split("-"))
    now_et = datetime(y, m, d, hour, minute, tzinfo=ET)
    assert ro._is_sunday_unpaid_report_due(now_et) is expected


def test_save_weekly_week_key_merges_reports_state() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "reports_state.json"
        path.write_text(
            '{"last_unpaid_owners_sunday_date_et": "2026-04-13"}\n',
            encoding="utf-8",
        )
        ro._save_last_weekly_reports_week_key(path, "2026-W16")
        data = ro._read_reports_state_json(path)
    assert data["last_weekly_reports_week_key"] == "2026-W16"
    assert data["last_unpaid_owners_sunday_date_et"] == "2026-04-13"
