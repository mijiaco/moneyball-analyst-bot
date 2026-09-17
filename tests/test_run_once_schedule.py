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
        ("2026-04-18", 14, 59, False),  # Sat before window
        ("2026-04-18", 15, 0, True),
        ("2026-04-18", 15, 30, True),
        ("2026-04-19", 15, 0, False),  # Sun (weekly batch is Saturday-only)
        ("2026-04-14", 15, 0, False),  # Tue
    ],
)
def test_weekly_reports_due_saturday_after_1500(
    when: str, hour: int, minute: int, expected: bool
) -> None:
    y, m, d = (int(p) for p in when.split("-"))
    now_et = datetime(y, m, d, hour, minute, tzinfo=ET)
    assert ro._is_weekly_reports_due(now_et) is expected


@pytest.mark.parametrize(
    "when, hour, minute, expected_slots",
    [
        ("2026-09-10", 19, 29, []),  # Thu before 7:30 PM
        ("2026-09-10", 19, 30, ["2026-09-10|19:30"]),  # Thu at 7:30 PM
        ("2026-09-10", 20, 0, ["2026-09-10|19:30"]),
        ("2026-09-13", 12, 14, []),  # Sun before first slot
        ("2026-09-13", 12, 15, ["2026-09-13|12:15"]),
        (
            "2026-09-13",
            15,
            30,
            ["2026-09-13|12:15", "2026-09-13|15:30"],
        ),
        (
            "2026-09-13",
            19,
            30,
            [
                "2026-09-13|12:15",
                "2026-09-13|15:30",
                "2026-09-13|19:30",
            ],
        ),
        ("2026-09-14", 19, 30, ["2026-09-14|19:30"]),  # Mon
        ("2026-09-15", 14, 59, []),  # Tue before 3:00 PM
        ("2026-09-15", 15, 0, ["2026-09-15|15:00"]),
        ("2026-09-16", 15, 0, ["2026-09-16|15:00"]),  # Wed
        ("2026-09-18", 14, 59, []),  # Fri before 3:00 PM
        ("2026-09-18", 15, 0, ["2026-09-18|15:00"]),
        ("2026-09-12", 14, 59, []),  # Sat before 3:00 PM
        ("2026-09-12", 15, 0, ["2026-09-12|15:00"]),
    ],
)
def test_open_roster_violations_slot_keys(
    when: str, hour: int, minute: int, expected_slots: list[str]
) -> None:
    y, m, d = (int(p) for p in when.split("-"))
    now_et = datetime(y, m, d, hour, minute, tzinfo=ET)
    assert ro._open_roster_violations_slot_keys(now_et) == expected_slots


def test_posted_roster_violations_slots_legacy_date() -> None:
    assert ro._posted_roster_violations_slots(
        {"last_roster_violations_date_et": "2026-09-10"}
    ) == {"2026-09-10|15:00"}


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


@pytest.mark.parametrize(
    "when, hour, minute, expected",
    [
        ("2026-04-19", 10, 59, False),  # Sun before 11:00 AM
        ("2026-04-19", 11, 0, True),
        ("2026-04-19", 18, 0, True),
        ("2026-04-18", 11, 0, False),  # Sat
        ("2026-04-13", 11, 0, False),  # Mon
    ],
)
def test_sunday_active_roster_demote_report_due_sunday_after_1100(
    when: str, hour: int, minute: int, expected: bool
) -> None:
    y, m, d = (int(p) for p in when.split("-"))
    now_et = datetime(y, m, d, hour, minute, tzinfo=ET)
    assert ro._is_sunday_active_roster_demote_report_due(now_et) is expected


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
