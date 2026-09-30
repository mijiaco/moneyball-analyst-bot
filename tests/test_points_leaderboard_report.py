"""Unit tests for the points leaderboard and lottery-balls reports."""

from __future__ import annotations

from src.points_leaderboard_report import (
    format_lottery_balls_report_text,
    format_points_leaderboard_text,
    lottery_ball_standings,
    lottery_balls_dedupe_key,
    points_leaderboard_dedupe_key,
    rank_standings_best_to_worst,
    standings_from_export,
)


def _export() -> dict:
    return {
        "leagueStandings": {
            "franchise": [
                {"id": "0001", "pf": "100.00", "h2hw": "2", "h2hl": "1", "h2ht": "0"},
                {"id": "0002", "pf": "80.50", "h2hw": "1", "h2hl": "2", "h2ht": "0"},
                {"id": "0003", "pf": "80.50", "h2hw": "0", "h2hl": "3", "h2ht": "0"},
                {"id": "0004", "pf": "90.00", "h2hw": "1", "h2hl": "1", "h2ht": "1"},
            ]
        }
    }


def test_standings_rank_best_to_worst_and_break_ties_on_record() -> None:
    names = {
        "0001": "Alpha",
        "0002": "Bravo",
        "0003": "Charlie",
        "0004": "Delta",
    }
    ranked = rank_standings_best_to_worst(standings_from_export(_export()), names)
    assert [row.franchise_id for row in ranked] == ["0001", "0004", "0002", "0003"]
    text = format_points_leaderboard_text(ranked, names)
    assert text.splitlines()[2:] == [
        " 1. Alpha — 100.00 — 2-1-0",
        " 2. Delta — 90.00 — 1-1-1",
        " 3. Bravo — 80.50 — 1-2-0",
        " 4. Charlie — 80.50 — 0-3-0",
    ]


def test_lottery_balls_worst_gets_most_balls() -> None:
    names = {
        "0001": "Alpha",
        "0002": "Bravo",
        "0003": "Charlie",
        "0004": "Delta",
    }
    ranked = rank_standings_best_to_worst(standings_from_export(_export()), names)
    balls = lottery_ball_standings(ranked, team_count=3)
    assert [(row.franchise_id, count) for row, count in balls] == [
        ("0003", 3),
        ("0002", 2),
        ("0004", 1),
    ]
    text = format_lottery_balls_report_text(balls, names)
    assert "1. Charlie — 80.50 — 0-3-0 — 3 balls" in text
    assert "3. Delta — 90.00 — 1-1-1 — 1 ball" in text


def test_equal_points_and_record_use_team_name() -> None:
    export = {
        "leagueStandings": {
            "franchise": [
                {"id": "0001", "pf": "10", "h2hw": "0", "h2hl": "1", "h2ht": "0"},
                {"id": "0002", "pf": "10", "h2hw": "0", "h2hl": "1", "h2ht": "0"},
            ]
        }
    }
    names = {"0001": "Zulu", "0002": "Alpha"}
    ranked = rank_standings_best_to_worst(standings_from_export(export), names)
    assert [row.franchise_id for row in ranked] == ["0002", "0001"]


def test_dedupe_keys_are_date_scoped() -> None:
    assert points_leaderboard_dedupe_key("2026-10-06") == "POINTS_LEADERBOARD|2026-10-06"
    assert lottery_balls_dedupe_key("2026-10-06") == "LOTTERY_BALLS|2026-10-06"
