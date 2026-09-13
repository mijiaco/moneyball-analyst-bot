"""Unit tests for slate top-scorer parsing and formatting."""

from __future__ import annotations

from datetime import datetime

from zoneinfo import ZoneInfo

from src.top_scorers_report import (
    NflGame,
    PlayerWeekScore,
    announced_top_scorer_slate_ids,
    due_top_scorer_slate_ids,
    format_top_scorers_report_text,
    nfl_week_from_schedule,
    parse_nfl_schedule_games,
    parse_live_scoring_points,
    parse_player_week_scores,
    player_team_from_label,
    scores_for_slate,
    scoring_week_key,
    should_build_slate_report,
    slate_id_for_kickoff,
    slate_is_final,
    top_scorers_by_position,
    top_scorers_dedupe_key,
    top_scorers_title,
    week_scores_from_exports,
)

ET = ZoneInfo("America/New_York")


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> str:
    return str(int(datetime(year, month, day, hour, minute, tzinfo=ET).timestamp()))


def test_slate_id_for_kickoff_windows() -> None:
    assert slate_id_for_kickoff(datetime(2026, 9, 13, 13, 0, tzinfo=ET)) == "early_sun"
    assert slate_id_for_kickoff(datetime(2026, 9, 13, 16, 25, tzinfo=ET)) == "late_sun"
    assert slate_id_for_kickoff(datetime(2026, 9, 13, 20, 20, tzinfo=ET)) == "snf"
    assert slate_id_for_kickoff(datetime(2026, 9, 14, 20, 15, tzinfo=ET)) == "mnf"
    assert slate_id_for_kickoff(datetime(2026, 9, 10, 20, 35, tzinfo=ET)) == "tnf"
    assert slate_id_for_kickoff(datetime(2026, 9, 9, 20, 20, tzinfo=ET)) == "wed"


def test_due_top_scorer_slate_ids() -> None:
    assert due_top_scorer_slate_ids(datetime(2026, 9, 13, 16, 14, tzinfo=ET)) == [
        "wed",
        "tnf",
    ]
    assert due_top_scorer_slate_ids(datetime(2026, 9, 13, 16, 15, tzinfo=ET)) == [
        "wed",
        "tnf",
        "early_sun",
    ]
    assert "late_sun" in due_top_scorer_slate_ids(
        datetime(2026, 9, 13, 20, 0, tzinfo=ET)
    )
    assert "snf" in due_top_scorer_slate_ids(datetime(2026, 9, 13, 23, 45, tzinfo=ET))
    monday_night = due_top_scorer_slate_ids(datetime(2026, 9, 14, 22, 0, tzinfo=ET))
    assert "snf" in monday_night
    assert "mnf" not in monday_night
    assert due_top_scorer_slate_ids(datetime(2026, 9, 15, 0, 29, tzinfo=ET))[-1] == "snf"
    tuesday = due_top_scorer_slate_ids(datetime(2026, 9, 15, 0, 30, tzinfo=ET))
    assert tuesday[-2:] == ["mnf", "cumulative"]


def test_parse_schedule_and_filter_final_slate() -> None:
    schedule = {
        "nflSchedule": {
            "week": "1",
            "matchup": [
                {
                    "kickoff": _ts(2026, 9, 13, 13, 0),
                    "gameSecondsRemaining": "0",
                    "team": [{"id": "BAL"}, {"id": "IND"}],
                },
                {
                    "kickoff": _ts(2026, 9, 13, 13, 0),
                    "gameSecondsRemaining": "3600",
                    "team": [{"id": "BUF"}, {"id": "HOU"}],
                },
                {
                    "kickoff": _ts(2026, 9, 13, 16, 25),
                    "gameSecondsRemaining": "3600",
                    "team": [{"id": "GBP"}, {"id": "MIN"}],
                },
            ],
        }
    }
    games = parse_nfl_schedule_games(schedule)
    assert nfl_week_from_schedule(schedule) == "1"
    assert scoring_week_key("2026", "1", games) == "2026-W01"
    assert slate_is_final(games, "early_sun") is False
    assert slate_is_final(games, "late_sun") is False
    assert should_build_slate_report("early_sun", games) is False


def test_scores_for_slate_and_top_five() -> None:
    games = [
        NflGame(
            slate_id="tnf",
            kickoff=datetime(2026, 9, 10, 20, 35, tzinfo=ET),
            is_final=True,
            team_ids=("SFO", "LAR"),
        ),
        NflGame(
            slate_id="early_sun",
            kickoff=datetime(2026, 9, 13, 13, 0, tzinfo=ET),
            is_final=False,
            team_ids=("BAL", "IND"),
        ),
    ]
    scores = parse_player_week_scores(
        {
            "playerScores": {
                "playerScore": [
                    {"id": "1", "score": "22.7"},
                    {"id": "2", "score": "14.9"},
                    {"id": "3", "score": "9.1"},
                    {"id": "4", "score": "0"},
                ]
            }
        },
        {
            "1": "Samuel, Deebo SFO WR",
            "2": "Nacua, Puka LAR WR",
            "3": "Flowers, Zay BAL WR",
            "4": "Kupp, Cooper SEA WR",
        },
    )
    assert player_team_from_label("Samuel, Deebo SFO WR") == "SFO"
    tnf = scores_for_slate(scores, games, "tnf")
    assert [row.player_id for row in tnf] == ["1", "2"]
    ranked = top_scorers_by_position(tnf)
    assert [row.label for row in ranked["WR"]] == [
        "Samuel, Deebo SFO WR",
        "Nacua, Puka LAR WR",
    ]
    text = format_top_scorers_report_text(
        ranked, title=top_scorers_title("tnf", week="1")
    )
    assert text.startswith("Top Scorers — Week 1 Thursday Night")
    assert "1. Samuel, Deebo SFO WR — 22.70" in text
    assert "Flowers" not in text


def test_format_empty_and_cumulative_waits_for_mnf() -> None:
    assert format_top_scorers_report_text({}, title="Top Scorers — Week Cumulative") == (
        "Top Scorers — Week Cumulative\n\nNo scorers yet."
    )
    games = [
        NflGame(
            slate_id="mnf",
            kickoff=datetime(2026, 9, 14, 20, 15, tzinfo=ET),
            is_final=False,
            team_ids=("DEN", "KCC"),
        )
    ]
    assert should_build_slate_report("cumulative", games) is False
    assert should_build_slate_report(
        "cumulative",
        [
            NflGame(
                slate_id="mnf",
                kickoff=datetime(2026, 9, 14, 20, 15, tzinfo=ET),
                is_final=True,
                team_ids=("DEN", "KCC"),
            )
        ],
    )
    assert should_build_slate_report("cumulative", []) is True


def test_top_scorers_sorts_and_limits() -> None:
    rows = [
        PlayerWeekScore("1", "RB", "A, One LAR RB", 10.0, "LAR"),
        PlayerWeekScore("2", "RB", "B, Two SFO RB", 15.5, "SFO"),
        PlayerWeekScore("3", "RB", "C, Three SEA RB", 15.5, "SEA"),
        PlayerWeekScore("4", "RB", "D, Four NEP RB", 8.0, "NEP"),
        PlayerWeekScore("5", "RB", "E, Five GBP RB", 7.0, "GBP"),
        PlayerWeekScore("6", "RB", "F, Six DAL RB", 6.0, "DAL"),
    ]
    ranked = top_scorers_by_position(rows)
    assert [row.player_id for row in ranked["RB"]] == ["2", "3", "1", "4", "5"]


def test_announced_top_scorer_slate_ids_ignores_cursor_only_slots() -> None:
    seen = {
        "TOP_SCORERS|2026-W01|wed",
        "TOP_SCORERS|2026-W01|tnf",
        "ROSTER_VIOLATIONS|2026-09-13|15:30",
    }
    assert announced_top_scorer_slate_ids(seen) == {"wed", "tnf"}
    assert "early_sun" not in announced_top_scorer_slate_ids(seen)
    assert top_scorers_dedupe_key("2026-W01", "early_sun") == (
        "TOP_SCORERS|2026-W01|early_sun"
    )


def test_week_scores_from_exports_fills_sunday_from_live_scoring() -> None:
    games = [
        NflGame(
            slate_id="early_sun",
            kickoff=datetime(2026, 9, 13, 13, 0, tzinfo=ET),
            is_final=True,
            team_ids=("BAL", "IND"),
        )
    ]
    player_scores = {
        "playerScores": {
            "playerScore": [{"id": "1", "score": "22.7"}]
        }
    }
    live = {
        "liveScoring": {
            "matchup": [
                {
                    "franchise": [
                        {
                            "players": {
                                "player": [
                                    {"id": "1", "score": "22.7", "status": "starter"},
                                    {"id": "9", "score": "27.4", "status": "starter"},
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }
    players = {
        "1": "Samuel, Deebo SFO WR",
        "9": "Jackson, Lamar BAL QB",
    }
    assert parse_live_scoring_points(live)["9"] == 27.4
    merged = week_scores_from_exports(player_scores, live, players)
    early = scores_for_slate(merged, games, "early_sun")
    assert [row.player_id for row in early] == ["9"]
    assert early[0].points == 27.4
