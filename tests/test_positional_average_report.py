"""Unit tests for Tuesday team QB / punter / kicker averages."""

from __future__ import annotations

from src.positional_average_report import (
    bye_adjustments_for_franchises,
    format_positional_averages_text,
    positional_averages_dedupe_key,
    positional_averages_for_playing_teams,
    team_position_scores_from_exports,
)


def _players() -> dict[str, str]:
    return {
        "qb1": "Patriots, New England NEP TMQB",
        "qb2": "Cowboys, Dallas DAL TMQB",
        "qb3": "Bears, Chicago CHI TMQB",
        "pn1": "Patriots, New England NEP TMPN",
        "pn2": "Cowboys, Dallas DAL TMPN",
        "pn3": "Bears, Chicago CHI TMPN",
        "pk1": "Patriots, New England NEP TMPK",
        "pk2": "Cowboys, Dallas DAL TMPK",
        "pk3": "Bears, Chicago CHI TMPK",
    }


def _export() -> dict:
    return {
        "playerScores": {
            "playerScore": [
                {"id": "qb1", "score": "10"},
                {"id": "qb2", "score": "30"},
                {"id": "qb3", "score": "0"},
                {"id": "pn1", "score": "0"},
                {"id": "pn2", "score": "8"},
                {"id": "pn3", "score": "12"},
                {"id": "pk1", "score": "9"},
                {"id": "pk2", "score": "11"},
                {"id": "pk3", "score": "1"},
            ]
        }
    }


def test_bye_team_is_excluded_and_zero_for_a_playing_team_counts() -> None:
    scores = team_position_scores_from_exports(_export(), _players())
    averages = positional_averages_for_playing_teams(scores, {"NEP", "DAL"})
    assert averages is not None
    assert [(row.label, round(row.average, 2), row.team_count) for row in averages] == [
        ("QB", 20.0, 2),
        ("Punter", 4.0, 2),
        ("Kicker", 10.0, 2),
    ]
    text = format_positional_averages_text(averages, week="4")
    assert text.splitlines() == [
        "Positional Averages — Week 4",
        "",
        "QB — 20.00",
        "Punter — 4.00",
        "Kicker — 10.00",
    ]


def test_bye_roster_player_adds_position_average_to_week_score() -> None:
    scores = team_position_scores_from_exports(_export(), _players())
    averages = positional_averages_for_playing_teams(scores, {"NEP", "DAL"})
    assert averages is not None
    rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [
                        {"id": "pk3", "status": "ROSTER"},
                        {"id": "pn3", "status": "INJURED_RESERVE"},
                        {"id": "pk1", "status": "ROSTER"},
                    ],
                },
                {
                    "id": "0002",
                    "player": [{"id": "qb3", "status": "TAXI_SQUAD"}],
                },
            ]
        }
    }
    adjustments = bye_adjustments_for_franchises(
        rosters,
        _players(),
        {"NEP", "DAL"},
        averages,
        {"0001": 100.0, "0002": 80.0},
        {"0001": "Alpha"},
    )
    assert len(adjustments) == 1
    row = adjustments[0]
    assert row.franchise_name == "Alpha"
    assert row.additional == 10.0
    assert row.week_score == 100.0
    assert row.adjusted_total == 110.0
    assert row.items[0].description == "Bears Kicker"
    text = format_positional_averages_text(averages, week="5", adjustments=adjustments)
    assert "Bye adjustments" in text
    assert "Alpha — +10.00 — 100.00 this week — 110.00 adjusted" in text
    assert "Bears Kicker +10.00" in text


def test_missing_playing_team_waits_for_a_complete_export() -> None:
    scores = team_position_scores_from_exports(_export(), _players())
    assert positional_averages_for_playing_teams(scores, {"NEP", "DAL", "GB"}) is None


def test_live_points_fill_a_missing_team_and_replace_a_lower_total() -> None:
    players = _players()
    players["pk4"] = "Packers, Green Bay GBP TMPK"
    scores = team_position_scores_from_exports(
        _export(),
        players,
        {"qb1": 12.0, "pk4": 7.5, "qb9": 5.0},
    )
    by_id = {row.player_id: row.points for row in scores}
    assert by_id["qb1"] == 12.0
    assert by_id["pk4"] == 7.5
    assert "qb9" not in by_id


def test_dedupe_key_is_week_scoped() -> None:
    assert positional_averages_dedupe_key("2026", "4") == "POSITIONAL_AVERAGES|2026-W04"
