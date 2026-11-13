"""Unit tests for the weekly Toilet Bowl and Mid Bowl brackets."""

from __future__ import annotations

from src.bowl_bracket_report import (
    bowl_bracket_dedupe_key,
    completed_week_from_teams,
    format_bowl_bracket_text,
    regular_season_weeks_remaining,
    seed_bowl_field,
)
from src.playoff_picture_report import PlayoffTeam

# Week 9 snapshot from the 2026-11-12 bracket reply, seeds 13-32.
_WEEK_9_SEEDS = {
    13: "BavAIRian FC",
    14: "Shough the Falcons",
    15: "Brute Force & Ignorance",
    16: "Daly City French Bulldogs",
    17: "Isn't That John Candy?",
    18: "T-Town Tornados",
    19: "The Lord of the TD's",
    20: "Mathis City Mayhem",
    21: "Chalupa Batmen",
    22: "Greatest Sheaux On Turf",
    23: "Stripes and Scales",
    24: "Gibbs Me the Ring Precious",
    25: "Sal Paradiso",
    26: "Bad Connection",
    27: "Blackout Blitz",
    28: "Cascade Wrecking Crew",
    29: "The Purple Curtain",
    30: "Harley Quinn",
    31: "Joker",
    32: "VC KILLER WHALES",
}


def _team(
    franchise_id: str,
    wins: int,
    losses: int,
    points_for: float,
    *,
    points_against: float = 0,
    ties: int = 0,
) -> PlayoffTeam:
    return PlayoffTeam(
        franchise_id=franchise_id,
        division_id="00",
        points_for=points_for,
        points_against=points_against,
        wins=wins,
        losses=losses,
        ties=ties,
    )


def _week_9_field() -> tuple[list[PlayoffTeam], dict[str, str]]:
    teams: list[PlayoffTeam] = []
    names: dict[str, str] = {}
    for seed in range(1, 33):
        franchise_id = f"{seed:04d}"
        name = _WEEK_9_SEEDS.get(seed, f"Seed {seed}")
        names[franchise_id] = name
        # Equal games, strictly decreasing points, so seed order is points for.
        teams.append(
            _team(
                franchise_id,
                wins=9,
                losses=0,
                points_for=1000 - seed,
                points_against=seed * 10,
            )
        )
    return teams, names


def test_points_against_does_not_change_seed() -> None:
    names = {"0001": "Low PA", "0002": "High PA"}
    low_pa = _team("0001", 5, 4, 100, points_against=50)
    high_pa = _team("0002", 5, 4, 110, points_against=400)
    seeded = seed_bowl_field([low_pa, high_pa], names)
    assert [row.team.franchise_id for row in seeded] == ["0002", "0001"]


def test_equal_points_for_keeps_points_against_from_breaking_the_tie() -> None:
    names = {"0001": "Alpha", "0002": "Zulu"}
    alpha = _team("0001", 5, 4, 100, points_against=900)
    zulu = _team("0002", 5, 4, 100, points_against=1)
    seeded = seed_bowl_field([zulu, alpha], names)
    assert [row.team.franchise_id for row in seeded] == ["0001", "0002"]


def test_week_9_bracket_matches_the_reference_snapshot() -> None:
    teams, names = _week_9_field()
    assert completed_week_from_teams(teams) == 9
    assert regular_season_weeks_remaining(9) == 4
    text = format_bowl_bracket_text(teams, names, completed_week=9)
    assert text is not None
    assert "Through Week 9. 4 regular-season weeks left." in text
    assert "The field is not set until after Week 13." in text
    assert "Winner gets the 1.33 rookie pick." in text
    assert "Winner gets the 2.33 rookie pick." in text
    assert "25 Sal Paradiso vs 32 VC KILLER WHALES" in text
    assert "26 Bad Connection vs 31 Joker" in text
    assert "27 Blackout Blitz vs 30 Harley Quinn" in text
    assert "28 Cascade Wrecking Crew vs 29 The Purple Curtain" in text
    assert (
        "Byes: 13 BavAIRian FC, 14 Shough the Falcons, "
        "15 Brute Force & Ignorance, 16 Daly City French Bulldogs"
    ) in text
    assert "17 Isn't That John Candy? vs 24 Gibbs Me the Ring Precious" in text
    assert "18 T-Town Tornados vs 23 Stripes and Scales" in text
    assert "19 The Lord of the TD's vs 22 Greatest Sheaux On Turf" in text
    assert "20 Mathis City Mayhem vs 21 Chalupa Batmen" in text


def test_field_is_set_after_week_13() -> None:
    teams, names = _week_9_field()
    text = format_bowl_bracket_text(teams, names, completed_week=13)
    assert text is not None
    assert "Through Week 13. No regular-season weeks left. The field is set." in text
    assert "not set" not in text


def test_short_field_is_not_posted() -> None:
    teams, names = _week_9_field()
    assert format_bowl_bracket_text(teams[:31], names) is None


def test_dedupe_key_is_dated() -> None:
    assert bowl_bracket_dedupe_key("2026-11-17") == "BOWL_BRACKETS|2026-11-17"
