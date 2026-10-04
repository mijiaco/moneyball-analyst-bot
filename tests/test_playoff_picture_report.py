"""Unit tests for the weekly playoff picture."""

from __future__ import annotations

from src.playoff_picture_report import (
    PlayoffTeam,
    format_playoff_picture_text,
    playoff_picture_dedupe_key,
    playoff_teams_from_exports,
    seed_playoff_picture,
)


def _team(
    franchise_id: str,
    division_id: str,
    wins: int,
    losses: int,
    points_for: float,
    points_against: float,
    ties: int = 0,
) -> PlayoffTeam:
    return PlayoffTeam(
        franchise_id=franchise_id,
        division_id=division_id,
        points_for=points_for,
        points_against=points_against,
        wins=wins,
        losses=losses,
        ties=ties,
    )


def test_seeds_follow_division_winners_then_record_then_points() -> None:
    teams = [
        _team("W0", "00", 8, 0, 100, 100),
        _team("W1", "01", 7, 1, 100, 100),
        _team("W2", "02", 6, 2, 100, 100),
        _team("W3", "03", 5, 3, 100, 100),
    ]
    for index in range(7):
        teams.append(_team(f"R{index}", f"{index % 4:02d}", 3, 5, 40 + index, 80))
    teams.append(_team("POINTS", "00", 0, 8, 900, 400))
    teams.append(_team("BETTER_RECORD_LOW_PF", "01", 1, 7, 50, 10))
    names = {team.franchise_id: team.franchise_id for team in teams}

    seeded = seed_playoff_picture(teams, names)
    assert [row.team.franchise_id for row in seeded] == [
        "W0",
        "W1",
        "W2",
        "W3",
        *[f"R{index}" for index in range(6, -1, -1)],
        "POINTS",
    ]
    assert [row.has_first_round_bye for row in seeded[:4]] == [True, True, True, True]
    assert all(not row.has_first_round_bye for row in seeded[4:])
    assert seeded[-1].seed == 12

    text = format_playoff_picture_text(seeded, names)
    assert text.splitlines()[2] == " 1. W0 — 8-0-0 — 100.00 — first-round bye"
    assert "12. POINTS — 0-8-0 — 900.00" in text
    assert text.splitlines()[-1] == "12. POINTS — 0-8-0 — 900.00"
    assert "Mid Bowl" not in text
    assert "Toilet Bowl" not in text


def test_record_ties_use_points_for_then_points_against() -> None:
    teams = [
        _team("DIV_HI_PF", "00", 8, 0, 200, 90),
        _team("DIV_LO_PF", "00", 8, 0, 150, 10),
        _team("W1", "01", 7, 1, 100, 100),
        _team("W2", "02", 7, 1, 100, 80),
        _team("W3", "03", 6, 2, 100, 100),
        _team("SAME_LOW_PA", "01", 4, 4, 80, 20),
        _team("SAME_HIGH_PA", "02", 4, 4, 80, 70),
        _team("POINTS_LOW_PA", "03", 0, 8, 500, 30),
        _team("POINTS_HIGH_PA", "01", 1, 7, 500, 90),
    ]
    for index in range(5):
        teams.append(_team(f"FILL{index}", "02", 3, 5, 10, 100))
    names = {team.franchise_id: team.franchise_id for team in teams}

    seeded = seed_playoff_picture(teams, names)
    by_seed = {row.seed: row.team.franchise_id for row in seeded}
    assert by_seed[1] == "DIV_HI_PF"
    assert by_seed[2] == "W2"
    assert by_seed[3] == "W1"
    assert "DIV_LO_PF" == by_seed[5]
    assert by_seed[6] == "SAME_LOW_PA"
    assert by_seed[7] == "SAME_HIGH_PA"
    assert by_seed[12] == "POINTS_LOW_PA"


def test_standings_export_joins_league_divisions() -> None:
    standings = {
        "leagueStandings": {
            "franchise": [
                {"id": "0001", "pf": "10.5", "pa": "8", "h2hw": "1", "h2hl": "0", "h2ht": "1"},
                {"id": "0002", "pf": "", "pa": "1", "h2hw": "0", "h2hl": "1", "h2ht": "0"},
            ]
        }
    }
    league = {
        "league": {
            "franchises": {
                "franchise": [
                    {"id": "0001", "name": "Alpha", "division": "01"},
                    {"id": "0002", "name": "Beta", "division": "00"},
                ]
            }
        }
    }
    teams = playoff_teams_from_exports(standings, league)
    assert len(teams) == 1
    assert teams[0].franchise_id == "0001"
    assert teams[0].division_id == "01"
    assert teams[0].record == "1-0-1"
    assert teams[0].points_for == 10.5
    assert teams[0].points_against == 8


def test_dedupe_key_and_empty_format() -> None:
    assert playoff_picture_dedupe_key("2026-10-06") == "PLAYOFF_PICTURE|2026-10-06"
    assert format_playoff_picture_text([], {}) == "Weekly Playoff Picture\n\nNo standings found."
