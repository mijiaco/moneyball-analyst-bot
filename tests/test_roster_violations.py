"""Unit tests for roster violation detection and formatting."""

from __future__ import annotations

from src.roster_violations import (
    IrEligibilityViolation,
    SalaryCapViolation,
    SlotLimitViolation,
    StarterRequirementViolation,
    find_ir_eligibility_violations,
    find_salary_cap_violations,
    find_slot_limit_violations,
    find_starter_requirement_violations,
    format_roster_violations_report_text,
    franchise_salaries_from_standings,
    franchise_salary_caps_from_league,
    injury_status_by_player_id,
    ir_eligible_statuses_from_env,
    league_slot_limits,
    player_position_from_label,
    starter_lineup_size,
    starter_position_minimums,
)


def test_injury_status_by_player_id_normalizes_rows() -> None:
    payload = {
        "injuries": {
            "injury": [
                {"id": "17484", "status": "Questionable", "details": "Undisclosed"},
                {"id": "17647", "status": "IR", "details": "Knee - ACL"},
            ]
        }
    }
    by_id = injury_status_by_player_id(payload)
    assert by_id["17484"]["status"] == "Questionable"
    assert by_id["17647"]["details"] == "Knee - ACL"


def test_ir_eligible_statuses_from_env_override(monkeypatch) -> None:
    monkeypatch.setenv("MFL_IR_ELIGIBLE_STATUSES", "IR, Out | Doubtful")
    statuses = ir_eligible_statuses_from_env()
    assert statuses == frozenset({"IR", "OUT", "DOUBTFUL"})


def test_find_ir_eligibility_violations_flags_questionable_on_ir() -> None:
    rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0020",
                    "player": [
                        {"id": "17484", "status": "INJURED_RESERVE"},
                        {"id": "17647", "status": "INJURED_RESERVE"},
                        {"id": "10001", "status": "ROSTER"},
                    ],
                }
            ]
        }
    }
    injuries = {
        "17484": {"status": "Questionable", "details": "Undisclosed"},
        "17647": {"status": "IR", "details": "Knee - ACL"},
    }
    players = {
        "17484": "Reid, Desmond FA RB",
        "17647": "Law, Kendrick DET WR",
    }
    violations = find_ir_eligibility_violations(rosters, injuries, players)
    assert len(violations) == 1
    assert violations[0].player_id == "17484"
    assert violations[0].injury_status == "Questionable"


def test_find_ir_eligibility_violations_missing_injury_is_violation() -> None:
    rosters = {
        "rosters": {
            "franchise": {
                "id": "0001",
                "player": {"id": "99999", "status": "INJURED_RESERVE"},
            }
        }
    }
    violations = find_ir_eligibility_violations(rosters, {}, {"99999": "Ghost, Player FA QB"})
    assert len(violations) == 1
    assert violations[0].injury_status == "(none)"


def test_find_slot_limit_violations_over_roster_and_taxi() -> None:
    rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0002",
                    "player": [
                        {"id": "1", "status": "ROSTER"},
                        {"id": "2", "status": "ROSTER"},
                        {"id": "3", "status": "ROSTER"},
                        {"id": "4", "status": "TAXI_SQUAD"},
                        {"id": "5", "status": "TAXI_SQUAD"},
                        {"id": "6", "status": "INJURED_RESERVE"},
                    ],
                }
            ]
        }
    }
    violations = find_slot_limit_violations(
        rosters,
        roster_limit=2,
        taxi_limit=1,
        ir_limit=2,
    )
    assert {(v.slot_name, v.count, v.limit) for v in violations} == {
        ("active roster", 3, 2),
        ("taxi squad", 2, 1),
    }


def test_league_slot_limits_reads_league_fields() -> None:
    league = {
        "league": {
            "rosterSize": "45",
            "taxiSquad": "12",
            "injuredReserve": "20",
        }
    }
    assert league_slot_limits(league) == {"roster": 45, "taxi": 12, "ir": 20}


def test_find_salary_cap_violations_over_cap_only() -> None:
    violations = find_salary_cap_violations(
        {"0001": 1000.0, "0002": 900.0, "0003": 950.0},
        {"0001": 995.0, "0002": 900.0, "0003": 960.0},
    )
    assert len(violations) == 1
    assert violations[0].franchise_id == "0001"
    assert violations[0].salary == 1000.0
    assert violations[0].cap == 995.0


def test_franchise_salary_helpers_parse_money() -> None:
    caps = franchise_salary_caps_from_league(
        {"league": {"franchises": {"franchise": [{"id": "0001", "salaryCapAmount": "995"}]}}}
    )
    salaries = franchise_salaries_from_standings(
        {"leagueStandings": {"franchise": [{"id": "0001", "salary": "$1,002.50"}]}}
    )
    assert caps["0001"] == 995.0
    assert salaries["0001"] == 1002.5


def test_player_position_from_label() -> None:
    assert player_position_from_label("Reid, Desmond FA RB") == "RB"
    assert player_position_from_label("Player 1") == ""


def test_starter_position_minimums_and_lineup_size() -> None:
    league = {
        "league": {
            "starters": {
                "count": "21",
                "position": [
                    {"name": "RB", "limit": "2-3"},
                    {"name": "S", "limit": "2-4"},
                    {"name": "WR", "limit": "0-4"},
                ],
            }
        }
    }
    assert starter_position_minimums(league) == {"RB": 2, "S": 2}
    assert starter_lineup_size(league) == 21


def test_find_starter_requirement_violations_short_position_and_total() -> None:
    rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0009",
                    "player": [
                        {"id": "1", "status": "ROSTER"},
                        {"id": "2", "status": "ROSTER"},
                        {"id": "3", "status": "TAXI_SQUAD"},
                        {"id": "4", "status": "INJURED_RESERVE"},
                    ],
                }
            ]
        }
    }
    players = {
        "1": "One, Player FA S",
        "2": "Two, Player DET WR",
        "3": "Three, Player DAL S",
        "4": "Four, Player KC S",
    }
    violations = find_starter_requirement_violations(
        rosters,
        players,
        position_minimums={"S": 2, "WR": 1},
        lineup_size=3,
    )
    assert {(v.position, v.have, v.need) for v in violations} == {
        ("TOTAL", 2, 3),
        ("S", 1, 2),
    }


def test_format_roster_violations_report_text_groups_by_team() -> None:
    text = format_roster_violations_report_text(
        {"0020": "California Cowboys"},
        [
            IrEligibilityViolation(
                franchise_id="0020",
                player_id="17484",
                player_label="Reid, Desmond FA RB",
                injury_status="Questionable",
                injury_details="Undisclosed",
            )
        ],
        [
            SlotLimitViolation(
                franchise_id="0020",
                slot_name="active roster",
                count=46,
                limit=45,
            )
        ],
        salary_cap_violations=[
            SalaryCapViolation(franchise_id="0020", salary=1002.5, cap=995.0)
        ],
        starter_requirement_violations=[
            StarterRequirementViolation(
                franchise_id="0020", position="S", have=1, need=2
            )
        ],
    )
    assert "Roster Violations" in text
    assert "**California Cowboys**" in text
    assert "IR eligibility: Reid, Desmond FA RB — Questionable (Undisclosed)" in text
    assert "Slot limit: active roster 46 (limit 45)" in text
    assert "Salary cap: $1,002.50 used / $995.00 cap ($7.50 over)" in text
    assert "Starting roster: S 1/2" in text


def test_format_roster_violations_report_text_none_found() -> None:
    text = format_roster_violations_report_text({}, [], [])
    assert text == "Roster Violations\n\nNo roster violations found."
