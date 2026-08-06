"""Unit tests for taxi-squad cut dead-money tracking."""

from __future__ import annotations

from src.rfa_state import FreeAgentMove
from src.taxi_cut_report import (
    format_taxi_cut_alert_text,
    format_taxi_cut_weekly_report_text,
    looks_like_taxi_dead_money,
    parse_dropped_adjustment_details,
    parse_salary_adjustments,
    taxi_players_from_rosters,
    unreimbursed_taxi_cuts,
    update_taxi_cut_state,
)


def test_parse_dropped_adjustment_details_moss() -> None:
    parsed = parse_dropped_adjustment_details(
        "Dropped Moss, Le'Veon MIA RB (Salary: $2.00, Original Contract: 3, Years Left: 3)"
    )
    assert parsed is not None
    label, salary, years = parsed
    assert label == "Moss, Le'Veon MIA RB"
    assert salary == 2.0
    assert years == 3


def test_looks_like_taxi_dead_money_matches_user_example() -> None:
    assert looks_like_taxi_dead_money(
        dead_money=7.5, salary=10.0, years_left=3, taxi_percent=25.0
    )
    assert looks_like_taxi_dead_money(
        dead_money=1.6, salary=2.0, years_left=3, taxi_percent=25.0
    )


def test_taxi_players_from_rosters() -> None:
    rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0018",
                    "player": [
                        {"id": "17479", "status": "TAXI_SQUAD", "salary": "2"},
                        {"id": "10001", "status": "ROSTER", "salary": "50"},
                    ],
                }
            ]
        }
    }
    taxi = taxi_players_from_rosters(rosters)
    assert taxi == {"17479": {"franchise_id": "0018", "salary": "2"}}


def test_update_detects_taxi_cut_via_history_and_refund() -> None:
    state = {
        "taxi_seen": {
            "17479": {"franchise_id": "0018", "salary": "2", "last_seen_ts": 100}
        },
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0018",
                    "timestamp": "200",
                    "amount": "1.6",
                    "description": (
                        "Dropped Moss, Le'Veon MIA RB "
                        "(Salary: $2.00, Original Contract: 3, Years Left: 3)"
                    ),
                    "id": "0",
                }
            }
        }
    )
    players = {"17479": "Moss, Le'Veon MIA RB"}
    drops = [
        FreeAgentMove(
            player_id="17479",
            franchise_id="0018",
            timestamp=200,
            is_add=False,
        )
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=adjustments,
        players_map=players,
        taxi_percent=25.0,
        now_ts=250,
    )
    assert len(new_cuts) == 1
    assert new_cuts[0].dead_money == 1.6
    assert len(unreimbursed_taxi_cuts(updated)) == 1

    refunded_adjustments = adjustments + parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0018",
                    "timestamp": "300",
                    "amount": "-1.6",
                    "description": "Taxi refund Moss",
                    "id": "1",
                }
            }
        }
    )
    updated2, new_cuts2 = update_taxi_cut_state(
        updated,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=refunded_adjustments,
        players_map=players,
        taxi_percent=25.0,
        now_ts=350,
    )
    assert new_cuts2 == []
    assert unreimbursed_taxi_cuts(updated2) == []


def test_update_clears_pending_when_dropped_adjustment_removed() -> None:
    """Refund via deleting the Dropped dead-money charge (no negative adj)."""
    adj_key = (
        "200|0018|1.60|Dropped Moss, Le'Veon MIA RB "
        "(Salary: $2.00, Original Contract: 3, Years Left: 3)"
    )
    state = {
        "taxi_seen": {},
        "pending_cuts": [
            {
                "player_id": "17479",
                "franchise_id": "0018",
                "timestamp": 200,
                "dead_money": 1.6,
                "salary": 2.0,
                "years_left": 3,
                "player_label": "Moss, Le'Veon MIA RB",
                "adjustment_key": adj_key,
                "refunded": False,
                "refund_ts": 0,
            }
        ],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=[],
        salary_adjustments=[],
        players_map={"17479": "Moss, Le'Veon MIA RB"},
        taxi_percent=25.0,
        now_ts=400,
    )
    assert new_cuts == []
    assert unreimbursed_taxi_cuts(updated) == []
    moss = updated["pending_cuts"][0]
    assert moss["refunded"] is True
    assert moss["refund_ts"] == 400


def test_update_keeps_pending_while_dropped_adjustment_still_present() -> None:
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0018",
                    "timestamp": "200",
                    "amount": "1.6",
                    "description": (
                        "Dropped Moss, Le'Veon MIA RB "
                        "(Salary: $2.00, Original Contract: 3, Years Left: 3)"
                    ),
                    "id": "0",
                }
            }
        }
    )
    adj_key = (
        "200|0018|1.60|Dropped Moss, Le'Veon MIA RB "
        "(Salary: $2.00, Original Contract: 3, Years Left: 3)"
    )
    state = {
        "taxi_seen": {},
        "pending_cuts": [
            {
                "player_id": "17479",
                "franchise_id": "0018",
                "timestamp": 200,
                "dead_money": 1.6,
                "salary": 2.0,
                "years_left": 3,
                "player_label": "Moss, Le'Veon MIA RB",
                "adjustment_key": adj_key,
                "refunded": False,
                "refund_ts": 0,
            }
        ],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=[],
        salary_adjustments=adjustments,
        players_map={"17479": "Moss, Le'Veon MIA RB"},
        taxi_percent=25.0,
        now_ts=400,
    )
    assert new_cuts == []
    assert len(unreimbursed_taxi_cuts(updated)) == 1


def test_update_detects_moss_via_taxi_dead_money_heuristic() -> None:
    state = {
        "taxi_seen": {},
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": False,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0018",
                    "timestamp": "1784839953",
                    "amount": "1.6",
                    "description": (
                        "Dropped Moss, Le'Veon MIA RB "
                        "(Salary: $2.00, Original Contract: 3, Years Left: 3)"
                    ),
                    "id": "0",
                }
            }
        }
    )
    players = {"17479": "Moss, Le'Veon MIA RB"}
    drops = [
        FreeAgentMove(
            player_id="17479",
            franchise_id="0018",
            timestamp=1784839953,
            is_add=False,
        )
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=adjustments,
        players_map=players,
        taxi_percent=25.0,
        now_ts=1784840000,
    )
    assert len(new_cuts) == 1
    assert new_cuts[0].player_id == "17479"
    assert unreimbursed_taxi_cuts(updated)[0]["dead_money"] == 1.6


def test_formatters() -> None:
    from src.taxi_cut_report import TaxiCutEvent

    cut = TaxiCutEvent(
        player_id="17479",
        franchise_id="0018",
        timestamp=1,
        dead_money=1.6,
        salary=2.0,
        years_left=3,
        player_label="Moss, Le'Veon MIA RB",
        adjustment_key="k",
    )
    alert = format_taxi_cut_alert_text(cut, {"0018": "The Purple Curtain"})
    assert "**The Purple Curtain**" in alert
    assert "Cap hit to refund: $1.60" in alert
    weekly = format_taxi_cut_weekly_report_text(
        [
            {
                "franchise_id": "0018",
                "player_label": "Moss, Le'Veon MIA RB",
                "dead_money": 1.6,
                "salary": 2.0,
                "years_left": 3,
            }
        ],
        {"0018": "The Purple Curtain"},
    )
    assert "Taxi Cut Cap Refunds Pending" in weekly
    assert "refund $1.60" in weekly
    assert format_taxi_cut_weekly_report_text([], {}) == (
        "Taxi Cut Cap Refunds Pending\n\nNo pending taxi-cut cap refunds."
    )
