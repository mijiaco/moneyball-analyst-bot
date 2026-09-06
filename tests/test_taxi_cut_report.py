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


def test_parse_dropped_adjustment_details_allows_other_notes() -> None:
    parsed = parse_dropped_adjustment_details(
        "Dropped Trayanum, Chip NYJ RB "
        "(Salary: $1.00, Other Notes: \r, Original Contract: 3, Years Left: 3)"
    )
    assert parsed is not None
    label, salary, years = parsed
    assert label == "Trayanum, Chip NYJ RB"
    assert salary == 1.0
    assert years == 3


def test_update_detects_multiple_taxi_cuts_same_franchise() -> None:
    state = {
        "taxi_seen": {
            "17488": {"franchise_id": "0005", "salary": "1", "last_seen_ts": 100},
            "17489": {"franchise_id": "0005", "salary": "1", "last_seen_ts": 100},
        },
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": [
                    {
                        "franchise_id": "0005",
                        "timestamp": "1786713784",
                        "amount": "0.8",
                        "description": (
                            "Dropped Stewart, Terion KCC RB "
                            "(Salary: $1.00, Original Contract: 3, Years Left: 3)"
                        ),
                        "id": "0",
                    },
                    {
                        "franchise_id": "0005",
                        "timestamp": "1786713804",
                        "amount": "0.8",
                        "description": (
                            "Dropped Trayanum, Chip NYJ RB "
                            "(Salary: $1.00, Other Notes: \r, "
                            "Original Contract: 3, Years Left: 3)"
                        ),
                        "id": "1",
                    },
                ]
            }
        }
    )
    players = {
        "17488": "Stewart, Terion KCC RB",
        "17489": "Trayanum, Chip NYJ RB",
    }
    drops = [
        FreeAgentMove(
            player_id="17488",
            franchise_id="0005",
            timestamp=1786713784,
            is_add=False,
        ),
        FreeAgentMove(
            player_id="17489",
            franchise_id="0005",
            timestamp=1786713804,
            is_add=False,
        ),
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=adjustments,
        players_map=players,
        taxi_percent=25.0,
        now_ts=1786713900,
    )
    assert [cut.player_id for cut in new_cuts] == ["17488", "17489"]
    assert len(unreimbursed_taxi_cuts(updated)) == 2
    assert "17488" not in updated["taxi_seen"]
    assert "17489" not in updated["taxi_seen"]

    from src.taxi_cut_report import group_taxi_cuts_for_alerts, format_taxi_cut_alert_text

    groups = group_taxi_cuts_for_alerts(new_cuts)
    assert len(groups) == 1
    assert len(groups[0]) == 2
    alert = format_taxi_cut_alert_text(
        groups[0], {"0005": "Brute Force & Ignorance"}
    )
    assert "Stewart, Terion KCC RB" in alert
    assert "Trayanum, Chip NYJ RB" in alert
    assert alert.count("Cap hit to refund:") == 2


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


def test_update_skips_active_roster_drop_with_taxi_like_dead_money() -> None:
    """Wilson-style false positive: IR->active then drop, never on taxi."""
    state = {
        "taxi_seen": {},
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0015",
                    "timestamp": "1788132578",
                    "amount": "0.8",
                    "description": (
                        "Dropped Wilson, Cedrick FA WR "
                        "(Salary: $1.00, Original Contract: 3, Years Left: 3)"
                    ),
                    "id": "0",
                }
            }
        }
    )
    assert looks_like_taxi_dead_money(
        dead_money=0.8, salary=1.0, years_left=3, taxi_percent=25.0
    )
    drops = [
        FreeAgentMove(
            player_id="13652",
            franchise_id="0015",
            timestamp=1788132578,
            is_add=False,
        )
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=adjustments,
        players_map={"13652": "Wilson, Cedrick FA WR"},
        taxi_percent=25.0,
        now_ts=1788132600,
    )
    assert new_cuts == []
    assert unreimbursed_taxi_cuts(updated) == []


def test_update_clears_taxi_history_when_player_moves_to_active() -> None:
    state = {
        "taxi_seen": {
            "13652": {"franchise_id": "0015", "salary": "1", "last_seen_ts": 100}
        },
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": {
                    "franchise_id": "0015",
                    "timestamp": "200",
                    "amount": "0.8",
                    "description": (
                        "Dropped Wilson, Cedrick FA WR "
                        "(Salary: $1.00, Original Contract: 3, Years Left: 3)"
                    ),
                    "id": "0",
                }
            }
        }
    )
    drops = [
        FreeAgentMove(
            player_id="13652",
            franchise_id="0015",
            timestamp=200,
            is_add=False,
        )
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=drops,
        salary_adjustments=adjustments,
        players_map={"13652": "Wilson, Cedrick FA WR"},
        taxi_percent=25.0,
        now_ts=250,
        non_taxi_roster_players={"13652": "0015"},
    )
    assert new_cuts == []
    assert "13652" not in updated["taxi_seen"]
    assert unreimbursed_taxi_cuts(updated) == []


def test_update_invalidates_pending_active_drop_using_taxi_history() -> None:
    """Wilson stays pending until TAXI history proves he was never on taxi."""
    wilson_key = (
        "1788132578|0015|0.80|Dropped Wilson, Cedrick FA WR "
        "(Salary: $1.00, Original Contract: 3, Years Left: 3)"
    )
    owens_key = (
        "1788531672|0004|3.20|Dropped Owens, Kejon FA RB "
        "(Salary: $4.00, Original Contract: 3, Years Left: 3)"
    )
    state = {
        "taxi_seen": {},
        "pending_cuts": [
            {
                "player_id": "13652",
                "franchise_id": "0015",
                "timestamp": 1788132578,
                "dead_money": 0.8,
                "salary": 1.0,
                "years_left": 3,
                "player_label": "Wilson, Cedrick FA WR",
                "adjustment_key": wilson_key,
                "refunded": False,
                "refund_ts": 0,
            },
            {
                "player_id": "17708",
                "franchise_id": "0004",
                "timestamp": 1788531672,
                "dead_money": 3.2,
                "salary": 4.0,
                "years_left": 3,
                "player_label": "Owens, Kejon FA RB",
                "adjustment_key": owens_key,
                "refunded": False,
                "refund_ts": 0,
            },
        ],
        "last_weekly_week_key": "",
        "initialized": True,
    }
    adjustments = parse_salary_adjustments(
        {
            "salaryAdjustments": {
                "salaryAdjustment": [
                    {
                        "franchise_id": "0015",
                        "timestamp": "1788132578",
                        "amount": "0.8",
                        "description": (
                            "Dropped Wilson, Cedrick FA WR "
                            "(Salary: $1.00, Original Contract: 3, Years Left: 3)"
                        ),
                        "id": "7",
                    },
                    {
                        "franchise_id": "0004",
                        "timestamp": "1788531672",
                        "amount": "3.2",
                        "description": (
                            "Dropped Owens, Kejon FA RB "
                            "(Salary: $4.00, Original Contract: 3, Years Left: 3)"
                        ),
                        "id": "13",
                    },
                ]
            }
        }
    )
    from src.taxi_cut_report import TaxiSquadMove

    taxi_moves = [
        TaxiSquadMove(
            player_id="17708",
            franchise_id="0004",
            timestamp=1780255946,
            is_demotion=True,
        )
    ]
    updated, new_cuts = update_taxi_cut_state(
        state,
        current_taxi_players={},
        free_agent_drops=[],
        salary_adjustments=adjustments,
        players_map={
            "13652": "Wilson, Cedrick FA WR",
            "17708": "Owens, Kejon FA RB",
        },
        taxi_percent=25.0,
        now_ts=1788650000,
        taxi_squad_moves=taxi_moves,
    )
    assert new_cuts == []
    pending = unreimbursed_taxi_cuts(updated)
    assert len(pending) == 1
    assert pending[0]["player_id"] == "17708"
    wilson = next(
        row for row in updated["pending_cuts"] if row["player_id"] == "13652"
    )
    assert wilson["refunded"] is True
    assert wilson.get("invalidated_not_taxi") is True


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
