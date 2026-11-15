"""Unit tests for Restricted Free Agent report logic (no network)."""

from __future__ import annotations

from src.google_sheets import (
    parse_top32_player_ids_from_rows,
    rfa_sheet_rows_from_active,
    summary_rows_from_active,
)
from src.rfa_report import (
    active_rfa_fingerprint,
    format_invalid_rfa_claim_text,
    format_rfa_report_text,
    invalid_claim_fingerprint,
    parse_bbid_waiver_claims,
    parse_free_agent_moves,
    update_rfa_state,
)


def test_parse_free_agent_drop_and_add() -> None:
    rows = [
        {
            "type": "FREE_AGENT",
            "franchise": "0001",
            "timestamp": "100",
            "transaction": "|13763,",
        },
        {
            "type": "FREE_AGENT",
            "franchise": "0003",
            "timestamp": "200",
            "transaction": "16161,|",
        },
    ]
    moves = parse_free_agent_moves(rows)
    assert len(moves) == 2
    assert moves[0].player_id == "13763"
    assert moves[0].is_add is False
    assert moves[0].franchise_id == "0001"
    assert moves[1].player_id == "16161"
    assert moves[1].is_add is True


def test_parse_bbid_waiver_claim() -> None:
    rows = [
        {
            "type": "BBID_WAIVER",
            "franchise": "0007",
            "timestamp": "300",
            "transaction": "15151|25|13692,",
        }
    ]
    claims = parse_bbid_waiver_claims(rows)
    assert len(claims) == 1
    assert claims[0].player_id == "15151"
    assert claims[0].bid == 25.0
    assert claims[0].dropped_player_ids == ("13692",)


def test_update_rfa_state_cut_then_claim_remove() -> None:
    state = {
        "active_rfas": {},
        "roster_salary_snapshot": {"0001": {"15151": "40"}},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "seed",
        "seen_invalid_claims": [],
    }
    names = {"0001": "Alpha", "0002": "Beta"}
    after_cut, invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={},
        franchise_names=names,
        free_agent_moves=parse_free_agent_moves(
            [
                {
                    "type": "FREE_AGENT",
                    "franchise": "0001",
                    "timestamp": "500",
                    "transaction": "|15151,",
                }
            ]
        ),
        bbid_claims=[],
        now_ts=600,
    )
    assert changed is True
    assert "15151" in after_cut["active_rfas"]
    assert after_cut["active_rfas"]["15151"]["last_salary"] == "40"
    assert after_cut["active_rfas"]["15151"]["cut_by_franchise_id"] == "0001"
    assert invalid == []

    after_claim, invalid2, changed2 = update_rfa_state(
        after_cut,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={"0002": {"15151": "40"}},
        franchise_names=names,
        free_agent_moves=parse_free_agent_moves(
            [
                {
                    "type": "FREE_AGENT",
                    "franchise": "0002",
                    "timestamp": "700",
                    "transaction": "15151,|",
                }
            ]
        ),
        bbid_claims=[],
        now_ts=800,
    )
    assert "15151" not in after_claim["active_rfas"]
    assert changed2 is True
    assert invalid2 == []


def test_update_rfa_state_invalid_bbid_claim() -> None:
    state = {
        "active_rfas": {
            "15151": {
                "last_salary": "40",
                "cut_ts": 100,
                "cut_by_franchise_id": "0001",
                "cut_by_name": "Alpha",
            }
        },
        "roster_salary_snapshot": {},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "abc",
        "seen_invalid_claims": [],
    }
    updated, invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={"0002": {"15151": "15"}},
        franchise_names={"0001": "Alpha", "0002": "Beta"},
        free_agent_moves=[],
        bbid_claims=parse_bbid_waiver_claims(
            [
                {
                    "type": "BBID_WAIVER",
                    "franchise": "0002",
                    "timestamp": "200",
                    "transaction": "15151|15|",
                }
            ]
        ),
        now_ts=300,
    )
    assert updated["active_rfas"]["15151"]["last_salary"] == "40"
    assert updated["active_rfas"]["15151"]["cut_ts"] == 100
    assert updated["active_rfas"]["15151"]["cut_by_franchise_id"] == "0001"
    assert len(invalid) == 1
    assert invalid[0].winning_bid == 15.0
    assert invalid[0].required_min == 40.0
    key = invalid_claim_fingerprint(invalid[0])
    assert key in updated["seen_invalid_claims"]

    # Second pass should not re-emit the same invalid claim or rewrite the row.
    updated2, invalid2, changed2 = update_rfa_state(
        updated,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={"0002": {"15151": "15"}},
        franchise_names={"0001": "Alpha", "0002": "Beta"},
        free_agent_moves=[],
        bbid_claims=parse_bbid_waiver_claims(
            [
                {
                    "type": "BBID_WAIVER",
                    "franchise": "0002",
                    "timestamp": "200",
                    "transaction": "15151|15|",
                }
            ]
        ),
        now_ts=400,
    )
    assert invalid2 == []
    assert changed2 is False
    assert updated2["active_rfas"]["15151"]["last_salary"] == "40"
    assert updated2["active_rfas"]["15151"]["cut_by_name"] == "Alpha"


def test_reversed_dollar_pickup_does_not_rewrite_rfa_row() -> None:
    state = {
        "active_rfas": {
            "14210": {
                "last_salary": "43",
                "cut_ts": 1794347829,
                "cut_by_franchise_id": "0018",
                "cut_by_name": "Chalupa Batmen",
            }
        },
        "roster_salary_snapshot": {},
        "highest_salary_by_player": {"14210": "43"},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "",
        "seen_invalid_claims": [],
    }
    state["list_fingerprint"] = active_rfa_fingerprint(state["active_rfas"])
    claimed, invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"14210"},
        current_salaries_by_franchise={"0003": {"14210": "1"}},
        franchise_names={"0003": "Gibbs Me the Ring Precious", "0018": "Chalupa Batmen"},
        free_agent_moves=parse_free_agent_moves(
            [
                {
                    "type": "FREE_AGENT",
                    "franchise": "0003",
                    "timestamp": "1794455752",
                    "transaction": "14210,|",
                }
            ]
        ),
        bbid_claims=[],
        players_map={"14210": "Pratt, Germaine IND LB"},
        dropped_salary_rows=[("Pratt, Germaine IND LB", 1.0)],
        now_ts=1794455800,
    )
    assert changed is False
    assert len(invalid) == 1
    assert invalid[0].winning_bid == 1.0
    assert claimed["active_rfas"]["14210"]["cut_by_franchise_id"] == "0018"
    assert claimed["active_rfas"]["14210"]["last_salary"] == "43"

    reversed_state, invalid2, changed2 = update_rfa_state(
        claimed,
        top32_player_ids={"14210"},
        current_salaries_by_franchise={},
        franchise_names={"0003": "Gibbs Me the Ring Precious", "0018": "Chalupa Batmen"},
        free_agent_moves=parse_free_agent_moves(
            [
                {
                    "type": "FREE_AGENT",
                    "franchise": "0003",
                    "timestamp": "1794460361",
                    "transaction": "|14210,",
                }
            ]
        ),
        bbid_claims=[],
        players_map={"14210": "Pratt, Germaine IND LB"},
        dropped_salary_rows=[
            ("Pratt, Germaine IND LB", 43.0),
            ("Pratt, Germaine IND LB", 1.0),
        ],
        now_ts=1794460400,
    )
    row = reversed_state["active_rfas"]["14210"]
    assert changed2 is False
    assert invalid2 == []
    assert row["last_salary"] == "43"
    assert row["cut_ts"] == 1794347829
    assert row["cut_by_name"] == "Chalupa Batmen"


def test_update_rfa_state_bootstrap_no_change() -> None:
    state = {
        "active_rfas": {},
        "roster_salary_snapshot": {},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "",
        "seen_invalid_claims": [],
    }
    updated, invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={"0001": {"15151": "40"}},
        franchise_names={"0001": "Alpha"},
        free_agent_moves=[],
        bbid_claims=[],
        now_ts=100,
    )
    assert changed is False
    assert invalid == []
    assert updated["roster_salary_snapshot"]["0001"]["15151"] == "40"


def test_update_rfa_state_ignores_non_top32_cut() -> None:
    from src.rfa_report import active_rfa_fingerprint

    state = {
        "active_rfas": {},
        "roster_salary_snapshot": {"0001": {"99999": "50"}},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": active_rfa_fingerprint({}),
        "seen_invalid_claims": [],
    }
    updated, invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"15151"},
        current_salaries_by_franchise={},
        franchise_names={"0001": "Alpha"},
        free_agent_moves=[],
        bbid_claims=[],
        now_ts=100,
    )
    assert updated["active_rfas"] == {}
    assert changed is False
    assert invalid == []


def test_format_rfa_report_text_includes_footer() -> None:
    text = format_rfa_report_text(
        {
            "15151": {
                "last_salary": "40",
                "cut_ts": 1700000000,
                "cut_by_name": "Alpha",
            }
        },
        {"15151": "Player One NE QB"},
        as_of_line="As of 2026-07-21",
    )
    assert "Restricted" not in text.split("\n")[0]  # title is embed-only
    assert "top 32 salaries" in text
    assert "Player One NE QB" in text
    assert "$40" in text
    assert "cut by Alpha" in text
    assert "highest salary $40" in text
    assert "highest salary or higher" in text
    assert (
        "Google Sheet: https://docs.google.com/spreadsheets/d/"
        "1FfFa9KMovU-js9wLNTsCnWrRU9L_N4ESro9nKGjx8aM/edit"
    ) in text


def test_reversed_pickup_keeps_highest_dropped_salary() -> None:
    state = {
        "active_rfas": {
            "14210": {
                "last_salary": "1",
                "cut_ts": 1794460361,
                "cut_by_franchise_id": "0003",
                "cut_by_name": "Gibbs Me the Ring Precious",
            }
        },
        "roster_salary_snapshot": {},
        "highest_salary_by_player": {"14210": "1"},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "seed",
        "seen_invalid_claims": [],
    }
    updated, _invalid, changed = update_rfa_state(
        state,
        top32_player_ids={"14210"},
        current_salaries_by_franchise={},
        franchise_names={"0003": "Gibbs Me the Ring Precious"},
        free_agent_moves=[],
        bbid_claims=[],
        players_map={"14210": "Pratt, Germaine IND LB"},
        dropped_salary_rows=[("Pratt, Germaine IND LB", 43.0)],
        now_ts=1794460400,
    )
    assert changed is True
    assert updated["active_rfas"]["14210"]["last_salary"] == "43"
    text = format_rfa_report_text(
        updated["active_rfas"],
        {"14210": "Pratt, Germaine IND LB"},
    )
    assert "highest salary $43" in text
    assert "last salary" not in text


def test_format_rfa_report_empty() -> None:
    text = format_rfa_report_text({}, {})
    assert "No restricted free agents at this time." in text


def test_format_invalid_rfa_claim_text() -> None:
    from src.rfa_report import InvalidRfaClaim

    claim = InvalidRfaClaim(
        player_id="15151",
        franchise_id="0002",
        timestamp=200,
        winning_bid=15.0,
        required_min=40.0,
        last_salary="40",
        cut_by_franchise_id="0001",
        cut_ts=100,
    )
    text = format_invalid_rfa_claim_text(
        claim,
        {"15151": "Star Player"},
        {"0001": "Alpha", "0002": "Beta"},
    )
    assert "Star Player" in text
    assert "Beta" in text
    assert "$15" in text
    assert "$40" in text


def test_parse_top32_player_ids_prefers_id_column() -> None:
    rows = [
        ["Player", "Player ID", "Pos"],
        ["Someone", "15151", "QB"],
        ["Other", "99901", "RB"],
    ]
    found = parse_top32_player_ids_from_rows(
        rows,
        {"15151": "Someone NE QB", "99901": "Other KC RB"},
    )
    assert found == {"15151", "99901"}


def test_parse_top32_player_ids_name_fallback() -> None:
    rows = [
        ["Player", "Pos"],
        ["Mahomes, Patrick", "QB"],
    ]
    found = parse_top32_player_ids_from_rows(
        rows,
        {"4090": "Mahomes, Patrick KC QB"},
    )
    assert found == {"4090"}


def test_parse_top32_skips_blank_row_and_rank_column() -> None:
    rows = [
        [],
        ["Pos Salary Rank", "Position", "Player", "Salary", "Original MFL Team Name"],
        ["1", "RB", "Robinson, Bijan ATL RB", "$221.00", "Team A"],
        ["2", "RB", "Achane, De'Von MIA RB", "$200.00", "Team B"],
    ]
    found = parse_top32_player_ids_from_rows(
        rows,
        {
            "17240": "Robinson, Bijan ATL RB",
            "16648": "Achane, De'Von MIA RB",
        },
    )
    assert found == {"17240", "16648"}


def test_parse_top32_prefers_full_name_team_pos_over_bare_duplicate() -> None:
    rows = [
        ["Position", "Player"],
        ["DT", "Phillips, Jordan MIA DT"],
    ]
    found = parse_top32_player_ids_from_rows(
        rows,
        {
            "12229": "Phillips, Jordan BUF DT",
            "17196": "Phillips, Jordan MIA DT",
        },
    )
    assert found == {"17196"}


def test_rfa_sheet_rows_from_active() -> None:
    rows = rfa_sheet_rows_from_active(
        {
            "15151": {
                "last_salary": "40",
                "cut_ts": 1700000000,
                "cut_by_name": "Alpha",
            }
        },
        {"15151": "Player One"},
    )
    assert rows[0][0] == "Player"
    assert rows[1][0] == "Player One"
    assert rows[1][1] == "15151"
    assert rows[1][-1] == "active"
    assert rows[1][2] == "$40.00"
    assert rows[1][5] == "$40.00"


def test_summary_rows_use_highest_salary_and_top32_rank() -> None:
    rows = summary_rows_from_active(
        {
            "14210": {
                "last_salary": "43",
                "cut_ts": 1794460361,
                "cut_by_name": "Gibbs Me the Ring Precious",
            }
        },
        {"14210": "Pratt, Germaine IND LB"},
        [
            ["Pos Salary Rank", "Position", "Player", "Salary", "Original MFL Team Name"],
            ["21", "LB", "Pratt, Germaine IND LB", "$43.00", "Chalupa Batmen"],
        ],
    )
    assert rows == [
        [
            "1",
            "21",
            "LB",
            "Pratt, Germaine IND LB",
            "$43.00",
            "Chalupa Batmen",
            "2026-11-12",
            "$43.00",
        ]
    ]
