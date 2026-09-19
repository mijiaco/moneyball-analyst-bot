"""Unit tests for post-trade roster simulation on trade embeds."""

from __future__ import annotations

from src.trade_roster_check import (
    apply_player_trade_to_rosters,
    apply_player_trade_to_salaries,
    player_ids_from_gave_up,
    post_trade_roster_warning_text,
)


def _league(*, roster_size: int = 2, cap: str = "995") -> dict:
    return {
        "league": {
            "rosterSize": str(roster_size),
            "taxiSquad": "12",
            "injuredReserve": "20",
            "franchises": {
                "franchise": [
                    {"id": "0001", "salaryCapAmount": cap},
                    {"id": "0002", "salaryCapAmount": cap},
                ]
            },
        }
    }


def _rosters_pending() -> dict:
    return {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [
                        {"id": "10", "status": "ROSTER", "salary": "10"},
                        {"id": "11", "status": "ROSTER", "salary": "10"},
                    ],
                },
                {
                    "id": "0002",
                    "player": [
                        {"id": "20", "status": "ROSTER", "salary": "50"},
                    ],
                },
            ]
        }
    }


def test_player_ids_from_gave_up_skips_picks() -> None:
    players = {"20": "Star, Player KC WR", "99": "Other, Player FA RB"}
    assert player_ids_from_gave_up("20,DP_2027_1_0002", players) == ["20"]
    assert player_ids_from_gave_up("Star, Player KC WR", players) == ["20"]


def test_apply_player_trade_to_rosters_moves_pending_player() -> None:
    applied = apply_player_trade_to_rosters(
        _rosters_pending(),
        franchise_a="0002",
        player_ids_a_to_b=["20"],
        franchise_b="0001",
        player_ids_b_to_a=[],
    )
    by_id = {
        row["id"]: {str(player["id"]) for player in row["player"]}
        for row in applied["rosters"]["franchise"]
    }
    assert by_id["0001"] == {"10", "11", "20"}
    assert by_id["0002"] == set()


def test_apply_player_trade_to_rosters_noop_when_already_on_receiver() -> None:
    processed = {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [
                        {"id": "10", "status": "ROSTER"},
                        {"id": "11", "status": "ROSTER"},
                        {"id": "20", "status": "ROSTER"},
                    ],
                },
                {"id": "0002", "player": []},
            ]
        }
    }
    applied = apply_player_trade_to_rosters(
        processed,
        franchise_a="0002",
        player_ids_a_to_b=["20"],
        franchise_b="0001",
        player_ids_b_to_a=[],
    )
    by_id = {
        row["id"]: [str(player["id"]) for player in row["player"]]
        for row in applied["rosters"]["franchise"]
    }
    assert by_id["0001"] == ["10", "11", "20"]
    assert by_id["0002"] == []


def test_apply_player_trade_to_salaries_shifts_amounts() -> None:
    adjusted = apply_player_trade_to_salaries(
        {"0001": 20.0, "0002": 50.0},
        {"0001": {"10": "10", "11": "10"}, "0002": {"20": "50"}},
        franchise_a="0002",
        player_ids_a_to_b=["20"],
        franchise_b="0001",
        player_ids_b_to_a=[],
        rosters_json=_rosters_pending(),
    )
    assert adjusted["0001"] == 70.0
    assert adjusted["0002"] == 0.0


def test_apply_player_trade_to_salaries_skips_when_rosters_already_moved() -> None:
    processed = {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [
                        {"id": "10", "status": "ROSTER", "salary": "10"},
                        {"id": "11", "status": "ROSTER", "salary": "10"},
                        {"id": "20", "status": "ROSTER", "salary": "50"},
                    ],
                },
                {"id": "0002", "player": []},
            ]
        }
    }
    adjusted = apply_player_trade_to_salaries(
        {"0001": 70.0, "0002": 0.0},
        {"0001": {"10": "10", "11": "10", "20": "50"}, "0002": {}},
        franchise_a="0002",
        player_ids_a_to_b=["20"],
        franchise_b="0001",
        player_ids_b_to_a=[],
        rosters_json=processed,
    )
    assert adjusted == {"0001": 70.0, "0002": 0.0}


def test_post_trade_roster_warning_pending_slot_overflow() -> None:
    tx = {
        "franchise": "0002",
        "franchise2": "0001",
        "franchise1_gave_up": "20",
        "franchise2_gave_up": "DP_2027_1_0001",
        "expires": "9999999999",
    }
    text = post_trade_roster_warning_text(
        tx,
        franchise_names={"0001": "Receivers", "0002": "Senders"},
        players_map={"10": "A, Player FA RB", "11": "B, Player FA WR", "20": "C, Player KC WR"},
        rosters_json=_rosters_pending(),
        league_json=_league(roster_size=2),
        injuries_by_id={},
        salary_by_franchise={"0001": 20.0, "0002": 50.0},
        player_salaries={"0001": {"10": "10", "11": "10"}, "0002": {"20": "50"}},
        now_unix=1_700_000_000,
    )
    assert "**Invalid roster after this trade**" in text
    assert "**Receivers**" in text
    assert "Slot limit: active roster 3 (limit 2)" in text


def test_post_trade_roster_warning_processed_does_not_double_salary() -> None:
    tx = {
        "franchise": "0002",
        "franchise2": "0001",
        "franchise1_gave_up": "20",
        "franchise2_gave_up": "",
        "expires": "1",
    }
    processed_rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [
                        {"id": "10", "status": "ROSTER", "salary": "10"},
                        {"id": "11", "status": "ROSTER", "salary": "10"},
                        {"id": "20", "status": "ROSTER", "salary": "50"},
                    ],
                },
                {"id": "0002", "player": []},
            ]
        }
    }
    text = post_trade_roster_warning_text(
        tx,
        franchise_names={"0001": "Receivers", "0002": "Senders"},
        players_map={"10": "A, Player FA RB", "11": "B, Player FA WR", "20": "C, Player KC WR"},
        rosters_json=processed_rosters,
        league_json=_league(roster_size=45, cap="995"),
        injuries_by_id={},
        salary_by_franchise={"0001": 70.0, "0002": 0.0},
        player_salaries={"0001": {"10": "10", "11": "10", "20": "50"}},
        now_unix=1_700_000_000,
    )
    assert text == ""


def test_post_trade_roster_warning_pending_does_not_double_updated_standings() -> None:
    """MFL often updates standings/rosters while expires is still in the veto window."""
    tx = {
        "franchise": "0001",
        "franchise2": "0002",
        "franchise1_gave_up": "10,11,12",
        "franchise2_gave_up": "20,21,22,23,DP_2027_1_0003",
        "expires": "9999999999",
    }
    processed_rosters = {
        "rosters": {
            "franchise": [
                {
                    "id": "0001",
                    "player": [{"id": str(100 + i), "status": "ROSTER"} for i in range(46)],
                },
                {
                    "id": "0002",
                    "player": [
                        {"id": "10", "status": "ROSTER", "salary": "40"},
                        {"id": "11", "status": "ROSTER", "salary": "1"},
                        {"id": "12", "status": "ROSTER", "salary": "75"},
                    ],
                },
            ]
        }
    }
    text = post_trade_roster_warning_text(
        tx,
        franchise_names={"0001": "Joker", "0002": "Brute Force & Ignorance"},
        players_map={
            "10": "Price, Jadarian SEA RB",
            "11": "Williams, C.J. JAC WR",
            "12": "Wilson, Michael ARI WR",
            "20": "Sampson, Dylan CLE RB",
            "21": "Gary, Rashan DAL DE",
            "22": "Kiser, Jack JAC LB",
            "23": "Perkins, Harold ATL LB",
        },
        rosters_json=processed_rosters,
        league_json=_league(roster_size=45, cap="967"),
        injuries_by_id={},
        salary_by_franchise={"0001": 900.0, "0002": 965.95},
        player_salaries={
            "0002": {"10": "40", "11": "1", "12": "75"},
            "0001": {"20": "40", "21": "24", "22": "8", "23": "5"},
        },
        now_unix=1_700_000_000,
    )
    assert "Brute Force" not in text
    assert "Salary cap" not in text
    assert "**Joker**" in text
    assert "Slot limit: active roster 46 (limit 45)" in text
