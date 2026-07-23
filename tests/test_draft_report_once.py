"""Unit tests for draft_report_once (no network)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.draft_notify import DraftPickSelection
from src.draft_report_once import (
    _resolve_only_team_franchise_id,
    _team_grade,
    build_per_team_draft_report_message_post_bodies,
    build_team_draft_report_embeds,
    format_year_draft_pick_token,
    trades_involving_franchise,
    trade_in_draft_report_window,
)


def test_resolve_only_team_franchise_id() -> None:
    fr = {"0001": "Alpha Squad", "0002": "Beta Bots"}
    assert _resolve_only_team_franchise_id("0001", fr) == "0001"
    assert _resolve_only_team_franchise_id("Beta", fr) == "0002"
    assert _resolve_only_team_franchise_id("Squad", fr) == "0001"
    assert _resolve_only_team_franchise_id("a", fr) is None  # substring in both names


def test_format_year_draft_pick_token() -> None:
    assert format_year_draft_pick_token("DP_0_25", 2026) == "Year 2026 Draft Pick 1.26"
    assert format_year_draft_pick_token("DP_1_4", 2026) == "Year 2026 Draft Pick 2.05"


def test_trade_in_draft_report_window() -> None:
    ct = ZoneInfo("America/Chicago")
    ts = datetime(2026, 5, 10, 12, 0, 0, tzinfo=ct).timestamp()
    tx = {"type": "TRADE", "timestamp": str(int(ts)), "franchise": "0001", "franchise2": "0002"}
    assert trade_in_draft_report_window(tx) is True

    before = datetime(2026, 5, 2, 11, 0, 0, tzinfo=ct).timestamp()
    tx2 = {"type": "TRADE", "timestamp": str(int(before)), "franchise": "0001", "franchise2": "0002"}
    assert trade_in_draft_report_window(tx2) is False

    after = datetime(2026, 5, 14, 19, 0, 0, tzinfo=ct).timestamp()
    tx3 = {"type": "TRADE", "timestamp": str(int(after)), "franchise": "0001", "franchise2": "0002"}
    assert trade_in_draft_report_window(tx3) is False


def test_team_grade() -> None:
    assert _team_grade("Glass Joe's Revenge", 5) == ("A", "")
    assert _team_grade("The Purple Curtain", 3) == (
        "F",
        "4th string TE, a couple of backup DLs and some undrafted guys. What a swing and a miss!",
    )
    assert _team_grade("Idle Team", 0) == ("N/A", "")


def test_trades_involving_franchise() -> None:
    ct = ZoneInfo("America/Chicago")
    mid = datetime(2026, 5, 10, 12, 0, 0, tzinfo=ct).timestamp()
    trades = [
        {
            "type": "TRADE",
            "timestamp": str(int(mid)),
            "franchise": "0001",
            "franchise2": "0002",
            "franchise1_gave_up": "DP_0_25",
            "franchise2_gave_up": "DP_1_4",
        }
    ]
    assert len(trades_involving_franchise("0001", trades)) == 1
    assert len(trades_involving_franchise("0002", trades)) == 1
    assert trades_involving_franchise("0099", trades) == []


def test_build_team_draft_report_embeds_structure() -> None:
    franchises = {"0002": "Alpha", "0001": "Beta"}
    s1 = DraftPickSelection("0001", "p1", 1, 23, 1, "1")
    s2 = DraftPickSelection("0001", "p2", 2, 5, 2, "2")
    by_fr = {"0001": [s1, s2]}
    ct = ZoneInfo("America/Chicago")
    mid = datetime(2026, 5, 10, 12, 0, 0, tzinfo=ct).timestamp()
    trades = [
        {
            "type": "TRADE",
            "timestamp": str(int(mid)),
            "franchise": "0001",
            "franchise2": "0002",
            "franchise1_gave_up": "DP_0_25",
            "franchise2_gave_up": "DP_1_4,DP_2_8",
        }
    ]
    players = {"p1": "Bernard, Germie PIT WR", "p2": "Trotter, Josiah TBB LB"}

    beta_embeds = build_team_draft_report_embeds(
        franchise_id="0001",
        franchise_names=franchises,
        draft_selections_by_franchise=by_fr,
        draft_window_trades=trades,
        players=players,
        season_year=2026,
    )
    assert len(beta_embeds) == 1
    be0 = beta_embeds[0]
    assert be0["title"].startswith("Draft report · Beta")
    assert "Selections:" in be0["description"]
    assert "**Overall grade:** **A**" in be0["description"]
    field_names = [f["name"] for f in be0["fields"]]
    assert "Rookie picks" in field_names
    assert "Trade 1" in field_names
    pick_field = next(f["value"] for f in be0["fields"] if f["name"] == "Rookie picks")
    assert "`1.23`" in pick_field
    assert "Bernard, Germie PIT WR" in pick_field
    trade_field = next(f["value"] for f in be0["fields"] if f["name"] == "Trade 1")
    assert "Year 2026 Draft Pick 1.26" in trade_field
    assert "Year 2026 Draft Pick 2.05; Year 2026 Draft Pick 3.09" in trade_field

    alpha_embeds = build_team_draft_report_embeds(
        franchise_id="0002",
        franchise_names=franchises,
        draft_selections_by_franchise=by_fr,
        draft_window_trades=trades,
        players=players,
        season_year=2026,
    )
    ae0 = alpha_embeds[0]
    assert "N/A" in ae0["description"]
    names = [f["name"] for f in ae0["fields"]]
    assert "Rookie picks" in names
    assert "Trade 1" in names


def test_build_per_team_draft_report_message_post_bodies_order() -> None:
    franchises = {"0002": "Alpha", "0001": "Beta"}
    groups = build_per_team_draft_report_message_post_bodies(
        franchise_names=franchises,
        draft_selections_by_franchise={},
        draft_window_trades=[],
        players={},
        season_year=2026,
    )
    assert len(groups) == 2
    assert groups[0][0]["embeds"][0]["title"].startswith("Draft report · Alpha")
    assert groups[1][0]["embeds"][0]["title"].startswith("Draft report · Beta")
