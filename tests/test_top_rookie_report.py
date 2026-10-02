"""Unit tests for the top-rookie-by-round report."""

from __future__ import annotations

from src.draft_notify import DraftPickSelection
from src.top_rookie_report import (
    display_nfl_team,
    display_player_name,
    format_top_rookie_by_round_text,
    last_completed_nfl_week,
    top_rookie_dedupe_key,
    top_rookies_by_round,
)


def _pick(player_id: str, round_number: int, pick_number: int) -> DraftPickSelection:
    return DraftPickSelection(
        franchise_id="0001",
        player_id=player_id,
        round_number=round_number,
        pick_number=pick_number,
        overall_index=pick_number,
        timestamp="",
    )


def test_display_name_and_team_match_report_style() -> None:
    assert display_player_name("Bailey, David") == "David Bailey"
    assert display_nfl_team("TBB") == "TB"
    assert display_nfl_team("NEP") == "NE"
    assert display_nfl_team("JAC") == "JAX"
    assert display_nfl_team("LVR") == "LV"
    assert display_nfl_team("NYJ") == "NYJ"


def test_top_rookie_by_round_uses_points_and_round_average() -> None:
    selections = [
        _pick("bailey", 1, 18),
        _pick("boston", 1, 2),
        _pick("zero", 1, 3),
        _pick("trotter", 2, 5),
        _pick("other", 2, 6),
    ]
    players = {
        "bailey": "Bailey, David NYJ DE",
        "boston": "Boston, Denzel CLE WR",
        "zero": "Zero, Zed FA RB",
        "trotter": "Trotter, Josiah TBB LB",
        "other": "Other, Owen CHI CB",
    }
    points = {"bailey": 51.4, "boston": 40.0, "trotter": 53.7, "other": 0.0}
    leaders = top_rookies_by_round(selections, points, players)
    text = format_top_rookie_by_round_text(leaders)
    assert text.splitlines()[2:] == [
        "Round 1: David Bailey (DE, NYJ) — 51.4 (avg 30.5)",
        "Round 2: Josiah Trotter (LB, TB) — 53.7 (avg 26.9)",
    ]


def test_tied_points_pick_name_order() -> None:
    selections = [_pick("b", 1, 1), _pick("a", 1, 2)]
    players = {"b": "Zulu, Zed NYJ RB", "a": "Alpha, Ann NYJ WR"}
    leaders = top_rookies_by_round(selections, {"a": 10.0, "b": 10.0}, players)
    assert leaders[0].player_id == "a"


def test_last_completed_week_skips_in_progress_week() -> None:
    assert last_completed_nfl_week("4", current_week_is_final=False) == 3
    assert last_completed_nfl_week("4", current_week_is_final=True) == 4
    assert last_completed_nfl_week("1", current_week_is_final=False) is None
    assert top_rookie_dedupe_key("2026-10-06") == "TOP_ROOKIE_BY_ROUND|2026-10-06"
