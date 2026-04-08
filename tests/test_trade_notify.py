"""Unit tests for trade fingerprinting, filtering, and formatting (no MFL network)."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

from src.mfl_client import player_points_by_id
from src.trade_notify import (
    TRADE_BAIT_COMMENTARY_LINES,
    TRADE_COMMENTARY_LINES,
    format_trade_bait_text,
    format_draft_token,
    format_future_pick_token,
    format_trade_text,
    is_trade_bait_too_old_to_announce,
    is_processed_trade,
    is_trade_too_old_to_announce,
    load_seen,
    save_seen,
    random_trade_commentary,
    trade_bait_notification_key,
    trade_dedupe_resolved,
    trade_fingerprint,
    trade_fingerprint_legacy,
    trade_notification_key,
    trade_notification_key_variants,
)


def test_trade_fingerprint_ignores_comments_and_normalizes_asset_order() -> None:
    base = {
        "timestamp": "1775415606",
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,DP_0_21,",
        "franchise2_gave_up": "DP_1_2,",
    }
    no_comment = dict(base)
    empty_comment = dict(base, comments="")
    assert trade_fingerprint(no_comment) == trade_fingerprint(empty_comment)
    with_text = dict(base, comments="hello")
    assert trade_fingerprint(with_text) == trade_fingerprint(no_comment)
    assert "1775415606" in trade_fingerprint(base)
    reordered = dict(
        base,
        franchise1_gave_up="DP_0_21,16257,",
    )
    assert trade_fingerprint(reordered) == trade_fingerprint(base)


def test_trade_fingerprint_legacy_still_varies_with_comments() -> None:
    base = {
        "timestamp": "1775415606",
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,DP_0_21,",
        "franchise2_gave_up": "DP_1_2,",
    }
    assert trade_fingerprint_legacy(dict(base, comments="a")) != trade_fingerprint_legacy(
        dict(base, comments="b")
    )


def test_trade_dedupe_resolved_migrates_legacy_seen_key() -> None:
    now = 2_000_000.0
    tx = {
        "timestamp": "1775415606",
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,DP_0_21,",
        "franchise2_gave_up": "DP_1_2,",
        "comments": "edited later",
    }
    legacy = trade_fingerprint_legacy(dict(tx, comments=""))
    seen = {legacy}
    skip, migrated = trade_dedupe_resolved(tx, seen, now, notify_once_per_trade=True)
    assert skip is True
    assert migrated is True
    assert trade_notification_key(tx, now, include_phase=False) in seen


def test_trade_notification_key_default_is_single_key() -> None:
    now = 2_000_000.0
    tx_p = {"expires": str(int(now + 3600)), "timestamp": "1", "franchise": "0001"}
    tx_c = {"expires": str(int(now - 1)), "timestamp": "1", "franchise": "0001"}
    assert trade_notification_key(tx_p, now) == trade_notification_key(tx_c, now)


def test_trade_notification_key_phase_when_enabled() -> None:
    now = 2_000_000.0
    tx_p = {"expires": str(int(now + 3600)), "timestamp": "1", "franchise": "0001"}
    tx_c = {"expires": str(int(now - 1)), "timestamp": "1", "franchise": "0001"}
    assert trade_notification_key(tx_p, now, include_phase=True).endswith("|P")
    assert trade_notification_key(tx_c, now, include_phase=True).endswith("|C")


def test_trade_notification_key_variants_include_legacy_suffixes() -> None:
    now = 2_000_000.0
    tx = {"expires": str(int(now + 3600)), "timestamp": "1", "franchise": "0001"}
    base, key_p, key_c = trade_notification_key_variants(tx, now)
    assert key_p == f"{base}|P"
    assert key_c == f"{base}|C"


def test_trade_bait_notification_key_stable() -> None:
    tb = {
        "franchise_id": "0007",
        "timestamp": "1775583753",
        "willGiveUp": "16644",
        "inExchangeFor": "Trading for picks",
    }
    key = trade_bait_notification_key(tb)
    assert key.startswith("TB|0007|1775583753|16644|")
    assert "Trading for picks" in key


def test_trade_bait_age_gate() -> None:
    now = 1_000_000.0
    tb = {"timestamp": str(int(now - 90_000))}
    assert is_trade_bait_too_old_to_announce(tb, now, 24) is True
    assert is_trade_bait_too_old_to_announce(tb, now, 0) is False


def test_is_trade_too_old_to_announce() -> None:
    now = 1_000_000.0
    tx = {"timestamp": str(int(now - 100_000))}
    assert is_trade_too_old_to_announce(tx, now, 24) is True
    assert is_trade_too_old_to_announce(tx, now, 0) is False
    assert is_trade_too_old_to_announce(tx, now, 200) is False


def test_is_processed_trade_expires_future() -> None:
    now = 1_000_000.0
    tx_pending = {"expires": str(int(now + 3600))}
    assert is_processed_trade(tx_pending, now) is False
    tx_done = {"expires": str(int(now - 1))}
    assert is_processed_trade(tx_done, now) is True
    assert is_processed_trade({}, now) is True
    assert is_processed_trade({"expires": ""}, now) is True


def test_format_draft_token() -> None:
    assert format_draft_token("DP_0_21", 2026) == "2026 draft R1.22"
    assert format_draft_token("DP_3_13", 2026) == "2026 draft R4.14"
    assert format_draft_token("XYZ", 2026) == "XYZ"


def test_format_future_pick_token() -> None:
    names = {"0022": "Plato's Academy"}
    assert "2027" in format_future_pick_token("FP_0022_2027_1", names)
    assert "Plato" in format_future_pick_token("FP_0022_2027_1", names)


def test_format_trade_text() -> None:
    tx = {
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,DP_0_21,",
        "franchise2_gave_up": "DP_1_2,",
        "comments": "note",
        "timestamp": "1775415606",
    }
    franchises = {"0009": "Team A", "0024": "Team B"}
    players = {"16257": "Felix Anudike-Uzomah KCC DE"}
    text = format_trade_text(tx, franchises, players, 2026)
    assert "**Team A** sends:" in text
    assert "**Team B** sends:" in text
    assert "* Felix" in text
    assert "* 2026 draft R1.22" in text
    assert "* 2026 draft R2.03" in text
    assert "note" in text


def test_random_trade_commentary_uses_expected_repository() -> None:
    assert random_trade_commentary() in TRADE_COMMENTARY_LINES
    assert random_trade_commentary(trade_bait=True) in TRADE_BAIT_COMMENTARY_LINES


def test_format_trade_text_includes_commentary_line(monkeypatch) -> None:
    calls: list[bool] = []

    def fake_commentary(*, trade_bait: bool = False) -> str:
        calls.append(trade_bait)
        return "TEST COMMENTARY"

    monkeypatch.setattr("src.trade_notify.random_trade_commentary", fake_commentary)
    tx = {
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,",
        "franchise2_gave_up": "DP_1_2,",
    }
    franchises = {"0009": "Team A", "0024": "Team B"}
    players = {"16257": "Felix Anudike-Uzomah KCC DE"}
    text = format_trade_text(tx, franchises, players, 2026)
    assert text.startswith("TEST COMMENTARY\n\n")
    assert calls == [False]


def test_format_trade_text_player_salary_bullets() -> None:
    tx = {
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,DP_0_15,",
        "franchise2_gave_up": "DP_1_2,DP_3_13,",
    }
    franchises = {"0009": "Brute Force", "0024": "Chalupa Batmen"}
    players = {"16257": "Greenard, Jonathan MIN DE"}
    salaries = {"0009": {"16257": "35"}}
    text = format_trade_text(tx, franchises, players, 2026, salaries)
    assert "* Greenard, Jonathan MIN DE ($35)" in text
    assert "* 2026 draft R1.16" in text
    assert "* 2026 draft R2.03" in text
    assert "* 2026 draft R4.14" in text


def test_format_trade_text_player_salary_and_points_bullets() -> None:
    tx = {
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "16257,",
        "franchise2_gave_up": "17000,",
    }
    franchises = {"0009": "Team A", "0024": "Team B"}
    players = {
        "16257": "Bowers, Brock LVR TE",
        "17000": "Smith-Njigba, Jaxon SEA WR",
    }
    salaries = {"0009": {"16257": "176"}, "0024": {"17000": "201"}}
    points = {"16257": 213.2, "17000": 367.4}
    text = format_trade_text(tx, franchises, players, 2026, salaries, points)
    assert "* Bowers, Brock LVR TE ($176, 213.20 pts)" in text
    assert "* Smith-Njigba, Jaxon SEA WR ($201, 367.40 pts)" in text


def test_format_trade_text_processed_human_assets_have_bullets_and_salary() -> None:
    tx = {
        "franchise": "0009",
        "franchise2": "0024",
        "franchise1_gave_up": "Campbell, Jack DET LB",
        "franchise2_gave_up": "Hamilton, Kyle BAL S; DP_1_19; DP_2_19",
        "comments": "Any thoughts on this?",
    }
    franchises = {"0009": "Brute Force & Ignorance", "0024": "Cascade Wrecking Crew"}
    players = {
        "101": "Campbell, Jack DET LB",
        "102": "Hamilton, Kyle BAL S",
    }
    salaries = {
        "0009": {"101": "47"},
        "0024": {"102": "28"},
    }
    text = format_trade_text(tx, franchises, players, 2026, salaries)
    assert "* Campbell, Jack DET LB ($47)" in text
    assert "* Hamilton, Kyle BAL S ($28)" in text
    assert "* 2026 draft R2.20" in text
    assert "* 2026 draft R3.20" in text
    assert "_Comments:_ Any thoughts on this?" in text


def test_format_trade_text_salary_fallback_across_franchises() -> None:
    tx = {
        "franchise": "0013",
        "franchise2": "0021",
        "franchise1_gave_up": "",
        "franchise2_gave_up": "15797,",
    }
    franchises = {"0013": "Gallica White Ermines", "0021": "#NAME?"}
    players = {"15797": "Dulcich, Greg MIA TE"}
    salaries = {"0013": {"15797": "22"}}
    text = format_trade_text(tx, franchises, players, 2026, salaries)
    assert "* Dulcich, Greg MIA TE ($22)" in text


def test_format_trade_bait_text_bullets_and_salary() -> None:
    tb = {
        "franchise_id": "0009",
        "willGiveUp": "16257,DP_0_21,",
        "inExchangeFor": "2027 picks",
    }
    franchises = {"0009": "Team A"}
    players = {"16257": "Greenard, Jonathan MIN DE"}
    salaries = {"0009": {"16257": "35"}}
    text = format_trade_bait_text(tb, franchises, players, 2026, salaries)
    assert "**Team A** is offering:" in text
    assert "* Greenard, Jonathan MIN DE ($35)" in text
    assert "* 2026 draft R1.22" in text
    assert "**Looking for:**" in text
    assert "* 2027 picks" in text


def test_format_trade_bait_text_includes_commentary_line(monkeypatch) -> None:
    calls: list[bool] = []

    def fake_commentary(*, trade_bait: bool = False) -> str:
        calls.append(trade_bait)
        return "TEST BAIT COMMENTARY"

    monkeypatch.setattr("src.trade_notify.random_trade_commentary", fake_commentary)
    tb = {
        "franchise_id": "0009",
        "willGiveUp": "16257,",
        "inExchangeFor": "2027 picks",
    }
    franchises = {"0009": "Team A"}
    players = {"16257": "Greenard, Jonathan MIN DE"}
    text = format_trade_bait_text(tb, franchises, players, 2026)
    assert text.startswith("TEST BAIT COMMENTARY\n\n")
    assert calls == [True]


def test_format_trade_bait_text_bullets_salary_and_points() -> None:
    tb = {
        "franchise_id": "0009",
        "willGiveUp": "16257,",
        "inExchangeFor": "2027 picks",
    }
    franchises = {"0009": "Team A"}
    players = {"16257": "Bowers, Brock LVR TE"}
    salaries = {"0009": {"16257": "176"}}
    points = {"16257": 213.2}
    text = format_trade_bait_text(tb, franchises, players, 2026, salaries, points)
    assert "* Bowers, Brock LVR TE ($176, 213.20 pts)" in text


def test_load_save_seen_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "seen.json"
        assert load_seen(path) == set()
        save_seen(path, {"a", "b"})
        assert load_seen(path) == {"a", "b"}


def test_fixture_sample_trade_json() -> None:
    """Fixture-style blob: structure only, no secrets."""
    raw = """
    {
      "type": "TRADE",
      "comments": "",
      "timestamp": "1700000000",
      "franchise2_gave_up": "DP_1_2,",
      "franchise": "0001",
      "franchise2": "0002",
      "expires": "1",
      "franchise1_gave_up": "12345,"
    }
    """
    tx = json.loads(raw)
    assert tx["type"] == "TRADE"
    assert trade_fingerprint(tx)
    assert is_processed_trade(tx, time.time()) is True


def test_player_points_by_id_reads_player_scores_rows() -> None:
    payload = {
        "playerScores": {
            "playerScore": [
                {"id": "16257", "score": "213.20"},
                {"id": "17000", "points": "367.4"},
                {"id": "17001", "fantasyPoints": "12"},
            ]
        }
    }
    points = player_points_by_id(payload)
    assert points["16257"] == 213.2
    assert points["17000"] == 367.4
    assert points["17001"] == 12.0
