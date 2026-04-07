"""Unit tests for trade fingerprinting, filtering, and formatting (no MFL network)."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

from src.trade_notify import (
    format_draft_token,
    format_future_pick_token,
    format_trade_text,
    is_processed_trade,
    is_trade_too_old_to_announce,
    load_seen,
    save_seen,
    trade_fingerprint,
    trade_notification_key,
)


def test_trade_fingerprint_includes_comments_and_empty_stable() -> None:
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
    assert trade_fingerprint(with_text) != trade_fingerprint(no_comment)
    assert "1775415606" in trade_fingerprint(base)


def test_trade_notification_key_phase() -> None:
    now = 2_000_000.0
    tx_p = {"expires": str(int(now + 3600)), "timestamp": "1", "franchise": "0001"}
    tx_c = {"expires": str(int(now - 1)), "timestamp": "1", "franchise": "0001"}
    assert trade_notification_key(tx_p, now).endswith("|P")
    assert trade_notification_key(tx_c, now).endswith("|C")


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
