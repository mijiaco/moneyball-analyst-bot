"""Trade fingerprinting, processed filter, and human-readable formatting."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import certifi
import httpx
from dotenv import load_dotenv

from src.discord_env import discord_target_channel_id
from src.mfl_client import (
    MflClient,
    accounting_balance_by_franchise,
    draft_picks_by_franchise,
    franchise_names_from_league,
    player_points_by_id,
    player_contract_years_by_franchise,
    player_salaries_by_franchise,
)
from src.mfl_env import (
    missing_mfl_connect_env_names,
    mfl_connect_env_help_suffix,
    mfl_connect_settings,
)

TRADE_COMMENTARY_LINES: tuple[str, ...] = (
    "What a trade! Just got off the phone with my sources, and this one has the league buzzing.",
    "Sources say this move came together fast - and yes, somebody definitely said 'all in.'",
    "I am told the phones were absolutely melting for this deal. Let's get to the details.",
    "This trade has major '3 a.m. group chat' energy, and the paperwork is already in.",
    "Big board shake-up: this one feels like a chess move with extra chaos.",
    "League sources: one side is buying upside, the other side is buying peace and quiet.",
    "This is the kind of deal that makes every rival manager pretend they saw it coming.",
    "Breaking-ish: this trade just dropped and my coffee suddenly tastes like deadlines.",
    "I'm told negotiations were 'cordial but competitive,' which is reporter-speak for nobody blinked.",
    "Sources close to the situation say spreadsheets were opened. Multiple tabs. Serious business.",
    "Per league chatter, this is not a drill - it's a full roster earthquake with paperwork.",
    "Hot take: this trade is either genius or chaos, and honestly both can be true.",
    "My sources texted me a single word: 'wow.' Then three fire emojis. Then 'details below.'",
    "I am hearing this was discussed in the group chat, the DM, and the parking lot. It's official.",
    "This trade is the fantasy equivalent of a surprise album drop - and the tracklist is wild.",
    "League sources: one side wanted picks, the other wanted peace. Compromise achieved.",
    "I'm told the veto window is about to feel real spicy, but the trade is already in.",
    "Big board update: this deal moved faster than my fantasy football hot takes.",
    "Sources say this one was 'mutually beneficial,' which is code for both sides think they won.",
    "This is a 'screenshot and send to the group chat' trade. Let's get into it.",
    "Per sources, multiple managers were 'notified.' In fantasy, that means everyone is already mad.",
    "I'm hearing this trade was built on trust, cap math, and a little bit of chaos.",
    "League sources: one franchise is reloading, the other is retooling - and I'm here for it.",
    "This is the kind of deal that makes you refresh the page twice because you don't believe it.",
    "Per my sources, the trade call lasted longer than expected because nobody wanted to hang up first.",
    "Sources: this move is 'football decisions,' which is what we say when the numbers are scary.",
    "I'm told both sides are 'excited about the fit,' which is the nicest way to say 'we gambled.'",
    "Breaking-ish: this trade just hit the wire and my fantasy brain is screaming in a good way.",
    "League sources: one side is buying a window, the other is buying flexibility. Let's see the terms.",
    "This trade has 'we talked about it for a week and then it happened in five minutes' energy.",
    "Per sources, the deal is in - and yes, somebody definitely said 'I can live with that.'",
    "I'm hearing this one clears the bar for 'interesting' and lands on 'okay, that's bold.'",
    "Sources: this trade is real, it's processed, and it's about to start a debate thread.",
)

TRADE_BAIT_COMMENTARY_LINES: tuple[str, ...] = (
    "Trade bait alert: the market is open and somebody is testing everyone's self-control.",
    "I'm hearing this manager is taking calls, texts, and probably carrier pigeons for offers.",
    "Sources: this listing is less 'window shopping' and more 'make me an offer I can't ignore.'",
    "Another trade bait post is up, and yes, the asking price is described as 'respectfully spicy.'",
    "The trade block just got louder - contenders are circling and calculators are out.",
    "Market watch: this team is signaling they're ready to talk business today.",
    "Bait dropped: the listing is live and rival managers are pretending they didn't read it yet.",
    "Sources say this manager is open for business - and 'business' is spelled with a capital B.",
    "I'm told the inbox is active and the tone is 'serious inquiries only.'",
    "Trade bait update: the market is officially open, and the cap sheet is officially nervous.",
    "Per sources, this is less 'feelers' and more 'I'm listening, but make it worth my time.'",
    "Another trade block post: the assets are listed, the drama is implied, and the DMs are open.",
    "League sources: one franchise is advertising inventory - and the vultures are circling nicely.",
    "I'm hearing this listing is 'flexible,' which is code for 'bring me something real.'",
    "Market watch: this team is putting names out there and the league is taking notes.",
    "Trade bait alert: the post is up, the thread is about to get spicy, and offers are welcome.",
    "Sources: this listing is basically a neon sign that says 'talk to me.'",
    "Per my sources, the trade block is open and the 'lowball' replies are already being drafted.",
    "I'm told this manager is 'exploring options,' which is the polite version of 'I'm selling.'",
    "Bait alert: the inventory is posted, and the negotiation soundtrack is already playing.",
    "League sources: one side is testing the market - and the market is testing them back.",
    "Trade bait update: the offer sheet is hypothetical, but the intent is very real.",
    "Sources say this post is 'open to conversation,' which means everyone is about to negotiate.",
    "I'm hearing this listing is designed to start conversations - and maybe a little chaos.",
    "Market watch: this team is signaling they're ready to move pieces if the price is right.",
)


def random_trade_commentary(*, trade_bait: bool = False) -> str:
    lines = TRADE_BAIT_COMMENTARY_LINES if trade_bait else TRADE_COMMENTARY_LINES
    return random.choice(lines)


def _normalize_gave_up_field(raw: str | None) -> str:
    """Stable ordering of comma-separated asset tokens (MFL sometimes reorders)."""
    toks = sorted(_split_gave_up(raw))
    return ",".join(toks)


def trade_fingerprint(tx: dict[str, Any]) -> str:
    """
    Dedupe identity for new posts: stable across comment edits and gave_up token order.

    Always uses timestamp + franchises + normalized assets (not MFL transaction_id / id).
    If we keyed by id, a later API change that starts sending ids would not match keys already
    stored in seen_trades.json and every trade in the lookback window would re-announce.
    """
    parts = [
        str(tx.get("timestamp", "")),
        str(tx.get("franchise", "")),
        str(tx.get("franchise2", "")),
        _normalize_gave_up_field(tx.get("franchise1_gave_up")),
        _normalize_gave_up_field(tx.get("franchise2_gave_up")),
    ]
    return "|".join(parts)


def trade_fingerprint_legacy(tx: dict[str, Any]) -> str:
    """Pre-change fingerprint (included comments). Used only to match existing seen_trades.json."""
    parts = [
        str(tx.get("timestamp", "")),
        str(tx.get("franchise", "")),
        str(tx.get("franchise2", "")),
        str(tx.get("franchise1_gave_up", "")),
        str(tx.get("franchise2_gave_up", "")),
        (str(tx.get("comments") or "").strip()),
    ]
    return "|".join(parts)


def _legacy_trade_seen_keys(tx: dict[str, Any]) -> list[str]:
    """Match older seen_trades entries that included comments in the fingerprint."""
    t_blank = dict(tx)
    t_blank["comments"] = ""
    bases = {trade_fingerprint_legacy(tx), trade_fingerprint_legacy(t_blank)}
    keys: list[str] = []
    for b in sorted(bases):
        for k in (b, f"{b}|P", f"{b}|C"):
            if k not in keys:
                keys.append(k)
    return keys


def trade_dedupe_resolved(
    tx: dict[str, Any],
    seen: set[str],
    now_unix: float,
    *,
    notify_once_per_trade: bool,
) -> tuple[bool, bool]:
    """
    Decide whether to skip announcing this trade.
    Returns (skip_announcement, seen_mutated_for_migration).

    If seen contains only a legacy key for this trade, adds the current key and sets mutated True.
    """
    key = trade_notification_key(
        tx, now_unix, include_phase=not notify_once_per_trade
    )
    base_stable, key_p, key_c = trade_notification_key_variants(tx, now_unix)

    # If seen ever contained id|-prefixed keys (short-lived id-based fingerprint), still skip
    # and migrate to the stable key so we do not double-announce.
    tid = tx.get("transaction_id") or tx.get("id")
    if tid is not None and str(tid).strip() != "":
        id_base = f"id|{str(tid).strip()}"
        id_variants = {id_base, f"{id_base}|P", f"{id_base}|C"}
        if seen.intersection(id_variants):
            if key not in seen:
                seen.add(key)
                return (True, True)
            return (True, False)

    if notify_once_per_trade:
        if key in seen or base_stable in seen or key_p in seen or key_c in seen:
            return (True, False)
    else:
        # Phase-aware mode: only suppress duplicates for the same phase key.
        # A legacy base key still means this trade was already announced once.
        if key in seen or base_stable in seen:
            return (True, False)

    for leg in _legacy_trade_seen_keys(tx):
        if leg in seen:
            if key not in seen:
                seen.add(key)
                return (True, True)
            return (True, False)
    return (False, False)


def trade_notification_key(
    tx: dict[str, Any],
    now_unix: float | None = None,
    *,
    include_phase: bool = False,
) -> str:
    """
    Dedupe key for notifications.
    - include_phase=False: one notification key per trade (default, no duplicates).
    - include_phase=True: pending/processed receive distinct suffixes (legacy behavior).
    """
    if not include_phase:
        return trade_fingerprint(tx)
    now = now_unix if now_unix is not None else time.time()
    phase = "P" if not is_processed_trade(tx, now) else "C"
    return f"{trade_fingerprint(tx)}|{phase}"


def trade_notification_key_variants(tx: dict[str, Any], now_unix: float) -> tuple[str, str, str]:
    """
    Return all key variants for backward-compatible dedupe checks:
    (base, pending-suffixed, processed-suffixed).
    """
    base = trade_notification_key(tx, now_unix, include_phase=False)
    return (base, f"{base}|P", f"{base}|C")


def trade_bait_notification_key(tb: dict[str, Any]) -> str:
    parts = [
        str(tb.get("franchise_id", "")),
        str(tb.get("timestamp", "")),
        str(tb.get("willGiveUp", "")),
        str(tb.get("inExchangeFor", "")).strip(),
    ]
    return "TB|" + "|".join(parts)


def trade_bait_updated_unix(tb: dict[str, Any]) -> float | None:
    raw = tb.get("timestamp")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def is_trade_bait_too_old_to_announce(
    tb: dict[str, Any], now_unix: float, max_age_hours: float
) -> bool:
    if max_age_hours <= 0:
        return False
    ts = trade_bait_updated_unix(tb)
    if ts is None:
        return False
    return (now_unix - ts) > max_age_hours * 3600.0


def trade_submitted_unix(tx: dict[str, Any]) -> float | None:
    raw = tx.get("timestamp")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def is_trade_too_old_to_announce(
    tx: dict[str, Any], now_unix: float, max_age_hours: float
) -> bool:
    """If max_age_hours <= 0, never too old (no age gate)."""
    if max_age_hours <= 0:
        return False
    ts = trade_submitted_unix(tx)
    if ts is None:
        return False
    return (now_unix - ts) > max_age_hours * 3600.0


def is_processed_trade(tx: dict[str, Any], now_unix: float | None = None) -> bool:
    """False while trade is still in veto/pending window (expires in the future)."""
    now = now_unix if now_unix is not None else time.time()
    expires_raw = tx.get("expires")
    if expires_raw is None or expires_raw == "":
        return True
    try:
        expires_at = float(expires_raw)
    except (TypeError, ValueError):
        return True
    return expires_at <= now


def _split_gave_up(raw: str | None) -> list[str]:
    if not raw:
        return []
    text = raw.strip()
    if not text:
        return []
    if ";" in text:
        return [p.strip() for p in text.split(";") if p.strip()]
    comma_parts = [p.strip() for p in text.split(",") if p.strip()]
    if not comma_parts:
        return []
    # Only split on commas for machine-style tokens.
    if all(
        part.startswith("DP_")
        or part.startswith("FP_")
        or re.fullmatch(r"\d+", part) is not None
        for part in comma_parts
    ):
        return comma_parts
    return [text.rstrip(",")]


def trade_sending_side_includes_own_future_year_pick(
    gave_up: str | None,
    sending_franchise_id: str,
    target_year: int,
) -> bool:
    """
    True if gave_up includes an FP token for this franchise's own pick in ``target_year``
    (``FP_<franchise_id>_<year>_<round>``).
    """
    tokens = _split_gave_up(gave_up)
    send = str(sending_franchise_id).strip()
    for t in tokens:
        if not t.startswith("FP_"):
            continue
        rest = t[3:].split("_")
        if len(rest) < 3:
            continue
        orig_fr, year_s, _rnd_s = rest[0], rest[1], rest[2]
        if orig_fr != send:
            continue
        try:
            year = int(year_s)
        except ValueError:
            continue
        if year == target_year:
            return True
    return False


def _build_player_name_index(players: dict[str, str]) -> dict[str, str]:
    """
    Build a normalized player label -> player id index.
    Keep only unique labels to avoid ambiguous salary lookups.
    """
    by_name: dict[str, str] = {}
    duplicates: set[str] = set()
    for pid, label in players.items():
        key = " ".join(str(label).strip().split()).casefold()
        if not key:
            continue
        existing_pid = by_name.get(key)
        if existing_pid is None:
            by_name[key] = pid
            continue
        if existing_pid != pid:
            duplicates.add(key)
    for dup in duplicates:
        by_name.pop(dup, None)
    return by_name


def format_future_pick_token(token: str, franchise_names: dict[str, str]) -> str:
    """
    Future / conditional pick tokens: FP_<franchise_id>_<year>_<round>
    (e.g. FP_0022_2027_1 -> 2027 R1 from that franchise).
    """
    if not token.startswith("FP_"):
        return token
    rest = token[3:].split("_")
    if len(rest) < 3:
        return token
    orig_fr, year_s, rnd_s = rest[0], rest[1], rest[2]
    team = franchise_names.get(orig_fr, f"Franchise {orig_fr}")
    try:
        year = int(year_s)
        rnd = int(rnd_s)
    except ValueError:
        return token
    return f"{year} R{rnd} (from {team})"


def _salary_for_player_on_franchise(
    salaries_by_franchise: dict[str, dict[str, str]],
    franchise_id: str,
    player_token: str,
) -> str | None:
    fr_map = salaries_by_franchise.get(franchise_id) or {}
    if player_token in fr_map:
        return fr_map[player_token]
    if not player_token.isdigit():
        return None
    want = int(player_token)
    for k, v in fr_map.items():
        if k.isdigit() and int(k) == want:
            return v
    # Fallback: some MFL rows do not align player asset side with current
    # sender roster; if this player has a unique salary league-wide, use it.
    found_salary: str | None = None
    for other_fr_map in salaries_by_franchise.values():
        if player_token in other_fr_map:
            salary = other_fr_map[player_token]
        else:
            salary = None
            for k, v in other_fr_map.items():
                if k.isdigit() and int(k) == want:
                    salary = v
                    break
        if salary is None:
            continue
        if found_salary is None:
            found_salary = salary
            continue
        if salary != found_salary:
            return None
    if found_salary is not None:
        return found_salary
    return None


def _contract_year_for_player_on_franchise(
    contract_years_by_franchise: dict[str, dict[str, str]],
    franchise_id: str,
    player_token: str,
) -> str | None:
    fr_map = contract_years_by_franchise.get(franchise_id) or {}
    if player_token in fr_map:
        return fr_map[player_token]
    if not player_token.isdigit():
        return None
    want = int(player_token)
    for k, v in fr_map.items():
        if k.isdigit() and int(k) == want:
            return v
    found_cy: str | None = None
    for other_fr_map in contract_years_by_franchise.values():
        if player_token in other_fr_map:
            cy = other_fr_map[player_token]
        else:
            cy = None
            for k, v in other_fr_map.items():
                if k.isdigit() and int(k) == want:
                    cy = v
                    break
        if cy is None:
            continue
        if found_cy is None:
            found_cy = cy
            continue
        if cy != found_cy:
            return None
    return found_cy


def _format_player_asset_suffix(
    sal: str | None,
    points: float | None,
    contract_yr: str | None,
) -> str | None:
    """Build `$N sal / M pts / K yr` with whole-number salary and points."""
    parts: list[str] = []
    if sal is not None:
        sal_s = str(sal).strip()
        try:
            sal_int = round(float(sal_s.replace(",", "")))
            parts.append(f"${sal_int} sal")
        except (TypeError, ValueError):
            parts.append(f"${sal_s} sal")
    if points is not None:
        parts.append(f"{round(points)} pts")
    if contract_yr is not None and str(contract_yr).strip() != "":
        parts.append(f"{str(contract_yr).strip()} yr")
    if not parts:
        return None
    return " / ".join(parts)


def format_draft_token(token: str, season_year: int) -> str:
    """
    MFL draft pick tokens use zero-based round and pick in DP_r_p
    (e.g. DP_0_21 -> season_year round 1 pick 22).
    """
    if not token.startswith("DP_"):
        return token
    body = token[3:]
    parts = body.split("_")
    if len(parts) < 2:
        return token
    try:
        round_0 = int(parts[0])
        pick_0 = int(parts[1])
    except ValueError:
        return token
    round_1 = round_0 + 1
    pick_1 = pick_0 + 1
    return f"{season_year} draft R{round_1}.{pick_1:02d}"


def format_asset_list(
    gave_up: str | None,
    players: dict[str, str],
    season_year: int,
    franchise_names: dict[str, str],
    sending_franchise_id: str,
    salaries_by_franchise: dict[str, dict[str, str]],
    points_by_player_id: dict[str, float] | None = None,
    player_name_to_id: dict[str, str] | None = None,
    contract_years_by_franchise: dict[str, dict[str, str]] | None = None,
) -> str:
    tokens = _split_gave_up(gave_up)
    if not tokens:
        return "* (nothing listed)"
    contract_map = (
        contract_years_by_franchise if contract_years_by_franchise is not None else {}
    )
    lines: list[str] = []
    for t in tokens:
        if t.startswith("DP_"):
            lines.append(format_draft_token(t, season_year))
        elif t.startswith("FP_"):
            lines.append(format_future_pick_token(t, franchise_names))
        else:
            resolved_token = t
            label = players.get(t)
            if label is None and player_name_to_id is not None:
                name_key = " ".join(t.strip().split()).casefold()
                resolved_token = player_name_to_id.get(name_key, t)
                label = players.get(resolved_token)
            if label is None:
                label = t if not t.isdigit() else f"Player id {t}"
            sal = _salary_for_player_on_franchise(
                salaries_by_franchise, sending_franchise_id, resolved_token
            )
            points = None
            if points_by_player_id is not None:
                points = points_by_player_id.get(str(resolved_token))
            cy = _contract_year_for_player_on_franchise(
                contract_map, sending_franchise_id, resolved_token
            )
            suffix = _format_player_asset_suffix(sal, points, cy)
            if suffix is not None:
                label = f"{label} ({suffix})"
            lines.append(label)
    return "\n".join(f"* {line}" for line in lines)


def format_trade_text(
    tx: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
    salaries_by_franchise: dict[str, dict[str, str]] | None = None,
    points_by_player_id: dict[str, float] | None = None,
    contract_years_by_franchise: dict[str, dict[str, str]] | None = None,
    *,
    accounting_balance_by_franchise: dict[str, float] | None = None,
    unpaid_accounting_threshold: float = 250.0,
    unpaid_future_pick_year: int = 2027,
) -> str:
    salaries = salaries_by_franchise if salaries_by_franchise is not None else {}
    contract_years = (
        contract_years_by_franchise if contract_years_by_franchise is not None else {}
    )
    player_name_to_id = _build_player_name_index(players)
    f1 = str(tx.get("franchise", ""))
    f2 = str(tx.get("franchise2", ""))
    name1 = franchise_names.get(f1, f"Franchise {f1}")
    name2 = franchise_names.get(f2, f"Franchise {f2}")
    side1 = format_asset_list(
        tx.get("franchise1_gave_up"),
        players,
        season_year,
        franchise_names,
        f1,
        salaries,
        points_by_player_id,
        player_name_to_id,
        contract_years,
    )
    side2 = format_asset_list(
        tx.get("franchise2_gave_up"),
        players,
        season_year,
        franchise_names,
        f2,
        salaries,
        points_by_player_id,
        player_name_to_id,
        contract_years,
    )
    comments = (tx.get("comments") or "").strip()
    header = f"**{name1}** sends:\n{side1}\n**{name2}** sends:\n{side2}"
    if comments:
        safe = comments.replace("`", "'")[:500]
        header += f"\n_Comments:_ {safe}"
    out_parts: list[str] = [random_trade_commentary(), header]
    if accounting_balance_by_franchise is not None:
        for fid, tname, gave in (
            (f1, name1, tx.get("franchise1_gave_up")),
            (f2, name2, tx.get("franchise2_gave_up")),
        ):
            if not trade_sending_side_includes_own_future_year_pick(
                gave, fid, unpaid_future_pick_year
            ):
                continue
            bal = float(accounting_balance_by_franchise.get(fid, 0.0))
            if bal < unpaid_accounting_threshold:
                out_parts.append(
                    f"Invalid trade. {tname} hasn't paid for 2027 picks yet."
                )
    return "\n\n".join(out_parts)


def format_trade_bait_text(
    tb: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
    salaries_by_franchise: dict[str, dict[str, str]] | None = None,
    points_by_player_id: dict[str, float] | None = None,
    contract_years_by_franchise: dict[str, dict[str, str]] | None = None,
) -> str:
    salaries = salaries_by_franchise if salaries_by_franchise is not None else {}
    player_name_to_id = _build_player_name_index(players)
    fid = str(tb.get("franchise_id", ""))
    team_name = franchise_names.get(fid, f"Franchise {fid}")
    give_up = format_asset_list(
        tb.get("willGiveUp"),
        players,
        season_year,
        franchise_names,
        fid,
        salaries,
        points_by_player_id,
        player_name_to_id,
        contract_years_by_franchise,
    )
    wants = (tb.get("inExchangeFor") or "").strip()
    body = f"**{team_name}** is offering:\n{give_up}"
    if wants:
        safe_wants = wants.replace("`", "'")[:500]
        body += f"\n**Looking for:**\n* {safe_wants}"
    return f"{random_trade_commentary(trade_bait=True)}\n\n{body}"


def load_seen(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return set()
    if isinstance(data, list):
        return {str(x) for x in data}
    return set()


def save_seen(path: Path, seen: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(seen)), encoding="utf-8")
    os.replace(tmp, path)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def top_trader_counts(
    transactions: list[dict[str, Any]],
    *,
    dedupe_by_trade: bool = True,
) -> Counter[str]:
    """
    Count trades per franchise id.
    Each TRADE increments both participating franchises by one.
    """
    counts: Counter[str] = Counter()
    seen_trade_fingerprints: set[str] = set()
    for tx in transactions:
        if tx.get("type") != "TRADE":
            continue
        if dedupe_by_trade:
            fingerprint = trade_fingerprint(tx)
            if fingerprint in seen_trade_fingerprints:
                continue
            seen_trade_fingerprints.add(fingerprint)
        franchise_1 = str(tx.get("franchise", "")).strip()
        franchise_2 = str(tx.get("franchise2", "")).strip()
        if franchise_1:
            counts[franchise_1] += 1
        if franchise_2:
            counts[franchise_2] += 1
    return counts


def format_top_traders_text(
    counts: Counter[str],
    franchise_names: dict[str, str],
    *,
    title: str = "Top Traders This Year",
    week_of_label: str | None = None,
    disclaimer: str | None = None,
    top_n: int = 10,
) -> str:
    if top_n <= 0:
        top_n = len(counts)
    ranked = sorted(
        counts.items(),
        key=lambda item: (-item[1], franchise_names.get(item[0], f"Franchise {item[0]}").casefold()),
    )
    if not ranked:
        lines = [title]
        if week_of_label:
            lines.append(f"Week of {week_of_label}")
        if disclaimer:
            lines.append(disclaimer)
        lines.extend(["", "No trades found in the selected window."])
        return "\n".join(lines)
    lines = [title]
    if week_of_label:
        lines.append(f"Week of {week_of_label}")
    if disclaimer:
        lines.append(disclaimer)
    lines.append("")
    for index, (franchise_id, trade_count) in enumerate(ranked[:top_n], start=1):
        team_name = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        trade_label = "Trade" if trade_count == 1 else "Trades"
        lines.append(f"{index}. {team_name} - {trade_count} {trade_label}")
    return "\n".join(lines)


def current_season_lookback_days(season_year: int) -> int:
    """
    Return DAYS window that covers Jan 1 through now for the provided season year.
    """
    now_utc = datetime.now(timezone.utc)
    season_start = datetime(season_year, 1, 1, tzinfo=timezone.utc)
    if now_utc <= season_start:
        return 1
    delta_days = (now_utc - season_start).days + 1
    return max(1, delta_days)


_CURRENT_YEAR_DRAFT_PICK = re.compile(
    r"^\s*Year\s+(?P<year>\d{4})\s+Draft\s+Pick\s+(?P<slot>[\d.]+)\s*$",
    re.IGNORECASE,
)
_ROUND_SLOT = re.compile(r"^\s*Round\s+(?P<slot>[\d.]+)\s*$", re.IGNORECASE)
_FUTURE_YEAR_ROUND_FROM = re.compile(
    r"^\s*Year\s+(?P<year>\d{4})\s+Round\s+(?P<round>\d+)\s+(?:Draft\s+Pick\s+)?from\s+(?P<from>.+?)\s*$",
    re.IGNORECASE,
)


def _format_compact_current_picks(
    current_lines: list[str],
    *,
    report_season_year: int | None,
) -> list[str]:
    if not current_lines:
        return ["* Current picks: None"]
    by_year: dict[str, list[str]] = {}
    round_only_slots: list[str] = []
    unmatched: list[str] = []
    for raw in current_lines:
        line = raw.strip()
        m_year = _CURRENT_YEAR_DRAFT_PICK.match(line)
        if m_year:
            y, slot = m_year.group("year"), m_year.group("slot")
            by_year.setdefault(y, []).append(slot)
            continue
        m_round = _ROUND_SLOT.match(line)
        if m_round and report_season_year is not None:
            by_year.setdefault(str(report_season_year), []).append(m_round.group("slot"))
            continue
        if m_round:
            round_only_slots.append(m_round.group("slot"))
            continue
        unmatched.append(line)
    out: list[str] = []
    for year in sorted(by_year.keys()):
        slots = by_year[year]
        out.append(f"* {year} Picks: {', '.join(slots)}")
    if round_only_slots:
        out.append(f"* Picks: {', '.join(round_only_slots)}")
    for entry in unmatched:
        out.append(f"* {entry}")
    if not out:
        return ["* Current picks: None"]
    return out


def _format_compact_future_picks(future_lines: list[str]) -> list[str]:
    if not future_lines:
        return ["* Future picks: None"]
    by_year: dict[str, list[str]] = {}
    unmatched: list[str] = []
    for raw in future_lines:
        line = raw.strip()
        m = _FUTURE_YEAR_ROUND_FROM.match(line)
        if m:
            y = m.group("year")
            r = m.group("round")
            frm = m.group("from").strip()
            by_year.setdefault(y, []).append(f"{r} ({frm})")
            continue
        unmatched.append(line)
    out: list[str] = []
    for year in sorted(by_year.keys()):
        parts = by_year[year]
        out.append(f"* {year} Picks: {', '.join(parts)}")
    for entry in unmatched:
        out.append(f"* Future: {entry}")
    if not out:
        return ["* Future picks: None"]
    return out


def format_draft_picks_report_text(
    franchise_names: dict[str, str],
    current_year_picks_by_franchise: dict[str, list[str]],
    future_year_picks_by_franchise: dict[str, list[str]],
    *,
    title: str = "Draft Picks Report (Current + Future)",
    report_season_year: int | None = None,
) -> str:
    team_ids = set(franchise_names.keys())
    team_ids.update(current_year_picks_by_franchise.keys())
    team_ids.update(future_year_picks_by_franchise.keys())
    sorted_team_ids = sorted(
        team_ids,
        key=lambda fid: franchise_names.get(fid, f"Franchise {fid}").casefold(),
    )

    if not sorted_team_ids:
        return f"{title}\n\nNo franchise data found."

    lines = [title, ""]
    for franchise_id in sorted_team_ids:
        team_name = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        current_lines = current_year_picks_by_franchise.get(franchise_id, [])
        future_lines = future_year_picks_by_franchise.get(franchise_id, [])
        lines.append(f"{team_name}")
        lines.extend(
            _format_compact_current_picks(
                current_lines,
                report_season_year=report_season_year,
            )
        )
        lines.extend(_format_compact_future_picks(future_lines))
        lines.append("")

    return "\n".join(lines).rstrip()


def cap_space_available_by_franchise(league_json: dict[str, Any]) -> dict[str, float]:
    """
    franchise id -> currently available cap room from league.franchises[*].bbidAvailableBalance.
    """
    league_block = league_json.get("league") or league_json
    franchises_block = league_block.get("franchises") or {}
    franchise_rows_raw = franchises_block.get("franchise")
    franchise_rows: list[dict[str, Any]]
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []

    out: dict[str, float] = {}
    for row in franchise_rows:
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        raw_amount = row.get("bbidAvailableBalance")
        if raw_amount is None or str(raw_amount).strip() == "":
            # Fallback for leagues that do not expose a dedicated available-balance field.
            raw_amount = row.get("salaryCapAmount")
        if raw_amount is None or str(raw_amount).strip() == "":
            continue
        raw_text = str(raw_amount).replace(",", "").strip()
        try:
            out[str(franchise_id)] = float(raw_text)
        except ValueError:
            continue
    return out


def format_cap_space_report_text(
    franchise_names: dict[str, str],
    cap_space_by_franchise: dict[str, float],
    *,
    title: str = "Cap Space Available by Team",
) -> str:
    ranked = sorted(
        cap_space_by_franchise.items(),
        key=lambda item: (
            -item[1],
            franchise_names.get(item[0], f"Franchise {item[0]}").casefold(),
        ),
    )
    if not ranked:
        return f"{title}\n\nNo cap-space data found."
    lines = [title, ""]
    for index, (franchise_id, cap_space) in enumerate(ranked, start=1):
        team_name = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        lines.append(f"{index}. {team_name} - ${round(cap_space):,} Available")
    return "\n".join(lines)


def roster_slot_counts_by_franchise(
    rosters_json: dict[str, Any],
) -> dict[str, dict[str, int]]:
    """
    franchise id -> {active, taxi, ir} player counts based on roster player status values.
    """
    rosters_block = rosters_json.get("rosters") or {}
    franchise_rows_raw = rosters_block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []

    out: dict[str, dict[str, int]] = {}
    for franchise_row in franchise_rows:
        franchise_id = franchise_row.get("id")
        if franchise_id is None:
            continue
        franchise_id_str = str(franchise_id)
        players_raw = franchise_row.get("player") or []
        if isinstance(players_raw, list):
            players = [player for player in players_raw if isinstance(player, dict)]
        elif isinstance(players_raw, dict):
            players = [players_raw]
        else:
            players = []
        active_count = 0
        taxi_count = 0
        ir_count = 0
        for player in players:
            status = str(player.get("status") or "").strip().upper()
            if "IR" in status:
                ir_count += 1
            elif "TAXI" in status:
                taxi_count += 1
            else:
                active_count += 1
        out[franchise_id_str] = {
            "active": active_count,
            "taxi": taxi_count,
            "ir": ir_count,
        }
    return out


def format_roster_breakdown_report_text(
    franchise_names: dict[str, str],
    slot_counts_by_franchise: dict[str, dict[str, int]],
    *,
    title: str = "Players by Team (Active / Taxi / IR / $ Cap Remain)",
    cap_available_by_franchise: dict[str, float] | None = None,
) -> str:
    team_ids = set(franchise_names.keys()) | set(slot_counts_by_franchise.keys())
    if not team_ids:
        return f"{title}\n\nNo roster data found."
    ranking: list[tuple[str, int, int, int, int]] = []
    for team_id in team_ids:
        counts = slot_counts_by_franchise.get(team_id, {})
        active_count = int(counts.get("active", 0))
        taxi_count = int(counts.get("taxi", 0))
        ir_count = int(counts.get("ir", 0))
        total_players = active_count + taxi_count + ir_count
        ranking.append((team_id, total_players, active_count, taxi_count, ir_count))
    ranking.sort(
        key=lambda row: (
            -row[1],
            franchise_names.get(row[0], f"Franchise {row[0]}").casefold(),
        )
    )
    lines = [title, ""]
    for index, (team_id, _total_players, active_count, taxi_count, ir_count) in enumerate(
        ranking, start=1
    ):
        team_name = franchise_names.get(team_id, f"Franchise {team_id}")
        if cap_available_by_franchise is None:
            lines.append(f"{index}) {team_name} - {active_count} / {taxi_count} / {ir_count}")
            continue
        cap_raw = cap_available_by_franchise.get(team_id)
        if cap_raw is None:
            cap_part = "—"
        else:
            cap_part = f"${int(round(cap_raw))}"
        lines.append(
            f"{index}) {team_name} - {active_count} / {taxi_count} / {ir_count} / {cap_part}"
        )
    return "\n".join(lines).rstrip()


def traded_own_future_pick_rounds_by_franchise(
    league_json: dict[str, Any],
    *,
    target_year: int,
    total_rounds: int,
) -> dict[str, list[int]]:
    """
    franchise id -> sorted list of own pick rounds traded for target year.
    Uses league.franchises[*].future_draft_picks (FP_<franchise_id>_<year>_<round>).
    """
    if total_rounds <= 0:
        return {}
    league_block = league_json.get("league") or league_json
    franchises_block = league_block.get("franchises") or {}
    franchise_rows_raw = franchises_block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []

    expected_rounds = set(range(1, total_rounds + 1))
    out: dict[str, list[int]] = {}
    for row in franchise_rows:
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        franchise_id_text = str(franchise_id)
        owned_own_rounds: set[int] = set()
        raw_tokens = str(row.get("future_draft_picks") or "").strip()
        if raw_tokens:
            for token in raw_tokens.split(","):
                token_text = token.strip()
                if not token_text.startswith("FP_"):
                    continue
                parts = token_text[3:].split("_")
                if len(parts) < 3:
                    continue
                token_franchise_id, token_year_text, token_round_text = (
                    parts[0],
                    parts[1],
                    parts[2],
                )
                if token_franchise_id != franchise_id_text:
                    continue
                try:
                    token_year = int(token_year_text)
                    token_round = int(token_round_text)
                except ValueError:
                    continue
                if token_year != target_year:
                    continue
                if token_round in expected_rounds:
                    owned_own_rounds.add(token_round)
        traded_rounds = sorted(expected_rounds - owned_own_rounds)
        if traded_rounds:
            out[franchise_id_text] = traded_rounds
    return out


def format_traded_future_picks_with_accounting_report_text(
    franchise_names: dict[str, str],
    traded_rounds_by_franchise: dict[str, list[int]],
    accounting_balance_by_franchise: dict[str, float],
    *,
    target_year: int,
    accounting_balance_under: float = 250.0,
) -> str:
    title = "Unpaid Owners / Traded Picks"
    if not traded_rounds_by_franchise:
        return (
            f"{title}\n\nNo teams have traded their own {target_year} picks."
        )
    filtered: dict[str, list[int]] = {}
    for franchise_id, rounds in traded_rounds_by_franchise.items():
        balance = float(accounting_balance_by_franchise.get(franchise_id, 0.0))
        if balance < accounting_balance_under:
            filtered[franchise_id] = rounds
    if not filtered:
        return (
            f"{title}\n\n"
            f"No teams qualify (traded own {target_year} picks and accounting balance "
            f"under ${accounting_balance_under:,.2f})."
        )
    ordered_ids = sorted(
        filtered.keys(),
        key=lambda franchise_id: franchise_names.get(
            franchise_id, f"Franchise {franchise_id}"
        ).casefold(),
    )
    lines = [
        title,
        "",
        f"Team Name | {target_year} Own Picks Traded | Accounting Balance",
        "--- | --- | ---",
    ]
    for franchise_id in ordered_ids:
        team_name = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        traded_rounds = filtered.get(franchise_id, [])
        rounds_text = ", ".join(str(round_number) for round_number in traded_rounds)
        accounting_balance = float(accounting_balance_by_franchise.get(franchise_id, 0.0))
        lines.append(f"{team_name} | {rounds_text} | ${accounting_balance:,.2f}")
    return "\n".join(lines)


async def dry_run(
    apply_dedupe: bool,
    *,
    last_trade_only: bool = False,
    top_traders_only: bool = False,
    top_traders_limit: int = 10,
    draft_picks_report_only: bool = False,
    cap_space_report_only: bool = False,
    roster_breakdown_report_only: bool = False,
    traded_2027_picks_report_only: bool = False,
) -> int:
    _dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_dotenv_path, override=True)
    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        print(
            f"Missing required env: {miss}. {mfl_connect_env_help_suffix()}",
            file=sys.stderr,
        )
        return 1
    host, year, league_id = connect
    api_key = os.environ.get("MFL_API_KEY") or None
    user_agent = os.environ.get("MFL_USER_AGENT") or None
    lookback = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
    max_age_hours = float(os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS", "48"))
    announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
    notify_once_per_trade = env_bool("MFL_NOTIFY_ONCE_PER_TRADE", True)
    season_year = int(year)
    data_dir = Path(__file__).resolve().parent.parent / "data"
    seen_path = data_dir / "seen_trades.json"
    players_cache = data_dir / "players_cache.json"
    current_year_lookback = current_season_lookback_days(season_year)

    seen: set[str] = load_seen(seen_path) if apply_dedupe else set()

    client = MflClient(
        host=host,
        year=year,
        league_id=league_id,
        api_key=api_key,
        user_agent=user_agent,
        players_cache_path=players_cache,
    )
    assets_json: dict[str, Any] = {}
    try:
        if traded_2027_picks_report_only:
            league_json = await client.fetch_league()
            await client.sleep_between_exports()
            accounting_json = await client.fetch_accounting()
            franchise_names = franchise_names_from_league(league_json)
            total_rounds = int(os.environ.get("MFL_DRAFT_ROUNDS", "6"))
            traded_rounds = traded_own_future_pick_rounds_by_franchise(
                league_json,
                target_year=2027,
                total_rounds=total_rounds,
            )
            accounting_totals = accounting_balance_by_franchise(accounting_json)
            unpaid_threshold = float(os.environ.get("MFL_UNPAID_ACCOUNTING_THRESHOLD", "250"))
            print(
                format_traded_future_picks_with_accounting_report_text(
                    franchise_names,
                    traded_rounds,
                    accounting_totals,
                    target_year=2027,
                    accounting_balance_under=unpaid_threshold,
                )
            )
            print(
                "(dry-run: unpaid/traded picks report; balances from TYPE=accounting export)",
                file=sys.stderr,
            )
            return 0

        transactions_lookback = current_year_lookback if top_traders_only else lookback
        transactions = await client.fetch_transactions_trade_days(transactions_lookback)
        await client.sleep_between_exports()
        league_json = await client.fetch_league()
        if draft_picks_report_only:
            await client.sleep_between_exports()
            assets_json = await client.fetch_assets()
        await client.sleep_between_exports()
        players = await client.get_players_map()
        await client.sleep_between_exports()
        rosters_json = await client.fetch_rosters()
        await client.sleep_between_exports()
        scores_json = await client.fetch_player_scores_current_year()
        await client.sleep_between_exports()
        accounting_json = await client.fetch_accounting()
    finally:
        await client.aclose()

    franchise_names = franchise_names_from_league(league_json)
    accounting_totals = accounting_balance_by_franchise(accounting_json)
    unpaid_threshold = float(os.environ.get("MFL_UNPAID_ACCOUNTING_THRESHOLD", "250"))
    salaries = player_salaries_by_franchise(rosters_json)
    contract_years = player_contract_years_by_franchise(rosters_json)
    points_by_player_id = player_points_by_id(scores_json)
    now = time.time()

    if top_traders_only:
        counts = top_trader_counts(transactions, dedupe_by_trade=True)
        print(
            format_top_traders_text(
                counts,
                franchise_names,
                title="Top Traders This Year",
                disclaimer=(
                    "Disclaimer: this includes some test trades from early in the year."
                ),
                top_n=top_traders_limit,
            )
        )
        print(
            (
                "(dry-run: top traders based on current year lookback: "
                f"{current_year_lookback} day window)"
            ),
            file=sys.stderr,
        )
        return 0

    if draft_picks_report_only:
        current_by_franchise, future_by_franchise = draft_picks_by_franchise(assets_json)
        print(
            format_draft_picks_report_text(
                franchise_names,
                current_by_franchise,
                future_by_franchise,
                report_season_year=int(year),
            )
        )
        print("(dry-run: draft picks report generated from assets export)", file=sys.stderr)
        return 0

    if cap_space_report_only:
        print(
            format_cap_space_report_text(
                franchise_names,
                cap_space_available_by_franchise(league_json),
            )
        )
        print("(dry-run: cap-space report generated from league export)", file=sys.stderr)
        return 0

    if roster_breakdown_report_only:
        print(
            format_roster_breakdown_report_text(
                franchise_names,
                roster_slot_counts_by_franchise(rosters_json),
                cap_available_by_franchise=cap_space_available_by_franchise(league_json),
            )
        )
        print("(dry-run: roster breakdown report generated from rosters export)", file=sys.stderr)
        return 0

    if last_trade_only:
        trades_only = [tx for tx in transactions if tx.get("type") == "TRADE"]
        if not trades_only:
            print("(dry-run: no TRADE rows in lookback window)", file=sys.stderr)
            return 0
        trades_only.sort(key=lambda t: trade_submitted_unix(t) or 0.0)
        tx = trades_only[-1]
        print(
            format_trade_text(
                tx,
                franchise_names,
                players,
                season_year,
                salaries,
                points_by_player_id,
                contract_years,
                accounting_balance_by_franchise=accounting_totals,
                unpaid_accounting_threshold=unpaid_threshold,
            )
        )
        pending = not is_processed_trade(tx, now)
        phase = "pending veto window" if pending else "processed"
        print(
            f"(dry-run: latest trade by upstream timestamp — {phase}; not posted, seen unchanged)",
            file=sys.stderr,
        )
        return 0

    new_count = 0
    seeded = 0
    for tx in transactions:
        if tx.get("type") != "TRADE":
            continue
        pending = not is_processed_trade(tx, now)
        if pending and not announce_pending:
            continue
        key = trade_notification_key(
            tx, now, include_phase=not notify_once_per_trade
        )
        if apply_dedupe:
            skip, migrated = trade_dedupe_resolved(
                tx, seen, now, notify_once_per_trade=notify_once_per_trade
            )
            if migrated:
                seeded += 1
            if skip:
                continue
        if is_trade_too_old_to_announce(tx, now, max_age_hours):
            if apply_dedupe:
                seen.add(key)
                seeded += 1
            continue
        print(
            format_trade_text(
                tx,
                franchise_names,
                players,
                season_year,
                salaries,
                points_by_player_id,
                contract_years,
                accounting_balance_by_franchise=accounting_totals,
                unpaid_accounting_threshold=unpaid_threshold,
            )
        )
        print("---")
        new_count += 1
        if apply_dedupe:
            seen.add(key)
    if apply_dedupe and (new_count or seeded):
        save_seen(seen_path, seen)
    print(
        f"(dry-run: {new_count} trade(s) printed, {seeded} old key(s) seeded to seen)",
        file=sys.stderr,
    )
    return 0


DISCORD_API_V10 = "https://discord.com/api/v10"


def _chunk_text_for_discord_embeds(text: str, max_len: int = 3900) -> list[str]:
    """Split long report text on paragraph boundaries for Discord embed limits."""
    sections = text.split("\n\n")
    chunks: list[str] = []
    current = ""
    for section in sections:
        candidate = section if not current else f"{current}\n\n{section}"
        if len(candidate) <= max_len:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(section) <= max_len:
            current = section
            continue
        lines = section.splitlines()
        line_chunk = ""
        for line in lines:
            line_candidate = line if not line_chunk else f"{line_chunk}\n{line}"
            if len(line_candidate) <= max_len:
                line_chunk = line_candidate
            else:
                if line_chunk:
                    chunks.append(line_chunk)
                line_chunk = line
        current = line_chunk
    if current:
        chunks.append(current)
    return chunks


async def _discord_post_embed(
    *,
    token: str,
    channel_id: str,
    title: str,
    description: str,
    color: int,
) -> bool:
    headers = {
        "Authorization": f"Bot {token}",
        "User-Agent": "DiscordBot (https://github.com/discord/discord-api-docs, 1.0)",
        "Content-Type": "application/json",
    }
    url = f"{DISCORD_API_V10}/channels/{channel_id}/messages"
    body_json = {"embeds": [{"title": title, "description": description, "color": color}]}
    async with httpx.AsyncClient(verify=certifi.where(), timeout=60.0, headers=headers) as dclient:
        response = await dclient.post(url, json=body_json)
        if response.status_code == 429:
            try:
                retry_after = float(response.json().get("retry_after", 2))
            except (json.JSONDecodeError, TypeError, ValueError):
                retry_after = 2.0
            await asyncio.sleep(retry_after)
            response = await dclient.post(url, json=body_json)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            print(
                f"Discord API error: {response.status_code} {response.text}",
                file=sys.stderr,
            )
            return False
    return True


async def post_roster_breakdown_embed_to_discord() -> int:
    """Post one roster + cap embed; channel from ``discord_target_channel_id()``."""
    _dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_dotenv_path, override=True)
    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    channel_id = discord_target_channel_id()
    if not token:
        print("DISCORD_BOT_TOKEN is required.", file=sys.stderr)
        return 1
    if not channel_id:
        print(
            "Set TEST_DISCORD_CHANNEL_ID, DISCORD_CHANNEL_ID, or PROD_DISCORD_CHANNEL_ID.",
            file=sys.stderr,
        )
        return 1

    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        print(
            f"Missing required env: {miss}. {mfl_connect_env_help_suffix()}",
            file=sys.stderr,
        )
        return 1

    host, year, league_id = connect
    api_key = os.environ.get("MFL_API_KEY") or None
    user_agent = os.environ.get("MFL_USER_AGENT") or None
    data_dir = Path(__file__).resolve().parent.parent / "data"
    players_cache = data_dir / "players_cache.json"

    client = MflClient(
        host=host,
        year=year,
        league_id=league_id,
        api_key=api_key,
        user_agent=user_agent,
        players_cache_path=players_cache,
    )
    try:
        league_json = await client.fetch_league()
        await client.sleep_between_exports()
        rosters_json = await client.fetch_rosters()
    finally:
        await client.aclose()

    franchise_names = franchise_names_from_league(league_json)
    roster_report = format_roster_breakdown_report_text(
        franchise_names,
        roster_slot_counts_by_franchise(rosters_json),
        cap_available_by_franchise=cap_space_available_by_franchise(league_json),
    )
    now_et = datetime.now(ZoneInfo("America/New_York"))
    as_of_line = f"As of {now_et.strftime('%Y-%m-%d %I:%M %p ET')}"
    body_text = roster_report.split("\n\n", 1)[1] if "\n\n" in roster_report else roster_report
    description = f"{as_of_line}\n\n{body_text}"
    if len(description) > 4096:
        description = description[:4093] + "..."
    embed_title = "Players by Team (Active / Taxi / IR / $ Cap Remain)"
    color = 3447003
    ok = await _discord_post_embed(
        token=token,
        channel_id=channel_id,
        title=embed_title,
        description=description,
        color=color,
    )
    if not ok:
        return 1

    print("Posted roster breakdown embed to Discord.", file=sys.stderr)
    return 0


async def post_draft_picks_embeds_to_discord() -> int:
    """Post draft picks report (chunked); channel from ``discord_target_channel_id()``."""
    _dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_dotenv_path, override=True)
    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    channel_id = discord_target_channel_id()
    if not token:
        print("DISCORD_BOT_TOKEN is required.", file=sys.stderr)
        return 1
    if not channel_id:
        print(
            "Set TEST_DISCORD_CHANNEL_ID, DISCORD_CHANNEL_ID, or PROD_DISCORD_CHANNEL_ID.",
            file=sys.stderr,
        )
        return 1

    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        print(
            f"Missing required env: {miss}. {mfl_connect_env_help_suffix()}",
            file=sys.stderr,
        )
        return 1

    host, year, league_id = connect
    season_year = int(year)
    api_key = os.environ.get("MFL_API_KEY") or None
    user_agent = os.environ.get("MFL_USER_AGENT") or None
    data_dir = Path(__file__).resolve().parent.parent / "data"
    players_cache = data_dir / "players_cache.json"

    client = MflClient(
        host=host,
        year=year,
        league_id=league_id,
        api_key=api_key,
        user_agent=user_agent,
        players_cache_path=players_cache,
    )
    try:
        league_json = await client.fetch_league()
        await client.sleep_between_exports()
        assets_json = await client.fetch_assets()
    finally:
        await client.aclose()

    franchise_names = franchise_names_from_league(league_json)
    current_map, future_map = draft_picks_by_franchise(assets_json)
    draft_report = format_draft_picks_report_text(
        franchise_names,
        current_map,
        future_map,
        report_season_year=season_year,
    )
    now_et = datetime.now(ZoneInfo("America/New_York"))
    as_of_line = f"As of {now_et.strftime('%Y-%m-%d %I:%M %p ET')}"
    body_text = draft_report.split("\n\n", 1)[1] if "\n\n" in draft_report else draft_report
    chunks = _chunk_text_for_discord_embeds(body_text, max_len=3900)
    total = len(chunks)
    color = 5793266
    for index, chunk in enumerate(chunks, start=1):
        title = "Draft Picks Report (Current + Future)"
        if total > 1:
            title = f"Draft Picks Report (Current + Future) ({index}/{total})"
        description = f"{as_of_line}\n\n{chunk}"
        if len(description) > 4096:
            description = description[:4093] + "..."
        ok = await _discord_post_embed(
            token=token,
            channel_id=channel_id,
            title=title,
            description=description,
            color=color,
        )
        if not ok:
            return 1
        if index < total:
            await asyncio.sleep(0.6)

    print(
        f"Posted draft picks report to Discord ({total} embed(s)).",
        file=sys.stderr,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch league trades and print formatted output.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from configured league and print processed trades (uses .env).",
    )
    parser.add_argument(
        "--with-dedupe",
        action="store_true",
        help="With --dry-run, skip fingerprints already in data/seen_trades.json and update it.",
    )
    parser.add_argument(
        "--last-trade",
        action="store_true",
        help="With --dry-run, print only the newest TRADE in the lookback (format/API check).",
    )
    parser.add_argument(
        "--top-traders",
        action="store_true",
        help="With --dry-run, print top franchises by trade count in the lookback window.",
    )
    parser.add_argument(
        "--top-limit",
        type=int,
        default=0,
        help="With --top-traders, number of ranked teams to print (0 = full list).",
    )
    parser.add_argument(
        "--draft-picks-report",
        action="store_true",
        help="With --dry-run, print each franchise's current and future draft picks.",
    )
    parser.add_argument(
        "--cap-space-report",
        action="store_true",
        help="With --dry-run, print cap space available by team.",
    )
    parser.add_argument(
        "--roster-breakdown-report",
        action="store_true",
        help="With --dry-run, print active/taxi/IR player counts by team.",
    )
    parser.add_argument(
        "--traded-2027-picks-report",
        action="store_true",
        help=(
            "With --dry-run, print Unpaid Owners / Traded Picks: teams that traded own 2027 picks "
            "with accounting balance under MFL_UNPAID_ACCOUNTING_THRESHOLD (default 250)."
        ),
    )
    parser.add_argument(
        "--post-roster-breakdown-discord",
        action="store_true",
        help=(
            "Post one roster + cap embed to Discord (channel from TEST_DISCORD_CHANNEL_ID "
            "or DISCORD_CHANNEL_ID / PROD_DISCORD_CHANNEL_ID; uses .env)."
        ),
    )
    parser.add_argument(
        "--post-draft-picks-discord",
        action="store_true",
        help=(
            "Post draft picks report embed(s) to Discord (same channel resolution as "
            "--post-roster-breakdown-discord)."
        ),
    )
    args = parser.parse_args()

    mode_flags = (
        args.dry_run,
        args.with_dedupe,
        args.last_trade,
        args.top_traders,
        args.draft_picks_report,
        args.cap_space_report,
        args.roster_breakdown_report,
        args.traded_2027_picks_report,
    )
    if args.post_roster_breakdown_discord and args.post_draft_picks_discord:
        parser.error(
            "use only one of --post-roster-breakdown-discord or --post-draft-picks-discord"
        )
    if args.post_roster_breakdown_discord:
        if any(mode_flags):
            parser.error(
                "--post-roster-breakdown-discord cannot be combined with --dry-run "
                "or other report flags"
            )
        raise SystemExit(asyncio.run(post_roster_breakdown_embed_to_discord()))
    if args.post_draft_picks_discord:
        if any(mode_flags):
            parser.error(
                "--post-draft-picks-discord cannot be combined with --dry-run "
                "or other report flags"
            )
        raise SystemExit(asyncio.run(post_draft_picks_embeds_to_discord()))

    if args.dry_run:
        if args.last_trade and args.with_dedupe:
            parser.error("--last-trade cannot be used with --with-dedupe")
        if args.last_trade and args.top_traders:
            parser.error("--last-trade cannot be used with --top-traders")
        if args.with_dedupe and args.top_traders:
            parser.error("--with-dedupe cannot be used with --top-traders")
        if args.last_trade and args.draft_picks_report:
            parser.error("--last-trade cannot be used with --draft-picks-report")
        if args.with_dedupe and args.draft_picks_report:
            parser.error("--with-dedupe cannot be used with --draft-picks-report")
        if args.top_traders and args.draft_picks_report:
            parser.error("--top-traders cannot be used with --draft-picks-report")
        if args.last_trade and args.cap_space_report:
            parser.error("--last-trade cannot be used with --cap-space-report")
        if args.with_dedupe and args.cap_space_report:
            parser.error("--with-dedupe cannot be used with --cap-space-report")
        if args.top_traders and args.cap_space_report:
            parser.error("--top-traders cannot be used with --cap-space-report")
        if args.draft_picks_report and args.cap_space_report:
            parser.error("--draft-picks-report cannot be used with --cap-space-report")
        if args.last_trade and args.roster_breakdown_report:
            parser.error("--last-trade cannot be used with --roster-breakdown-report")
        if args.with_dedupe and args.roster_breakdown_report:
            parser.error("--with-dedupe cannot be used with --roster-breakdown-report")
        if args.top_traders and args.roster_breakdown_report:
            parser.error("--top-traders cannot be used with --roster-breakdown-report")
        if args.draft_picks_report and args.roster_breakdown_report:
            parser.error("--draft-picks-report cannot be used with --roster-breakdown-report")
        if args.cap_space_report and args.roster_breakdown_report:
            parser.error("--cap-space-report cannot be used with --roster-breakdown-report")
        if args.last_trade and args.traded_2027_picks_report:
            parser.error("--last-trade cannot be used with --traded-2027-picks-report")
        if args.with_dedupe and args.traded_2027_picks_report:
            parser.error("--with-dedupe cannot be used with --traded-2027-picks-report")
        if args.top_traders and args.traded_2027_picks_report:
            parser.error("--top-traders cannot be used with --traded-2027-picks-report")
        if args.draft_picks_report and args.traded_2027_picks_report:
            parser.error("--draft-picks-report cannot be used with --traded-2027-picks-report")
        if args.cap_space_report and args.traded_2027_picks_report:
            parser.error("--cap-space-report cannot be used with --traded-2027-picks-report")
        if args.roster_breakdown_report and args.traded_2027_picks_report:
            parser.error("--roster-breakdown-report cannot be used with --traded-2027-picks-report")
        raise SystemExit(
            asyncio.run(
                dry_run(
                    apply_dedupe=args.with_dedupe,
                    last_trade_only=args.last_trade,
                    top_traders_only=args.top_traders,
                    top_traders_limit=args.top_limit,
                    draft_picks_report_only=args.draft_picks_report,
                    cap_space_report_only=args.cap_space_report,
                    roster_breakdown_report_only=args.roster_breakdown_report,
                    traded_2027_picks_report_only=args.traded_2027_picks_report,
                )
            )
        )
    parser.print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
