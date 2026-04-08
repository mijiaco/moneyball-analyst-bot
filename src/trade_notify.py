"""Trade fingerprinting, processed filter, and human-readable formatting."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.mfl_client import (
    MflClient,
    franchise_names_from_league,
    player_points_by_id,
    player_salaries_by_franchise,
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
)

TRADE_BAIT_COMMENTARY_LINES: tuple[str, ...] = (
    "Trade bait alert: the market is open and somebody is testing everyone's self-control.",
    "I'm hearing this manager is taking calls, texts, and probably carrier pigeons for offers.",
    "Sources: this listing is less 'window shopping' and more 'make me an offer I can't ignore.'",
    "Another trade bait post is up, and yes, the asking price is described as 'respectfully spicy.'",
    "The trade block just got louder - contenders are circling and calculators are out.",
    "Market watch: this team is signaling they're ready to talk business today.",
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
    Uses MFL transaction id when present; otherwise timestamp + franchises + normalized assets.
    """
    tid = tx.get("transaction_id") or tx.get("id")
    if tid is not None and str(tid).strip() != "":
        return f"id|{str(tid).strip()}"
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

    if key in seen or base_stable in seen or key_p in seen or key_c in seen:
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
) -> str:
    tokens = _split_gave_up(gave_up)
    if not tokens:
        return "* (nothing listed)"
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
            if sal is not None and points is not None:
                label = f"{label} (${sal}, {points:.2f} pts)"
            elif sal is not None:
                label = f"{label} (${sal})"
            elif points is not None:
                label = f"{label} ({points:.2f} pts)"
            lines.append(label)
    return "\n".join(f"* {line}" for line in lines)


def format_trade_text(
    tx: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
    salaries_by_franchise: dict[str, dict[str, str]] | None = None,
    points_by_player_id: dict[str, float] | None = None,
) -> str:
    salaries = salaries_by_franchise if salaries_by_franchise is not None else {}
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
    )
    comments = (tx.get("comments") or "").strip()
    header = f"**{name1}** sends:\n{side1}\n**{name2}** sends:\n{side2}"
    if comments:
        safe = comments.replace("`", "'")[:500]
        header += f"\n_Comments:_ {safe}"
    return f"{random_trade_commentary()}\n\n{header}"


def format_trade_bait_text(
    tb: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
    salaries_by_franchise: dict[str, dict[str, str]] | None = None,
    points_by_player_id: dict[str, float] | None = None,
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


async def dry_run(apply_dedupe: bool, *, last_trade_only: bool = False) -> int:
    load_dotenv()
    host = os.environ.get("MFL_HOST", "www45.myfantasyleague.com")
    year = os.environ.get("MFL_YEAR", "2026")
    league_id = os.environ.get("MFL_LEAGUE_ID", "40468")
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

    seen: set[str] = load_seen(seen_path) if apply_dedupe else set()

    client = MflClient(
        host=host,
        year=year,
        league_id=league_id,
        api_key=api_key,
        user_agent=user_agent,
        players_cache_path=players_cache,
    )
    try:
        transactions = await client.fetch_transactions_trade_days(lookback)
        await client.sleep_between_exports()
        league_json = await client.fetch_league()
        await client.sleep_between_exports()
        players = await client.get_players_map()
        await client.sleep_between_exports()
        rosters_json = await client.fetch_rosters()
        await client.sleep_between_exports()
        scores_json = await client.fetch_player_scores_current_year()
    finally:
        await client.aclose()

    franchise_names = franchise_names_from_league(league_json)
    salaries = player_salaries_by_franchise(rosters_json)
    points_by_player_id = player_points_by_id(scores_json)
    now = time.time()

    if last_trade_only:
        trades_only = [tx for tx in transactions if tx.get("type") == "TRADE"]
        if not trades_only:
            print("(dry-run: no TRADE rows in lookback window)", file=sys.stderr)
            return 0
        trades_only.sort(key=lambda t: trade_submitted_unix(t) or 0.0)
        tx = trades_only[-1]
        print(
            format_trade_text(
                tx, franchise_names, players, season_year, salaries, points_by_player_id
            )
        )
        pending = not is_processed_trade(tx, now)
        phase = "pending veto window" if pending else "processed"
        print(
            f"(dry-run: latest trade by MFL timestamp — {phase}; not posted, seen unchanged)",
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
                tx, franchise_names, players, season_year, salaries, points_by_player_id
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch MFL trades and print formatted output.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from MFL and print processed trades (uses .env).",
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
    args = parser.parse_args()
    if args.dry_run:
        if args.last_trade and args.with_dedupe:
            parser.error("--last-trade cannot be used with --with-dedupe")
        raise SystemExit(
            asyncio.run(
                dry_run(
                    apply_dedupe=args.with_dedupe,
                    last_trade_only=args.last_trade,
                )
            )
        )
    parser.print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
