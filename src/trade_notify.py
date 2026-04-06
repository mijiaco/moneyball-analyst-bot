"""Trade fingerprinting, processed filter, and human-readable formatting."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.mfl_client import MflClient, franchise_names_from_league


def trade_fingerprint(tx: dict[str, Any]) -> str:
    """Dedupe identity: teams, assets, MFL timestamp, and comment text (empty if none)."""
    parts = [
        str(tx.get("timestamp", "")),
        str(tx.get("franchise", "")),
        str(tx.get("franchise2", "")),
        str(tx.get("franchise1_gave_up", "")),
        str(tx.get("franchise2_gave_up", "")),
        (str(tx.get("comments") or "").strip()),
    ]
    return "|".join(parts)


def trade_notification_key(tx: dict[str, Any], now_unix: float | None = None) -> str:
    """
    Dedupe key for notifications. Pending (veto window) and processed get different suffixes
    so the same deal can notify twice: proposed, then completed.
    """
    now = now_unix if now_unix is not None else time.time()
    phase = "P" if not is_processed_trade(tx, now) else "C"
    return f"{trade_fingerprint(tx)}|{phase}"


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
    return [p.strip() for p in raw.split(",") if p.strip()]


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
) -> str:
    tokens = _split_gave_up(gave_up)
    if not tokens:
        return "(nothing listed)"
    lines: list[str] = []
    for t in tokens:
        if t.startswith("DP_"):
            lines.append(format_draft_token(t, season_year))
        elif t.startswith("FP_"):
            lines.append(format_future_pick_token(t, franchise_names))
        else:
            lines.append(players.get(t, f"Player id {t}"))
    return "; ".join(lines)


def format_trade_text(
    tx: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
) -> str:
    f1 = str(tx.get("franchise", ""))
    f2 = str(tx.get("franchise2", ""))
    name1 = franchise_names.get(f1, f"Franchise {f1}")
    name2 = franchise_names.get(f2, f"Franchise {f2}")
    side1 = format_asset_list(
        tx.get("franchise1_gave_up"), players, season_year, franchise_names
    )
    side2 = format_asset_list(
        tx.get("franchise2_gave_up"), players, season_year, franchise_names
    )
    comments = (tx.get("comments") or "").strip()
    header = f"**{name1}** sends: {side1}\n**{name2}** sends: {side2}"
    if comments:
        safe = comments.replace("`", "'")[:500]
        header += f"\n_Comments:_ {safe}"
    return header


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


async def dry_run(apply_dedupe: bool) -> int:
    load_dotenv()
    host = os.environ.get("MFL_HOST", "www45.myfantasyleague.com")
    year = os.environ.get("MFL_YEAR", "2026")
    league_id = os.environ.get("MFL_LEAGUE_ID", "40468")
    api_key = os.environ.get("MFL_API_KEY") or None
    user_agent = os.environ.get("MFL_USER_AGENT") or None
    lookback = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
    max_age_hours = float(os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS", "48"))
    announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
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
    finally:
        await client.aclose()

    franchise_names = franchise_names_from_league(league_json)
    now = time.time()
    new_count = 0
    seeded = 0
    for tx in transactions:
        if tx.get("type") != "TRADE":
            continue
        pending = not is_processed_trade(tx, now)
        if pending and not announce_pending:
            continue
        key = trade_notification_key(tx, now)
        if apply_dedupe and key in seen:
            continue
        if is_trade_too_old_to_announce(tx, now, max_age_hours):
            if apply_dedupe:
                seen.add(key)
                seeded += 1
            continue
        print(format_trade_text(tx, franchise_names, players, season_year))
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
    args = parser.parse_args()
    if args.dry_run:
        raise SystemExit(asyncio.run(dry_run(apply_dedupe=args.with_dedupe)))
    parser.print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
