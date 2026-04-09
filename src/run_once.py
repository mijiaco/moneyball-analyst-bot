"""One poll cycle and outbound REST posts (no gateway). For scheduled runners."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import httpx
from dotenv import load_dotenv

from src.mfl_client import MflClient, draft_picks_by_franchise, franchise_names_from_league
from src.mfl_env import (
    missing_mfl_connect_env_names,
    mfl_connect_env_help_suffix,
    mfl_connect_settings,
)
from src.trade_notify import (
    current_season_lookback_days,
    env_bool,
    format_draft_picks_report_text,
    format_top_traders_text,
    load_seen,
    save_seen,
    top_trader_counts,
)
from src.trade_poll_core import poll_trades_for_new_messages

logger = logging.getLogger(__name__)

DISCORD_API = "https://discord.com/api/v10"


async def _post_embed_return_ok(
    client: httpx.AsyncClient,
    channel_id: str,
    payload,
) -> bool:
    body = {
        "embeds": [
            {
                "title": payload.title,
                "description": payload.description,
                "color": payload.color,
            }
        ]
    }
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    response = await client.post(url, json=body)
    if response.status_code == 429:
        try:
            retry_after = float(response.json().get("retry_after", 2))
        except (json.JSONDecodeError, TypeError, ValueError):
            retry_after = 2.0
        await asyncio.sleep(retry_after)
        response = await client.post(url, json=body)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.exception("Discord API error: %s %s", response.status_code, response.text)
        return False
    return True


def _weekly_reports_state_path(data_dir: Path) -> Path:
    return data_dir / "reports_state.json"


def _load_last_weekly_reports_week_key(state_path: Path) -> str:
    if not state_path.is_file():
        return ""
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    week_key = payload.get("last_weekly_reports_week_key")
    return str(week_key).strip() if week_key is not None else ""


def _save_last_weekly_reports_week_key(state_path: Path, week_key: str) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"last_weekly_reports_week_key": week_key}, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp, state_path)


def _current_week_key_et(now_et: datetime) -> str:
    iso_year, iso_week, _ = now_et.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _is_weekly_reports_due(now_et: datetime) -> bool:
    return now_et.weekday() == 4 and now_et.hour >= 17


def _chunk_text_by_sections(text: str, max_len: int = 3900) -> list[str]:
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


async def _async_main() -> int:
    load_dotenv()
    token = os.environ.get("DISCORD_BOT_TOKEN")
    channel_id = os.environ.get("DISCORD_CHANNEL_ID")
    if not token or not channel_id:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            logger.error(
                "DISCORD_BOT_TOKEN and/or DISCORD_CHANNEL_ID are empty. "
                "Add them as repository secrets: Settings → Secrets and variables → Actions "
                "(names DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID must match exactly)."
            )
        else:
            logger.error("DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required (e.g. in .env)")
        return 1

    data_dir = Path(__file__).resolve().parent.parent / "data"
    seen_path = data_dir / "seen_trades.json"
    players_cache = data_dir / "players_cache.json"
    reports_state_path = _weekly_reports_state_path(data_dir)
    seen = load_seen(seen_path)

    lookback = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
    _max_age_raw = (os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS") or "").strip()
    announce_max_age = float(_max_age_raw) if _max_age_raw else 48.0
    announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
    notify_once_per_trade = env_bool("MFL_NOTIFY_ONCE_PER_TRADE", True)
    announce_trade_bait = env_bool("MFL_ANNOUNCE_TRADE_BAIT", True)
    weekly_reports_enabled = env_bool("MFL_WEEKLY_REPORTS_ENABLED", True)
    weekly_reports_include_draft_picks = env_bool(
        "MFL_WEEKLY_REPORTS_INCLUDE_DRAFT_PICKS", True
    )

    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        logger.error(
            "Missing required env: %s. %s",
            miss,
            mfl_connect_env_help_suffix(),
        )
        return 1
    host, year, league_id = connect
    season_year = int(year)

    mfl = MflClient(
        host=host,
        year=year,
        league_id=league_id,
        api_key=os.environ.get("MFL_API_KEY") or None,
        user_agent=os.environ.get("MFL_USER_AGENT") or None,
        players_cache_path=players_cache,
    )
    updated_reports_state = False
    try:
        pending_posts, updated = await poll_trades_for_new_messages(
            mfl,
            seen,
            lookback_days=lookback,
            announce_pending=announce_pending,
            announce_max_age_hours=announce_max_age,
            season_year=season_year,
            notify_once_per_trade=notify_once_per_trade,
            announce_trade_bait=announce_trade_bait,
        )
        weekly_report_payloads: list[tuple[str, str, int]] = []
        if weekly_reports_enabled:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            if _is_weekly_reports_due(now_et):
                current_week_key = _current_week_key_et(now_et)
                last_week_key = _load_last_weekly_reports_week_key(reports_state_path)
                if current_week_key != last_week_key:
                    lookback_days = current_season_lookback_days(season_year)
                    transactions = await mfl.fetch_transactions_trade_days(lookback_days)
                    await mfl.sleep_between_exports()
                    league_json = await mfl.fetch_league()
                    franchise_names = franchise_names_from_league(league_json)
                    top_traders_text = format_top_traders_text(
                        top_trader_counts(transactions, dedupe_by_trade=True),
                        franchise_names,
                        title="Top Traders This Year",
                        week_of_label=now_et.date().isoformat(),
                        disclaimer=(
                            "Disclaimer: this includes some test trades from early in the year."
                        ),
                        top_n=0,
                    )
                    top_description = (
                        top_traders_text.split("\n\n", 1)[1]
                        if "\n\n" in top_traders_text
                        else top_traders_text
                    )
                    weekly_report_payloads.append(
                        ("Top Traders This Year", top_description, 15844367)
                    )

                    if weekly_reports_include_draft_picks:
                        await mfl.sleep_between_exports()
                        assets_json = await mfl.fetch_assets()
                        current_map, future_map = draft_picks_by_franchise(assets_json)
                        draft_report_text = format_draft_picks_report_text(
                            franchise_names,
                            current_map,
                            future_map,
                        )
                        draft_chunks = _chunk_text_by_sections(draft_report_text, max_len=3900)
                        total_chunks = len(draft_chunks)
                        for index, chunk in enumerate(draft_chunks, start=1):
                            chunk_title = "Draft Picks Report (Current + Future)"
                            if total_chunks > 1:
                                chunk_title = (
                                    "Draft Picks Report (Current + Future) "
                                    f"({index}/{total_chunks})"
                                )
                            weekly_report_payloads.append((chunk_title, chunk, 5793266))

                    for report_title, report_description, report_color in weekly_report_payloads:
                        pending_posts.append(
                            (
                                f"WEEKLY_REPORT|{current_week_key}|{report_title}",
                                type(
                                    "Payload",
                                    (),
                                    {
                                        "title": report_title,
                                        "description": report_description,
                                        "color": report_color,
                                    },
                                )(),
                            )
                        )
                    _save_last_weekly_reports_week_key(reports_state_path, current_week_key)
                    updated_reports_state = True
    except httpx.HTTPStatusError as exc:
        logger.exception("Upstream HTTP error: %s", exc)
        return 1
    except Exception:
        logger.exception("Upstream fetch failed")
        return 1
    finally:
        await mfl.aclose()

    headers = {
        "Authorization": f"Bot {token}",
        "User-Agent": "DiscordBot (https://github.com/discord/discord-api-docs, 1.0)",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(
        verify=certifi.where(),
        timeout=60.0,
        headers=headers,
    ) as dclient:
        for key, payload in pending_posts:
            ok = await _post_embed_return_ok(dclient, channel_id, payload)
            if not ok:
                if updated:
                    save_seen(seen_path, seen)
                return 1
            seen.add(key)
            updated = True

    if updated:
        save_seen(seen_path, seen)
        logger.info("Updated dedupe state (%s keys)", len(seen))
    if updated_reports_state:
        logger.info("Updated weekly reports state")
    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    raise SystemExit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
