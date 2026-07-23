"""One-shot draft picks report for a league. Run: python -m src.future_picks_report_once --league-id 27743 --host www48.myfantasyleague.com --api-key-env BC_MFL_API_KEY"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from src.mfl_client import (
    MflClient,
    assets_export_has_franchise_data,
    draft_picks_by_franchise,
    franchise_names_from_league,
    future_draft_picks_by_franchise_from_export,
)
from src.mfl_env import missing_mfl_connect_env_names, mfl_connect_env_help_suffix, mfl_connect_settings
from src.trade_notify import (
    _chunk_text_for_discord_embeds,
    _discord_post_embed,
    format_draft_picks_report_text,
)

logger = logging.getLogger(__name__)

_EMBED_COLOR = 5793266


def _report_title(league_id: str, *, chunk_index: int = 0, chunk_total: int = 1) -> str:
    base = f"Draft Picks Report (Future) · L{league_id}"
    if chunk_total > 1 and chunk_index > 0:
        return f"{base} ({chunk_index}/{chunk_total})"
    return base


def _as_of_label_et(now_et: datetime) -> str:
    return now_et.strftime("%Y-%m-%d %I:%M %p ET")


def _api_key_from_env(var_name: str) -> str | None:
    return (os.environ.get(var_name) or "").strip() or None


async def _load_draft_picks_by_franchise(
    mfl: MflClient,
    *,
    league_json: dict,
    api_key: str | None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    franchise_names = franchise_names_from_league(league_json)
    if api_key:
        await mfl.sleep_between_exports()
        assets_json = await mfl.fetch_assets()
        if assets_export_has_franchise_data(assets_json):
            return draft_picks_by_franchise(assets_json)

    await mfl.sleep_between_exports()
    future_draft_picks_json = await mfl.fetch_future_draft_picks()
    future_map = future_draft_picks_by_franchise_from_export(
        future_draft_picks_json,
        franchise_names,
    )
    return {}, future_map


async def _async_main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Post draft picks report to Discord (one-shot, league override).",
    )
    parser.add_argument(
        "--league-id",
        metavar="ID",
        required=True,
        help="MFL league id (e.g. 27743); overrides MFL_LEAGUE_ID from env.",
    )
    parser.add_argument(
        "--host",
        metavar="HOST",
        default="",
        help="MFL host only (e.g. www48.myfantasyleague.com); overrides MFL_HOST from env.",
    )
    parser.add_argument(
        "--api-key-env",
        metavar="VAR",
        default="MFL_API_KEY",
        help="Environment variable name for the MFL API key (e.g. BC_MFL_API_KEY).",
    )
    parser.add_argument(
        "--omit-api-key",
        action="store_true",
        help="Do not send an API key (futureDraftPicks export only; no 2026 picks).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report to stdout; do not post to Discord.",
    )
    args = parser.parse_args()

    league_id = str(args.league_id).strip()
    if not league_id:
        logger.error("--league-id must be non-empty.")
        return 1

    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        logger.error("Missing required env: %s. %s", miss, mfl_connect_env_help_suffix())
        return 1
    env_host, year, _env_league = connect
    host = str(args.host or env_host).strip()
    if not host:
        logger.error("MFL host is required (--host or MFL_HOST).")
        return 1
    season_year = int(year)

    api_key: str | None = None
    if not args.omit_api_key:
        api_key = _api_key_from_env(str(args.api_key_env or "MFL_API_KEY").strip())
        if not api_key:
            logger.warning(
                "No API key in %s; falling back to futureDraftPicks only (no %s picks).",
                args.api_key_env,
                season_year,
            )

    user_agent = (os.environ.get("MFL_USER_AGENT") or "").strip() or None
    data_dir = Path(__file__).resolve().parent.parent / "data"
    players_cache = data_dir / "players_cache.json"

    mfl = MflClient(
        host,
        year,
        league_id,
        api_key=api_key,
        user_agent=user_agent,
        players_cache_path=players_cache,
    )
    try:
        league_json = await mfl.fetch_league()
        current_map, future_map = await _load_draft_picks_by_franchise(
            mfl,
            league_json=league_json,
            api_key=api_key,
        )
    finally:
        await mfl.aclose()

    franchise_names = franchise_names_from_league(league_json)
    report_text = format_draft_picks_report_text(
        franchise_names,
        current_map,
        future_map,
        title=f"Draft Picks Report (Future) · L{league_id}",
        report_season_year=season_year,
    )

    if args.dry_run:
        print(report_text)
        return 0

    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    channel_id = (os.environ.get("DISCORD_CHANNEL_ID") or "").strip()
    if not token or not channel_id:
        logger.error("DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required unless --dry-run.")
        return 1

    now_et = datetime.now(ZoneInfo("America/New_York"))
    as_of_line = f"As of {_as_of_label_et(now_et)}"
    body_text = report_text.split("\n\n", 1)[1] if "\n\n" in report_text else report_text
    chunks = _chunk_text_for_discord_embeds(body_text, max_len=3900)
    total = len(chunks)

    for index, chunk in enumerate(chunks, start=1):
        title = _report_title(league_id, chunk_index=index, chunk_total=total)
        description = f"{as_of_line}\n\n{chunk}"
        if len(description) > 4096:
            description = description[:4093] + "..."
        ok = await _discord_post_embed(
            token=token,
            channel_id=channel_id,
            title=title,
            description=description,
            color=_EMBED_COLOR,
        )
        if not ok:
            return 1
        if index < total:
            await asyncio.sleep(0.6)

    logger.info("Posted draft picks report for league %s (%s embed(s)).", league_id, total)
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(_async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
