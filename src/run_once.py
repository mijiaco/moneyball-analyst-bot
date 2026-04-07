"""Single MFL poll + Discord REST posts (no gateway). For GitHub Actions cron."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import certifi
import httpx
from dotenv import load_dotenv

from src.mfl_client import MflClient
from src.trade_notify import env_bool, load_seen, save_seen
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
    seen = load_seen(seen_path)

    lookback = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
    announce_max_age = float(os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS", "48"))
    announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
    season_year = int(os.environ.get("MFL_YEAR", "2026"))

    mfl = MflClient(
        host=os.environ.get("MFL_HOST", "www45.myfantasyleague.com"),
        year=os.environ.get("MFL_YEAR", "2026"),
        league_id=os.environ.get("MFL_LEAGUE_ID", "40468"),
        api_key=os.environ.get("MFL_API_KEY") or None,
        user_agent=os.environ.get("MFL_USER_AGENT") or None,
        players_cache_path=players_cache,
    )
    try:
        pending_posts, updated = await poll_trades_for_new_messages(
            mfl,
            seen,
            lookback_days=lookback,
            announce_pending=announce_pending,
            announce_max_age_hours=announce_max_age,
            season_year=season_year,
        )
    except httpx.HTTPStatusError as exc:
        logger.exception("MFL HTTP error: %s", exc)
        return 1
    except Exception:
        logger.exception("MFL fetch failed")
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
        logger.info("Updated seen trades (%s keys)", len(seen))
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
