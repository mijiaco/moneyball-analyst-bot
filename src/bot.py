"""Discord bot: poll MFL for processed trades and post to a channel."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import certifi
import discord
import httpx
from dotenv import load_dotenv

from src.mfl_client import MflClient, franchise_names_from_league
from src.trade_poll_core import poll_trades_for_new_messages
from src.trade_notify import (
    current_season_lookback_days,
    env_bool,
    format_top_traders_text,
    load_seen,
    save_seen,
    top_trader_counts,
)

logger = logging.getLogger(__name__)


class TradeBot(discord.Client):
    def __init__(
        self,
        *,
        connector: aiohttp.BaseConnector,
        ws_connector: aiohttp.BaseConnector,
    ) -> None:
        intents = discord.Intents.default()
        super().__init__(
            intents=intents,
            connector=connector,
            ws_connector=ws_connector,
        )
        self._channel_id: int = int(os.environ["DISCORD_CHANNEL_ID"])
        self._poll_interval = int(os.environ.get("MFL_POLL_INTERVAL_SECONDS", "180"))
        self._lookback_days = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
        self._host = os.environ.get("MFL_HOST", "www45.myfantasyleague.com")
        self._year = os.environ.get("MFL_YEAR", "2026")
        self._league_id = os.environ.get("MFL_LEAGUE_ID", "40468")
        self._api_key = os.environ.get("MFL_API_KEY") or None
        self._user_agent = os.environ.get("MFL_USER_AGENT") or None
        self._season_year = int(self._year)
        self._data_dir = Path(__file__).resolve().parent.parent / "data"
        self._seen_path = self._data_dir / "seen_trades.json"
        self._players_cache = self._data_dir / "players_cache.json"
        self._weekly_top_traders_state_path = self._data_dir / "top_traders_weekly_state.json"
        self._seen: set[str] = load_seen(self._seen_path)
        self._mfl: MflClient | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._announce_max_age_hours = float(
            os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS", "48")
        )
        self._announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
        self._notify_once_per_trade = env_bool("MFL_NOTIFY_ONCE_PER_TRADE", True)
        self._announce_trade_bait = env_bool("MFL_ANNOUNCE_TRADE_BAIT", True)
        self._weekly_top_traders_enabled = env_bool("MFL_WEEKLY_TOP_TRADERS_ENABLED", True)
        self._weekly_top_traders_last_week_key = self._load_weekly_top_traders_week_key()

    def _load_weekly_top_traders_week_key(self) -> str:
        if not self._weekly_top_traders_state_path.is_file():
            return ""
        try:
            payload = json.loads(
                self._weekly_top_traders_state_path.read_text(encoding="utf-8")
            )
        except (json.JSONDecodeError, OSError):
            return ""
        week_key = payload.get("last_week_key")
        return str(week_key).strip() if week_key is not None else ""

    def _save_weekly_top_traders_week_key(self, week_key: str) -> None:
        self._weekly_top_traders_state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._weekly_top_traders_state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_week_key": week_key}), encoding="utf-8")
        os.replace(tmp, self._weekly_top_traders_state_path)

    def _current_week_key_et(self, now_et: datetime) -> str:
        iso_year, iso_week, _ = now_et.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"

    def _is_weekly_top_traders_due(self, now_et: datetime) -> bool:
        return now_et.weekday() == 4 and now_et.hour >= 17

    async def _maybe_send_weekly_top_traders(self, channel: discord.TextChannel) -> None:
        if not self._weekly_top_traders_enabled:
            return
        if self._mfl is None:
            return
        now_et = datetime.now(ZoneInfo("America/New_York"))
        if not self._is_weekly_top_traders_due(now_et):
            return
        week_key = self._current_week_key_et(now_et)
        if self._weekly_top_traders_last_week_key == week_key:
            return

        lookback_days = current_season_lookback_days(self._season_year)
        transactions = await self._mfl.fetch_transactions_trade_days(lookback_days)
        await self._mfl.sleep_between_exports()
        league_json = await self._mfl.fetch_league()
        franchise_names = franchise_names_from_league(league_json)
        counts = top_trader_counts(transactions, dedupe_by_trade=True)
        full_text = format_top_traders_text(
            counts,
            franchise_names,
            title="Top Traders This Year",
            week_of_label=now_et.date().isoformat(),
            disclaimer="Disclaimer: this includes some test trades from early in the year.",
            top_n=0,
        )
        description = full_text.split("\n\n", 1)[1] if "\n\n" in full_text else full_text
        if len(description) > 4096:
            description = description[:4093] + "..."
        embed = discord.Embed(
            title="Top Traders This Year",
            description=description,
            color=discord.Color.gold(),
        )
        await channel.send(embed=embed)
        self._weekly_top_traders_last_week_key = week_key
        self._save_weekly_top_traders_week_key(week_key)

    async def setup_hook(self) -> None:
        self._mfl = MflClient(
            host=self._host,
            year=self._year,
            league_id=self._league_id,
            api_key=self._api_key,
            user_agent=self._user_agent,
            players_cache_path=self._players_cache,
        )
        self._poll_task = asyncio.create_task(self._poll_forever())

    async def close(self) -> None:
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._mfl:
            await self._mfl.aclose()
        await super().close()

    async def on_ready(self) -> None:
        logger.info("Logged in as %s (%s)", self.user, self.user.id if self.user else None)

    async def _poll_forever(self) -> None:
        await self.wait_until_ready()
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error in poll loop")
            await asyncio.sleep(float(self._poll_interval))

    async def _poll_once(self) -> None:
        assert self._mfl is not None
        channel = self.get_channel(self._channel_id)
        if not isinstance(channel, discord.TextChannel):
            logger.error("DISCORD_CHANNEL_ID must be a text channel id (got %s)", self._channel_id)
            return

        try:
            pending_posts, updated = await poll_trades_for_new_messages(
                self._mfl,
                self._seen,
                lookback_days=self._lookback_days,
                announce_pending=self._announce_pending,
                announce_max_age_hours=self._announce_max_age_hours,
                season_year=self._season_year,
                notify_once_per_trade=self._notify_once_per_trade,
                announce_trade_bait=self._announce_trade_bait,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                logger.warning("MFL 429; backing off 120s")
                await asyncio.sleep(120.0)
            else:
                logger.exception("MFL HTTP error: %s", exc)
            return
        except Exception:
            logger.exception("MFL fetch failed")
            return

        for key, payload in pending_posts:
            embed = discord.Embed(
                title=payload.title,
                description=payload.description,
                color=payload.color,
            )
            try:
                await channel.send(embed=embed)
            except discord.DiscordException:
                logger.exception("Failed to send Discord message")
                continue
            self._seen.add(key)
            updated = True

        try:
            await self._maybe_send_weekly_top_traders(channel)
        except Exception:
            logger.exception("Failed weekly Top Traders post")

        if updated:
            save_seen(self._seen_path, self._seen)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    load_dotenv()
    if not os.environ.get("DISCORD_BOT_TOKEN"):
        logger.error("DISCORD_BOT_TOKEN is required")
        sys.exit(1)
    if not os.environ.get("DISCORD_CHANNEL_ID"):
        logger.error("DISCORD_CHANNEL_ID is required")
        sys.exit(1)

    asyncio.run(_run_bot(os.environ["DISCORD_BOT_TOKEN"]))


async def _run_bot(token: str) -> None:
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    ws_connector = aiohttp.TCPConnector(ssl=ssl_ctx)
    bot = TradeBot(connector=connector, ws_connector=ws_connector)
    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    main()
