"""Shared MFL fetch + trade selection logic for the Discord bot and GitHub Actions runner."""

from __future__ import annotations

import time
from typing import Any

import discord
from src.mfl_client import MflClient, franchise_names_from_league
from src.trade_notify import (
    format_trade_text,
    is_processed_trade,
    is_trade_too_old_to_announce,
    trade_notification_key,
)

DISCORD_DESCRIPTION_LIMIT = 4096
_TRADE_EMBED_COLOR = discord.Color.dark_green().value
_TRADE_EMBED_TITLE = "Trade"


class TradeMessagePayload:
    __slots__ = ("title", "description", "color")

    def __init__(self, title: str, description: str, color: int) -> None:
        self.title = title
        self.description = description
        self.color = color


async def poll_trades_for_new_messages(
    mfl: MflClient,
    seen: set[str],
    *,
    lookback_days: int,
    announce_pending: bool,
    announce_max_age_hours: float,
    season_year: int,
) -> tuple[list[tuple[str, TradeMessagePayload]], bool]:
    """
    Fetch MFL. Mutates seen only for old-trade silent seeds.
    Returns (list of (dedupe_key, payload) to post — keys not yet in seen), seen_dirty).
    Caller must add keys to seen after each successful Discord post.
    """
    transactions = await mfl.fetch_transactions_trade_days(lookback_days)
    await mfl.sleep_between_exports()
    league_json = await mfl.fetch_league()
    await mfl.sleep_between_exports()
    players = await mfl.get_players_map()

    franchise_names = franchise_names_from_league(league_json)
    now = time.time()
    out: list[tuple[str, TradeMessagePayload]] = []
    updated = False

    for tx in transactions:
        if tx.get("type") != "TRADE":
            continue
        if not announce_pending and not is_processed_trade(tx, now):
            continue
        key = trade_notification_key(tx, now)
        if key in seen:
            continue
        if is_trade_too_old_to_announce(tx, now, announce_max_age_hours):
            seen.add(key)
            updated = True
            continue
        body = format_trade_text(tx, franchise_names, players, season_year)
        if len(body) > DISCORD_DESCRIPTION_LIMIT:
            body = body[: DISCORD_DESCRIPTION_LIMIT - 3] + "..."
        out.append((key, TradeMessagePayload(_TRADE_EMBED_TITLE, body, _TRADE_EMBED_COLOR)))

    return out, updated
