"""Shared MFL fetch + trade selection logic for the Discord bot and GitHub Actions runner."""

from __future__ import annotations

import time
from typing import Any

import discord
from src.mfl_client import (
    MflClient,
    franchise_names_from_league,
    player_points_by_id,
    player_salaries_by_franchise,
)
from src.trade_notify import (
    format_trade_bait_text,
    format_trade_text,
    is_trade_bait_too_old_to_announce,
    is_processed_trade,
    is_trade_too_old_to_announce,
    trade_bait_notification_key,
    trade_dedupe_resolved,
    trade_notification_key,
)

DISCORD_DESCRIPTION_LIMIT = 4096
_TRADE_EMBED_COLOR = discord.Color.dark_green().value
_TRADE_EMBED_TITLE = "Trade"
_TRADE_BAIT_EMBED_COLOR = discord.Color.blurple().value
_TRADE_BAIT_EMBED_TITLE = "Trade bait update"


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
    notify_once_per_trade: bool,
    announce_trade_bait: bool,
) -> tuple[list[tuple[str, TradeMessagePayload]], bool]:
    """
    Fetch MFL. Mutates seen only for old-trade silent seeds.
    Returns (list of (dedupe_key, payload) to post — keys not yet in seen), seen_dirty).
    Caller must add keys to seen after each successful Discord post.
    """
    transactions = await mfl.fetch_transactions_trade_days(lookback_days)
    await mfl.sleep_between_exports()
    trade_baits = await mfl.fetch_trade_baits()
    await mfl.sleep_between_exports()
    league_json = await mfl.fetch_league()
    await mfl.sleep_between_exports()
    players = await mfl.get_players_map()
    await mfl.sleep_between_exports()
    rosters_json = await mfl.fetch_rosters()
    await mfl.sleep_between_exports()
    try:
        player_scores_json = await mfl.fetch_player_scores_current_year()
    except Exception:
        # Keep trade polling alive even when playerScores export is unavailable.
        player_scores_json = {}
    salaries_by_franchise = player_salaries_by_franchise(rosters_json)
    points_by_player_id = player_points_by_id(player_scores_json)

    franchise_names = franchise_names_from_league(league_json)
    now = time.time()
    out: list[tuple[str, TradeMessagePayload]] = []
    updated = False

    for tx in transactions:
        if tx.get("type") != "TRADE":
            continue
        if not announce_pending and not is_processed_trade(tx, now):
            continue
        skip, migrated = trade_dedupe_resolved(
            tx, seen, now, notify_once_per_trade=notify_once_per_trade
        )
        if migrated:
            updated = True
        if skip:
            continue
        key = trade_notification_key(
            tx, now, include_phase=not notify_once_per_trade
        )
        if is_trade_too_old_to_announce(tx, now, announce_max_age_hours):
            seen.add(key)
            updated = True
            continue
        body = format_trade_text(
            tx,
            franchise_names,
            players,
            season_year,
            salaries_by_franchise,
            points_by_player_id,
        )
        if len(body) > DISCORD_DESCRIPTION_LIMIT:
            body = body[: DISCORD_DESCRIPTION_LIMIT - 3] + "..."
        out.append((key, TradeMessagePayload(_TRADE_EMBED_TITLE, body, _TRADE_EMBED_COLOR)))

    if announce_trade_bait:
        for tb in trade_baits:
            key = trade_bait_notification_key(tb)
            if key in seen:
                continue
            if is_trade_bait_too_old_to_announce(tb, now, announce_max_age_hours):
                seen.add(key)
                updated = True
                continue
            body = format_trade_bait_text(
                tb,
                franchise_names,
                players,
                season_year,
                salaries_by_franchise,
                points_by_player_id,
            )
            if len(body) > DISCORD_DESCRIPTION_LIMIT:
                body = body[: DISCORD_DESCRIPTION_LIMIT - 3] + "..."
            out.append(
                (key, TradeMessagePayload(_TRADE_BAIT_EMBED_TITLE, body, _TRADE_BAIT_EMBED_COLOR))
            )

    return out, updated
