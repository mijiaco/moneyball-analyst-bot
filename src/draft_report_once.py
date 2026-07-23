"""One-shot post-draft report: Discord embed cards, one post per team (picks + that team's trades). Run: python -m src.draft_report_once"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import certifi
import httpx
from dotenv import load_dotenv

from src.draft_notify import DraftPickSelection, rookie_salary_by_slot, selected_draft_picks_from_results
from src.mfl_client import MflClient, franchise_names_from_league
from src.mfl_env import missing_mfl_connect_env_names, mfl_connect_env_help_suffix, mfl_connect_settings
from src.trade_notify import format_future_pick_token, trade_submitted_unix, _split_gave_up

logger = logging.getLogger(__name__)

DISCORD_API = "https://discord.com/api/v10"
# Match draft-pick announcements (gold) for visual consistency.
_DRAFT_REPORT_EMBED_COLOR = 0xF1C40F
_DISCORD_MAX_EMBEDS_PER_MESSAGE = 10
_DISCORD_MAX_FIELDS_PER_EMBED = 25
_DISCORD_MAX_FIELD_VALUE = 1020
_DISCORD_MAX_EMBED_DESCRIPTION = 4096

# Inclusive window in America/Chicago (per request).
_DRAFT_REPORT_TZ = ZoneInfo("America/Chicago")
_DRAFT_TRADES_START = datetime(2026, 5, 2, 12, 0, 0, tzinfo=_DRAFT_REPORT_TZ)
_DRAFT_TRADES_END = datetime(2026, 5, 14, 18, 0, 0, tzinfo=_DRAFT_REPORT_TZ)

_PURPLE_CURTAIN = "The Purple Curtain"
_PURPLE_CURTAIN_F_COMMENT = (
    "4th string TE, a couple of backup DLs and some undrafted guys. What a swing and a miss!"
)


def _window_unix_bounds() -> tuple[float, float]:
    return (_DRAFT_TRADES_START.timestamp(), _DRAFT_TRADES_END.timestamp())


def _transaction_lookback_days() -> int:
    """Ensure MFL's TRADE+DAYS window reaches back to the draft trade window start."""
    now = time.time()
    start_ts = _DRAFT_TRADES_START.timestamp()
    days_since_start = (now - start_ts) / 86400.0 + 2.0
    return max(30, int(days_since_start))


def trade_in_draft_report_window(tx: dict[str, Any]) -> bool:
    if str(tx.get("type") or "").strip() != "TRADE":
        return False
    ts = trade_submitted_unix(tx)
    if ts is None:
        return False
    lo, hi = _window_unix_bounds()
    return lo <= ts <= hi


def format_year_draft_pick_token(token: str, season_year: int) -> str:
    """DP_* zero-based round/pick -> Year Y Draft Pick r.pp"""
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
    return f"Year {season_year} Draft Pick {round_1}.{pick_1:02d}"


def _format_gave_up_asset_list(
    gave_up: str | None,
    players: dict[str, str],
    franchise_names: dict[str, str],
    season_year: int,
) -> str:
    tokens = _split_gave_up(gave_up)
    if not tokens:
        return "_Nothing listed_"
    parts: list[str] = []
    for t in tokens:
        if t.startswith("DP_"):
            parts.append(format_year_draft_pick_token(t, season_year))
        elif t.startswith("FP_"):
            parts.append(format_future_pick_token(t, franchise_names))
        elif t.isdigit() or (players.get(t) is not None):
            pid = t
            label = players.get(pid)
            if label is None:
                label = f"Player id {pid}"
            parts.append(label)
        else:
            parts.append(t)
    return "; ".join(parts)


def trades_involving_franchise(
    franchise_id: str,
    draft_window_trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fid = str(franchise_id).strip()
    matched: list[dict[str, Any]] = []
    for tx in draft_window_trades:
        f1 = str(tx.get("franchise", "")).strip()
        f2 = str(tx.get("franchise2", "")).strip()
        if fid and (fid == f1 or fid == f2):
            matched.append(tx)
    return sorted(
        matched,
        key=lambda t: (trade_submitted_unix(t) or 0.0, str(t.get("timestamp", ""))),
    )


def _format_trade_field_body(
    tx: dict[str, Any],
    franchise_names: dict[str, str],
    players: dict[str, str],
    season_year: int,
) -> str:
    f1 = str(tx.get("franchise", "")).strip()
    f2 = str(tx.get("franchise2", "")).strip()
    n1 = franchise_names.get(f1, f"Franchise {f1}")
    n2 = franchise_names.get(f2, f"Franchise {f2}")
    a1 = _format_gave_up_asset_list(tx.get("franchise1_gave_up"), players, franchise_names, season_year)
    a2 = _format_gave_up_asset_list(tx.get("franchise2_gave_up"), players, franchise_names, season_year)
    return f"**{n1}**\n{a1}\n\n**{n2}**\n{a2}"


def _pick_display_line(selection: DraftPickSelection, players: dict[str, str]) -> str:
    label = players.get(selection.player_id, f"Player id {selection.player_id}")
    salary = rookie_salary_by_slot(selection)
    return f"`{selection.slot}` · {label} (R) · **${salary}**"


def _split_lines_into_field_values(lines: list[str], max_len: int = _DISCORD_MAX_FIELD_VALUE) -> list[str]:
    if not lines:
        return ["—"]
    chunks: list[str] = []
    buf: list[str] = []
    for line in lines:
        candidate = "\n".join(buf + [line])
        if len(candidate) <= max_len:
            buf.append(line)
            continue
        if buf:
            chunks.append("\n".join(buf))
            buf = [line]
            if len(line) > max_len:
                chunks.append(line[: max_len - 1] + "…")
                buf = []
            continue
        if len(line) > max_len:
            chunks.append(line[: max_len - 1] + "…")
            buf = []
        else:
            buf = [line]
    if buf:
        chunks.append("\n".join(buf))
    return chunks


def _embed_base(team: str, *, title_suffix: str = "") -> dict[str, Any]:
    title = f"Draft report · {team}"
    if title_suffix:
        title = f"{title} · {title_suffix}"
    if len(title) > 256:
        title = title[:253] + "..."
    return {
        "title": title,
        "color": _DRAFT_REPORT_EMBED_COLOR,
        "footer": {"text": "Trades: May 2 12:00 PM – May 14 6:00 PM CT · rookie $ = slot scale"},
    }


def _team_grade(team_name: str, pick_count: int) -> tuple[str, str]:
    if pick_count == 0:
        return "N/A", ""
    if team_name.strip() == _PURPLE_CURTAIN:
        return "F", _PURPLE_CURTAIN_F_COMMENT
    return "A", ""


def build_team_draft_report_embeds(
    *,
    franchise_id: str,
    franchise_names: dict[str, str],
    draft_selections_by_franchise: dict[str, list[DraftPickSelection]],
    draft_window_trades: list[dict[str, Any]],
    players: dict[str, str],
    season_year: int,
) -> list[dict[str, Any]]:
    team = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
    picks = draft_selections_by_franchise.get(franchise_id, [])
    grade, note = _team_grade(team, len(picks))
    summary_lines = [
        f"**Selections:** {len(picks)}",
        f"**Overall grade:** **{grade}**",
    ]
    if note:
        summary_lines.append(note)
    summary = "\n".join(summary_lines)

    field_rows: list[dict[str, str]] = []
    if picks:
        pick_lines = [_pick_display_line(sel, players) for sel in picks]
        for i, chunk in enumerate(_split_lines_into_field_values(pick_lines)):
            name = "Rookie picks" if i == 0 else f"Rookie picks ({i + 1})"
            field_rows.append({"name": name[:256], "value": chunk[:_DISCORD_MAX_FIELD_VALUE]})
    else:
        field_rows.append({"name": "Rookie picks", "value": "_No picks in this draft._"})

    team_trades = trades_involving_franchise(franchise_id, draft_window_trades)
    if team_trades:
        for i, tx in enumerate(team_trades, start=1):
            body = _format_trade_field_body(tx, franchise_names, players, season_year)
            if len(body) > _DISCORD_MAX_FIELD_VALUE:
                body = body[: _DISCORD_MAX_FIELD_VALUE - 1] + "…"
            field_rows.append({"name": f"Trade {i}"[:256], "value": body})
    else:
        field_rows.append(
            {"name": "Trades (draft window)", "value": "_No trades involving this team._"}
        )

    embeds: list[dict[str, Any]] = []
    for start in range(0, len(field_rows), _DISCORD_MAX_FIELDS_PER_EMBED):
        portion = field_rows[start : start + _DISCORD_MAX_FIELDS_PER_EMBED]
        part_num = start // _DISCORD_MAX_FIELDS_PER_EMBED
        suffix = "" if part_num == 0 else f"cont. {part_num + 1}"
        emb = _embed_base(team, title_suffix=suffix)
        if part_num == 0:
            desc = summary
            if len(desc) > _DISCORD_MAX_EMBED_DESCRIPTION:
                desc = desc[: _DISCORD_MAX_EMBED_DESCRIPTION - 3] + "..."
            emb["description"] = desc
        else:
            emb["description"] = "_Additional picks / trades below._"
        emb["fields"] = [
            {"name": row["name"], "value": row["value"], "inline": False} for row in portion
        ]
        embeds.append(emb)
    return embeds


def build_team_draft_report_message_post_bodies(
    *,
    franchise_id: str,
    franchise_names: dict[str, str],
    draft_selections_by_franchise: dict[str, list[DraftPickSelection]],
    draft_window_trades: list[dict[str, Any]],
    players: dict[str, str],
    season_year: int,
) -> list[dict[str, Any]]:
    embeds = build_team_draft_report_embeds(
        franchise_id=franchise_id,
        franchise_names=franchise_names,
        draft_selections_by_franchise=draft_selections_by_franchise,
        draft_window_trades=draft_window_trades,
        players=players,
        season_year=season_year,
    )
    return [{"embeds": embeds[i : i + _DISCORD_MAX_EMBEDS_PER_MESSAGE]} for i in range(0, len(embeds), _DISCORD_MAX_EMBEDS_PER_MESSAGE)]


def build_per_team_draft_report_message_post_bodies(
    *,
    franchise_names: dict[str, str],
    draft_selections_by_franchise: dict[str, list[DraftPickSelection]],
    draft_window_trades: list[dict[str, Any]],
    players: dict[str, str],
    season_year: int,
) -> list[list[dict[str, Any]]]:
    sorted_ids = sorted(
        franchise_names.keys(),
        key=lambda fid: franchise_names.get(fid, fid).casefold(),
    )
    return [
        build_team_draft_report_message_post_bodies(
            franchise_id=fid,
            franchise_names=franchise_names,
            draft_selections_by_franchise=draft_selections_by_franchise,
            draft_window_trades=draft_window_trades,
            players=players,
            season_year=season_year,
        )
        for fid in sorted_ids
    ]


async def _post_discord_embed_payload(
    client: httpx.AsyncClient,
    token: str,
    channel_id: str,
    payload: dict[str, Any],
) -> bool:
    url = f"{DISCORD_API}/channels/{channel_id}/messages"
    headers = {"Authorization": f"Bot {token}"}
    response = await client.post(url, headers=headers, json=payload)
    if response.status_code == 429:
        try:
            retry_after = float(response.json().get("retry_after", 2))
        except (json.JSONDecodeError, TypeError, ValueError, KeyError):
            retry_after = 2.0
        await asyncio.sleep(retry_after)
        response = await client.post(url, headers=headers, json=payload)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.exception("Discord API error: %s %s", response.status_code, response.text)
        return False
    return True


def _resolve_only_team_franchise_id(
    only_team: str,
    franchise_names: dict[str, str],
) -> str | None:
    """Match MFL franchise id or unique case-insensitive team name substring."""
    raw = only_team.strip()
    if not raw:
        return None
    if raw in franchise_names:
        return raw
    key = raw.casefold()
    matches = [
        fid for fid, name in franchise_names.items() if key in str(name).casefold()
    ]
    if len(matches) == 1:
        return matches[0]
    return None


async def _async_main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Post draft report to Discord (embed cards, one team per post).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report to stdout; do not post to Discord.",
    )
    parser.add_argument(
        "--only-team",
        metavar="ID_OR_NAME",
        default="",
        help="Franchise id (e.g. 0007) or unique substring of team name; limits output/posts to that team.",
    )
    args = parser.parse_args()

    connect = mfl_connect_settings()
    if connect is None:
        miss = ", ".join(missing_mfl_connect_env_names())
        logger.error("Missing required env: %s. %s", miss, mfl_connect_env_help_suffix())
        return 1
    host, year, league_id = connect
    season_year = int(year)

    api_key = (os.environ.get("MFL_API_KEY") or "").strip() or None
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
        await mfl.sleep_between_exports()
        draft_results_json = await mfl.fetch_draft_results()
        await mfl.sleep_between_exports()
        transactions = await mfl.fetch_transactions_trade_days(_transaction_lookback_days())
        await mfl.sleep_between_exports()
        players = await mfl.get_players_map()
    finally:
        await mfl.aclose()

    franchise_names = franchise_names_from_league(league_json)

    selections = selected_draft_picks_from_results(draft_results_json)
    by_fr: dict[str, list[DraftPickSelection]] = {}
    for sel in selections:
        by_fr.setdefault(sel.franchise_id, []).append(sel)
    for fid in by_fr:
        by_fr[fid].sort(key=lambda s: s.overall_index)

    window_trades = [tx for tx in transactions if trade_in_draft_report_window(tx)]

    only_raw = str(args.only_team or "").strip()
    if only_raw:
        fid_only = _resolve_only_team_franchise_id(only_raw, franchise_names)
        if fid_only is None:
            names_preview = sorted({f"{k}: {v}" for k, v in franchise_names.items()})[:30]
            logger.error(
                "Could not resolve --only-team %r to a single franchise. "
                "Use an MFL franchise id or a unique name substring. Sample: %s",
                only_raw,
                names_preview,
            )
            return 1
        team_payload_groups = [
            build_team_draft_report_message_post_bodies(
                franchise_id=fid_only,
                franchise_names=franchise_names,
                draft_selections_by_franchise=by_fr,
                draft_window_trades=window_trades,
                players=players,
                season_year=season_year,
            )
        ]
    else:
        team_payload_groups = build_per_team_draft_report_message_post_bodies(
            franchise_names=franchise_names,
            draft_selections_by_franchise=by_fr,
            draft_window_trades=window_trades,
            players=players,
            season_year=season_year,
        )

    if args.dry_run:
        sep = "\n\n" + "=" * 72 + "\n\n"
        printed: list[str] = []
        for group in team_payload_groups:
            inner: list[str] = []
            for bi, body in enumerate(group):
                label = f"--- message {bi + 1}/{len(group)} (same team) ---\n" if len(group) > 1 else ""
                inner.append(label + json.dumps(body, indent=2))
            printed.append("\n".join(inner))
        print(sep.join(printed))
        return 0

    token = (os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    channel_id = (os.environ.get("DISCORD_CHANNEL_ID") or "").strip()
    if not token or not channel_id:
        logger.error("DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required unless --dry-run.")
        return 1

    async with httpx.AsyncClient(timeout=60.0, verify=certifi.where()) as client:
        for i, group in enumerate(team_payload_groups):
            if i:
                await asyncio.sleep(0.6)
            for body in group:
                ok = await _post_discord_embed_payload(client, token, channel_id, body)
                if not ok:
                    return 1
                await asyncio.sleep(0.6)
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(_async_main()))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
