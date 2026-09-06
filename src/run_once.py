"""One poll cycle and outbound REST posts (no gateway). For scheduled runners."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import httpx
from dotenv import load_dotenv

from src.mfl_client import (
    MflClient,
    accounting_balance_by_franchise,
    draft_picks_by_franchise,
    franchise_names_from_league,
    player_salaries_by_franchise,
)
from src.mfl_env import (
    missing_mfl_connect_env_names,
    mfl_connect_env_help_suffix,
    mfl_connect_settings,
)
from src.google_sheets import fetch_top32_player_ids, sync_rfa_sheet
from src.rfa_report import (
    INVALID_RFA_CLAIM_COLOR,
    INVALID_RFA_CLAIM_TITLE,
    RFA_REPORT_COLOR,
    RFA_REPORT_TITLE,
    format_invalid_rfa_claim_text,
    format_rfa_report_text,
    invalid_claim_fingerprint,
    load_rfa_state,
    parse_bbid_waiver_claims,
    parse_free_agent_moves,
    save_rfa_state,
    update_rfa_state,
)
from src.roster_violations import (
    ROSTER_VIOLATIONS_COLOR,
    ROSTER_VIOLATIONS_TITLE,
    find_ir_eligibility_violations,
    find_salary_cap_violations,
    find_slot_limit_violations,
    find_starter_requirement_violations,
    format_roster_violations_report_text,
    franchise_salaries_from_standings,
    franchise_salary_caps_from_league,
    injury_status_by_player_id,
    ir_eligible_statuses_from_env,
    league_slot_limits,
    starter_lineup_size,
    starter_position_minimums,
)
from src.taxi_cut_report import (
    TAXI_CUT_ALERT_COLOR,
    TAXI_CUT_ALERT_TITLE,
    TAXI_CUT_WEEKLY_COLOR,
    TAXI_CUT_WEEKLY_TITLE,
    format_taxi_cut_alert_text,
    format_taxi_cut_weekly_report_text,
    group_taxi_cuts_for_alerts,
    include_taxi_salary_percent,
    load_taxi_cut_state,
    non_taxi_roster_players_from_rosters,
    parse_salary_adjustments,
    save_taxi_cut_state,
    taxi_cut_alert_fingerprint,
    taxi_players_from_rosters,
    unreimbursed_taxi_cuts,
    update_taxi_cut_state,
    parse_taxi_squad_moves,
)
from src.trade_notify import (
    cap_space_available_by_franchise,
    current_season_lookback_days,
    env_bool,
    format_draft_picks_report_text,
    format_roster_breakdown_report_text,
    format_traded_future_picks_with_accounting_report_text,
    format_top_traders_text,
    load_seen,
    roster_slot_counts_by_franchise,
    save_seen,
    top_trader_counts,
    traded_own_future_pick_rounds_by_franchise,
)
from src.trade_poll_core import TradeMessagePayload, poll_trades_for_new_messages
from src.weekly_claim import claim_weekly_reports_week, weekly_report_dedupe_key

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


def _read_reports_state_json(state_path: Path) -> dict[str, Any]:
    if not state_path.is_file():
        return {}
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_reports_state_json(state_path: Path, data: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
    os.replace(tmp, state_path)


def _load_last_weekly_reports_week_key(state_path: Path) -> str:
    payload = _read_reports_state_json(state_path)
    week_key = payload.get("last_weekly_reports_week_key")
    return str(week_key).strip() if week_key is not None else ""


def _save_last_weekly_reports_week_key(state_path: Path, week_key: str) -> None:
    data = _read_reports_state_json(state_path)
    data["last_weekly_reports_week_key"] = week_key
    _write_reports_state_json(state_path, data)


def _current_week_key_et(now_et: datetime) -> str:
    iso_year, iso_week, _ = now_et.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _is_weekly_reports_due(now_et: datetime) -> bool:
    # Saturday at/after 3:00 PM Eastern Time
    return now_et.weekday() == 5 and now_et.hour >= 15


def _is_sunday_unpaid_report_due(now_et: datetime) -> bool:
    # Sunday at/after 1:00 PM Eastern Time
    return now_et.weekday() == 6 and now_et.hour >= 13


def _as_of_label_et(now_et: datetime) -> str:
    return now_et.strftime("%Y-%m-%d %I:%M %p ET")


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
    rfa_state_path = data_dir / "rfa_state.json"
    taxi_cut_state_path = data_dir / "taxi_cut_state.json"
    seen = load_seen(seen_path)

    lookback = int(os.environ.get("MFL_TRADE_LOOKBACK_DAYS", "14"))
    _max_age_raw = (os.environ.get("MFL_ANNOUNCE_MAX_AGE_HOURS") or "").strip()
    announce_max_age = float(_max_age_raw) if _max_age_raw else 48.0
    announce_pending = env_bool("MFL_ANNOUNCE_PENDING_TRADES", True)
    notify_once_per_trade = env_bool("MFL_NOTIFY_ONCE_PER_TRADE", True)
    announce_trade_bait = env_bool("MFL_ANNOUNCE_TRADE_BAIT", True)
    announce_draft_picks = env_bool("MFL_ANNOUNCE_DRAFT_PICKS", True)
    weekly_reports_enabled = env_bool("MFL_WEEKLY_REPORTS_ENABLED", True)
    weekly_reports_include_draft_picks = env_bool(
        "MFL_WEEKLY_REPORTS_INCLUDE_DRAFT_PICKS", True
    )
    weekly_reports_include_roster_breakdown = env_bool(
        "MFL_WEEKLY_REPORTS_INCLUDE_ROSTER_BREAKDOWN", True
    )
    weekly_reports_include_roster_violations = env_bool(
        "MFL_WEEKLY_REPORTS_INCLUDE_ROSTER_VIOLATIONS", True
    )
    weekly_reports_include_taxi_cut_refunds = env_bool(
        "MFL_WEEKLY_REPORTS_INCLUDE_TAXI_CUT_REFUNDS", True
    )
    sunday_unpaid_report_enabled = env_bool("MFL_SUNDAY_UNPAID_REPORT_ENABLED", True)
    rfa_report_enabled = env_bool("MFL_RFA_REPORT_ENABLED", True)
    rfa_invalid_claim_alerts_enabled = env_bool(
        "MFL_RFA_INVALID_CLAIM_ALERTS_ENABLED", True
    )
    rfa_lookback_days = int(os.environ.get("MFL_RFA_LOOKBACK_DAYS", str(lookback)))
    taxi_cut_alerts_enabled = env_bool("MFL_TAXI_CUT_ALERTS_ENABLED", True)
    taxi_cut_lookback_days = int(
        os.environ.get("MFL_TAXI_CUT_LOOKBACK_DAYS", str(lookback))
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
    updated_rfa_state = False
    updated_taxi_cut_state = False
    # Freeze schedule gates at process start so a pre-3pm ET poll cannot
    # cross into the Saturday window mid-fetch and race a 3pm poll.
    schedule_now_et = datetime.now(ZoneInfo("America/New_York"))
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
            announce_draft_picks=announce_draft_picks,
        )
        weekly_report_payloads: list[tuple[str, str, int]] = []
        if weekly_reports_enabled:
            now_et = schedule_now_et
            as_of_line = f"As of {_as_of_label_et(now_et)}"
            if _is_weekly_reports_due(now_et):
                current_week_key = _current_week_key_et(now_et)
                should_post_weekly, weekly_state_changed = claim_weekly_reports_week(
                    reports_state_path,
                    current_week_key,
                    load_last=_load_last_weekly_reports_week_key,
                    save_last=_save_last_weekly_reports_week_key,
                )
                if weekly_state_changed:
                    updated_reports_state = True
                if should_post_weekly:
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
                    top_description = f"{as_of_line}\n\n{top_description}"
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
                            report_season_year=season_year,
                        )
                        draft_chunks = _chunk_text_by_sections(draft_report_text, max_len=3900)
                        total_chunks = len(draft_chunks)
                        for index, chunk in enumerate(draft_chunks, start=1):
                            chunk_title = "Draft Picks Report (Future)"
                            if total_chunks > 1:
                                chunk_title = (
                                    "Draft Picks Report (Future) "
                                    f"({index}/{total_chunks})"
                                )
                            chunk_with_as_of = f"{as_of_line}\n\n{chunk}"
                            weekly_report_payloads.append(
                                (chunk_title, chunk_with_as_of, 5793266)
                            )

                    rosters_json: dict[str, Any] | None = None
                    if (
                        weekly_reports_include_roster_breakdown
                        or weekly_reports_include_roster_violations
                    ):
                        await mfl.sleep_between_exports()
                        rosters_json = await mfl.fetch_rosters()

                    if weekly_reports_include_roster_breakdown:
                        assert rosters_json is not None
                        roster_report = format_roster_breakdown_report_text(
                            franchise_names,
                            roster_slot_counts_by_franchise(rosters_json),
                            cap_available_by_franchise=cap_space_available_by_franchise(
                                league_json
                            ),
                        )
                        roster_description = (
                            f"{as_of_line}\n\n"
                            + (
                                roster_report.split("\n\n", 1)[1]
                                if "\n\n" in roster_report
                                else roster_report
                            )
                        )
                        weekly_report_payloads.append(
                            (
                                "Players by Team (Active / Taxi / IR / $ Cap Remain)",
                                roster_description,
                                3447003,
                            )
                        )

                    if weekly_reports_include_roster_violations:
                        assert rosters_json is not None
                        await mfl.sleep_between_exports()
                        injuries_json = await mfl.fetch_injuries()
                        await mfl.sleep_between_exports()
                        standings_json = await mfl.fetch_league_standings()
                        await mfl.sleep_between_exports()
                        players_map = await mfl.get_players_map()
                        slot_limits = league_slot_limits(league_json)
                        violations_report = format_roster_violations_report_text(
                            franchise_names,
                            find_ir_eligibility_violations(
                                rosters_json,
                                injury_status_by_player_id(injuries_json),
                                players_map,
                                eligible_statuses=ir_eligible_statuses_from_env(),
                            ),
                            find_slot_limit_violations(
                                rosters_json,
                                roster_limit=slot_limits["roster"],
                                taxi_limit=slot_limits["taxi"],
                                ir_limit=slot_limits["ir"],
                            ),
                            salary_cap_violations=find_salary_cap_violations(
                                franchise_salaries_from_standings(standings_json),
                                franchise_salary_caps_from_league(league_json),
                            ),
                            starter_requirement_violations=find_starter_requirement_violations(
                                rosters_json,
                                players_map,
                                position_minimums=starter_position_minimums(league_json),
                                lineup_size=starter_lineup_size(league_json),
                            ),
                        )
                        violations_description = (
                            f"{as_of_line}\n\n"
                            + (
                                violations_report.split("\n\n", 1)[1]
                                if "\n\n" in violations_report
                                else violations_report
                            )
                        )
                        weekly_report_payloads.append(
                            (
                                ROSTER_VIOLATIONS_TITLE,
                                violations_description,
                                ROSTER_VIOLATIONS_COLOR,
                            )
                        )

                    for report_title, report_description, report_color in weekly_report_payloads:
                        report_key = weekly_report_dedupe_key(
                            current_week_key, report_title
                        )
                        if report_key in seen:
                            continue
                        pending_posts.append(
                            (
                                report_key,
                                TradeMessagePayload(
                                    report_title,
                                    report_description,
                                    report_color,
                                ),
                            )
                        )

        if sunday_unpaid_report_enabled:
            now_sun = schedule_now_et
            if _is_sunday_unpaid_report_due(now_sun):
                reports_state = _read_reports_state_json(reports_state_path)
                today_et = now_sun.date().isoformat()
                if reports_state.get("last_unpaid_owners_sunday_date_et") != today_et:
                    await mfl.sleep_between_exports()
                    league_json = await mfl.fetch_league()
                    await mfl.sleep_between_exports()
                    accounting_json = await mfl.fetch_accounting()
                    franchise_names = franchise_names_from_league(league_json)
                    total_rounds = int(os.environ.get("MFL_DRAFT_ROUNDS", "6"))
                    traded_rounds = traded_own_future_pick_rounds_by_franchise(
                        league_json, target_year=2027, total_rounds=total_rounds
                    )
                    accounting_totals = accounting_balance_by_franchise(accounting_json)
                    unpaid_threshold = float(
                        os.environ.get("MFL_UNPAID_ACCOUNTING_THRESHOLD", "250")
                    )
                    report_text = format_traded_future_picks_with_accounting_report_text(
                        franchise_names,
                        traded_rounds,
                        accounting_totals,
                        target_year=2027,
                        accounting_balance_under=unpaid_threshold,
                    )
                    as_of_line = f"As of {_as_of_label_et(now_sun)}"
                    disclaimer = (
                        "These teams owe the balance for 2027 for trading away 1 or more of "
                        "their 2027 picks. Note: this list could be outdated if MFL accounting "
                        "balance hasn't been updated."
                    )
                    body = (
                        report_text.split("\n\n", 1)[1]
                        if "\n\n" in report_text
                        else report_text
                    )
                    description = f"{as_of_line}\n\n{disclaimer}\n\n{body}"
                    if len(description) > 4096:
                        description = description[:4093] + "..."
                    sunday_key = f"SUNDAY_UNPAID|{today_et}"
                    if sunday_key not in seen:
                        pending_posts.append(
                            (
                                sunday_key,
                                TradeMessagePayload(
                                    "Unpaid Owners / Traded Picks",
                                    description,
                                    15105570,
                                ),
                            )
                        )
                    reports_state["last_unpaid_owners_sunday_date_et"] = today_et
                    _write_reports_state_json(reports_state_path, reports_state)
                    updated_reports_state = True

        if rfa_report_enabled:
            now_rfa = schedule_now_et
            as_of_rfa = f"As of {_as_of_label_et(now_rfa)}"
            rfa_state = load_rfa_state(rfa_state_path)
            await mfl.sleep_between_exports()
            players = await mfl.get_players_map()
            top32_ids = fetch_top32_player_ids(players)
            if top32_ids is None:
                logger.warning("RFA report skipped: could not load top-32 players from Sheets")
            else:
                await mfl.sleep_between_exports()
                league_json = await mfl.fetch_league()
                franchise_names = franchise_names_from_league(league_json)
                await mfl.sleep_between_exports()
                rosters_json = await mfl.fetch_rosters()
                salaries = player_salaries_by_franchise(rosters_json)
                await mfl.sleep_between_exports()
                fa_txs = await mfl.fetch_transactions_by_type(
                    "FREE_AGENT",
                    days=rfa_lookback_days,
                )
                await mfl.sleep_between_exports()
                bbid_txs = await mfl.fetch_transactions_by_type(
                    "BBID_WAIVER",
                    days=rfa_lookback_days,
                )
                fa_moves = parse_free_agent_moves(fa_txs)
                bbid_claims = parse_bbid_waiver_claims(bbid_txs)
                updated_state, invalid_claims, list_changed = update_rfa_state(
                    rfa_state,
                    top32_player_ids=top32_ids,
                    current_salaries_by_franchise=salaries,
                    franchise_names=franchise_names,
                    free_agent_moves=fa_moves,
                    bbid_claims=bbid_claims,
                )

                should_post_weekly = False
                if _is_weekly_reports_due(now_rfa):
                    week_key = _current_week_key_et(now_rfa)
                    if week_key != str(updated_state.get("last_rfa_weekly_week_key") or ""):
                        should_post_weekly = True
                        updated_state["last_rfa_weekly_week_key"] = week_key

                should_post_report = list_changed or should_post_weekly
                if should_post_report:
                    report_text = format_rfa_report_text(
                        updated_state.get("active_rfas") or {},
                        players,
                        as_of_line=as_of_rfa,
                    )
                    if len(report_text) > 4096:
                        report_text = report_text[:4093] + "..."
                    dedupe_suffix = (
                        f"WEEKLY|{updated_state.get('last_rfa_weekly_week_key')}"
                        if should_post_weekly and not list_changed
                        else f"CHANGE|{updated_state.get('list_fingerprint')}"
                    )
                    report_key = f"RFA_REPORT|{dedupe_suffix}"
                    if report_key not in seen:
                        pending_posts.append(
                            (
                                report_key,
                                TradeMessagePayload(
                                    RFA_REPORT_TITLE,
                                    report_text,
                                    RFA_REPORT_COLOR,
                                ),
                            )
                        )
                    try:
                        sync_rfa_sheet(updated_state.get("active_rfas") or {}, players)
                    except Exception:
                        logger.exception("Failed syncing RFA Google Sheet")

                if rfa_invalid_claim_alerts_enabled:
                    for claim in invalid_claims:
                        key = invalid_claim_fingerprint(claim)
                        if key in seen:
                            continue
                        description = format_invalid_rfa_claim_text(
                            claim, players, franchise_names
                        )
                        pending_posts.append(
                            (
                                key,
                                TradeMessagePayload(
                                    INVALID_RFA_CLAIM_TITLE,
                                    description,
                                    INVALID_RFA_CLAIM_COLOR,
                                ),
                            )
                        )

                save_rfa_state(rfa_state_path, updated_state)
                updated_rfa_state = True

        if taxi_cut_alerts_enabled or weekly_reports_include_taxi_cut_refunds:
            now_taxi = schedule_now_et
            as_of_taxi = f"As of {_as_of_label_et(now_taxi)}"
            taxi_state = load_taxi_cut_state(taxi_cut_state_path)
            await mfl.sleep_between_exports()
            league_json = await mfl.fetch_league()
            franchise_names = franchise_names_from_league(league_json)
            taxi_percent = include_taxi_salary_percent(league_json)
            await mfl.sleep_between_exports()
            rosters_json = await mfl.fetch_rosters()
            current_taxi = taxi_players_from_rosters(rosters_json)
            non_taxi_roster = non_taxi_roster_players_from_rosters(rosters_json)
            await mfl.sleep_between_exports()
            fa_txs = await mfl.fetch_transactions_by_type(
                "FREE_AGENT",
                days=taxi_cut_lookback_days,
            )
            await mfl.sleep_between_exports()
            taxi_history_days = max(
                taxi_cut_lookback_days,
                current_season_lookback_days(season_year),
            )
            taxi_txs = await mfl.fetch_transactions_by_type(
                "TAXI",
                days=taxi_history_days,
            )
            await mfl.sleep_between_exports()
            adjustments_json = await mfl.fetch_salary_adjustments()
            await mfl.sleep_between_exports()
            players = await mfl.get_players_map()
            fa_drops = [
                move
                for move in parse_free_agent_moves(fa_txs)
                if not move.is_add
            ]
            updated_taxi_state, new_taxi_cuts = update_taxi_cut_state(
                taxi_state,
                current_taxi_players=current_taxi,
                free_agent_drops=fa_drops,
                salary_adjustments=parse_salary_adjustments(adjustments_json),
                players_map=players,
                taxi_percent=taxi_percent,
                now_ts=int(time.time()),
                non_taxi_roster_players=non_taxi_roster,
                taxi_squad_moves=parse_taxi_squad_moves(taxi_txs),
            )

            if taxi_cut_alerts_enabled:
                unseen_cuts = [
                    cut
                    for cut in new_taxi_cuts
                    if taxi_cut_alert_fingerprint(cut) not in seen
                ]
                for cut_group in group_taxi_cuts_for_alerts(unseen_cuts):
                    keys = [taxi_cut_alert_fingerprint(cut) for cut in cut_group]
                    pending_posts.append(
                        (
                            keys,
                            TradeMessagePayload(
                                TAXI_CUT_ALERT_TITLE,
                                format_taxi_cut_alert_text(
                                    cut_group, franchise_names
                                ),
                                TAXI_CUT_ALERT_COLOR,
                            ),
                        )
                    )

            if weekly_reports_include_taxi_cut_refunds and _is_weekly_reports_due(
                now_taxi
            ):
                week_key = _current_week_key_et(now_taxi)
                if week_key != str(updated_taxi_state.get("last_weekly_week_key") or ""):
                    pending_rows = unreimbursed_taxi_cuts(updated_taxi_state)
                    report_text = format_taxi_cut_weekly_report_text(
                        pending_rows,
                        franchise_names,
                    )
                    body = (
                        report_text.split("\n\n", 1)[1]
                        if "\n\n" in report_text
                        else report_text
                    )
                    description = f"{as_of_taxi}\n\n{body}"
                    if len(description) > 4096:
                        description = description[:4093] + "..."
                    report_key = f"WEEKLY_REPORT|{week_key}|{TAXI_CUT_WEEKLY_TITLE}"
                    if report_key not in seen:
                        pending_posts.append(
                            (
                                report_key,
                                TradeMessagePayload(
                                    TAXI_CUT_WEEKLY_TITLE,
                                    description,
                                    TAXI_CUT_WEEKLY_COLOR,
                                ),
                            )
                        )
                    updated_taxi_state["last_weekly_week_key"] = week_key

            save_taxi_cut_state(taxi_cut_state_path, updated_taxi_state)
            updated_taxi_cut_state = True
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
        for key_or_keys, payload in pending_posts:
            keys = (
                [key_or_keys]
                if isinstance(key_or_keys, str)
                else [str(key) for key in key_or_keys]
            )
            ok = await _post_embed_return_ok(dclient, channel_id, payload)
            if not ok:
                if updated:
                    save_seen(seen_path, seen)
                return 1
            for key in keys:
                seen.add(key)
            updated = True

    if updated:
        save_seen(seen_path, seen)
        logger.info("Updated dedupe state (%s keys)", len(seen))
    if updated_reports_state:
        logger.info("Updated weekly reports state")
    if updated_rfa_state:
        logger.info("Updated RFA report state")
    if updated_taxi_cut_state:
        logger.info("Updated taxi-cut refund state")
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
