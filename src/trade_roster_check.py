"""Post-trade roster validity for Discord trade embeds."""

from __future__ import annotations

import copy
import re
import time
from typing import Any

from src.roster_violations import (
    find_ir_eligibility_violations,
    find_salary_cap_violations,
    find_slot_limit_violations,
    find_starter_requirement_violations,
    format_trade_parties_roster_violations_text,
    franchise_salary_caps_from_league,
    ir_eligible_statuses_from_env,
    league_slot_limits,
    starter_lineup_size,
    starter_position_minimums,
)

_MONEY_RE = re.compile(r"[^0-9.\-]")


def _split_gave_up(raw: str | None) -> list[str]:
    if not raw:
        return []
    text = raw.strip()
    if not text:
        return []
    if ";" in text:
        return [part.strip() for part in text.split(";") if part.strip()]
    comma_parts = [part.strip() for part in text.split(",") if part.strip()]
    if not comma_parts:
        return []
    if all(
        part.startswith("DP_")
        or part.startswith("FP_")
        or re.fullmatch(r"\d+", part) is not None
        for part in comma_parts
    ):
        return comma_parts
    return [text.rstrip(",")]


def player_ids_from_gave_up(
    gave_up: str | None,
    players_map: dict[str, str],
) -> list[str]:
    """Resolve trade-side tokens to player ids (skips draft-pick tokens)."""
    name_index: dict[str, str] = {}
    duplicates: set[str] = set()
    for player_id, label in players_map.items():
        key = " ".join(str(label).strip().split()).casefold()
        if not key:
            continue
        existing = name_index.get(key)
        if existing is None:
            name_index[key] = player_id
        elif existing != player_id:
            duplicates.add(key)
    for dup in duplicates:
        name_index.pop(dup, None)

    out: list[str] = []
    for token in _split_gave_up(gave_up):
        if token.startswith("DP_") or token.startswith("FP_"):
            continue
        if re.fullmatch(r"\d+", token):
            out.append(token)
            continue
        name_key = " ".join(token.strip().split()).casefold()
        resolved = name_index.get(name_key)
        if resolved:
            out.append(resolved)
    return out


def _franchise_rows(rosters_json: dict[str, Any]) -> list[dict[str, Any]]:
    block = rosters_json.get("rosters") or {}
    rows_raw = block.get("franchise")
    if isinstance(rows_raw, list):
        return [row for row in rows_raw if isinstance(row, dict)]
    if isinstance(rows_raw, dict):
        return [rows_raw]
    return []


def _player_id_set(franchise_row: dict[str, Any]) -> set[str]:
    raw = franchise_row.get("player") or []
    if isinstance(raw, dict):
        rows = [raw]
    elif isinstance(raw, list):
        rows = [row for row in raw if isinstance(row, dict)]
    else:
        rows = []
    return {str(row.get("id") or "") for row in rows if row.get("id") is not None}


def _franchise_players(franchise_row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = franchise_row.get("player") or []
    if isinstance(raw, list):
        rows = [row for row in raw if isinstance(row, dict)]
    elif isinstance(raw, dict):
        rows = [raw]
    else:
        rows = []
    franchise_row["player"] = rows
    return rows


def apply_player_trade_to_rosters(
    rosters_json: dict[str, Any],
    *,
    franchise_a: str,
    player_ids_a_to_b: list[str],
    franchise_b: str,
    player_ids_b_to_a: list[str],
) -> dict[str, Any]:
    """
    Copy rosters and move listed players A↔B when they still sit on the sender.

    If the trade is already processed, players are already on the receiver and
    this is a no-op for those ids.
    """
    applied = copy.deepcopy(rosters_json)
    franchise_rows = _franchise_rows(applied)
    if not franchise_rows:
        return applied
    by_id = {
        str(row.get("id")): row for row in franchise_rows if row.get("id") is not None
    }
    sender_a = by_id.get(str(franchise_a).strip())
    sender_b = by_id.get(str(franchise_b).strip())
    if sender_a is None or sender_b is None:
        return applied

    def _move(source: dict[str, Any], dest: dict[str, Any], player_id: str) -> None:
        source_players = _franchise_players(source)
        dest_players = _franchise_players(dest)
        dest_ids = {str(row.get("id") or "") for row in dest_players}
        if player_id in dest_ids:
            return
        match: dict[str, Any] | None = None
        kept: list[dict[str, Any]] = []
        for row in source_players:
            if match is None and str(row.get("id") or "") == player_id:
                match = row
                continue
            kept.append(row)
        if match is None:
            return
        source["player"] = kept
        dest_players.append(match)
        dest["player"] = dest_players

    for player_id in player_ids_a_to_b:
        _move(sender_a, sender_b, player_id)
    for player_id in player_ids_b_to_a:
        _move(sender_b, sender_a, player_id)
    return applied


def _salary_amount(raw: str | None) -> float:
    if raw is None or str(raw).strip() == "":
        return 0.0
    text = _MONEY_RE.sub("", str(raw).strip())
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _player_ids_still_on_sender(
    rosters_json: dict[str, Any],
    *,
    sender_id: str,
    receiver_id: str,
    player_ids: list[str],
) -> list[str]:
    """Ids that still need a salary move: on sender, not already on receiver."""
    by_id = {
        str(row.get("id")): row
        for row in _franchise_rows(rosters_json)
        if row.get("id") is not None
    }
    sender = by_id.get(str(sender_id).strip())
    receiver = by_id.get(str(receiver_id).strip())
    if sender is None or receiver is None:
        return []
    on_sender = _player_id_set(sender)
    on_receiver = _player_id_set(receiver)
    still: list[str] = []
    for player_id in player_ids:
        if player_id in on_receiver or player_id not in on_sender:
            continue
        still.append(player_id)
    return still


def apply_player_trade_to_salaries(
    salary_by_franchise: dict[str, float],
    player_salaries: dict[str, dict[str, str]],
    *,
    franchise_a: str,
    player_ids_a_to_b: list[str],
    franchise_b: str,
    player_ids_b_to_a: list[str],
    rosters_json: dict[str, Any] | None = None,
) -> dict[str, float]:
    """
    Shift standings salary for players still sitting on the sender.

    If rosters already show the trade, this is a no-op so we do not double-count
    leagueStandings totals that MFL updated during the pending/veto window.
    """
    adjusted = dict(salary_by_franchise)
    a = str(franchise_a).strip()
    b = str(franchise_b).strip()
    if rosters_json is not None:
        player_ids_a_to_b = _player_ids_still_on_sender(
            rosters_json,
            sender_id=a,
            receiver_id=b,
            player_ids=player_ids_a_to_b,
        )
        player_ids_b_to_a = _player_ids_still_on_sender(
            rosters_json,
            sender_id=b,
            receiver_id=a,
            player_ids=player_ids_b_to_a,
        )
    a_map = player_salaries.get(a) or {}
    b_map = player_salaries.get(b) or {}
    for player_id in player_ids_a_to_b:
        amount = _salary_amount(a_map.get(player_id) or b_map.get(player_id))
        if amount == 0:
            continue
        if a in adjusted:
            adjusted[a] = adjusted[a] - amount
        if b in adjusted:
            adjusted[b] = adjusted[b] + amount
    for player_id in player_ids_b_to_a:
        amount = _salary_amount(b_map.get(player_id) or a_map.get(player_id))
        if amount == 0:
            continue
        if b in adjusted:
            adjusted[b] = adjusted[b] - amount
        if a in adjusted:
            adjusted[a] = adjusted[a] + amount
    return adjusted


def _trade_is_processed(tx: dict[str, Any], now_unix: float | None = None) -> bool:
    now = now_unix if now_unix is not None else time.time()
    expires_raw = tx.get("expires")
    if expires_raw is None or expires_raw == "":
        return True
    try:
        expires_at = float(expires_raw)
    except (TypeError, ValueError):
        return True
    return expires_at <= now


def post_trade_roster_warning_text(
    tx: dict[str, Any],
    *,
    franchise_names: dict[str, str],
    players_map: dict[str, str],
    rosters_json: dict[str, Any],
    league_json: dict[str, Any],
    injuries_by_id: dict[str, dict[str, str]],
    salary_by_franchise: dict[str, float],
    player_salaries: dict[str, dict[str, str]],
    now_unix: float | None = None,
) -> str:
    franchise_a = str(tx.get("franchise") or "").strip()
    franchise_b = str(tx.get("franchise2") or "").strip()
    if not franchise_a or not franchise_b:
        return ""
    a_to_b = player_ids_from_gave_up(tx.get("franchise1_gave_up"), players_map)
    b_to_a = player_ids_from_gave_up(tx.get("franchise2_gave_up"), players_map)
    applied_rosters = apply_player_trade_to_rosters(
        rosters_json,
        franchise_a=franchise_a,
        player_ids_a_to_b=a_to_b,
        franchise_b=franchise_b,
        player_ids_b_to_a=b_to_a,
    )
    if _trade_is_processed(tx, now_unix):
        applied_salaries = dict(salary_by_franchise)
    else:
        applied_salaries = apply_player_trade_to_salaries(
            salary_by_franchise,
            player_salaries,
            franchise_a=franchise_a,
            player_ids_a_to_b=a_to_b,
            franchise_b=franchise_b,
            player_ids_b_to_a=b_to_a,
            rosters_json=rosters_json,
        )
    slot_limits = league_slot_limits(league_json)
    return format_trade_parties_roster_violations_text(
        {franchise_a, franchise_b},
        franchise_names,
        find_ir_eligibility_violations(
            applied_rosters,
            injuries_by_id,
            players_map,
            eligible_statuses=ir_eligible_statuses_from_env(),
        ),
        find_slot_limit_violations(
            applied_rosters,
            roster_limit=slot_limits["roster"],
            taxi_limit=slot_limits["taxi"],
            ir_limit=slot_limits["ir"],
        ),
        salary_cap_violations=find_salary_cap_violations(
            applied_salaries,
            franchise_salary_caps_from_league(league_json),
        ),
        starter_requirement_violations=find_starter_requirement_violations(
            applied_rosters,
            players_map,
            position_minimums=starter_position_minimums(league_json),
            lineup_size=starter_lineup_size(league_json),
        ),
    )


def append_trade_roster_warning(body: str, warning: str, *, limit: int) -> str:
    text = body.rstrip()
    if warning:
        text = f"{text}\n\n{warning}"
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text
