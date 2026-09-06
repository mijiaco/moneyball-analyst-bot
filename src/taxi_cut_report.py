"""Taxi-squad cut detection, dead-money refund tracking, and Discord formatters."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.rfa_state import FreeAgentMove, parse_salary_float

TAXI_CUT_ALERT_TITLE = "Taxi Squad Cut — Cap Refund Needed"
TAXI_CUT_ALERT_COLOR = 15105570  # orange / warning
TAXI_CUT_WEEKLY_TITLE = "Taxi Cut Cap Refunds Pending"
TAXI_CUT_WEEKLY_COLOR = 15105570

# MFL sometimes inserts fields such as ``Other Notes:`` (often with a bare CR)
# between Salary and Original Contract.
_DROPPED_ADJ_RE = re.compile(
    r"^Dropped\s+(?P<label>.+?)\s*\(\s*Salary:\s*\$(?P<salary>[0-9.,]+)\s*,"
    r"(?:.*?\s*)?"
    r"Original Contract:\s*(?P<original>\d+)\s*,"
    r"\s*Years Left:\s*(?P<years>\d+)\s*\)\s*$",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class TaxiSquadMove:
    """One player demotion to or promotion from the taxi squad."""

    player_id: str
    franchise_id: str
    timestamp: int
    is_demotion: bool


def parse_taxi_squad_moves(transactions: list[dict[str, Any]]) -> list[TaxiSquadMove]:
    """
    TAXI transactions use ``demoted`` / ``promoted`` comma-separated player ids.
    """
    moves: list[TaxiSquadMove] = []
    for row in transactions:
        if str(row.get("type") or "").upper() != "TAXI":
            continue
        franchise_id = str(row.get("franchise") or "").strip()
        if not franchise_id:
            continue
        ts = _as_int_ts(row.get("timestamp"))
        for pid in str(row.get("demoted") or "").split(","):
            player_id = pid.strip()
            if not player_id:
                continue
            moves.append(
                TaxiSquadMove(
                    player_id=player_id,
                    franchise_id=franchise_id,
                    timestamp=ts,
                    is_demotion=True,
                )
            )
        for pid in str(row.get("promoted") or "").split(","):
            player_id = pid.strip()
            if not player_id:
                continue
            moves.append(
                TaxiSquadMove(
                    player_id=player_id,
                    franchise_id=franchise_id,
                    timestamp=ts,
                    is_demotion=False,
                )
            )
    moves.sort(key=lambda move: (move.timestamp, move.franchise_id, move.player_id))
    return moves


def was_on_taxi_at_timestamp(
    taxi_moves: list[TaxiSquadMove],
    *,
    player_id: str,
    franchise_id: str,
    at_ts: int,
) -> bool:
    """
    Replay taxi demote/promote history and return whether the player was on that
    franchise's taxi squad immediately before ``at_ts``.
    """
    on_taxi = False
    for move in taxi_moves:
        if move.timestamp > at_ts:
            break
        if move.franchise_id != franchise_id or move.player_id != player_id:
            continue
        on_taxi = move.is_demotion
    return on_taxi


@dataclass(frozen=True)
class SalaryAdjustment:
    franchise_id: str
    timestamp: int
    amount: float
    description: str
    adjustment_id: str


@dataclass(frozen=True)
class TaxiCutEvent:
    player_id: str
    franchise_id: str
    timestamp: int
    dead_money: float
    salary: float
    years_left: int
    player_label: str
    adjustment_key: str


def _as_int_ts(raw: Any) -> int:
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return 0


def _money_cents(amount: float) -> int:
    return int(round(amount * 100))


def format_money(amount: float) -> str:
    return f"${amount:,.2f}"


def taxi_cut_alert_fingerprint(cut: TaxiCutEvent) -> str:
    return f"TAXI_CUT|{cut.franchise_id}|{cut.player_id}|{cut.timestamp}"


def load_taxi_cut_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return _empty_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _empty_state()
    if not isinstance(raw, dict):
        return _empty_state()
    taxi_seen = raw.get("taxi_seen")
    pending = raw.get("pending_cuts")
    return {
        "taxi_seen": taxi_seen if isinstance(taxi_seen, dict) else {},
        "pending_cuts": pending if isinstance(pending, list) else [],
        "last_weekly_week_key": str(raw.get("last_weekly_week_key") or ""),
        "initialized": bool(raw.get("initialized")),
    }


def save_taxi_cut_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "taxi_seen": state.get("taxi_seen") or {},
        "pending_cuts": state.get("pending_cuts") or [],
        "last_weekly_week_key": str(state.get("last_weekly_week_key") or ""),
        "initialized": bool(state.get("initialized")),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _empty_state() -> dict[str, Any]:
    return {
        "taxi_seen": {},
        "pending_cuts": [],
        "last_weekly_week_key": "",
        "initialized": False,
    }


def taxi_players_from_rosters(rosters_json: dict[str, Any]) -> dict[str, dict[str, str]]:
    """player_id -> {franchise_id, salary} for TAXI_SQUAD roster slots."""
    out: dict[str, dict[str, str]] = {}
    for franchise_id_str, player in _iter_roster_players(rosters_json):
        status = str(player.get("status") or "").strip().upper()
        if "TAXI" not in status:
            continue
        player_id = player.get("id")
        if player_id is None or str(player_id).strip() == "":
            continue
        salary = str(player.get("salary") or "").strip()
        out[str(player_id)] = {
            "franchise_id": franchise_id_str,
            "salary": salary,
        }
    return out


def non_taxi_roster_players_from_rosters(
    rosters_json: dict[str, Any],
) -> dict[str, str]:
    """player_id -> franchise_id for active / IR (non-taxi) roster slots."""
    out: dict[str, str] = {}
    for franchise_id_str, player in _iter_roster_players(rosters_json):
        status = str(player.get("status") or "").strip().upper()
        if "TAXI" in status:
            continue
        player_id = player.get("id")
        if player_id is None or str(player_id).strip() == "":
            continue
        out[str(player_id)] = franchise_id_str
    return out


def _iter_roster_players(
    rosters_json: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    block = rosters_json.get("rosters") or {}
    franchise_rows_raw = block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []
    for franchise_row in franchise_rows:
        franchise_id = franchise_row.get("id")
        if franchise_id is None:
            continue
        franchise_id_str = str(franchise_id)
        players_raw = franchise_row.get("player") or []
        if isinstance(players_raw, list):
            players = [player for player in players_raw if isinstance(player, dict)]
        elif isinstance(players_raw, dict):
            players = [players_raw]
        else:
            players = []
        for player in players:
            out.append((franchise_id_str, player))
    return out


def parse_salary_adjustments(adjustments_json: dict[str, Any]) -> list[SalaryAdjustment]:
    block = adjustments_json.get("salaryAdjustments") or {}
    rows_raw = block.get("salaryAdjustment")
    if isinstance(rows_raw, list):
        rows = [row for row in rows_raw if isinstance(row, dict)]
    elif isinstance(rows_raw, dict):
        rows = [rows_raw]
    else:
        rows = []
    out: list[SalaryAdjustment] = []
    for row in rows:
        franchise_id = str(row.get("franchise_id") or "").strip()
        if not franchise_id:
            continue
        amount = parse_salary_float(row.get("amount"))
        if amount is None:
            continue
        out.append(
            SalaryAdjustment(
                franchise_id=franchise_id,
                timestamp=_as_int_ts(row.get("timestamp")),
                amount=amount,
                description=str(row.get("description") or "").strip(),
                adjustment_id=str(row.get("id") or "").strip(),
            )
        )
    return out


def parse_dropped_adjustment_details(
    description: str,
) -> tuple[str, float, int] | None:
    """Return (player_label, salary, years_left) from a Dropped salary-adjustment blurb."""
    match = _DROPPED_ADJ_RE.match(description.strip())
    if not match:
        return None
    salary = parse_salary_float(match.group("salary"))
    if salary is None:
        return None
    try:
        years_left = int(match.group("years"))
    except ValueError:
        return None
    label = match.group("label").strip()
    if not label:
        return None
    return label, salary, years_left


def include_taxi_salary_percent(league_json: dict[str, Any]) -> float | None:
    league_block = league_json.get("league") or league_json
    raw = league_block.get("includeTaxiWithSalary")
    value = parse_salary_float(raw)
    if value is None:
        return None
    return value


def looks_like_taxi_dead_money(
    *,
    dead_money: float,
    salary: float,
    years_left: int,
    taxi_percent: float | None,
    tolerance: float = 0.15,
) -> bool:
    """
    Heuristic: taxi dead money ~= salary * years_left * (taxi_percent/100).

    Example: $10 * 3y * 25% = $7.50. Moss $2 * 3 * 25% = $1.50 (~$1.60 actual).
    """
    if taxi_percent is None or years_left <= 0 or salary <= 0:
        return False
    expected = salary * float(years_left) * (taxi_percent / 100.0)
    return abs(dead_money - expected) <= max(tolerance, abs(expected) * 0.1)


def _player_id_for_dropped_label(
    label: str,
    players_map: dict[str, str],
) -> str | None:
    """Match adjustment label (`Moss, Le'Veon MIA RB`) to a players_map entry."""
    needle = " ".join(label.casefold().split())
    if not needle:
        return None
    exact: list[str] = []
    partial: list[str] = []
    for player_id, mapped in players_map.items():
        mapped_norm = " ".join(str(mapped).casefold().split())
        if mapped_norm == needle:
            exact.append(player_id)
        elif needle in mapped_norm or mapped_norm.startswith(needle):
            partial.append(player_id)
    if len(exact) == 1:
        return exact[0]
    if len(partial) == 1:
        return partial[0]
    return None


def _adjustment_key(adj: SalaryAdjustment) -> str:
    return f"{adj.timestamp}|{adj.franchise_id}|{adj.amount:.2f}|{adj.description}"


def update_taxi_cut_state(
    state: dict[str, Any],
    *,
    current_taxi_players: dict[str, dict[str, str]],
    free_agent_drops: list[FreeAgentMove],
    salary_adjustments: list[SalaryAdjustment],
    players_map: dict[str, str],
    taxi_percent: float | None,
    now_ts: int,
    non_taxi_roster_players: dict[str, str] | None = None,
    taxi_squad_moves: list[TaxiSquadMove] | None = None,
) -> tuple[dict[str, Any], list[TaxiCutEvent]]:
    """
    Refresh taxi history, detect new taxi cuts, apply refund matches.

    A cut is treated as reimbursed when either:
    - a matching negative salary adjustment appears on the franchise, or
    - the originating Dropped dead-money adjustment is no longer present
      (commish deleted the charge instead of posting a refund line).

    Only players previously observed on this franchise's taxi squad are treated
    as taxi cuts. Dead-money that merely *looks* like taxi % (common on cheap
    active-roster cuts) is not enough on its own.

    When ``taxi_squad_moves`` is provided, unreimbursed pending cuts for players
    who were not on taxi at drop time are invalidated (clears heuristic false
    positives such as active-roster / IR drops).

    Returns (updated_state, newly_detected_cuts_for_immediate_alerts).
    """
    taxi_seen_raw = state.get("taxi_seen") or {}
    taxi_seen: dict[str, dict[str, Any]] = {
        str(pid): dict(meta)
        for pid, meta in taxi_seen_raw.items()
        if isinstance(meta, dict)
    }
    pending_raw = state.get("pending_cuts") or []
    pending: list[dict[str, Any]] = [
        dict(row) for row in pending_raw if isinstance(row, dict)
    ]
    pending_keys = {
        str(row.get("adjustment_key") or "")
        for row in pending
        if str(row.get("adjustment_key") or "")
    }

    for player_id, meta in current_taxi_players.items():
        taxi_seen[str(player_id)] = {
            "franchise_id": str(meta.get("franchise_id") or ""),
            "salary": str(meta.get("salary") or ""),
            "last_seen_ts": now_ts,
        }

    # Player moved taxi -> active/IR: clear history so a later active drop is
    # not treated as a taxi cut.
    if non_taxi_roster_players:
        for player_id, franchise_id in non_taxi_roster_players.items():
            seen_meta = taxi_seen.get(player_id)
            if not seen_meta:
                continue
            if str(seen_meta.get("franchise_id") or "") != str(franchise_id):
                continue
            taxi_seen.pop(player_id, None)

    if taxi_squad_moves:
        for move in taxi_squad_moves:
            if move.is_demotion:
                continue
            seen_meta = taxi_seen.get(move.player_id)
            if not seen_meta:
                continue
            if str(seen_meta.get("franchise_id") or "") != move.franchise_id:
                continue
            taxi_seen.pop(move.player_id, None)

    drops_by_franchise_player: dict[tuple[str, str], FreeAgentMove] = {}
    for move in free_agent_drops:
        if move.is_add:
            continue
        drops_by_franchise_player[(move.franchise_id, move.player_id)] = move

    new_cuts: list[TaxiCutEvent] = []
    for adj in salary_adjustments:
        if adj.amount <= 0:
            continue
        parsed = parse_dropped_adjustment_details(adj.description)
        if parsed is None:
            continue
        label, salary, years_left = parsed
        player_id = _player_id_for_dropped_label(label, players_map)
        if player_id is None:
            continue
        adj_key = _adjustment_key(adj)
        if adj_key in pending_keys:
            continue

        seen_meta = taxi_seen.get(player_id)
        on_taxi_history = bool(
            seen_meta
            and str(seen_meta.get("franchise_id") or "") == adj.franchise_id
        )
        if not on_taxi_history:
            continue

        drop = drops_by_franchise_player.get((adj.franchise_id, player_id))
        cut_ts = adj.timestamp or (drop.timestamp if drop else 0)
        cut = TaxiCutEvent(
            player_id=player_id,
            franchise_id=adj.franchise_id,
            timestamp=cut_ts,
            dead_money=adj.amount,
            salary=salary,
            years_left=years_left,
            player_label=players_map.get(player_id) or label,
            adjustment_key=adj_key,
        )
        pending.append(
            {
                "player_id": cut.player_id,
                "franchise_id": cut.franchise_id,
                "timestamp": cut.timestamp,
                "dead_money": cut.dead_money,
                "salary": cut.salary,
                "years_left": cut.years_left,
                "player_label": cut.player_label,
                "adjustment_key": cut.adjustment_key,
                "refunded": False,
                "refund_ts": 0,
            }
        )
        pending_keys.add(adj_key)
        new_cuts.append(cut)
        taxi_seen.pop(player_id, None)

    if taxi_squad_moves:
        for row in pending:
            if row.get("refunded"):
                continue
            player_id = str(row.get("player_id") or "")
            franchise_id = str(row.get("franchise_id") or "")
            cut_ts = _as_int_ts(row.get("timestamp"))
            if not player_id or not franchise_id:
                continue
            if was_on_taxi_at_timestamp(
                taxi_squad_moves,
                player_id=player_id,
                franchise_id=franchise_id,
                at_ts=cut_ts,
            ):
                continue
            row["refunded"] = True
            row["refund_ts"] = now_ts
            row["invalidated_not_taxi"] = True

    refund_candidates = sorted(
        [adj for adj in salary_adjustments if adj.amount < 0],
        key=lambda row: row.timestamp,
    )
    for adj in refund_candidates:
        refund_cents = _money_cents(abs(adj.amount))
        for row in pending:
            if row.get("refunded"):
                continue
            if str(row.get("franchise_id") or "") != adj.franchise_id:
                continue
            if _money_cents(float(row.get("dead_money") or 0)) != refund_cents:
                continue
            cut_ts = _as_int_ts(row.get("timestamp"))
            if adj.timestamp and cut_ts and adj.timestamp < cut_ts:
                continue
            row["refunded"] = True
            row["refund_ts"] = adj.timestamp
            break

    # Commish often clears taxi dead money by deleting the Dropped charge
    # rather than posting a matching negative adjustment.
    current_adjustment_keys = {_adjustment_key(adj) for adj in salary_adjustments}
    for row in pending:
        if row.get("refunded"):
            continue
        adj_key = str(row.get("adjustment_key") or "")
        if not adj_key:
            continue
        if adj_key not in current_adjustment_keys:
            row["refunded"] = True
            row["refund_ts"] = now_ts

    updated = {
        "taxi_seen": taxi_seen,
        "pending_cuts": pending,
        "last_weekly_week_key": str(state.get("last_weekly_week_key") or ""),
        "initialized": True,
    }
    new_cuts.sort(key=lambda cut: (cut.timestamp, cut.franchise_id, cut.player_id))
    return updated, new_cuts


def unreimbursed_taxi_cuts(state: dict[str, Any]) -> list[dict[str, Any]]:
    pending = state.get("pending_cuts") or []
    rows = [
        dict(row)
        for row in pending
        if isinstance(row, dict) and not bool(row.get("refunded"))
    ]
    rows.sort(
        key=lambda row: (
            _as_int_ts(row.get("timestamp")),
            str(row.get("franchise_id") or ""),
            str(row.get("player_id") or ""),
        )
    )
    return rows


def group_taxi_cuts_for_alerts(
    cuts: list[TaxiCutEvent],
) -> list[list[TaxiCutEvent]]:
    """Group cuts by franchise so one Discord embed can list every cut in a batch."""
    grouped: dict[str, list[TaxiCutEvent]] = {}
    order: list[str] = []
    for cut in cuts:
        if cut.franchise_id not in grouped:
            order.append(cut.franchise_id)
            grouped[cut.franchise_id] = []
        grouped[cut.franchise_id].append(cut)
    return [grouped[franchise_id] for franchise_id in order]


def format_taxi_cut_alert_text(
    cut: TaxiCutEvent | list[TaxiCutEvent],
    franchise_names: dict[str, str],
) -> str:
    cuts = cut if isinstance(cut, list) else [cut]
    if not cuts:
        return ""
    franchise_id = cuts[0].franchise_id
    team = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
    lines = [f"**{team}**"]
    for index, row in enumerate(cuts):
        if index:
            lines.append("")
        lines.extend(
            [
                f"* Player: {row.player_label}",
                f"* Taxi salary: {format_money(row.salary)}",
                f"* Cap hit to refund: {format_money(row.dead_money)}",
                f"* Contract years left: {row.years_left}",
            ]
        )
    return "\n".join(lines)


def format_taxi_cut_weekly_report_text(
    pending_cuts: list[dict[str, Any]],
    franchise_names: dict[str, str],
    *,
    title: str = TAXI_CUT_WEEKLY_TITLE,
) -> str:
    if not pending_cuts:
        return f"{title}\n\nNo pending taxi-cut cap refunds."
    lines_by_franchise: dict[str, list[str]] = {}
    for row in pending_cuts:
        franchise_id = str(row.get("franchise_id") or "")
        label = str(row.get("player_label") or row.get("player_id") or "Unknown")
        dead_money = float(row.get("dead_money") or 0)
        salary = float(row.get("salary") or 0)
        years_left = int(row.get("years_left") or 0)
        bullet = (
            f"* {label} — refund {format_money(dead_money)} "
            f"(taxi salary {format_money(salary)}, {years_left} yr left)"
        )
        lines_by_franchise.setdefault(franchise_id, []).append(bullet)
    franchise_ids = sorted(
        lines_by_franchise.keys(),
        key=lambda fid: franchise_names.get(fid, f"Franchise {fid}").casefold(),
    )
    lines = [title, ""]
    for franchise_id in franchise_ids:
        team = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        lines.append(f"**{team}**")
        lines.extend(lines_by_franchise[franchise_id])
        lines.append("")
    return "\n".join(lines).rstrip()
