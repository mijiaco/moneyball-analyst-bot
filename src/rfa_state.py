"""Restricted Free Agent state machine and MFL transaction parsers."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# MFL player ids are typically 4+ digits (avoid matching sheet rank numbers like 1..32).
_PLAYER_ID_RE = re.compile(r"^\d{4,6}$")


@dataclass(frozen=True)
class FreeAgentMove:
    """One player add or drop from a FREE_AGENT transaction."""

    player_id: str
    franchise_id: str
    timestamp: int
    is_add: bool


@dataclass(frozen=True)
class BbidWaiverClaim:
    """Completed BBID_WAIVER win (added player + bid spent)."""

    player_id: str
    franchise_id: str
    timestamp: int
    bid: float
    dropped_player_ids: tuple[str, ...]


@dataclass(frozen=True)
class InvalidRfaClaim:
    player_id: str
    franchise_id: str
    timestamp: int
    winning_bid: float
    required_min: float
    last_salary: str
    cut_by_franchise_id: str
    cut_ts: int


def _as_int_ts(raw: Any) -> int:
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return 0


def parse_salary_float(raw: str | float | int | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).replace(",", "").replace("$", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _split_player_ids(raw: str) -> list[str]:
    out: list[str] = []
    for token in str(raw or "").split(","):
        pid = token.strip()
        if not pid:
            continue
        if pid.upper().startswith(("DP_", "FP_")):
            continue
        out.append(pid)
    return out


def is_likely_player_id(value: str) -> bool:
    return bool(_PLAYER_ID_RE.match(value.strip()))


def parse_free_agent_moves(transactions: list[dict[str, Any]]) -> list[FreeAgentMove]:
    """
    FREE_AGENT ``transaction`` is ``added|dropped`` (comma-separated player ids).
    Example drop-only: ``|13763,``  Example add-only: ``16161,|``
    """
    moves: list[FreeAgentMove] = []
    for row in transactions:
        if str(row.get("type") or "").upper() != "FREE_AGENT":
            continue
        franchise_id = str(row.get("franchise") or "").strip()
        if not franchise_id:
            continue
        ts = _as_int_ts(row.get("timestamp"))
        raw = str(row.get("transaction") or "")
        if "|" not in raw:
            continue
        added_raw, dropped_raw = raw.split("|", 1)
        for pid in _split_player_ids(added_raw):
            moves.append(
                FreeAgentMove(
                    player_id=pid,
                    franchise_id=franchise_id,
                    timestamp=ts,
                    is_add=True,
                )
            )
        for pid in _split_player_ids(dropped_raw):
            moves.append(
                FreeAgentMove(
                    player_id=pid,
                    franchise_id=franchise_id,
                    timestamp=ts,
                    is_add=False,
                )
            )
    return moves


def parse_bbid_waiver_claims(transactions: list[dict[str, Any]]) -> list[BbidWaiverClaim]:
    """
    BBID_WAIVER ``transaction`` is ``player_id|bbid_spent|dropped``.
    """
    claims: list[BbidWaiverClaim] = []
    for row in transactions:
        if str(row.get("type") or "").upper() != "BBID_WAIVER":
            continue
        franchise_id = str(row.get("franchise") or "").strip()
        if not franchise_id:
            continue
        ts = _as_int_ts(row.get("timestamp"))
        raw = str(row.get("transaction") or "")
        parts = raw.split("|")
        if len(parts) < 2:
            continue
        player_ids = _split_player_ids(parts[0])
        if not player_ids:
            continue
        bid = parse_salary_float(parts[1])
        if bid is None:
            continue
        dropped = _split_player_ids(parts[2]) if len(parts) > 2 else []
        claims.append(
            BbidWaiverClaim(
                player_id=player_ids[0],
                franchise_id=franchise_id,
                timestamp=ts,
                bid=bid,
                dropped_player_ids=tuple(dropped),
            )
        )
    return claims


def flatten_roster_player_locations(
    salaries_by_franchise: dict[str, dict[str, str]],
) -> dict[str, tuple[str, str]]:
    """player_id -> (franchise_id, salary). Last franchise wins if duplicated."""
    out: dict[str, tuple[str, str]] = {}
    for franchise_id, players in salaries_by_franchise.items():
        for player_id, salary in players.items():
            out[str(player_id)] = (str(franchise_id), str(salary))
    return out


def active_rfa_fingerprint(active_rfas: dict[str, dict[str, Any]]) -> str:
    parts: list[str] = []
    for player_id in sorted(active_rfas.keys()):
        entry = active_rfas[player_id]
        parts.append(
            "|".join(
                [
                    str(player_id),
                    str(entry.get("last_salary") or ""),
                    str(entry.get("cut_ts") or ""),
                    str(entry.get("cut_by_franchise_id") or ""),
                ]
            )
        )
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def invalid_claim_fingerprint(claim: InvalidRfaClaim) -> str:
    return (
        f"INVALID_RFA|{claim.player_id}|{claim.franchise_id}|"
        f"{claim.timestamp}|{claim.winning_bid}|{claim.required_min}"
    )


def _empty_rfa_state() -> dict[str, Any]:
    return {
        "active_rfas": {},
        "roster_salary_snapshot": {},
        "last_rfa_weekly_week_key": "",
        "list_fingerprint": "",
        "seen_invalid_claims": [],
    }


def load_rfa_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return _empty_rfa_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _empty_rfa_state()
    if not isinstance(raw, dict):
        return _empty_rfa_state()
    active = raw.get("active_rfas")
    if not isinstance(active, dict):
        active = {}
    snapshot = raw.get("roster_salary_snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    seen = raw.get("seen_invalid_claims")
    if not isinstance(seen, list):
        seen = []
    return {
        "active_rfas": {str(k): v for k, v in active.items() if isinstance(v, dict)},
        "roster_salary_snapshot": snapshot,
        "last_rfa_weekly_week_key": str(raw.get("last_rfa_weekly_week_key") or ""),
        "list_fingerprint": str(raw.get("list_fingerprint") or ""),
        "seen_invalid_claims": [str(x) for x in seen],
    }


def save_rfa_state(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def update_rfa_state(
    state: dict[str, Any],
    *,
    top32_player_ids: set[str],
    current_salaries_by_franchise: dict[str, dict[str, str]],
    franchise_names: dict[str, str],
    free_agent_moves: list[FreeAgentMove],
    bbid_claims: list[BbidWaiverClaim],
    now_ts: int | None = None,
) -> tuple[dict[str, Any], list[InvalidRfaClaim], bool]:
    """
    Apply roster diff + transactions to RFA state.

    Returns (updated_state, new_invalid_claims, list_changed).
    """
    poll_ts = int(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    active: dict[str, dict[str, Any]] = {
        str(k): dict(v) for k, v in (state.get("active_rfas") or {}).items()
    }
    prev_snapshot_raw = state.get("roster_salary_snapshot") or {}
    prev_locations = flatten_roster_player_locations(
        {
            str(fid): {str(pid): str(sal) for pid, sal in players.items()}
            for fid, players in prev_snapshot_raw.items()
            if isinstance(players, dict)
        }
    )
    current_locations = flatten_roster_player_locations(current_salaries_by_franchise)

    drop_ts_by_player: dict[str, tuple[int, str]] = {}
    for move in free_agent_moves:
        if move.is_add:
            continue
        prev = drop_ts_by_player.get(move.player_id)
        if prev is None or move.timestamp >= prev[0]:
            drop_ts_by_player[move.player_id] = (move.timestamp, move.franchise_id)

    for player_id, (franchise_id, salary) in prev_locations.items():
        if player_id not in top32_player_ids:
            continue
        if player_id in current_locations:
            continue
        if player_id in active:
            continue
        cut_ts = drop_ts_by_player[player_id][0] if player_id in drop_ts_by_player else poll_ts
        active[player_id] = {
            "last_salary": salary,
            "cut_ts": cut_ts,
            "cut_by_franchise_id": franchise_id,
            "cut_by_name": franchise_names.get(franchise_id, f"Franchise {franchise_id}"),
        }

    invalid_claims: list[InvalidRfaClaim] = []
    seen_invalid = set(str(x) for x in (state.get("seen_invalid_claims") or []))

    def _maybe_invalid(
        player_id: str,
        franchise_id: str,
        timestamp: int,
        winning_bid: float,
    ) -> None:
        entry = active.get(player_id)
        if entry is None:
            return
        required = parse_salary_float(entry.get("last_salary"))
        if required is None:
            return
        if winning_bid >= required:
            return
        claim = InvalidRfaClaim(
            player_id=player_id,
            franchise_id=franchise_id,
            timestamp=timestamp,
            winning_bid=winning_bid,
            required_min=required,
            last_salary=str(entry.get("last_salary") or ""),
            cut_by_franchise_id=str(entry.get("cut_by_franchise_id") or ""),
            cut_ts=int(entry.get("cut_ts") or 0),
        )
        key = invalid_claim_fingerprint(claim)
        if key in seen_invalid:
            return
        invalid_claims.append(claim)
        seen_invalid.add(key)

    for claim in bbid_claims:
        if claim.player_id in active:
            _maybe_invalid(
                claim.player_id,
                claim.franchise_id,
                claim.timestamp,
                claim.bid,
            )

    for player_id in list(active.keys()):
        if player_id not in current_locations:
            continue
        franchise_id, new_salary = current_locations[player_id]
        bid = parse_salary_float(new_salary)
        add_ts = 0
        for move in free_agent_moves:
            if move.is_add and move.player_id == player_id and move.timestamp >= add_ts:
                add_ts = move.timestamp
                franchise_id = move.franchise_id
        for claim in bbid_claims:
            if claim.player_id == player_id and claim.timestamp >= add_ts:
                add_ts = claim.timestamp
                franchise_id = claim.franchise_id
                bid = claim.bid
        if add_ts <= 0:
            add_ts = poll_ts
        if bid is not None:
            _maybe_invalid(player_id, franchise_id, add_ts, bid)
        del active[player_id]

    prev_fp = str(state.get("list_fingerprint") or "")
    new_fingerprint = active_rfa_fingerprint(active)
    bootstrapping = not prev_snapshot_raw and not prev_fp
    list_changed = (not bootstrapping) and (new_fingerprint != prev_fp)

    updated = {
        "active_rfas": active,
        "roster_salary_snapshot": {
            str(fid): {str(pid): str(sal) for pid, sal in players.items()}
            for fid, players in current_salaries_by_franchise.items()
        },
        "last_rfa_weekly_week_key": str(state.get("last_rfa_weekly_week_key") or ""),
        "list_fingerprint": new_fingerprint,
        "seen_invalid_claims": sorted(seen_invalid),
    }
    return updated, invalid_claims, list_changed
