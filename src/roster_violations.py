"""Detect and format fantasy roster violations (IR, slots, salary cap, starters)."""

from __future__ import annotations

import os
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

# Default matches common MFL setup "IR or Out or Doubtful" plus optional S/H lists.
# Questionable/Probable are intentionally excluded (matches MFL IR warning for Q).
DEFAULT_IR_ELIGIBLE_STATUSES: frozenset[str] = frozenset(
    {
        "IR",
        "IR-PUP",
        "IR-NFI",
        "IR-R",
        "OUT",
        "DOUBTFUL",
        "SUSPENDED",
        "HOLDOUT",
        "RETIRED",
        "INACTIVE",
        "EXEMPT",
        "PUP",
        "NFI",
    }
)

ROSTER_VIOLATIONS_TITLE = "Roster Violations"
ROSTER_VIOLATIONS_COLOR = 15105570  # orange / warning

_MONEY_RE = re.compile(r"[^0-9.\-]")


@dataclass(frozen=True)
class IrEligibilityViolation:
    franchise_id: str
    player_id: str
    player_label: str
    injury_status: str
    injury_details: str


@dataclass(frozen=True)
class SlotLimitViolation:
    franchise_id: str
    slot_name: str
    count: int
    limit: int


@dataclass(frozen=True)
class SalaryCapViolation:
    franchise_id: str
    salary: float
    cap: float


@dataclass(frozen=True)
class StarterRequirementViolation:
    franchise_id: str
    position: str
    have: int
    need: int


def ir_eligible_statuses_from_env(
    raw: str | None = None,
    *,
    default: frozenset[str] = DEFAULT_IR_ELIGIBLE_STATUSES,
) -> frozenset[str]:
    """
    Parse comma/pipe-separated injury statuses that may occupy IR.

    Env: MFL_IR_ELIGIBLE_STATUSES (e.g. "IR,Out,Doubtful,Suspended,Holdout").
    """
    text = (raw if raw is not None else os.environ.get("MFL_IR_ELIGIBLE_STATUSES") or "").strip()
    if not text:
        return default
    parts = [part.strip().upper() for part in text.replace("|", ",").split(",")]
    cleaned = {part for part in parts if part}
    return frozenset(cleaned) if cleaned else default


def injury_status_by_player_id(injuries_json: dict[str, Any]) -> dict[str, dict[str, str]]:
    """player_id -> {status, details} from TYPE=injuries export."""
    block = injuries_json.get("injuries") or {}
    rows_raw = block.get("injury")
    if isinstance(rows_raw, list):
        rows = [row for row in rows_raw if isinstance(row, dict)]
    elif isinstance(rows_raw, dict):
        rows = [rows_raw]
    else:
        rows = []
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        player_id = row.get("id")
        if player_id is None or str(player_id).strip() == "":
            continue
        status = str(row.get("status") or "").strip()
        details = str(row.get("details") or "").strip()
        out[str(player_id)] = {"status": status, "details": details}
    return out


def _normalize_franchise_rows(rosters_json: dict[str, Any]) -> list[dict[str, Any]]:
    block = rosters_json.get("rosters") or {}
    franchise_rows_raw = block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        return [row for row in franchise_rows_raw if isinstance(row, dict)]
    if isinstance(franchise_rows_raw, dict):
        return [franchise_rows_raw]
    return []


def _normalize_player_rows(franchise_row: dict[str, Any]) -> list[dict[str, Any]]:
    players_raw = franchise_row.get("player") or []
    if isinstance(players_raw, list):
        return [player for player in players_raw if isinstance(player, dict)]
    if isinstance(players_raw, dict):
        return [players_raw]
    return []


def _is_ir_roster_status(status: str) -> bool:
    upper = status.strip().upper()
    return upper == "IR" or "INJURED_RESERVE" in upper


def _is_taxi_roster_status(status: str) -> bool:
    return "TAXI" in status.strip().upper()


def _player_display_label(player_id: str, players_map: dict[str, str]) -> str:
    label = players_map.get(player_id)
    if label and str(label).strip():
        return str(label).strip()
    return f"Player {player_id}"


def player_position_from_label(label: str) -> str:
    """
    Extract position from get_players_map labels (`Name TEAM POS`).

    Name may contain spaces/commas; team and position are trailing single tokens.
    """
    parts = str(label).strip().rsplit(None, 2)
    if len(parts) < 3:
        return ""
    return parts[-1].strip().upper()


def _parse_money(raw: Any) -> float | None:
    if raw is None:
        return None
    text = _MONEY_RE.sub("", str(raw).strip())
    if not text or text in {".", "-", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _money_cents(amount: float) -> int:
    return int(round(amount * 100))


def _format_money(amount: float) -> str:
    return f"${amount:,.2f}"


def franchise_salary_caps_from_league(league_json: dict[str, Any]) -> dict[str, float]:
    """franchise id -> salaryCapAmount from TYPE=league franchises."""
    league_block = league_json.get("league") or league_json
    franchises_block = league_block.get("franchises") or {}
    franchise_rows_raw = franchises_block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []
    out: dict[str, float] = {}
    for row in franchise_rows:
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        amount = _parse_money(row.get("salaryCapAmount"))
        if amount is None:
            continue
        out[str(franchise_id)] = amount
    return out


def franchise_salaries_from_standings(standings_json: dict[str, Any]) -> dict[str, float]:
    """franchise id -> salary total from TYPE=leagueStandings."""
    block = standings_json.get("leagueStandings") or {}
    franchise_rows_raw = block.get("franchise")
    if isinstance(franchise_rows_raw, list):
        franchise_rows = [row for row in franchise_rows_raw if isinstance(row, dict)]
    elif isinstance(franchise_rows_raw, dict):
        franchise_rows = [franchise_rows_raw]
    else:
        franchise_rows = []
    out: dict[str, float] = {}
    for row in franchise_rows:
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        amount = _parse_money(row.get("salary"))
        if amount is None:
            continue
        out[str(franchise_id)] = amount
    return out


def find_salary_cap_violations(
    salary_by_franchise: dict[str, float],
    cap_by_franchise: dict[str, float],
) -> list[SalaryCapViolation]:
    """Franchises whose standings salary exceeds their salaryCapAmount."""
    violations: list[SalaryCapViolation] = []
    for franchise_id, salary in salary_by_franchise.items():
        cap = cap_by_franchise.get(franchise_id)
        if cap is None:
            continue
        if _money_cents(salary) <= _money_cents(cap):
            continue
        violations.append(
            SalaryCapViolation(franchise_id=franchise_id, salary=salary, cap=cap)
        )
    violations.sort(key=lambda row: (-(row.salary - row.cap), row.franchise_id))
    return violations


def _parse_position_limit(limit_raw: Any) -> tuple[int, int] | None:
    text = str(limit_raw or "").strip()
    if not text:
        return None
    if "-" in text:
        low_raw, high_raw = text.split("-", 1)
        try:
            return int(low_raw.strip()), int(high_raw.strip())
        except ValueError:
            return None
    try:
        value = int(text)
    except ValueError:
        return None
    return value, value


def starter_position_minimums(league_json: dict[str, Any]) -> dict[str, int]:
    """position -> minimum starters required from league.starters."""
    league_block = league_json.get("league") or league_json
    starters_block = league_block.get("starters") or {}
    positions_raw = starters_block.get("position")
    if isinstance(positions_raw, list):
        position_rows = [row for row in positions_raw if isinstance(row, dict)]
    elif isinstance(positions_raw, dict):
        position_rows = [positions_raw]
    else:
        position_rows = []
    out: dict[str, int] = {}
    for row in position_rows:
        name = str(row.get("name") or "").strip().upper()
        if not name:
            continue
        parsed = _parse_position_limit(row.get("limit"))
        if parsed is None:
            continue
        minimum, _maximum = parsed
        if minimum <= 0:
            continue
        out[name] = minimum
    return out


def starter_lineup_size(league_json: dict[str, Any]) -> int | None:
    """Total starter count from league.starters.count when present."""
    league_block = league_json.get("league") or league_json
    starters_block = league_block.get("starters") or {}
    raw = starters_block.get("count")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = int(str(raw).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def find_starter_requirement_violations(
    rosters_json: dict[str, Any],
    players_map: dict[str, str],
    *,
    position_minimums: dict[str, int],
    lineup_size: int | None = None,
    player_positions: dict[str, str] | None = None,
) -> list[StarterRequirementViolation]:
    """
    Active-roster depth short of league starter minimums.

    Uses non-IR / non-taxi players only. Position comes from ``player_positions``
    when provided, otherwise from trailing tokens on ``players_map`` labels.
    """
    if not position_minimums and lineup_size is None:
        return []
    violations: list[StarterRequirementViolation] = []
    for franchise_row in _normalize_franchise_rows(rosters_json):
        franchise_id = franchise_row.get("id")
        if franchise_id is None:
            continue
        franchise_id_str = str(franchise_id)
        position_counts: Counter[str] = Counter()
        active_count = 0
        for player in _normalize_player_rows(franchise_row):
            status = str(player.get("status") or "")
            if _is_ir_roster_status(status) or _is_taxi_roster_status(status):
                continue
            active_count += 1
            player_id_raw = player.get("id")
            if player_id_raw is None:
                continue
            player_id = str(player_id_raw)
            if player_positions is not None:
                position = str(player_positions.get(player_id) or "").strip().upper()
            else:
                position = player_position_from_label(
                    _player_display_label(player_id, players_map)
                )
            if position:
                position_counts[position] += 1
        if lineup_size is not None and active_count < lineup_size:
            violations.append(
                StarterRequirementViolation(
                    franchise_id=franchise_id_str,
                    position="TOTAL",
                    have=active_count,
                    need=lineup_size,
                )
            )
        for position, need in sorted(position_minimums.items()):
            have = int(position_counts.get(position, 0))
            if have >= need:
                continue
            violations.append(
                StarterRequirementViolation(
                    franchise_id=franchise_id_str,
                    position=position,
                    have=have,
                    need=need,
                )
            )
    violations.sort(key=lambda row: (row.franchise_id, row.position))
    return violations


def find_ir_eligibility_violations(
    rosters_json: dict[str, Any],
    injuries_by_player_id: dict[str, dict[str, str]],
    players_map: dict[str, str],
    *,
    eligible_statuses: frozenset[str] = DEFAULT_IR_ELIGIBLE_STATUSES,
) -> list[IrEligibilityViolation]:
    """Players on IR whose NFL injury status is missing or not IR-eligible."""
    eligible = {status.upper() for status in eligible_statuses}
    violations: list[IrEligibilityViolation] = []
    for franchise_row in _normalize_franchise_rows(rosters_json):
        franchise_id = franchise_row.get("id")
        if franchise_id is None:
            continue
        franchise_id_str = str(franchise_id)
        for player in _normalize_player_rows(franchise_row):
            roster_status = str(player.get("status") or "")
            if not _is_ir_roster_status(roster_status):
                continue
            player_id_raw = player.get("id")
            if player_id_raw is None or str(player_id_raw).strip() == "":
                continue
            player_id = str(player_id_raw)
            injury = injuries_by_player_id.get(player_id) or {}
            injury_status = str(injury.get("status") or "").strip()
            injury_details = str(injury.get("details") or "").strip()
            if injury_status and injury_status.upper() in eligible:
                continue
            violations.append(
                IrEligibilityViolation(
                    franchise_id=franchise_id_str,
                    player_id=player_id,
                    player_label=_player_display_label(player_id, players_map),
                    injury_status=injury_status or "(none)",
                    injury_details=injury_details,
                )
            )
    violations.sort(
        key=lambda row: (
            row.franchise_id,
            row.player_label.casefold(),
            row.player_id,
        )
    )
    return violations


def league_slot_limits(league_json: dict[str, Any]) -> dict[str, int | None]:
    """roster / taxi / IR slot caps from TYPE=league (None when unset/unparseable)."""
    league_block = league_json.get("league") or league_json

    def _parse_limit(key: str) -> int | None:
        raw = league_block.get(key)
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return int(str(raw).strip())
        except ValueError:
            return None

    return {
        "roster": _parse_limit("rosterSize"),
        "taxi": _parse_limit("taxiSquad"),
        "ir": _parse_limit("injuredReserve"),
    }


def find_slot_limit_violations(
    rosters_json: dict[str, Any],
    *,
    roster_limit: int | None,
    taxi_limit: int | None,
    ir_limit: int | None,
) -> list[SlotLimitViolation]:
    """Franchises exceeding active / taxi / IR slot caps from league settings."""
    violations: list[SlotLimitViolation] = []
    for franchise_row in _normalize_franchise_rows(rosters_json):
        franchise_id = franchise_row.get("id")
        if franchise_id is None:
            continue
        franchise_id_str = str(franchise_id)
        active_count = 0
        taxi_count = 0
        ir_count = 0
        for player in _normalize_player_rows(franchise_row):
            status = str(player.get("status") or "")
            if _is_ir_roster_status(status):
                ir_count += 1
            elif _is_taxi_roster_status(status):
                taxi_count += 1
            else:
                active_count += 1
        checks = (
            ("active roster", active_count, roster_limit),
            ("taxi squad", taxi_count, taxi_limit),
            ("IR", ir_count, ir_limit),
        )
        for slot_name, count, limit in checks:
            if limit is None or count <= limit:
                continue
            violations.append(
                SlotLimitViolation(
                    franchise_id=franchise_id_str,
                    slot_name=slot_name,
                    count=count,
                    limit=limit,
                )
            )
    violations.sort(key=lambda row: (row.franchise_id, row.slot_name))
    return violations


def format_roster_violations_report_text(
    franchise_names: dict[str, str],
    ir_violations: list[IrEligibilityViolation],
    slot_violations: list[SlotLimitViolation],
    *,
    salary_cap_violations: list[SalaryCapViolation] | None = None,
    starter_requirement_violations: list[StarterRequirementViolation] | None = None,
    title: str = ROSTER_VIOLATIONS_TITLE,
) -> str:
    """Discord-style description body (title line + blank + bullets by team)."""
    salary_rows = salary_cap_violations or []
    starter_rows = starter_requirement_violations or []
    if not ir_violations and not slot_violations and not salary_rows and not starter_rows:
        return f"{title}\n\nNo roster violations found."

    lines_by_franchise: dict[str, list[str]] = {}
    for violation in ir_violations:
        detail_part = f" ({violation.injury_details})" if violation.injury_details else ""
        bullet = (
            f"* IR eligibility: {violation.player_label} — "
            f"{violation.injury_status}{detail_part}"
        )
        lines_by_franchise.setdefault(violation.franchise_id, []).append(bullet)
    for violation in slot_violations:
        bullet = (
            f"* Slot limit: {violation.slot_name} {violation.count} "
            f"(limit {violation.limit})"
        )
        lines_by_franchise.setdefault(violation.franchise_id, []).append(bullet)
    for violation in salary_rows:
        over = violation.salary - violation.cap
        bullet = (
            f"* Salary cap: {_format_money(violation.salary)} used / "
            f"{_format_money(violation.cap)} cap "
            f"({_format_money(over)} over)"
        )
        lines_by_franchise.setdefault(violation.franchise_id, []).append(bullet)
    for violation in starter_rows:
        if violation.position == "TOTAL":
            bullet = (
                f"* Starting roster: {violation.have} active players "
                f"(need {violation.need} starters)"
            )
        else:
            bullet = (
                f"* Starting roster: {violation.position} "
                f"{violation.have}/{violation.need}"
            )
        lines_by_franchise.setdefault(violation.franchise_id, []).append(bullet)

    franchise_ids = sorted(
        lines_by_franchise.keys(),
        key=lambda franchise_id: franchise_names.get(
            franchise_id, f"Franchise {franchise_id}"
        ).casefold(),
    )
    lines = [title, ""]
    for franchise_id in franchise_ids:
        team_name = franchise_names.get(franchise_id, f"Franchise {franchise_id}")
        lines.append(f"**{team_name}**")
        lines.extend(lines_by_franchise[franchise_id])
        lines.append("")
    return "\n".join(lines).rstrip()
