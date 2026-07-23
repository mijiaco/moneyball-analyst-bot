"""Restricted Free Agent Discord formatting and public re-exports."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.rfa_state import (
    BbidWaiverClaim,
    FreeAgentMove,
    InvalidRfaClaim,
    active_rfa_fingerprint,
    invalid_claim_fingerprint,
    is_likely_player_id,
    load_rfa_state,
    parse_bbid_waiver_claims,
    parse_free_agent_moves,
    parse_salary_float,
    save_rfa_state,
    update_rfa_state,
)

RFA_REPORT_TITLE = "Restricted Free Agent Report"
INVALID_RFA_CLAIM_TITLE = "Invalid Restricted Free Agent Claim"
RFA_REPORT_COLOR = 10181046  # purple
INVALID_RFA_CLAIM_COLOR = 15158332  # red

__all__ = [
    "BbidWaiverClaim",
    "FreeAgentMove",
    "INVALID_RFA_CLAIM_COLOR",
    "INVALID_RFA_CLAIM_TITLE",
    "InvalidRfaClaim",
    "RFA_REPORT_COLOR",
    "RFA_REPORT_TITLE",
    "active_rfa_fingerprint",
    "format_cut_date",
    "format_invalid_rfa_claim_text",
    "format_money",
    "format_rfa_report_text",
    "invalid_claim_fingerprint",
    "is_likely_player_id",
    "load_rfa_state",
    "parse_bbid_waiver_claims",
    "parse_free_agent_moves",
    "save_rfa_state",
    "update_rfa_state",
]


def format_cut_date(cut_ts: int) -> str:
    if cut_ts <= 0:
        return "unknown date"
    dt = datetime.fromtimestamp(cut_ts, tz=ZoneInfo("America/New_York"))
    return dt.strftime("%Y-%m-%d")


def format_money(raw: str | float | int) -> str:
    value = parse_salary_float(raw)
    if value is None:
        return f"${raw}"
    if float(value).is_integer():
        return f"${int(value)}"
    return f"${value:g}"


def format_rfa_report_text(
    active_rfas: dict[str, dict[str, Any]],
    players: dict[str, str],
    *,
    as_of_line: str | None = None,
) -> str:
    intro = (
        "The following players had top 32 salaries and have restricted waiver wire salaries:"
    )
    footer = (
        "If you bid on a player on the list above, you need to bid the last cut salary or higher."
    )
    lines: list[str] = []
    if as_of_line:
        lines.append(as_of_line)
        lines.append("")
    lines.append(intro)
    lines.append("")
    if not active_rfas:
        lines.append("No restricted free agents at this time.")
    else:
        for player_id in sorted(
            active_rfas.keys(),
            key=lambda pid: (players.get(pid) or pid).lower(),
        ):
            entry = active_rfas[player_id]
            label = players.get(player_id) or f"Player {player_id}"
            salary = format_money(entry.get("last_salary") or "?")
            cut_date = format_cut_date(int(entry.get("cut_ts") or 0))
            team = str(entry.get("cut_by_name") or entry.get("cut_by_franchise_id") or "?")
            lines.append(f"* {label} — last salary {salary} — cut {cut_date} — cut by {team}")
    lines.append("")
    lines.append(footer)
    return "\n".join(lines)


def format_invalid_rfa_claim_text(
    claim: InvalidRfaClaim,
    players: dict[str, str],
    franchise_names: dict[str, str],
) -> str:
    player_label = players.get(claim.player_id) or f"Player {claim.player_id}"
    winner = franchise_names.get(claim.franchise_id, f"Franchise {claim.franchise_id}")
    cutter = franchise_names.get(
        claim.cut_by_franchise_id,
        claim.cut_by_franchise_id or "unknown",
    )
    cut_date = format_cut_date(claim.cut_ts)
    return "\n".join(
        [
            f"**{player_label}** was claimed below the restricted free agent price.",
            "",
            f"* Winning team: {winner}",
            f"* Winning bid: {format_money(claim.winning_bid)}",
            f"* Required minimum (last cut salary): {format_money(claim.required_min)}",
            f"* Originally cut by: {cutter} on {cut_date}",
        ]
    )
