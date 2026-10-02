"""Tuesday top-rookie-by-round report (current-year draft, completed NFL weeks)."""

from __future__ import annotations

from dataclasses import dataclass

from src.draft_notify import DraftPickSelection
from src.roster_violations import player_position_from_label
from src.top_scorers_report import player_team_from_label

TOP_ROOKIE_BY_ROUND_TITLE = "Top Rookie by Round"
TOP_ROOKIE_BY_ROUND_COLOR = 5763719  # green

# MFL player exports use a few nonstandard NFL abbreviations.
_MFL_TEAM_ABBREVIATIONS = {
    "GBP": "GB",
    "JAC": "JAX",
    "KCC": "KC",
    "LVR": "LV",
    "NEP": "NE",
    "NOS": "NO",
    "SFO": "SF",
    "TBB": "TB",
}


@dataclass(frozen=True)
class RoundRookieLeader:
    round_number: int
    player_id: str
    name: str
    position: str
    team: str
    points: float
    round_average: float


def top_rookie_dedupe_key(date_et: str) -> str:
    return f"TOP_ROOKIE_BY_ROUND|{date_et}"


def last_completed_nfl_week(
    current_week: str,
    *,
    current_week_is_final: bool,
) -> int | None:
    """Latest NFL week whose games are all final.

    When the schedule has already rolled to the next week, that week is not
    final yet, so scoring stops at the previous week.
    """
    try:
        week = int(str(current_week).strip())
    except (TypeError, ValueError):
        return None
    if week <= 0:
        return None
    if current_week_is_final:
        return week
    if week <= 1:
        return None
    return week - 1


def display_player_name(raw_name: str) -> str:
    """MFL ``Last, First`` -> ``First Last``."""
    text = str(raw_name or "").strip()
    if "," not in text:
        return text
    last, first = text.split(",", 1)
    flipped = f"{first.strip()} {last.strip()}".strip()
    return flipped or text


def display_nfl_team(mfl_team: str) -> str:
    team = str(mfl_team or "").strip().upper()
    if not team:
        return ""
    return _MFL_TEAM_ABBREVIATIONS.get(team, team)


def _player_name_from_label(label: str) -> str:
    parts = str(label).strip().rsplit(None, 2)
    if len(parts) >= 3:
        return parts[0].strip()
    return str(label).strip()


def top_rookies_by_round(
    selections: list[DraftPickSelection],
    points_by_player: dict[str, float],
    players_map: dict[str, str],
) -> list[RoundRookieLeader]:
    """Highest scorer in each draft round, plus that round's average points."""
    by_round: dict[int, list[DraftPickSelection]] = {}
    for selection in selections:
        by_round.setdefault(selection.round_number, []).append(selection)

    leaders: list[RoundRookieLeader] = []
    for round_number in sorted(by_round):
        picks = by_round[round_number]
        if not picks:
            continue
        points = [points_by_player.get(pick.player_id, 0.0) for pick in picks]
        round_average = sum(points) / len(points)

        def sort_key(pick: DraftPickSelection) -> tuple[float, str, str]:
            label = players_map.get(pick.player_id) or ""
            name = display_player_name(_player_name_from_label(label))
            return (-points_by_player.get(pick.player_id, 0.0), name.casefold(), pick.player_id)

        leader = min(picks, key=sort_key)
        label = players_map.get(leader.player_id) or ""
        leaders.append(
            RoundRookieLeader(
                round_number=round_number,
                player_id=leader.player_id,
                name=display_player_name(_player_name_from_label(label))
                or f"Player {leader.player_id}",
                position=player_position_from_label(label),
                team=display_nfl_team(player_team_from_label(label)),
                points=points_by_player.get(leader.player_id, 0.0),
                round_average=round_average,
            )
        )
    return leaders


def _player_suffix(leader: RoundRookieLeader) -> str:
    if leader.position and leader.team:
        return f" ({leader.position}, {leader.team})"
    if leader.position:
        return f" ({leader.position})"
    if leader.team:
        return f" ({leader.team})"
    return ""


def format_top_rookie_by_round_text(
    leaders: list[RoundRookieLeader],
    *,
    title: str = TOP_ROOKIE_BY_ROUND_TITLE,
) -> str:
    if not leaders:
        return f"{title}\n\nNo draft results found."
    lines = [title, ""]
    for leader in leaders:
        lines.append(
            f"Round {leader.round_number}: {leader.name}{_player_suffix(leader)} "
            f"— {leader.points:.1f} (avg {leader.round_average:.1f})"
        )
    return "\n".join(lines)
