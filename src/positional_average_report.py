"""Tuesday averages for team QB, punter, and kicker (bye teams omitted)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.roster_violations import player_position_from_label
from src.top_scorers_report import player_team_from_label

POSITIONAL_AVERAGES_TITLE = "Positional Averages"
POSITIONAL_AVERAGES_COLOR = 7419530  # steel blue

# MFL team-position units: one score per NFL team.
TEAM_POSITIONS: tuple[tuple[str, str], ...] = (
    ("TMQB", "QB"),
    ("TMPN", "Punter"),
    ("TMPK", "Kicker"),
)
_TEAM_POSITION_CODES = {code for code, _label in TEAM_POSITIONS}


@dataclass(frozen=True)
class TeamPositionScore:
    player_id: str
    position_code: str
    nfl_team: str
    points: float


@dataclass(frozen=True)
class PositionalAverage:
    position_code: str
    label: str
    average: float
    team_count: int


@dataclass(frozen=True)
class ByeAdjustmentItem:
    description: str
    points: float


@dataclass(frozen=True)
class FranchiseByeAdjustment:
    franchise_name: str
    week_score: float
    additional: float
    items: tuple[ByeAdjustmentItem, ...]

    @property
    def adjusted_total(self) -> float:
        return self.week_score + self.additional


def positional_averages_dedupe_key(year: str, week: str) -> str:
    try:
        week_text = f"{int(str(week).strip()):02d}"
    except (TypeError, ValueError):
        week_text = str(week).strip()
    return f"POSITIONAL_AVERAGES|{year}-W{week_text}"


def positional_averages_title(week: str) -> str:
    week_text = str(week).strip()
    if not week_text:
        return POSITIONAL_AVERAGES_TITLE
    return f"{POSITIONAL_AVERAGES_TITLE} — Week {week_text}"


def _as_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


def _as_float(raw: Any) -> float | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(str(raw).strip())
    except ValueError:
        return None


def _score_from_row(
    row: dict[str, Any],
    players_map: dict[str, str],
) -> TeamPositionScore | None:
    player_id = str(row.get("id") or "").strip()
    if not player_id:
        return None
    points = _as_float(row.get("score") or row.get("points") or row.get("fantasyPoints"))
    if points is None:
        return None
    label = players_map.get(player_id) or ""
    position_code = player_position_from_label(label)
    nfl_team = player_team_from_label(label)
    if position_code not in _TEAM_POSITION_CODES or not nfl_team:
        return None
    return TeamPositionScore(
        player_id=player_id,
        position_code=position_code,
        nfl_team=nfl_team,
        points=points,
    )


def team_position_scores_from_exports(
    scores_json: dict[str, Any],
    players_map: dict[str, str],
    live_points: dict[str, float] | None = None,
) -> list[TeamPositionScore]:
    """Team QB / punter / kicker rows, including zeros. Live points fill gaps."""
    block = scores_json.get("playerScores") or scores_json.get("playerscores") or {}
    rows = _as_rows(block.get("playerScore") or block.get("player"))
    by_player: dict[str, TeamPositionScore] = {}
    for row in rows:
        parsed = _score_from_row(row, players_map)
        if parsed is not None:
            by_player[parsed.player_id] = parsed
    for player_id, points in (live_points or {}).items():
        existing = by_player.get(player_id)
        if existing is not None:
            if points > existing.points:
                by_player[player_id] = TeamPositionScore(
                    player_id=existing.player_id,
                    position_code=existing.position_code,
                    nfl_team=existing.nfl_team,
                    points=points,
                )
            continue
        parsed = _score_from_row(
            {"id": player_id, "score": points},
            players_map,
        )
        if parsed is not None:
            by_player[player_id] = parsed
    return list(by_player.values())


def positional_averages_for_playing_teams(
    scores: list[TeamPositionScore],
    playing_teams: set[str],
) -> list[PositionalAverage] | None:
    """
    Average each team position across NFL teams that played.

    Returns None until every playing team has a score at each position, so a
    lagging export is retried instead of posted short. Teams that are not in
    ``playing_teams`` (bye week) are left out, including a zero score.
    """
    if not playing_teams:
        return None
    points_by_position: dict[str, dict[str, float]] = {
        code: {} for code, _label in TEAM_POSITIONS
    }
    for row in scores:
        bucket = points_by_position.get(row.position_code)
        if bucket is None or row.nfl_team not in playing_teams:
            continue
        bucket[row.nfl_team] = row.points
    averages: list[PositionalAverage] = []
    for code, label in TEAM_POSITIONS:
        points_by_team = points_by_position[code]
        if set(points_by_team) != playing_teams:
            return None
        team_count = len(playing_teams)
        averages.append(
            PositionalAverage(
                position_code=code,
                label=label,
                average=sum(points_by_team.values()) / team_count,
                team_count=team_count,
            )
        )
    return averages


def _team_nickname(label: str) -> str:
    parts = str(label).strip().rsplit(None, 2)
    name = parts[0].strip() if len(parts) >= 3 else str(label).strip()
    if "," in name:
        return name.split(",", 1)[0].strip()
    return name


def _is_active_roster_status(status: str) -> bool:
    upper = status.strip().upper()
    if not upper:
        return False
    if upper == "IR" or "INJURED_RESERVE" in upper:
        return False
    if "TAXI" in upper:
        return False
    return True


def week_scores_by_franchise(weekly_json: dict[str, Any]) -> dict[str, float]:
    """franchise id -> official week score from TYPE=weeklyResults."""
    block = weekly_json.get("weeklyResults") or weekly_json
    scores: dict[str, float] = {}
    for matchup in _as_rows(block.get("matchup")):
        for franchise in _as_rows(matchup.get("franchise")):
            franchise_id = str(franchise.get("id") or "").strip()
            score = _as_float(franchise.get("score"))
            if franchise_id and score is not None:
                scores[franchise_id] = score
    return scores


def bye_adjustments_for_franchises(
    rosters_json: dict[str, Any],
    players_map: dict[str, str],
    playing_teams: set[str],
    averages: list[PositionalAverage],
    week_scores: dict[str, float],
    franchise_names: dict[str, str],
) -> list[FranchiseByeAdjustment]:
    """
    Active-roster team QB / punter / kicker whose NFL team is on bye.

    MFL scores that player 0. The franchise receives that position's average
    for each such player, added onto the official week score.
    """
    average_by_code = {row.position_code: row for row in averages}
    label_by_code = {code: label for code, label in TEAM_POSITIONS}
    bye_players: dict[str, tuple[str, str]] = {}
    for player_id, label in players_map.items():
        position_code = player_position_from_label(label)
        nfl_team = player_team_from_label(label)
        if position_code not in _TEAM_POSITION_CODES or not nfl_team:
            continue
        if nfl_team in playing_teams:
            continue
        position_label = label_by_code[position_code]
        bye_players[str(player_id)] = (
            position_code,
            f"{_team_nickname(label)} {position_label}",
        )
    if not bye_players:
        return []
    block = rosters_json.get("rosters") or rosters_json
    grouped: dict[str, list[ByeAdjustmentItem]] = {}
    for franchise in _as_rows(block.get("franchise")):
        franchise_id = str(franchise.get("id") or "").strip()
        if not franchise_id or franchise_id not in week_scores:
            continue
        items: list[ByeAdjustmentItem] = []
        for player in _as_rows(franchise.get("player")):
            if not _is_active_roster_status(str(player.get("status") or "")):
                continue
            bye_player = bye_players.get(str(player.get("id") or "").strip())
            if bye_player is None:
                continue
            position_code, description = bye_player
            average = average_by_code.get(position_code)
            if average is None:
                continue
            items.append(ByeAdjustmentItem(description, average.average))
        if items:
            grouped[franchise_id] = items
    adjustments: list[FranchiseByeAdjustment] = []
    for franchise_id, items in grouped.items():
        ordered = tuple(sorted(items, key=lambda item: item.description.casefold()))
        additional = sum(item.points for item in ordered)
        week_score = week_scores[franchise_id]
        adjustments.append(
            FranchiseByeAdjustment(
                franchise_name=franchise_names.get(
                    franchise_id, f"Franchise {franchise_id}"
                ),
                week_score=week_score,
                additional=additional,
                items=ordered,
            )
        )
    adjustments.sort(
        key=lambda row: (-row.additional, row.franchise_name.casefold())
    )
    return adjustments


def format_positional_averages_text(
    averages: list[PositionalAverage],
    *,
    week: str,
    adjustments: list[FranchiseByeAdjustment] | None = None,
) -> str:
    title = positional_averages_title(week)
    if not averages:
        return f"{title}\n\nNo scores found."
    lines = [title, ""]
    for row in averages:
        lines.append(f"{row.label} — {row.average:.2f}")
    if adjustments:
        lines.append("")
        lines.append("Bye adjustments")
        for row in adjustments:
            lines.append(
                f"{row.franchise_name} — +{row.additional:.2f} — "
                f"{row.week_score:.2f} this week — {row.adjusted_total:.2f} adjusted"
            )
            for item in row.items:
                lines.append(f"  {item.description} +{item.points:.2f}")
    return "\n".join(lines)
