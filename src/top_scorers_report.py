"""Weekly NFL-slate top scorers (league scoring) for Discord embeds."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.roster_violations import player_position_from_label

ET = ZoneInfo("America/New_York")

TOP_SCORERS_COLOR = 3066993  # teal
TOP_SCORERS_LIMIT = 5
TOP_SCORERS_POSITIONS: tuple[str, ...] = (
    "QB",
    "RB",
    "WR",
    "TE",
    "PK",
    "DT",
    "DE",
    "LB",
    "CB",
    "S",
)

# NFL week order Wed→Tue so Sunday slates stay due through Tuesday catch-up.
_NFL_WEEKDAY_ORDER = {2: 0, 3: 1, 4: 2, 5: 3, 6: 4, 0: 5, 1: 6}

# slate_id -> (weekday, hour, minute) earliest ET post time
_SLATE_DUE: dict[str, tuple[int, int, int]] = {
    "wed": (2, 23, 45),
    "tnf": (3, 23, 45),
    "early_sun": (6, 16, 15),
    "late_sun": (6, 20, 0),
    "snf": (6, 23, 45),
    "mnf": (1, 0, 30),
    "cumulative": (1, 0, 30),
}

SLATE_TITLES: dict[str, str] = {
    "wed": "Wednesday",
    "tnf": "Thursday Night",
    "early_sun": "Sunday Early (1:00 PM)",
    "late_sun": "Sunday Late (4:00 PM)",
    "snf": "Sunday Night",
    "mnf": "Monday Night",
    "cumulative": "Week Cumulative",
}

SLATE_PROCESS_ORDER: tuple[str, ...] = (
    "wed",
    "tnf",
    "early_sun",
    "late_sun",
    "snf",
    "mnf",
    "cumulative",
)


@dataclass(frozen=True)
class NflGame:
    slate_id: str
    kickoff: datetime
    is_final: bool
    team_ids: tuple[str, ...]


@dataclass(frozen=True)
class PlayerWeekScore:
    player_id: str
    position: str
    label: str
    points: float
    nfl_team: str


def player_team_from_label(label: str) -> str:
    parts = str(label).strip().rsplit(None, 2)
    if len(parts) < 3:
        return ""
    return parts[-2].strip().upper()


def slate_id_for_kickoff(kickoff: datetime) -> str:
    kickoff_et = kickoff.astimezone(ET)
    weekday = kickoff_et.weekday()
    hour = kickoff_et.hour
    if weekday == 6 and hour < 15:
        return "early_sun"
    if weekday == 6 and hour < 18:
        return "late_sun"
    if weekday == 6:
        return "snf"
    if weekday == 0:
        return "mnf"
    if weekday == 3:
        return "tnf"
    if weekday == 2:
        return "wed"
    return "other"


def due_top_scorer_slate_ids(now_et: datetime) -> list[str]:
    """Slate ids whose ET post clock has been reached in the current NFL week."""
    now_et = now_et.astimezone(ET)
    now_ord = _NFL_WEEKDAY_ORDER[now_et.weekday()]
    now_hm = (now_et.hour, now_et.minute)
    due: list[str] = []
    for slate_id in SLATE_PROCESS_ORDER:
        weekday, hour, minute = _SLATE_DUE[slate_id]
        slot_ord = _NFL_WEEKDAY_ORDER[weekday]
        if now_ord > slot_ord or (now_ord == slot_ord and now_hm >= (hour, minute)):
            due.append(slate_id)
    return due


def nfl_week_from_schedule(schedule_json: dict[str, Any]) -> str:
    block = schedule_json.get("nflSchedule") or schedule_json
    raw = block.get("week") or block.get("currentWeek")
    return str(raw).strip() if raw is not None and str(raw).strip() else ""


def scoring_week_key(year: str, week: str, games: list[NflGame]) -> str:
    if week:
        try:
            return f"{year}-W{int(week):02d}"
        except ValueError:
            return f"{year}-W{week}"
    if games:
        first = min(game.kickoff for game in games)
        return first.astimezone(ET).date().isoformat()
    return f"{year}-unknown"


def top_scorers_title(slate_id: str, *, week: str = "") -> str:
    slate_label = SLATE_TITLES.get(slate_id, slate_id)
    if week:
        return f"Top Scorers — Week {week} {slate_label}"
    return f"Top Scorers — {slate_label}"


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


def parse_nfl_schedule_games(schedule_json: dict[str, Any]) -> list[NflGame]:
    block = schedule_json.get("nflSchedule") or schedule_json
    games: list[NflGame] = []
    for matchup in _as_rows(block.get("matchup")):
        kickoff_raw = matchup.get("kickoff")
        try:
            kickoff = datetime.fromtimestamp(int(str(kickoff_raw)), tz=ET)
        except (TypeError, ValueError):
            continue
        remaining = _as_float(matchup.get("gameSecondsRemaining"))
        is_final = remaining is not None and remaining <= 0
        teams = tuple(
            str(team.get("id") or "").strip().upper()
            for team in _as_rows(matchup.get("team"))
            if str(team.get("id") or "").strip()
        )
        games.append(
            NflGame(
                slate_id=slate_id_for_kickoff(kickoff),
                kickoff=kickoff,
                is_final=is_final,
                team_ids=teams,
            )
        )
    return games


def parse_player_week_scores(
    scores_json: dict[str, Any],
    players_map: dict[str, str],
) -> list[PlayerWeekScore]:
    block = scores_json.get("playerScores") or scores_json.get("playerscores") or {}
    rows = _as_rows(block.get("playerScore") or block.get("player"))
    out: list[PlayerWeekScore] = []
    for row in rows:
        player_id = str(row.get("id") or "").strip()
        points = _as_float(
            row.get("score") or row.get("points") or row.get("fantasyPoints")
        )
        if not player_id or points is None or points <= 0:
            continue
        label = players_map.get(player_id) or f"Player {player_id}"
        position = player_position_from_label(label) or str(
            row.get("position") or ""
        ).strip().upper()
        if not position:
            continue
        out.append(
            PlayerWeekScore(
                player_id=player_id,
                position=position,
                label=label,
                points=points,
                nfl_team=player_team_from_label(label),
            )
        )
    return out


def games_for_slate(games: list[NflGame], slate_id: str) -> list[NflGame]:
    return [game for game in games if game.slate_id == slate_id]


def slate_is_final(games: list[NflGame], slate_id: str) -> bool:
    slate_games = games_for_slate(games, slate_id)
    return bool(slate_games) and all(game.is_final for game in slate_games)


def scores_for_slate(
    scores: list[PlayerWeekScore],
    games: list[NflGame],
    slate_id: str,
) -> list[PlayerWeekScore]:
    if slate_id == "cumulative":
        return list(scores)
    teams = {
        team_id
        for game in games_for_slate(games, slate_id)
        if game.is_final
        for team_id in game.team_ids
    }
    return [row for row in scores if row.nfl_team in teams]


def top_scorers_by_position(
    scores: list[PlayerWeekScore],
    *,
    limit: int = TOP_SCORERS_LIMIT,
    positions: tuple[str, ...] = TOP_SCORERS_POSITIONS,
) -> dict[str, list[PlayerWeekScore]]:
    by_position: dict[str, list[PlayerWeekScore]] = defaultdict(list)
    for row in scores:
        by_position[row.position].append(row)
    ranked: dict[str, list[PlayerWeekScore]] = {}
    for position in positions:
        rows = sorted(
            by_position.get(position, []),
            key=lambda row: (-row.points, row.label.casefold()),
        )
        if rows:
            ranked[position] = rows[:limit]
    return ranked


def format_top_scorers_report_text(
    ranked: dict[str, list[PlayerWeekScore]],
    *,
    title: str,
) -> str:
    if not ranked:
        return f"{title}\n\nNo scorers yet."
    lines = [title, ""]
    for position, rows in ranked.items():
        lines.append(f"**{position}**")
        for index, row in enumerate(rows, start=1):
            lines.append(f"{index}. {row.label} — {row.points:.2f}")
        lines.append("")
    return "\n".join(lines).rstrip()


def should_build_slate_report(
    slate_id: str,
    games: list[NflGame],
) -> bool:
    """True when this slate is ready to format (final games, or cumulative after MNF)."""
    if slate_id == "cumulative":
        mnf_games = games_for_slate(games, "mnf")
        return (not mnf_games) or all(game.is_final for game in mnf_games)
    return slate_is_final(games, slate_id)
