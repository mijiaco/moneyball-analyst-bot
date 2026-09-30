"""Season points leaderboard and lottery-balls Discord reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

POINTS_LEADERBOARD_TITLE = "Points Leaderboard"
LOTTERY_BALLS_TITLE = "Lottery Balls Report"
POINTS_LEADERBOARD_COLOR = 15844367  # gold
LOTTERY_BALLS_COLOR = 10181046  # purple
LOTTERY_TEAM_COUNT = 8


@dataclass(frozen=True)
class FranchiseStanding:
    franchise_id: str
    points: float
    wins: int
    losses: int
    ties: int

    @property
    def record(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ties}"


def points_leaderboard_dedupe_key(date_et: str) -> str:
    return f"POINTS_LEADERBOARD|{date_et}"


def lottery_balls_dedupe_key(date_et: str) -> str:
    return f"LOTTERY_BALLS|{date_et}"


def _int_field(row: dict[str, Any], key: str) -> int:
    try:
        return int(str(row.get(key) or "0").strip())
    except ValueError:
        return 0


def _points_field(row: dict[str, Any]) -> float | None:
    raw = str(row.get("pf") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def standings_from_export(standings_json: dict[str, Any]) -> list[FranchiseStanding]:
    """Franchise rows from TYPE=leagueStandings (season points and H2H record)."""
    block = standings_json.get("leagueStandings") or {}
    rows_raw = block.get("franchise")
    if isinstance(rows_raw, list):
        rows = [row for row in rows_raw if isinstance(row, dict)]
    elif isinstance(rows_raw, dict):
        rows = [rows_raw]
    else:
        rows = []
    out: list[FranchiseStanding] = []
    for row in rows:
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        points = _points_field(row)
        if points is None:
            continue
        out.append(
            FranchiseStanding(
                franchise_id=str(franchise_id),
                points=points,
                wins=_int_field(row, "h2hw"),
                losses=_int_field(row, "h2hl"),
                ties=_int_field(row, "h2ht"),
            )
        )
    return out


def _better_first_key(
    row: FranchiseStanding,
    franchise_names: dict[str, str],
) -> tuple[float, int, int, int, str]:
    """Best points first. Equal points: better record, then team name."""
    name = franchise_names.get(row.franchise_id, f"Franchise {row.franchise_id}")
    return (-row.points, -row.wins, row.losses, row.ties, name.casefold())


def rank_standings_best_to_worst(
    rows: list[FranchiseStanding],
    franchise_names: dict[str, str],
) -> list[FranchiseStanding]:
    return sorted(rows, key=lambda row: _better_first_key(row, franchise_names))


def lottery_ball_standings(
    ranked_best_to_worst: list[FranchiseStanding],
    *,
    team_count: int = LOTTERY_TEAM_COUNT,
) -> list[tuple[FranchiseStanding, int]]:
    """Worst teams first. The worst team gets ``team_count`` balls; the last gets 1."""
    if team_count <= 0:
        return []
    worst_first = list(reversed(ranked_best_to_worst))[:team_count]
    ball_count = len(worst_first)
    return [
        (row, ball_count - index) for index, row in enumerate(worst_first)
    ]


def _team_name(franchise_id: str, franchise_names: dict[str, str]) -> str:
    return franchise_names.get(franchise_id, f"Franchise {franchise_id}")


def format_points_leaderboard_text(
    ranked: list[FranchiseStanding],
    franchise_names: dict[str, str],
    *,
    title: str = POINTS_LEADERBOARD_TITLE,
) -> str:
    if not ranked:
        return f"{title}\n\nNo standings found."
    lines = [title, ""]
    for index, row in enumerate(ranked, start=1):
        lines.append(
            f"{index:>2}. {_team_name(row.franchise_id, franchise_names)} "
            f"— {row.points:.2f} — {row.record}"
        )
    return "\n".join(lines)


def format_lottery_balls_report_text(
    ball_rows: list[tuple[FranchiseStanding, int]],
    franchise_names: dict[str, str],
    *,
    title: str = LOTTERY_BALLS_TITLE,
) -> str:
    if not ball_rows:
        return f"{title}\n\nNo standings found."
    lines = [title, ""]
    for index, (row, balls) in enumerate(ball_rows, start=1):
        ball_label = "ball" if balls == 1 else "balls"
        lines.append(
            f"{index}. {_team_name(row.franchise_id, franchise_names)} "
            f"— {row.points:.2f} — {row.record} — {balls} {ball_label}"
        )
    return "\n".join(lines)
