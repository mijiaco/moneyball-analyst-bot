"""Tuesday playoff-picture report (computed seeds from standings)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PLAYOFF_PICTURE_TITLE = "Weekly Playoff Picture"
PLAYOFF_PICTURE_COLOR = 2123412  # navy
FIRST_ROUND_BYE_COUNT = 4
PLAYOFF_FIELD_SIZE = 12
RECORD_SEED_END = 11


@dataclass(frozen=True)
class PlayoffTeam:
    franchise_id: str
    division_id: str
    points_for: float
    points_against: float
    wins: int
    losses: int
    ties: int

    @property
    def record(self) -> str:
        return f"{self.wins}-{self.losses}-{self.ties}"

    @property
    def games(self) -> int:
        return self.wins + self.losses + self.ties

    @property
    def win_pct(self) -> float:
        if self.games <= 0:
            return 0.0
        return (self.wins + 0.5 * self.ties) / self.games


@dataclass(frozen=True)
class SeededPlayoffTeam:
    seed: int
    team: PlayoffTeam
    has_first_round_bye: bool


def playoff_picture_dedupe_key(date_et: str) -> str:
    return f"PLAYOFF_PICTURE|{date_et}"


def _franchise_rows(container: Any) -> list[dict[str, Any]]:
    if isinstance(container, list):
        return [row for row in container if isinstance(row, dict)]
    if isinstance(container, dict):
        return [container]
    return []


def _int_field(row: dict[str, Any], key: str) -> int:
    try:
        return int(str(row.get(key) or "0").strip())
    except ValueError:
        return 0


def _optional_float(raw: Any) -> float | None:
    text = str(raw if raw is not None else "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def division_ids_from_league(league_json: dict[str, Any]) -> dict[str, str]:
    """franchise id -> division id from TYPE=league."""
    block = league_json.get("league") or league_json
    franchises = (block.get("franchises") or {}).get("franchise")
    out: dict[str, str] = {}
    for row in _franchise_rows(franchises):
        franchise_id = row.get("id")
        division_id = row.get("division")
        if franchise_id is None or division_id is None:
            continue
        division_text = str(division_id).strip()
        if not division_text:
            continue
        out[str(franchise_id)] = division_text
    return out


def playoff_teams_from_exports(
    standings_json: dict[str, Any],
    league_json: dict[str, Any],
) -> list[PlayoffTeam]:
    """
    Franchise rows from TYPE=standings (payload key ``leagueStandings``)
    joined to division ids on TYPE=league.
    """
    block = standings_json.get("leagueStandings") or standings_json.get("standings") or {}
    divisions = division_ids_from_league(league_json)
    out: list[PlayoffTeam] = []
    for row in _franchise_rows(block.get("franchise")):
        franchise_id = row.get("id")
        if franchise_id is None:
            continue
        points_for = _optional_float(row.get("pf"))
        if points_for is None:
            continue
        points_against = _optional_float(row.get("pa"))
        out.append(
            PlayoffTeam(
                franchise_id=str(franchise_id),
                division_id=divisions.get(str(franchise_id), ""),
                points_for=points_for,
                points_against=(
                    points_against if points_against is not None else float("inf")
                ),
                wins=_int_field(row, "h2hw"),
                losses=_int_field(row, "h2hl"),
                ties=_int_field(row, "h2ht"),
            )
        )
    return out


def _team_name(franchise_id: str, franchise_names: dict[str, str]) -> str:
    return franchise_names.get(franchise_id, f"Franchise {franchise_id}")


def _record_sort_key(
    team: PlayoffTeam,
    franchise_names: dict[str, str],
) -> tuple[float, float, float, str, str]:
    """Better win percentage, then points for, then fewer points against."""
    return (
        -team.win_pct,
        -team.points_for,
        team.points_against,
        _team_name(team.franchise_id, franchise_names).casefold(),
        team.franchise_id,
    )


def _points_sort_key(
    team: PlayoffTeam,
    franchise_names: dict[str, str],
) -> tuple[float, float, str, str]:
    """Higher points for, then fewer points against."""
    return (
        -team.points_for,
        team.points_against,
        _team_name(team.franchise_id, franchise_names).casefold(),
        team.franchise_id,
    )


def seed_playoff_picture(
    teams: list[PlayoffTeam],
    franchise_names: dict[str, str],
) -> list[SeededPlayoffTeam]:
    """
    Seeds 1-4 are division winners (first-round bye), ordered by record.
    Seeds 5-11 are the next-best records. Seed 12 is the highest points
    total among the teams still left.
    """
    division_ids = sorted({team.division_id for team in teams if team.division_id})
    division_winners = [
        min(
            (team for team in teams if team.division_id == division_id),
            key=lambda team: _record_sort_key(team, franchise_names),
        )
        for division_id in division_ids
    ]
    bye_winners = sorted(
        division_winners,
        key=lambda team: _record_sort_key(team, franchise_names),
    )[:FIRST_ROUND_BYE_COUNT]
    bye_ids = {team.franchise_id for team in bye_winners}
    pool = [team for team in teams if team.franchise_id not in bye_ids]
    record_slots = max(0, RECORD_SEED_END - len(bye_winners))
    if len(bye_winners) + len(pool) >= PLAYOFF_FIELD_SIZE:
        record_slots = min(record_slots, PLAYOFF_FIELD_SIZE - 1 - len(bye_winners))
    else:
        record_slots = min(record_slots, len(pool))
    wild_cards = sorted(
        pool, key=lambda team: _record_sort_key(team, franchise_names)
    )[:record_slots]
    wild_ids = {team.franchise_id for team in wild_cards}
    rest = [team for team in pool if team.franchise_id not in wild_ids]
    points_seed: list[PlayoffTeam] = []
    if len(bye_winners) + len(wild_cards) < PLAYOFF_FIELD_SIZE and rest:
        points_seed = [
            min(rest, key=lambda team: _points_sort_key(team, franchise_names))
        ]
    ordered = [*bye_winners, *wild_cards, *points_seed]
    bye_count = len(bye_winners)
    return [
        SeededPlayoffTeam(
            seed=index,
            team=team,
            has_first_round_bye=index <= bye_count,
        )
        for index, team in enumerate(ordered, start=1)
    ]


def format_playoff_picture_text(
    seeded: list[SeededPlayoffTeam],
    franchise_names: dict[str, str],
    *,
    title: str = PLAYOFF_PICTURE_TITLE,
) -> str:
    if not seeded:
        return f"{title}\n\nNo standings found."
    lines = [title, ""]
    for row in seeded:
        bye_note = " — first-round bye" if row.has_first_round_bye else ""
        lines.append(
            f"{row.seed:>2}. {_team_name(row.team.franchise_id, franchise_names)} "
            f"— {row.team.record} — {row.team.points_for:.2f}{bye_note}"
        )
    return "\n".join(lines)
