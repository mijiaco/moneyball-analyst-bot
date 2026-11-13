"""Tuesday Toilet Bowl and Mid Bowl bracket report from current standings."""

from __future__ import annotations

from dataclasses import dataclass

from src.playoff_picture_report import PlayoffTeam

BOWL_BRACKET_TITLE = "Weekly Toilet Bowl / Mid Bowl"
BOWL_BRACKET_COLOR = 10053324  # brown
REGULAR_SEASON_WEEKS = 13
LEAGUE_TEAM_COUNT = 32
TOILET_SEED_START = 25
TOILET_SEED_END = 32
MID_SEED_START = 13
MID_SEED_END = 24
MID_BYE_END = 16
TOILET_PRIZE = "1.33 rookie pick"
MID_PRIZE = "2.33 rookie pick"


@dataclass(frozen=True)
class SeededBowlTeam:
    seed: int
    team: PlayoffTeam


def bowl_bracket_dedupe_key(date_et: str) -> str:
    return f"BOWL_BRACKETS|{date_et}"


def _team_name(franchise_id: str, franchise_names: dict[str, str]) -> str:
    return franchise_names.get(franchise_id, f"Franchise {franchise_id}")


def _record_points_sort_key(
    team: PlayoffTeam,
    franchise_names: dict[str, str],
) -> tuple[float, float, str, str]:
    """Better win percentage, then points for. Points against is not a tiebreak."""
    return (
        -team.win_pct,
        -team.points_for,
        _team_name(team.franchise_id, franchise_names).casefold(),
        team.franchise_id,
    )


def completed_week_from_teams(teams: list[PlayoffTeam]) -> int:
    """Smallest games-played total, so an in-progress week is not treated as done."""
    if not teams:
        return 0
    return min(team.games for team in teams)


def regular_season_weeks_remaining(completed_week: int) -> int:
    return max(0, REGULAR_SEASON_WEEKS - max(0, completed_week))


def seed_bowl_field(
    teams: list[PlayoffTeam],
    franchise_names: dict[str, str],
) -> list[SeededBowlTeam]:
    """Seeds 1-32 by record, with points for as the only tiebreaker."""
    ordered = sorted(
        teams, key=lambda team: _record_points_sort_key(team, franchise_names)
    )
    return [
        SeededBowlTeam(seed=index, team=team)
        for index, team in enumerate(ordered, start=1)
    ]


def _seeded_slice(
    seeded: list[SeededBowlTeam],
    start: int,
    end: int,
) -> list[SeededBowlTeam]:
    by_seed = {row.seed: row for row in seeded}
    return [by_seed[seed] for seed in range(start, end + 1)]


def _high_low_pairs(
    rows: list[SeededBowlTeam],
) -> list[tuple[SeededBowlTeam, SeededBowlTeam]]:
    half = len(rows) // 2
    return [(rows[index], rows[-1 - index]) for index in range(half)]


def _matchup_line(
    left: SeededBowlTeam,
    right: SeededBowlTeam,
    franchise_names: dict[str, str],
) -> str:
    return (
        f"{left.seed} {_team_name(left.team.franchise_id, franchise_names)} vs "
        f"{right.seed} {_team_name(right.team.franchise_id, franchise_names)}"
    )


def _field_status_line(completed_week: int) -> str:
    weeks_left = regular_season_weeks_remaining(completed_week)
    week_word = "week" if weeks_left == 1 else "weeks"
    if completed_week <= 0:
        through = "No completed week yet."
    else:
        through = f"Through Week {completed_week}."
    if weeks_left:
        remaining = f"{weeks_left} regular-season {week_word} left."
    else:
        remaining = "No regular-season weeks left."
    if completed_week < REGULAR_SEASON_WEEKS:
        field = "The field is not set until after Week 13."
    else:
        field = "The field is set."
    return f"{through} {remaining} {field}"


def format_bowl_bracket_text(
    teams: list[PlayoffTeam],
    franchise_names: dict[str, str],
    *,
    completed_week: int | None = None,
    title: str = BOWL_BRACKET_TITLE,
) -> str | None:
    """
    Projected Week 14 brackets from current seeds.

    Later rounds reseed, so this does not print a Week 15 or Week 16 pairing.
    Returns None when the league export does not contain 32 teams.
    """
    if len(teams) < LEAGUE_TEAM_COUNT:
        return None
    seeded = seed_bowl_field(teams, franchise_names)
    if len(seeded) < LEAGUE_TEAM_COUNT:
        return None
    week = (
        completed_week_from_teams(teams)
        if completed_week is None
        else completed_week
    )
    toilet = _seeded_slice(seeded, TOILET_SEED_START, TOILET_SEED_END)
    mid = _seeded_slice(seeded, MID_SEED_START, MID_SEED_END)
    byes = mid[: MID_BYE_END - MID_SEED_START + 1]
    first_round = mid[MID_BYE_END - MID_SEED_START + 1 :]
    lines = [
        title,
        "",
        _field_status_line(week),
        "",
        "Toilet Bowl",
        (
            "Week 14 quarterfinals. Winner gets the "
            f"{TOILET_PRIZE}. Week 15 semifinals reseed; Week 16 final."
        ),
    ]
    lines.extend(
        _matchup_line(left, right, franchise_names)
        for left, right in _high_low_pairs(toilet)
    )
    bye_text = ", ".join(
        f"{row.seed} {_team_name(row.team.franchise_id, franchise_names)}"
        for row in byes
    )
    lines.extend(
        [
            "",
            "Mid Bowl",
            (
                "Week 14 first round. Seeds 13–16 have byes. Winner gets the "
                f"{MID_PRIZE}."
            ),
            f"Byes: {bye_text}",
        ]
    )
    lines.extend(
        _matchup_line(left, right, franchise_names)
        for left, right in _high_low_pairs(first_round)
    )
    return "\n".join(lines)
