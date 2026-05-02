"""Draft pick notification formatting and dedupe helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DRAFT_PICK_COMMENTARY_LINES: tuple[str, ...] = (
    "The consensus big boards were right on this one! What a franchise-altering pick.",
    "That pick has instant-impact written all over it.",
    "The war room looks calm, but the rest of the league just got louder.",
    "This is the kind of selection that makes rival managers check their depth charts twice.",
    "Clean fit, clear role, and plenty of upside. That is draft night efficiency.",
    "Sources say the card went in fast. No hesitation on this one.",
    "The board broke just right, and this franchise did not overthink it.",
    "That is a swing with real ceiling. The tape grinders are nodding.",
    "A need, a value, and a little bit of swagger all in one pick.",
    "This one had been circled in pencil, then ink, then probably highlighter.",
    "The scouting department gets a gold star for this fit.",
    "That pick should make the group chat pause for a second.",
    "A little patience, a little conviction, and now a new building block.",
    "That is a selection with Sunday juice and dynasty intrigue.",
    "The board said value. The team said thank you.",
    "Not every pick needs fireworks, but this one brought a few anyway.",
    "There is a lot to like here if you enjoy upside and headaches for opponents.",
    "This front office clearly had a plan and stuck the landing.",
    "The pick is in, and the projection models are already stretching.",
    "That is a roster-builder's pick: clean, logical, and dangerous.",
    "The room got deeper, and the league got a fresh debate topic.",
    "This has all the makings of a pick people claim they loved all along.",
    "A strong selection from a team that knew exactly where the board was headed.",
    "The value police are letting this one through with a smile.",
    "That is the kind of pick that makes the next few managers recalculate.",
    "The fit is obvious, the upside is real, and the draft keeps moving.",
    "No need to get cute when the right player is sitting there.",
    "That is a confident card turn-in from a team building with purpose.",
    "The analysts wanted a splash. The front office delivered a cannonball.",
    "This pick has a path to matter sooner than people think.",
    "The board rewarded patience, and this team took full advantage.",
    "That is a sharp addition to a room that just got more interesting.",
    "A classic draft-night blend of value, need, and future arguments.",
    "This one will look tidy on the depth chart and spicy in the mentions.",
    "The pick is official, and the fit makes plenty of sense.",
    "That is a franchise betting on traits, opportunity, and a little magic.",
    "The league let this player slide, and someone finally made them pay.",
    "This is a pick with enough upside to make everyone pretend they were interested.",
    "A smart board read from a team that did not chase noise.",
    "The newest addition brings a fresh angle to this roster build.",
    "That pick has sleeper buzz trying very hard not to become regular buzz.",
    "The card is in, and the roster math just changed.",
    "That is how you add talent without making the room too complicated.",
    "A disciplined pick with enough ceiling to keep the analysts talking.",
    "The selection is official, and the depth chart has a new wrinkle.",
    "This is the kind of pick that rewards homework.",
    "The front office trusted the grade, and the room gets better.",
    "That player was too good to leave on the board much longer.",
    "A strong fit and a clean value. Draft rooms love when those line up.",
    "That is a fresh name for opponents to start worrying about.",
)


@dataclass(frozen=True)
class DraftPickSelection:
    franchise_id: str
    player_id: str
    round_number: int
    pick_number: int
    overall_index: int
    timestamp: str

    @property
    def slot(self) -> str:
        return f"{self.round_number}.{self.pick_number:02d}"


def _normalize_dict_rows(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


def selected_draft_picks_from_results(
    draft_results_json: dict[str, Any],
) -> list[DraftPickSelection]:
    draft_results = draft_results_json.get("draftResults") or {}
    draft_units = _normalize_dict_rows(draft_results.get("draftUnit"))
    selections: list[DraftPickSelection] = []
    overall_index = 0
    for draft_unit in draft_units:
        for draft_pick in _normalize_dict_rows(draft_unit.get("draftPick")):
            overall_index += 1
            player_id = str(draft_pick.get("player") or "").strip()
            franchise_id = str(draft_pick.get("franchise") or "").strip()
            if not player_id or not franchise_id:
                continue
            try:
                round_number = int(str(draft_pick.get("round") or "").strip())
                pick_number = int(str(draft_pick.get("pick") or "").strip())
            except ValueError:
                continue
            selections.append(
                DraftPickSelection(
                    franchise_id=franchise_id,
                    player_id=player_id,
                    round_number=round_number,
                    pick_number=pick_number,
                    overall_index=overall_index,
                    timestamp=str(draft_pick.get("timestamp") or "").strip(),
                )
            )
    return selections


def draft_pick_notification_key(selection: DraftPickSelection, season_year: int) -> str:
    return (
        f"DRAFT_PICK|{season_year}|{selection.slot}|"
        f"{selection.franchise_id}|{selection.player_id}"
    )


def draft_pick_selected_unix(selection: DraftPickSelection) -> float | None:
    if not selection.timestamp:
        return None
    try:
        return float(selection.timestamp)
    except (TypeError, ValueError):
        return None


def is_draft_pick_too_old_to_announce(
    selection: DraftPickSelection,
    now_unix: float,
    max_age_hours: float,
) -> bool:
    if max_age_hours <= 0:
        return False
    selected_at = draft_pick_selected_unix(selection)
    if selected_at is None:
        return False
    return (now_unix - selected_at) > max_age_hours * 3600.0


def draft_pick_embed_title(selection: DraftPickSelection) -> str:
    return f"Draft Status Update ({selection.slot})"


def _player_position(player_label: str) -> str:
    parts = player_label.strip().split()
    if len(parts) < 2:
        return ""
    return parts[-1]


def _possessive(team_name: str) -> str:
    return f"{team_name}'" if team_name.endswith("s") else f"{team_name}'s"


def _format_salary(raw_salary: str) -> str:
    try:
        return f"${round(float(raw_salary.replace(',', '').strip()))}"
    except (TypeError, ValueError):
        return f"${raw_salary.strip()}"


def _format_points(points: float) -> str:
    return f"{points:.1f} pts"


def _player_suffix(
    player_id: str,
    franchise_id: str,
    salaries_by_franchise: dict[str, dict[str, str]],
    points_by_player_id: dict[str, float],
) -> str:
    parts: list[str] = []
    salary = (salaries_by_franchise.get(franchise_id) or {}).get(player_id)
    if salary is not None and str(salary).strip() != "":
        parts.append(_format_salary(str(salary)))
    points = points_by_player_id.get(player_id)
    if points is not None:
        parts.append(_format_points(points))
    return f" ({' / '.join(parts)})" if parts else ""


def _roster_player_ids_at_position(
    rosters_json: dict[str, Any],
    franchise_id: str,
    players: dict[str, str],
    position: str,
) -> list[str]:
    rosters_block = rosters_json.get("rosters") or {}
    for franchise_row in _normalize_dict_rows(rosters_block.get("franchise")):
        if str(franchise_row.get("id") or "").strip() != franchise_id:
            continue
        matching_player_ids: list[str] = []
        for player_row in _normalize_dict_rows(franchise_row.get("player")):
            player_id = str(player_row.get("id") or "").strip()
            if not player_id:
                continue
            player_label = players.get(player_id, "")
            if _player_position(player_label) == position:
                matching_player_ids.append(player_id)
        return matching_player_ids
    return []


def format_draft_pick_text(
    selection: DraftPickSelection,
    franchise_names: dict[str, str],
    players: dict[str, str],
    rosters_json: dict[str, Any],
    salaries_by_franchise: dict[str, dict[str, str]],
    points_by_player_id: dict[str, float],
) -> str:
    team_name = franchise_names.get(selection.franchise_id, f"Franchise {selection.franchise_id}")
    drafted_player = players.get(selection.player_id, f"Player id {selection.player_id}")
    drafted_position = _player_position(drafted_player)
    room_label = f"{drafted_position}s" if drafted_position else "players"
    quote = DRAFT_PICK_COMMENTARY_LINES[
        (selection.overall_index - 1) % len(DRAFT_PICK_COMMENTARY_LINES)
    ]

    room_player_ids = _roster_player_ids_at_position(
        rosters_json,
        selection.franchise_id,
        players,
        drafted_position,
    )
    if selection.player_id not in room_player_ids:
        room_player_ids.append(selection.player_id)
    room_player_ids.sort(key=lambda player_id: players.get(player_id, player_id).casefold())

    room_lines: list[str] = []
    for player_id in room_player_ids:
        player_label = players.get(player_id, f"Player id {player_id}")
        suffix = _player_suffix(
            player_id,
            selection.franchise_id,
            salaries_by_franchise,
            points_by_player_id,
        )
        if player_id == selection.player_id:
            room_lines.append(f"* **{player_label}**{suffix}")
        else:
            room_lines.append(f"* {player_label}{suffix}")
    if not room_lines:
        room_lines.append("* (no matching rostered players found)")

    return "\n\n".join(
        [
            quote,
            f"{team_name} selects **{drafted_player}** at {selection.slot}.",
            f"{_possessive(team_name)} newest room of {room_label} is now:\n\n"
            + "\n".join(room_lines),
        ]
    )
