"""Google Sheets helpers for RFA eligibility (read) and report sync (write)."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Callable

logger = logging.getLogger(__name__)

DEFAULT_SPREADSHEET_ID = "1FfFa9KMovU-js9wLNTsCnWrRU9L_N4ESro9nKGjx8aM"
DEFAULT_TOP32_TAB = "Top 32 At Position"
DEFAULT_RFA_TAB = "Restricted Free Agents"

_SHEETS_SCOPE = ("https://www.googleapis.com/auth/spreadsheets",)
_NAME_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


def spreadsheet_id_from_env() -> str:
    return (
        os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID") or DEFAULT_SPREADSHEET_ID
    ).strip()


def top32_tab_from_env() -> str:
    return (os.environ.get("GOOGLE_SHEETS_TOP32_TAB") or DEFAULT_TOP32_TAB).strip()


def rfa_tab_from_env() -> str:
    return (os.environ.get("GOOGLE_SHEETS_RFA_TAB") or DEFAULT_RFA_TAB).strip()


def load_service_account_info() -> dict[str, Any] | None:
    """
    Load service-account JSON from GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON or path)
    or GOOGLE_SERVICE_ACCOUNT_FILE.
    """
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        if raw.startswith("{"):
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.error("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")
                return None
            return data if isinstance(data, dict) else None
        path = raw
        try:
            data = json.loads(open(path, encoding="utf-8").read())
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Failed reading GOOGLE_SERVICE_ACCOUNT_JSON path %s: %s", path, exc)
            return None
        return data if isinstance(data, dict) else None

    file_path = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or "").strip()
    if not file_path:
        return None
    try:
        data = json.loads(open(file_path, encoding="utf-8").read())
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("Failed reading GOOGLE_SERVICE_ACCOUNT_FILE %s: %s", file_path, exc)
        return None
    return data if isinstance(data, dict) else None


def build_sheets_service(credentials_info: dict[str, Any]) -> Any:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=list(_SHEETS_SCOPE),
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def normalize_player_name(value: str) -> str:
    text = str(value or "").strip().lower()
    # MFL often uses "Last, First"
    if "," in text:
        last, first = text.split(",", 1)
        text = f"{first.strip()} {last.strip()}"
    return _NAME_NORMALIZE_RE.sub("", text)


def _strip_team_pos_suffix(value: str) -> str:
    """Drop trailing NFL team + position tokens when present (e.g. ATL RB)."""
    parts = str(value or "").split()
    if (
        len(parts) >= 3
        and parts[-1].replace(".", "").isalpha()
        and 1 <= len(parts[-1]) <= 3
        and parts[-2].replace(".", "").isalpha()
        and 2 <= len(parts[-2]) <= 3
    ):
        return " ".join(parts[:-2])
    return str(value or "").strip()


def player_name_match_keys(value: str) -> set[str]:
    """Normalized keys used to match sheet names to MFL player labels."""
    raw = str(value or "").strip()
    if not raw:
        return set()
    keys = {normalize_player_name(raw)}
    bare = _strip_team_pos_suffix(raw)
    if bare:
        keys.add(normalize_player_name(bare))
    return {key for key in keys if key}


def _header_index_map(header_row: list[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for idx, cell in enumerate(header_row):
        key = normalize_player_name(str(cell or ""))
        if key and key not in out:
            out[key] = idx
    return out


def _cell(row: list[Any], index: int | None) -> str:
    if index is None or index < 0 or index >= len(row):
        return ""
    return str(row[index] or "").strip()


def _find_header_row(rows: list[list[Any]]) -> tuple[int, list[Any]] | None:
    """Skip leading blank rows; prefer a row that includes a Player column."""
    for idx, row in enumerate(rows):
        if not row or not any(str(cell or "").strip() for cell in row):
            continue
        indexes = _header_index_map(row)
        if any(key in indexes for key in ("player", "playername", "name", "playerid", "mflid", "mflplayerid")):
            return idx, row
    for idx, row in enumerate(rows):
        if row and any(str(cell or "").strip() for cell in row):
            return idx, row
    return None


def parse_top32_player_ids_from_rows(
    rows: list[list[Any]],
    players_map: dict[str, str],
) -> set[str]:
    """
    Parse top-32 eligibility player ids from sheet rows.

    Prefers columns named like player id / mfl id. Falls back to player name
    matched against ``players_map`` (id -> display label).
    """
    if not rows:
        return set()
    found_header = _find_header_row(rows)
    if found_header is None:
        return set()
    header_idx, header = found_header
    indexes = _header_index_map(header)
    id_col = None
    for key in ("playerid", "mflid", "mflplayerid"):
        if key in indexes:
            id_col = indexes[key]
            break
    name_col = None
    for key in ("player", "playername", "name"):
        if key in indexes:
            name_col = indexes[key]
            break
    position_col = None
    for key in ("position", "pos"):
        if key in indexes:
            position_col = indexes[key]
            break
    body = rows[header_idx + 1 :]
    if id_col is None and name_col is None:
        name_col = 0

    name_to_ids: dict[str, list[str]] = {}
    for pid, label in players_map.items():
        for key in player_name_match_keys(label):
            name_to_ids.setdefault(key, []).append(str(pid))

    found: set[str] = set()
    ambiguous: list[str] = []
    missing: list[str] = []

    from src.rfa_report import is_likely_player_id

    def _filter_by_position(candidate_ids: list[str], position: str) -> list[str]:
        pos = position.strip().upper()
        if not pos:
            return candidate_ids
        narrowed: list[str] = []
        for pid in candidate_ids:
            label = str(players_map.get(pid) or "")
            parts = label.split()
            if parts and parts[-1].upper() == pos:
                narrowed.append(pid)
        return narrowed or candidate_ids

    for row in body:
        if not row or not any(str(cell or "").strip() for cell in row):
            continue
        raw_id = _cell(row, id_col) if id_col is not None else ""
        if raw_id and is_likely_player_id(raw_id):
            found.add(raw_id.strip())
            continue
        raw_name = _cell(row, name_col) if name_col is not None else ""
        if not raw_name:
            continue
        if is_likely_player_id(raw_name):
            found.add(raw_name)
            continue
        # Prefer full "Name TEAM POS" key before bare name (handles duplicate names).
        full_key = normalize_player_name(raw_name)
        matches = list(dict.fromkeys(name_to_ids.get(full_key) or []))
        if not matches:
            for key in player_name_match_keys(raw_name):
                matches.extend(name_to_ids.get(key) or [])
            matches = list(dict.fromkeys(matches))
        if position_col is not None and len(matches) > 1:
            matches = _filter_by_position(matches, _cell(row, position_col))
            matches = list(dict.fromkeys(matches))
        if len(matches) == 1:
            found.add(matches[0])
        elif len(matches) > 1:
            ambiguous.append(raw_name)
        else:
            missing.append(raw_name)

    if ambiguous:
        logger.warning(
            "Ambiguous top-32 player name matches (skipped): %s",
            ", ".join(ambiguous[:10]),
        )
    if missing:
        logger.warning(
            "Unmatched top-32 player names (skipped): %s",
            ", ".join(missing[:10]),
        )
    return found


def read_sheet_values(service: Any, spreadsheet_id: str, range_a1: str) -> list[list[Any]]:
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_a1)
        .execute()
    )
    values = result.get("values") or []
    return values if isinstance(values, list) else []


def ensure_sheet_tab(service: Any, spreadsheet_id: str, title: str) -> None:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets") or []
    for sheet in sheets:
        props = sheet.get("properties") or {}
        if str(props.get("title") or "") == title:
            return
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
    ).execute()


def write_rfa_report_rows(
    service: Any,
    spreadsheet_id: str,
    tab_title: str,
    rows: list[list[str]],
) -> None:
    ensure_sheet_tab(service, spreadsheet_id, tab_title)
    range_a1 = f"'{tab_title}'!A1"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_title}'",
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


def rfa_sheet_rows_from_active(
    active_rfas: dict[str, dict[str, Any]],
    players: dict[str, str],
) -> list[list[str]]:
    from src.rfa_report import format_cut_date, format_money

    header = [
        "Player",
        "Player ID",
        "Last Salary",
        "Cut Date",
        "Cut By Team",
        "Restricted Bid",
        "Status",
    ]
    body: list[list[str]] = []
    for player_id in sorted(
        active_rfas.keys(),
        key=lambda pid: (players.get(pid) or pid).lower(),
    ):
        entry = active_rfas[player_id]
        salary = str(entry.get("last_salary") or "")
        body.append(
            [
                players.get(player_id) or f"Player {player_id}",
                str(player_id),
                format_money(salary),
                format_cut_date(int(entry.get("cut_ts") or 0)),
                str(entry.get("cut_by_name") or entry.get("cut_by_franchise_id") or ""),
                format_money(salary),
                "active",
            ]
        )
    return [header, *body]


def fetch_top32_player_ids(
    players_map: dict[str, str],
    *,
    service_factory: Callable[[dict[str, Any]], Any] | None = None,
) -> set[str] | None:
    """
    Return top-32 player ids from the configured sheet, or None if unavailable.
    """
    info = load_service_account_info()
    if info is None:
        logger.warning("Google service account not configured; skipping top-32 sheet read")
        return None
    factory = service_factory or build_sheets_service
    service = factory(info)
    spreadsheet_id = spreadsheet_id_from_env()
    tab = top32_tab_from_env()
    rows = read_sheet_values(service, spreadsheet_id, f"'{tab}'!A:Z")
    return parse_top32_player_ids_from_rows(rows, players_map)


def sync_rfa_sheet(
    active_rfas: dict[str, dict[str, Any]],
    players: dict[str, str],
    *,
    service_factory: Callable[[dict[str, Any]], Any] | None = None,
) -> bool:
    info = load_service_account_info()
    if info is None:
        logger.warning("Google service account not configured; skipping RFA sheet write")
        return False
    factory = service_factory or build_sheets_service
    service = factory(info)
    spreadsheet_id = spreadsheet_id_from_env()
    tab = rfa_tab_from_env()
    rows = rfa_sheet_rows_from_active(active_rfas, players)
    write_rfa_report_rows(service, spreadsheet_id, tab, rows)
    return True
