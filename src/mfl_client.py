"""Async MyFantasyLeague export API client with players file cache."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import certifi
import httpx

PLAYERS_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60


def _normalize_transaction_list(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


class MflClient:
    def __init__(
        self,
        host: str,
        year: str,
        league_id: str,
        api_key: str | None = None,
        user_agent: str | None = None,
        players_cache_path: Path | None = None,
    ) -> None:
        self._base = f"https://{host}/{year}/export"
        self._league_id = league_id
        self._api_key = api_key or None
        headers: dict[str, str] = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=60.0,
            follow_redirects=True,
            verify=certifi.where(),
        )
        self._players_cache_path = players_cache_path

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get_json(self, extra_params: dict[str, str]) -> Any:
        params = self._params(extra_params)
        last_err: BaseException | None = None
        for attempt in range(3):
            try:
                response = await self._client.get(self._base, params=params)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, OSError) as exc:
                last_err = exc
                await asyncio.sleep(1.0 * (attempt + 1))
        assert last_err is not None
        raise last_err

    def _params(self, extra: dict[str, str]) -> dict[str, str]:
        params: dict[str, str] = {"L": self._league_id, "JSON": "1", **extra}
        if self._api_key:
            params["APIKEY"] = self._api_key
        return params

    async def fetch_transactions_trade_days(self, days: int) -> list[dict[str, Any]]:
        data = await self._get_json(
            {
                "TYPE": "transactions",
                "TRANS_TYPE": "TRADE",
                "DAYS": str(days),
            }
        )
        block = data.get("transactions") or {}
        return _normalize_transaction_list(block.get("transaction"))

    async def fetch_league(self) -> dict[str, Any]:
        data = await self._get_json({"TYPE": "league"})
        return data if isinstance(data, dict) else {}

    async def fetch_rosters(self) -> dict[str, Any]:
        data = await self._get_json({"TYPE": "rosters"})
        return data if isinstance(data, dict) else {}

    async def _fetch_players_live(self) -> dict[str, Any]:
        data = await self._get_json({"TYPE": "players"})
        return data if isinstance(data, dict) else {}

    async def get_players_map(self) -> dict[str, str]:
        """
        Returns player_id -> display name (name + NFL team + position when available).
        Cached on disk for up to 24 hours.
        """
        cache_path = self._players_cache_path
        now = time.time()
        if cache_path and cache_path.is_file():
            try:
                raw = json.loads(cache_path.read_text(encoding="utf-8"))
                saved_at = float(raw.get("saved_at", 0))
                if now - saved_at < PLAYERS_CACHE_MAX_AGE_SECONDS:
                    players = raw.get("players")
                    if isinstance(players, dict):
                        return {str(k): str(v) for k, v in players.items()}
            except (json.JSONDecodeError, OSError, TypeError, ValueError):
                pass

        data = await self._fetch_players_live()
        players_block = data.get("players") or {}
        player_entries = players_block.get("player")
        entries = _normalize_transaction_list(player_entries)

        result: dict[str, str] = {}
        for row in entries:
            pid = row.get("id")
            if pid is None:
                continue
            name = row.get("name") or row.get("full_name") or pid
            team = row.get("team") or ""
            pos = row.get("position") or ""
            parts = [str(name)]
            if team:
                parts.append(str(team))
            if pos:
                parts.append(str(pos))
            result[str(pid)] = " ".join(parts)

        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"saved_at": now, "players": result}
            tmp = cache_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp, cache_path)

        return result

    async def sleep_between_exports(self, seconds: float = 1.0) -> None:
        """MFL recommends spacing requests (~1s between distinct exports)."""
        import asyncio

        await asyncio.sleep(seconds)


def player_salaries_by_franchise(rosters_json: dict[str, Any]) -> dict[str, dict[str, str]]:
    """
    franchise_id -> player_id -> salary string (MFL cap / auction amount, e.g. '35').
    """
    out: dict[str, dict[str, str]] = {}
    block = rosters_json.get("rosters") or {}
    fr_rows = _normalize_transaction_list(block.get("franchise"))
    for fr in fr_rows:
        fid = fr.get("id")
        if fid is None:
            continue
        fid_s = str(fid)
        inner: dict[str, str] = {}
        for p in _normalize_transaction_list(fr.get("player")):
            pid = p.get("id")
            if pid is None:
                continue
            sal = p.get("salary")
            if sal is None or str(sal).strip() == "":
                continue
            inner[str(pid)] = str(sal).strip()
        if inner:
            out[fid_s] = inner
    return out


def franchise_names_from_league(league_json: dict[str, Any]) -> dict[str, str]:
    """franchise id (e.g. '0001') -> team name."""
    franchises_block = league_json.get("league") or league_json
    franchises = franchises_block.get("franchises") or {}
    fr_list = franchises.get("franchise")
    rows = _normalize_transaction_list(fr_list)
    out: dict[str, str] = {}
    for row in rows:
        fid = row.get("id")
        if fid is None:
            continue
        name = row.get("name") or fid
        out[str(fid)] = str(name)
    return out
