"""Read league connection settings from the environment (no baked-in defaults)."""

from __future__ import annotations

import os


def mfl_connect_settings() -> tuple[str, str, str] | None:
    """Return (host, year, league_id) if all are non-empty; otherwise None."""
    host = os.environ.get("MFL_HOST", "").strip()
    year = os.environ.get("MFL_YEAR", "").strip()
    league_id = os.environ.get("MFL_LEAGUE_ID", "").strip()
    if host and year and league_id:
        return host, year, league_id
    return None


def missing_mfl_connect_env_names() -> list[str]:
    host = os.environ.get("MFL_HOST", "").strip()
    year = os.environ.get("MFL_YEAR", "").strip()
    league_id = os.environ.get("MFL_LEAGUE_ID", "").strip()
    missing: list[str] = []
    if not host:
        missing.append("MFL_HOST")
    if not year:
        missing.append("MFL_YEAR")
    if not league_id:
        missing.append("MFL_LEAGUE_ID")
    return missing


def mfl_connect_env_help_suffix() -> str:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return (
            "Add repository Variables (Settings → Secrets and variables → Actions → Variables) "
            "named MFL_HOST, MFL_YEAR, and MFL_LEAGUE_ID."
        )
    return "Set them in the environment or a local .env file."
