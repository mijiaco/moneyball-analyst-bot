"""Resolve Discord channel id from environment (test channel overrides prod)."""

from __future__ import annotations

import os


def discord_target_channel_id() -> str:
    """Prefer TEST_DISCORD_CHANNEL_ID when set and non-empty, else DISCORD, else PROD alias."""
    for key in ("TEST_DISCORD_CHANNEL_ID", "DISCORD_CHANNEL_ID", "PROD_DISCORD_CHANNEL_ID"):
        raw = os.environ.get(key)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip()
    return ""
