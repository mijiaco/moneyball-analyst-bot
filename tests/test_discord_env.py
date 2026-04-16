"""Unit tests for discord_env channel resolution."""

from __future__ import annotations

import os

import pytest

from src.discord_env import discord_production_channel_id, discord_target_channel_id


def test_prefers_test_channel_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.setenv("TEST_DISCORD_CHANNEL_ID", "111")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "222")
    assert discord_target_channel_id() == "111"


def test_falls_back_to_discord_channel_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "333")
    assert discord_target_channel_id() == "333"


def test_falls_back_to_prod_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("TEST_DISCORD_CHANNEL_ID", "DISCORD_CHANNEL_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PROD_DISCORD_CHANNEL_ID", "444")
    assert discord_target_channel_id() == "444"


def test_empty_test_channel_falls_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DISCORD_CHANNEL_ID", "   ")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "555")
    assert discord_target_channel_id() == "555"


def test_returns_empty_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("TEST_DISCORD_CHANNEL_ID", "DISCORD_CHANNEL_ID", "PROD_DISCORD_CHANNEL_ID"):
        monkeypatch.delenv(key, raising=False)
    assert discord_target_channel_id() == ""


def test_production_channel_ignores_test(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DISCORD_CHANNEL_ID", "999")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "main-ch")
    assert discord_production_channel_id() == "main-ch"


def test_production_channel_prefers_discord_over_prod_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TEST_DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "aaa")
    monkeypatch.setenv("PROD_DISCORD_CHANNEL_ID", "bbb")
    assert discord_production_channel_id() == "aaa"


def test_production_channel_empty_when_only_test_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_DISCORD_CHANNEL_ID", "999")
    monkeypatch.delenv("DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.delenv("PROD_DISCORD_CHANNEL_ID", raising=False)
    assert discord_production_channel_id() == ""
