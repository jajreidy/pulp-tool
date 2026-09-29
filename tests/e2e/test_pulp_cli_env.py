"""Tests for pulp-cli subprocess environment in e2e scripts."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
sys.path.insert(0, str(E2E_DIR))

from pulp_cli_env import env_for_pulp_cli  # noqa: E402


def test_env_for_pulp_cli_overrides_root_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", "/")
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.delenv("TEST_WORKSPACE", raising=False)
    monkeypatch.delenv("PULP_TOOL_PATH", raising=False)
    env = env_for_pulp_cli()
    assert env["HOME"] == "/tmp"
    assert env["XDG_CACHE_HOME"] == f"/tmp/pulp-cli-cache-{os.getuid()}"


def test_env_for_pulp_cli_uses_test_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_WORKSPACE", "/data/test-ws")
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp/.cache")
    env = env_for_pulp_cli()
    assert env["XDG_CACHE_HOME"] == "/data/test-ws/.pulp-cli-cache"


def test_env_for_pulp_cli_preserves_custom_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", "/home/tester")
    monkeypatch.setenv("XDG_CACHE_HOME", "/home/tester/cache")
    monkeypatch.delenv("TEST_WORKSPACE", raising=False)
    monkeypatch.delenv("PULP_TOOL_PATH", raising=False)
    env = env_for_pulp_cli()
    assert env["HOME"] == "/home/tester"
    assert env["XDG_CACHE_HOME"] == "/home/tester/cache"
