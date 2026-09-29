"""Writable cache/home for ``pulp`` CLI subprocesses in restricted Tekton pods."""

import os
from pathlib import Path

# Image build runs ``pulp`` as root and can leave these dirs unwritable to arbitrary UIDs.
_LEGACY_SHARED_CACHE_DIRS = frozenset({"/tmp/.cache", "/tmp/pulp-cli-cache"})


def _pulp_cli_cache_home(env: dict[str, str]) -> str:
    for key in ("TEST_WORKSPACE", "PULP_TOOL_PATH"):
        base = env.get(key)
        if base:
            return str(Path(base) / ".pulp-cli-cache")
    xdg = (env.get("XDG_CACHE_HOME") or "").strip()
    if xdg and xdg not in _LEGACY_SHARED_CACHE_DIRS:
        return xdg
    return f"/tmp/pulp-cli-cache-{os.getuid()}"


def env_for_pulp_cli() -> dict[str, str]:
    """Return a copy of the environment safe for ``pulp`` (pulp-glue OpenAPI cache)."""
    env = os.environ.copy()
    home = env.get("HOME", "")
    if not home or home == "/":
        env["HOME"] = "/tmp"
    env["XDG_CACHE_HOME"] = _pulp_cli_cache_home(env)
    return env
