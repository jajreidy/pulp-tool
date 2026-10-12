"""Limits for distribution artifact downloads."""

from __future__ import annotations

import os

from .constants import DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES

_ENV_MAX_BYTES = "PULP_TOOL_MAX_DOWNLOAD_BYTES"


def max_artifact_download_bytes() -> int:
    """Return the maximum allowed bytes per artifact download (env override)."""
    raw = os.environ.get(_ENV_MAX_BYTES, "").strip()
    if not raw:
        return DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES
    try:
        value = int(raw, 10)
    except ValueError:
        return DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES
    return value if value > 0 else DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES


__all__ = ["max_artifact_download_bytes"]
