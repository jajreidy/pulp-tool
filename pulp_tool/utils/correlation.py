"""HTTP correlation ID resolution (Konflux / pulp-cli ``cid``-style traceability)."""

from __future__ import annotations

import os
from typing import Any, Optional

CORRELATION_HEADER = "X-Correlation-ID"
ENV_CORRELATION = "PULP_TOOL_CORRELATION_ID"


def _strip_opt(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def resolve_correlation_id(
    *,
    config_value: Any = None,
    env_value: Optional[str] = None,
    namespace: Optional[str] = None,
    build_id: Optional[str] = None,
) -> Optional[str]:
    """
    Resolve correlation ID: config ``correlation_id`` > env > ``namespace/build_id`` > ``build_id``.

    Args:
        config_value: Optional ``cli.correlation_id`` from TOML.
        env_value: Explicit env override; if None, reads ``PULP_TOOL_CORRELATION_ID``.
        namespace: CLI or runtime namespace for derived ID.
        build_id: CLI or runtime build id for derived ID.
    """
    if config_value is not None:
        cv = _strip_opt(str(config_value))
        if cv:
            return cv
    ev = _strip_opt(env_value if env_value is not None else os.environ.get(ENV_CORRELATION))
    if ev:
        return ev
    ns = _strip_opt(namespace)
    bid = _strip_opt(build_id)
    if ns and bid:
        return f"{ns}/{bid}"
    if bid:
        return bid
    return None


__all__ = ["CORRELATION_HEADER", "ENV_CORRELATION", "resolve_correlation_id"]
