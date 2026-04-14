"""Typed errors for CLI and library callers (pulp-glue–style narrow exceptions)."""

from __future__ import annotations

from typing import Optional

import httpx


class PulpToolError(Exception):
    """Base class for pulp-tool failures that should map to stable exit behavior."""


class PulpToolConfigError(PulpToolError):
    """Invalid or missing configuration."""


class PulpToolHTTPError(httpx.HTTPError):
    """HTTP response from Pulp was not successful (after receiving a response)."""

    def __init__(self, message: str, *, response: Optional[httpx.Response] = None) -> None:
        super().__init__(message)
        self.response = response


__all__ = ["PulpToolError", "PulpToolConfigError", "PulpToolHTTPError"]
