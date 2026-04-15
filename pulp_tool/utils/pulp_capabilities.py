"""Helpers for interpreting Pulp ``/status/`` JSON (e.g. component versions)."""

from __future__ import annotations

from typing import Dict


def versions_from_status_payload(data: dict) -> Dict[str, str]:
    """Extract ``component -> version`` from Pulp status JSON."""
    out: Dict[str, str] = {}
    for item in data.get("versions", []) or []:
        if isinstance(item, dict):
            comp = item.get("component")
            ver = item.get("version")
            if isinstance(comp, str) and isinstance(ver, str):
                out[comp] = ver
    return out


__all__ = ["versions_from_status_payload"]
