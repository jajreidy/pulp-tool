"""Optional Pulp / plugin version checks (pulp-glue ``PluginRequirement``–style gate)."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, Tuple

from ..models.pulp_api import StatusResponse

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient  # pragma: no cover

# Minimum versions for RPM upload / artifact flows; adjust when APIs require newer Pulp.
_MIN_CORE_VERSION: Tuple[int, ...] = (3, 14)
_MIN_RPM_VERSION: Tuple[int, ...] = (3, 14)


def _version_tuple(version: str) -> Tuple[int, ...]:
    """Parse leading numeric segments from a Pulp version string (e.g. ``3.21.0``)."""
    parts: list[int] = []
    for segment in version.split("."):
        match = re.match(r"^(\d+)", segment)
        if match:
            parts.append(int(match.group(1)))
        else:
            break
    return tuple(parts) if parts else (0,)


def _meets_minimum(current: str, minimum: Tuple[int, ...]) -> bool:
    return _version_tuple(current) >= minimum


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


def ensure_pulp_capabilities(client: "PulpClient", *, operation: str = "this operation") -> None:
    """
    GET ``/status/`` and verify pulpcore and rpm plugin meet minimum versions.

    Raises:
        RuntimeError: If status cannot be read or a required component is too old / missing.
    """
    url = client._url("status/")
    response = client.session.get(url, timeout=client.timeout, **client.request_params)
    if not response.is_success:
        raise RuntimeError(
            f"Cannot verify Pulp capabilities for {operation}: "
            f"status returned {response.status_code} - {response.text[:500]}"
        )
    try:
        payload = response.json()
        StatusResponse.model_validate(payload)
        versions = versions_from_status_payload(payload)
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError(f"Cannot parse Pulp status response for {operation}: {exc}") from exc

    core_ver = versions.get("core") or versions.get("pulpcore")
    if not core_ver:
        logging.warning("Pulp status has no core version; skipping minimum version check")
        return
    if not _meets_minimum(core_ver, _MIN_CORE_VERSION):
        raise RuntimeError(
            f"Pulpcore version {core_ver} is below minimum {_MIN_CORE_VERSION[0]}.{_MIN_CORE_VERSION[1]} "
            f"required for {operation}. Upgrade Pulp or contact your administrator."
        )

    rpm_ver = versions.get("rpm")
    if rpm_ver and not _meets_minimum(rpm_ver, _MIN_RPM_VERSION):
        raise RuntimeError(
            f"pulp_rpm version {rpm_ver} is below minimum {_MIN_RPM_VERSION[0]}.{_MIN_RPM_VERSION[1]} "
            f"required for {operation}. Upgrade the rpm plugin or contact your administrator."
        )


__all__ = ["ensure_pulp_capabilities", "versions_from_status_payload"]
