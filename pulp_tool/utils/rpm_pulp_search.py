"""
Pulp RPM package search helpers (shared by search-by CLI and upload --overwrite).

Parses RPM list API responses and runs the same queries as search-by.
"""

from typing import TYPE_CHECKING, List, Optional

import httpx

from pulp_tool.models.pulp_api import RpmPackageResponse

if TYPE_CHECKING:
    from pulp_tool.api.pulp_client import PulpClient


def parse_rpm_response(response: httpx.Response) -> List[RpmPackageResponse]:
    """Parse paginated RPM response into RpmPackageResponse list."""
    response.raise_for_status()
    results_raw = response.json().get("results", [])
    packages: List[RpmPackageResponse] = []
    for item in results_raw:
        try:
            packages.append(RpmPackageResponse(**item))
        except Exception:
            pass
    return packages


def search_pulp_for_rpms(client: "PulpClient", checksums: List[str]) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching the given checksums (pkgId / sha256)."""
    return parse_rpm_response(client.get_rpm_by_pkgIDs(checksums))


def search_pulp_by_filenames(client: "PulpClient", filenames: List[str]) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching the given filenames."""
    return parse_rpm_response(client.get_rpm_by_filenames(filenames))


def search_pulp_by_signed_by(client: "PulpClient", signed_by: str) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching the given signed_by key."""
    return parse_rpm_response(client.get_rpm_by_signed_by([signed_by]))


def search_pulp_for_rpms_with_signed_by(
    client: "PulpClient", checksums: List[str], signed_by: str
) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching checksums AND signed_by (single API call)."""
    return parse_rpm_response(client.get_rpm_by_checksums_and_signed_by(checksums, signed_by))


def search_pulp_by_filenames_with_signed_by(
    client: "PulpClient", filenames: List[str], signed_by: str
) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching filenames AND signed_by (single API call)."""
    return parse_rpm_response(client.get_rpm_by_filenames_and_signed_by(filenames, signed_by))


def search_rpms_by_checksums_for_overwrite(
    client: "PulpClient",
    checksums: List[str],
    signed_by: Optional[str],
) -> List[RpmPackageResponse]:
    """
    Find RPM content units by SHA256 (pkgId), optionally scoped with signed_by.

    Used by upload --overwrite to locate packages to remove before re-uploading.
    """
    if not checksums:
        return []
    sb = signed_by.strip() if signed_by and signed_by.strip() else None
    if sb:
        return search_pulp_for_rpms_with_signed_by(client, checksums, sb)
    return search_pulp_for_rpms(client, checksums)


__all__ = [
    "parse_rpm_response",
    "search_pulp_by_filenames",
    "search_pulp_by_filenames_with_signed_by",
    "search_pulp_by_signed_by",
    "search_pulp_for_rpms",
    "search_pulp_for_rpms_with_signed_by",
    "search_rpms_by_checksums_for_overwrite",
]
