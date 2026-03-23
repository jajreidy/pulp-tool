"""
RPM upload --overwrite: locate existing package units in Pulp and remove them from the target repo.
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, List, Optional

from pulp_tool.utils.rpm_pulp_search import search_rpms_by_checksums_for_overwrite

if TYPE_CHECKING:
    from pulp_tool.api.pulp_client import PulpClient

# pulp_href__in URL length / Pulp limits — align with other chunked queries
_PULP_HREF_IN_CHUNK = 20


def sha256_hex_file(path: str) -> str:
    """Compute lowercase hex SHA256 of file contents."""
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().lower()


def filter_rpm_hrefs_in_repository_version(
    client: PulpClient,
    repository_version_href: str,
    candidate_hrefs: List[str],
) -> List[str]:
    """
    Return candidate RPM package pulp_hrefs that exist in the given repository version.

    Uses GET /content/rpm/packages/ with repository_version and pulp_href__in filters.
    """
    if not candidate_hrefs or not repository_version_href:
        return []
    unique = list(dict.fromkeys(candidate_hrefs))
    confirmed: List[str] = []
    for i in range(0, len(unique), _PULP_HREF_IN_CHUNK):
        chunk = unique[i : i + _PULP_HREF_IN_CHUNK]
        chunk_set = set(chunk)
        results, _next, _prev, _count = client.list_rpm_packages(
            repository_version=repository_version_href,
            pulp_href__in=",".join(chunk),
        )
        for pkg in results:
            if pkg.pulp_href in chunk_set:
                confirmed.append(pkg.pulp_href)
    return list(dict.fromkeys(confirmed))


def remove_rpms_matching_local_files_from_repository(
    client: PulpClient,
    rpm_paths: List[str],
    rpm_repository_href: str,
    signed_by: Optional[str],
) -> int:
    """
    Search Pulp by SHA256 of local RPM files; remove matching RPM package units from the repo.

    Only removes units present in the repository's latest version.

    Returns:
        Number of content hrefs passed to remove_content_units.
    """
    if not rpm_paths:
        return 0

    checksums = [sha256_hex_file(p) for p in rpm_paths]
    checksums = list(dict.fromkeys(checksums))

    packages = search_rpms_by_checksums_for_overwrite(client, checksums, signed_by)
    candidate_hrefs = list(dict.fromkeys(p.pulp_href for p in packages))
    if not candidate_hrefs:
        logging.info("Overwrite: no RPM packages in Pulp matched local file checksums")
        return 0

    repo = client.fetch_rpm_repository_by_href(rpm_repository_href)
    version_href = repo.latest_version_href
    if not version_href:
        logging.info("Overwrite: repository has no latest_version_href; skip remove")
        return 0

    to_remove = filter_rpm_hrefs_in_repository_version(client, version_href, candidate_hrefs)
    if not to_remove:
        logging.info("Overwrite: matched packages are not in the target repository version; nothing to remove")
        return 0

    logging.warning(
        "Overwrite: removing %d RPM package unit(s) from repository before upload",
        len(to_remove),
    )
    task = client.modify_repository_content(rpm_repository_href, remove_content_units=to_remove)
    finished = client.wait_for_finished_task(task.pulp_href)
    if finished.is_failed:
        raise RuntimeError(f"Overwrite remove_content_units task failed: {finished.error}")
    return len(to_remove)


__all__ = [
    "filter_rpm_hrefs_in_repository_version",
    "remove_rpms_matching_local_files_from_repository",
    "sha256_hex_file",
]
