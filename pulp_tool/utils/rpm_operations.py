"""
RPM operations for Pulp.

This module provides utilities for processing and uploading RPM files,
including checksum calculation, batch processing, and parallel uploads.
"""

import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from .constants import DEFAULT_MAX_WORKERS

# Batch size for RPM upload batching (distinct from DEFAULT_CHUNK_SIZE used for API request line size)
BATCH_SIZE = 50


def _create_batches(items: List[str], batch_size: int = BATCH_SIZE) -> Generator[List[str], None, None]:
    """
    Split a list into batches of specified size using a generator.

    Args:
        items: List of items to split into batches
        batch_size: Maximum number of items per batch

    Yields:
        List of items for each batch
    """
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def _calculate_sha256_checksum(file_path: str) -> str:
    """
    Calculate SHA256 checksum of a file.

    Args:
        file_path: Path to the file to calculate checksum for

    Returns:
        SHA256 checksum as hexadecimal string

    Raises:
        FileNotFoundError: If the file does not exist
        IOError: If there's an error reading the file
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    sha256_hash = hashlib.sha256()

    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
    except IOError as e:
        raise IOError(f"Error reading file {file_path}: {e}") from e

    return sha256_hash.hexdigest()


def _get_nvra(result: Dict[str, Any]) -> str:
    """
    Get Name-Version-Release-Architecture (NVRA) from Pulp response.

    Args:
        result: Dictionary containing RPM package information from Pulp

    Returns:
        NVRA string in format "name-version-release.arch"
    """
    return f"{result.get('name')}-{result.get('version')}-" f"{result.get('release')}.{result.get('arch')}"


def upload_rpms_parallel(
    client,
    rpms_to_upload: Union[List[str], List[Tuple[str, Dict[str, str], str]]],
    labels: Optional[Dict[str, str]] = None,
    arch: Optional[str] = None,
) -> List[str]:
    """
    Upload RPMs in parallel and return artifact hrefs.

    This function uploads multiple RPM files concurrently using a thread pool
    for improved performance.

    Args:
        client: PulpClient instance for API interactions
        rpms_to_upload: Either:
            - List of RPM file paths (requires labels and arch parameters)
            - List of (rpm_path, labels, arch) tuples (labels and arch params ignored)
        labels: Labels to attach to all uploaded content (ignored if rpms_to_upload contains tuples)
        arch: Architecture for all uploaded RPMs (ignored if rpms_to_upload contains tuples)

    Returns:
        List of artifact hrefs for successfully uploaded RPMs

    Examples:
        # Old style - all RPMs share same labels and arch
        >>> hrefs = upload_rpms_parallel(client, ["/path/rpm1.rpm", "/path/rpm2.rpm"],
        ...                              labels={"build_id": "123"}, arch="x86_64")

        # New style - each RPM has its own labels and arch
        >>> rpm_infos = [
        ...     ("/path/rpm1.rpm", {"build_id": "123", "name": "pkg1"}, "x86_64"),
        ...     ("/path/rpm2.rpm", {"build_id": "123", "name": "pkg2"}, "noarch"),
        ... ]
        >>> hrefs = upload_rpms_parallel(client, rpm_infos)
    """
    if not rpms_to_upload:
        return []

    # Detect if we're using the new style (list of tuples) or old style (list of paths)
    # Check first element to determine the calling style
    first_item = rpms_to_upload[0]
    is_tuple_style = isinstance(first_item, (tuple, list))

    rpm_infos: List[Tuple[str, Dict[str, str], str]]
    if not is_tuple_style:
        # Old style - validate required parameters
        if labels is None or arch is None:
            raise ValueError("labels and arch parameters are required when rpms_to_upload is a list of paths")

        # Convert to tuple style for uniform processing
        # Type cast since we know rpms_to_upload is List[str] here
        rpm_infos = [(rpm_path, labels, arch) for rpm_path in rpms_to_upload]  # type: ignore
        logging.info("Uploading %d RPM file(s) for %s", len(rpms_to_upload), arch)
    else:
        # New style - already in tuple format
        # Type cast since we know rpms_to_upload is List[Tuple[...]] here
        rpm_infos = rpms_to_upload  # type: ignore
        logging.info("Uploading %d RPM file(s)", len(rpms_to_upload))

    artifacts = []
    with ThreadPoolExecutor(thread_name_prefix="upload_rpms", max_workers=DEFAULT_MAX_WORKERS) as executor:
        futures = {
            executor.submit(client.upload_content, rpm_path, rpm_labels, file_type="rpm", arch=rpm_arch): rpm_path
            for rpm_path, rpm_labels, rpm_arch in rpm_infos
        }

        for future in as_completed(futures):
            rpm_path = futures[future]
            logging.info("Uploading RPM: %s", os.path.basename(rpm_path))
            try:
                artifact_href = future.result()
                artifacts.append(artifact_href)
            except Exception as e:  # pylint: disable=broad-except
                # Log the error but continue processing other uploads
                logging.error("Failed to upload %s: %s", rpm_path, e)

    return artifacts


__all__ = [
    "upload_rpms_parallel",
]
