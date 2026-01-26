"""
Artifact type detection utilities.

This module provides functions for detecting artifact types from filenames
and organizing artifacts by their content type.
"""

import os
import re
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from ..models.artifacts import ArtifactMetadata
from .constants import SUPPORTED_ARCHITECTURES


def detect_artifact_type(artifact_name: str) -> Optional[str]:
    """
    Detect artifact type from artifact name.

    Args:
        artifact_name: Name of the artifact (filename)

    Returns:
        Artifact type ('rpm', 'log', 'sbom') or None if type cannot be determined

    Example:
        >>> detect_artifact_type("package.rpm")
        'rpm'
        >>> detect_artifact_type("build.log")
        'log'
        >>> detect_artifact_type("sbom.json")
        'sbom'
    """
    artifact_name_lower = artifact_name.lower()

    if "sbom" in artifact_name_lower:
        return "sbom"
    if "log" in artifact_name_lower:
        return "log"
    if "rpm" in artifact_name_lower:
        return "rpm"

    return None


def build_artifact_url(artifact_name: str, artifact_type: str, distros: Dict[str, str]) -> Optional[str]:
    """
    Build the download URL for an artifact based on its type.

    Args:
        artifact_name: Name of the artifact
        artifact_type: Type of artifact ('rpm', 'log', 'sbom')
        distros: Dictionary mapping artifact types to distribution base URLs

    Returns:
        Full URL for downloading the artifact, or None if type is invalid

    Example:
        >>> distros = {"rpms": "https://example.com/rpms/", "logs": "https://example.com/logs/"}
        >>> build_artifact_url("package.rpm", "rpm", distros)
        'https://example.com/rpms/Packages/l/package.rpm'
    """
    if artifact_type == "sbom":
        return f"{distros.get('sbom', '')}{artifact_name}"
    if artifact_type == "log":
        return f"{distros.get('logs', '')}{artifact_name}"
    if artifact_type == "rpm":
        return f"{distros.get('rpms', '')}Packages/l/{artifact_name}"

    return None


def extract_architecture_from_metadata(metadata: Union[Dict[str, Any], ArtifactMetadata]) -> str:
    """
    Extract architecture from artifact metadata.

    Args:
        metadata: Artifact metadata (ArtifactMetadata model or dict)

    Returns:
        Architecture string, defaulting to 'noarch' if not found

    Example:
        >>> from pulp_tool.models.artifacts import ArtifactMetadata
        >>> metadata = ArtifactMetadata(labels={"arch": "x86_64"})
        >>> extract_architecture_from_metadata(metadata)
        'x86_64'
    """
    if isinstance(metadata, ArtifactMetadata):
        return metadata.arch or "noarch"

    return metadata.get("labels", {}).get("arch", "noarch")


def categorize_artifacts_by_type(
    artifacts: Dict[str, Any],
    distros: Dict[str, str],
    content_types: Optional[List[str]] = None,
    archs: Optional[List[str]] = None,
) -> List[Tuple[str, str, str, str]]:
    """
    Categorize artifacts and prepare download information.

    Args:
        artifacts: Dictionary of artifacts (can be ArtifactMetadata or dict)
        distros: Dictionary of distribution URLs
        content_types: Optional list of content types to filter (rpm, log, sbom)
        archs: Optional list of architectures to filter

    Returns:
        List of tuples: (artifact_name, file_url, arch, artifact_type)
    """
    download_tasks = []

    for artifact_name, metadata in artifacts.items():
        # Extract architecture from metadata
        arch = extract_architecture_from_metadata(metadata)

        # Detect artifact type
        artifact_type = detect_artifact_type(artifact_name)
        if not artifact_type:
            logging.debug("Skipping %s: could not determine artifact type", artifact_name)
            continue

        # Build download URL
        file_url = build_artifact_url(artifact_name, artifact_type, distros)
        if not file_url:
            logging.debug("Skipping %s: could not build download URL", artifact_name)
            continue

        # Apply content type filter
        if content_types and artifact_type not in content_types:
            logging.debug("Skipping %s: content type %s not in filter %s", artifact_name, artifact_type, content_types)
            continue

        # Apply architecture filter
        if archs and arch not in archs:
            logging.debug("Skipping %s: architecture %s not in filter %s", artifact_name, arch, archs)
            continue

        download_tasks.append((artifact_name, file_url, arch, artifact_type))

    return download_tasks


def detect_arch_from_filepath(filepath: str) -> Optional[str]:
    """
    Try to detect architecture from file path.

    This function checks if any supported architecture appears in the file path
    as a directory segment (e.g., /path/to/x86_64/package.rpm).

    Args:
        filepath: Path to file

    Returns:
        Architecture string if detected, None otherwise

    Example:
        >>> detect_arch_from_filepath("/path/to/x86_64/package.rpm")
        'x86_64'
        >>> detect_arch_from_filepath("/path/to/aarch64/package.rpm")
        'aarch64'
        >>> detect_arch_from_filepath("/path/to/package.rpm")
        None
    """
    # This handles cases like /path/to/x86_64/package.rpm or /path/to/aarch64/package.rpm
    path_lower = filepath.lower()
    for arch in SUPPORTED_ARCHITECTURES:
        # Check if architecture appears in the path as a directory segment
        # Pattern ensures there's at least one character before and after the /arch/ segment
        arch_pattern = rf"[^/\\][/\\]{re.escape(arch)}[/\\][^/\\]"
        if re.search(arch_pattern, path_lower, re.IGNORECASE):
            return arch

    return None


def detect_arch_from_rpm_filename(rpm_path: str) -> Optional[str]:
    """
    Try to detect architecture from RPM filename.

    This function checks the RPM filename pattern (name-version-release.arch.rpm)
    to extract the architecture.

    Args:
        rpm_path: Path to RPM file (filename will be extracted)

    Returns:
        Architecture string if detected, None otherwise

    Example:
        >>> detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.x86_64.rpm")
        'x86_64'
        >>> detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.aarch64.rpm")
        'aarch64'
        >>> detect_arch_from_rpm_filename("/path/to/package.rpm")
        None
    """
    # This handles cases like package-1.0.0-1.x86_64.rpm or package-1.0.0-1.aarch64.rpm
    filename = os.path.basename(rpm_path)
    match = re.search(r"\.([a-z0-9_]+)\.rpm$", filename, re.IGNORECASE)
    if match:
        arch = match.group(1)
        # Check if the detected arch from filename is in supported architectures
        if arch in SUPPORTED_ARCHITECTURES:
            return arch

    return None


__all__ = [
    "detect_artifact_type",
    "build_artifact_url",
    "extract_architecture_from_metadata",
    "categorize_artifacts_by_type",
    "detect_arch_from_filepath",
    "detect_arch_from_rpm_filename",
]
