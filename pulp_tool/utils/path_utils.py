"""File path helpers for artifact download layout."""

import os
from pathlib import Path
from typing import Optional

from .constants import SUPPORTED_ARCHITECTURES


def sanitize_arch_for_path(arch: str) -> str:
    """
    Validate and sanitize an architecture segment for filesystem paths.

    Args:
        arch: Architecture label (e.g. x86_64)

    Returns:
        Sanitized architecture string

    Raises:
        ValueError: If arch is empty or not in SUPPORTED_ARCHITECTURES
    """
    clean = os.path.basename(arch.replace("\\", "/"))
    if not clean or clean not in SUPPORTED_ARCHITECTURES:
        raise ValueError(f"Unsupported or invalid architecture: {arch!r}")
    return clean


def resolve_path_under_base(base_path: Path, relative_key: str) -> Path:
    """
    Resolve ``relative_key`` under ``base_path`` without allowing path traversal.

    Args:
        base_path: Resolved base directory
        relative_key: Relative path key from artifact metadata

    Returns:
        Resolved path guaranteed to be under ``base_path``

    Raises:
        ValueError: If the resolved path escapes ``base_path``
    """
    base = base_path.resolve()
    candidate = (base / relative_key).resolve()
    if not candidate.is_relative_to(base):
        raise ValueError(f"Path escapes base directory: {relative_key!r}")
    return candidate


def get_artifact_save_path(filename: str, arch: str, artifact_type: str, base_dir: Optional[str] = None) -> str:
    """
    Determine the save path for an artifact based on its type.

    RPM and SBOM files are saved at the top level (or under ``base_dir``).
    Log files are saved under ``logs/<arch>/``.

    Args:
        filename: Name of the file to save
        arch: Architecture for organizing the file path
        artifact_type: Type of artifact (rpm, log, sbom)
        base_dir: Optional base directory (defaults to current directory)

    Returns:
        Full path where the artifact should be saved
    """
    file_basename = os.path.basename(filename)

    if artifact_type == "log":
        safe_arch = sanitize_arch_for_path(arch)
        if base_dir:
            file_full_filename = os.path.join(base_dir, "logs", safe_arch, file_basename)
        else:
            file_full_filename = os.path.join("logs", safe_arch, file_basename)
        os.makedirs(os.path.dirname(file_full_filename), exist_ok=True)
    elif base_dir:
        file_full_filename = os.path.join(base_dir, file_basename)
    else:
        file_full_filename = file_basename

    return file_full_filename


__all__ = [
    "get_artifact_save_path",
    "resolve_path_under_base",
    "sanitize_arch_for_path",
]
