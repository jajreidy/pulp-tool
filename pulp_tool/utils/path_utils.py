"""File path helpers for artifact download layout."""

import os
from typing import Optional


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
        if base_dir:
            file_full_filename = os.path.join(base_dir, "logs", arch, file_basename)
        else:
            file_full_filename = os.path.join("logs", arch, file_basename)
        os.makedirs(os.path.dirname(file_full_filename), exist_ok=True)
    elif base_dir:
        file_full_filename = os.path.join(base_dir, file_basename)
    else:
        file_full_filename = file_basename

    return file_full_filename


__all__ = [
    "get_artifact_save_path",
]
