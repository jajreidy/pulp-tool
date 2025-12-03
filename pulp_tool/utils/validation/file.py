"""
File path validation utilities.

This module provides functions for validating file paths, ensuring files exist,
are readable, and not empty.
"""

import logging
import os

from ...utils.constants import MIN_FILE_SIZE


def validate_file_path(file_path: str, file_type: str) -> None:
    """
    Validate file exists, is readable, and not empty.

    Uses guard clauses for early validation failure.

    Args:
        file_path: Path to the file to validate
        file_type: Type of file for error messages (e.g., 'RPM', 'SBOM')

    Raises:
        FileNotFoundError: If the file does not exist
        PermissionError: If the file cannot be read
        ValueError: If the file is empty

    Example:
        >>> validate_file_path("/path/to/file.rpm", "RPM")  # doctest: +SKIP
    """
    # Guard clause: file must exist
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_type} file not found: {file_path}")

    # Guard clause: file must be readable
    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"Cannot read {file_type} file: {file_path}")

    # Guard clause: file must not be empty
    file_size = os.path.getsize(file_path)
    if file_size == MIN_FILE_SIZE:
        raise ValueError(f"{file_type} file is empty: {file_path}")

    logging.debug("%s file size: %d bytes", file_type, file_size)


__all__ = ["validate_file_path"]
