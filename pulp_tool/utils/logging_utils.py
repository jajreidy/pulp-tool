"""
Logging utilities for consistent operation and artifact logging.

This module provides standardized logging functions to eliminate
code duplication and ensure consistent log formatting.
"""

import logging
from typing import Dict, List, Optional


def log_operation_start(operation: str, **details) -> None:
    """
    Log the start of an operation with standardized format.

    Args:
        operation: Description of the operation
        **details: Additional details to log as key=value pairs
    """
    if details:
        detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
        logging.info("Starting %s (%s)", operation, detail_str)
    else:
        logging.info("Starting %s", operation)


def log_operation_complete(operation: str, **details) -> None:
    """
    Log the completion of an operation with standardized format.

    Args:
        operation: Description of the operation
        **details: Additional details to log as key=value pairs
    """
    if details:
        detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
        logging.info("Completed %s (%s)", operation, detail_str)
    else:
        logging.info("Completed %s", operation)


def log_artifact_summary(
    artifact_counts: Dict[str, int], *, operation: str = "Processed", level: int = logging.INFO
) -> None:
    """
    Log a summary of artifact counts with proper pluralization.

    Args:
        artifact_counts: Dictionary mapping artifact type to count
        operation: Operation description (e.g., "Downloaded", "Uploaded")
        level: Logging level to use
    """
    if not artifact_counts or all(count == 0 for count in artifact_counts.values()):
        logging.log(level, "%s: No artifacts", operation)
        return

    parts = []
    for artifact_type, count in artifact_counts.items():
        if count > 0:
            formatted = format_count_with_unit(count, artifact_type)
            parts.append(formatted)

    if parts:
        logging.log(level, "%s: %s", operation, ", ".join(parts))


def format_count_with_unit(count: int, unit: str, *, singular: Optional[str] = None) -> str:
    """
    Format a count with proper pluralization.

    Args:
        count: Number to format
        unit: Unit name (will be pluralized if count != 1)
        singular: Optional explicit singular form (defaults to unit)

    Returns:
        Formatted string like "5 files" or "1 file"

    Examples:
        >>> format_count_with_unit(1, "RPM")
        '1 RPM'
        >>> format_count_with_unit(5, "RPM")
        '5 RPMs'
        >>> format_count_with_unit(1, "repositories", singular="repository")
        '1 repository'
    """
    if count == 1:
        return f"{count} {singular or unit}"

    # Simple pluralization - add 's' if not already present
    if unit.endswith("s"):
        plural = unit
    else:
        plural = f"{unit}s"

    return f"{count} {plural}"


def format_artifact_counts(counts: Dict[str, int]) -> str:
    """
    Format artifact counts as a comma-separated string.

    Args:
        counts: Dictionary mapping artifact type to count

    Returns:
        Formatted string like "5 RPMs, 3 logs, 1 SBOM"

    Examples:
        >>> format_artifact_counts({"rpms": 5, "logs": 3, "sboms": 1})
        '5 RPMs, 3 logs, 1 SBOM'
    """
    # Map plural forms to singular for proper formatting
    singular_map = {
        "rpms": "RPM",
        "logs": "log",
        "sboms": "SBOM",
        "artifacts": "artifact",
    }

    parts = []
    for artifact_type, count in counts.items():
        if count > 0:
            unit = singular_map.get(artifact_type, artifact_type)
            formatted = format_count_with_unit(count, unit)
            parts.append(formatted)

    return ", ".join(parts) if parts else "No artifacts"


def log_file_size(file_path: str, file_type: str, size_bytes: int) -> None:
    """
    Log file size in human-readable format.

    Args:
        file_path: Path to the file
        file_type: Type of file for logging
        size_bytes: Size in bytes
    """
    size_str = format_file_size(size_bytes)
    logging.debug("%s file '%s': %s", file_type, file_path, size_str)


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string (e.g., "1.5 MB", "500 KB")

    Examples:
        >>> format_file_size(0)
        '0 B'
        >>> format_file_size(1024)
        '1.0 KB'
        >>> format_file_size(1536)
        '1.5 KB'
    """
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size_float = float(size_bytes)

    while size_float >= 1024 and i < len(size_names) - 1:
        size_float /= 1024.0
        i += 1

    return f"{size_float:.1f} {size_names[i]}"


def log_progress(current: int, total: int, operation: str, *, interval: int = 10) -> None:
    """
    Log progress at regular intervals.

    Args:
        current: Current progress count
        total: Total items to process
        operation: Operation description
        interval: Log every N items (default: 10)
    """
    if current % interval == 0 or current == total:
        percentage = (current / total * 100) if total > 0 else 0
        logging.info("%s: %d/%d (%.1f%%)", operation, current, total, percentage)


def log_summary_separator(title: Optional[str] = None, width: int = 80) -> None:
    """
    Log a visual separator line with optional title.

    Args:
        title: Optional title to display in separator
        width: Width of separator line
    """
    if title:
        logging.info("=" * width)
        logging.info(title)
        logging.info("=" * width)
    else:
        logging.info("=" * width)


def log_list_items(items: List[str], prefix: str = "  - ", level: int = logging.INFO) -> None:
    """
    Log a list of items with consistent formatting.

    Args:
        items: List of items to log
        prefix: Prefix for each item
        level: Logging level to use
    """
    for item in items:
        logging.log(level, "%s%s", prefix, item)


__all__ = [
    "log_operation_start",
    "log_operation_complete",
    "log_artifact_summary",
    "format_count_with_unit",
    "format_artifact_counts",
    "log_file_size",
    "format_file_size",
    "log_progress",
    "log_summary_separator",
    "log_list_items",
]
