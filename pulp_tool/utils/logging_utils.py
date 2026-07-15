"""Logging helpers for artifact count summaries."""

from typing import Dict, Optional


def format_count_with_unit(count: int, unit: str, *, singular: Optional[str] = None) -> str:
    """
    Format a count with proper pluralization.

    Args:
        count: Number to format
        unit: Unit name (will be pluralized if count != 1)
        singular: Optional explicit singular form (defaults to unit)

    Returns:
        Formatted string like "5 files" or "1 file"
    """
    if count == 1:
        return f"{count} {singular or unit}"

    plural = unit if unit.endswith("s") else f"{unit}s"
    return f"{count} {plural}"


def format_artifact_counts(counts: Dict[str, int]) -> str:
    """
    Format artifact counts as a comma-separated string.

    Args:
        counts: Dictionary mapping artifact type to count

    Returns:
        Formatted string like "5 RPMs, 3 logs, 1 SBOM"
    """
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
            parts.append(format_count_with_unit(count, unit))

    return ", ".join(parts) if parts else "No artifacts"


__all__ = [
    "format_count_with_unit",
    "format_artifact_counts",
]
