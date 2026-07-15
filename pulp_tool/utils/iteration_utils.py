"""Iteration helpers for pulled artifact collections."""

from typing import Dict, Iterator, List, Optional, Tuple

from ..models.artifacts import PulledArtifacts, ArtifactFile

ARTIFACT_TYPES = ["rpms", "sboms", "logs"]


def iterate_artifact_types(
    pulled_artifacts: PulledArtifacts, *, types: Optional[List[str]] = None
) -> Iterator[Tuple[str, Dict[str, ArtifactFile]]]:
    """
    Iterate over artifact types and their collections.

    Args:
        pulled_artifacts: PulledArtifacts model containing artifacts
        types: Optional list of types to iterate (defaults to all)

    Yields:
        Tuples of (artifact_type, artifacts_dict)
    """
    types_to_iterate = types or ARTIFACT_TYPES

    for artifact_type in types_to_iterate:
        artifacts_dict = getattr(pulled_artifacts, artifact_type, {})
        if artifacts_dict:
            yield artifact_type, artifacts_dict


def iterate_all_artifacts(
    pulled_artifacts: PulledArtifacts, *, types: Optional[List[str]] = None
) -> Iterator[Tuple[str, str, ArtifactFile]]:
    """
    Iterate over all individual artifacts across types.

    Args:
        pulled_artifacts: PulledArtifacts model containing artifacts
        types: Optional list of types to iterate (defaults to all)

    Yields:
        Tuples of (artifact_type, artifact_name, artifact_data)
    """
    for artifact_type, artifacts_dict in iterate_artifact_types(pulled_artifacts, types=types):
        for artifact_name, artifact_data in artifacts_dict.items():
            yield artifact_type, artifact_name, artifact_data


def count_artifacts(pulled_artifacts: PulledArtifacts, *, types: Optional[List[str]] = None) -> Dict[str, int]:
    """
    Count artifacts by type.

    Args:
        pulled_artifacts: PulledArtifacts model containing artifacts
        types: Optional list of types to count (defaults to all)

    Returns:
        Dictionary mapping artifact_type to count
    """
    counts = {}

    for artifact_type, artifacts_dict in iterate_artifact_types(pulled_artifacts, types=types):
        counts[artifact_type] = len(artifacts_dict)

    return counts


__all__ = [
    "ARTIFACT_TYPES",
    "iterate_artifact_types",
    "iterate_all_artifacts",
    "count_artifacts",
]
