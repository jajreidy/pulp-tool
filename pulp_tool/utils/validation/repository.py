"""
Repository validation utilities.

This module provides functions for validating repository setup and configuration.
"""

from typing import Dict, List, Tuple

from ...utils.constants import REPOSITORY_TYPES


def validate_repository_setup(repositories: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Validate that repository setup is complete.

    Args:
        repositories: Dictionary mapping repository identifiers to repository references
                     Expected keys: rpms_prn, rpms_href, logs_prn, logs_href,
                                   sbom_prn, sbom_href, artifacts_prn, artifacts_href

    Returns:
        Tuple of (is_valid, list_of_errors)

    Example:
        >>> repos = {
        ...     "rpms_prn": "/pulp/api/v3/repositories/rpm/rpm/123/",
        ...     "rpms_href": "/pulp/api/v3/repositories/rpm/rpm/123/"
        ... }
        >>> is_valid, errors = validate_repository_setup(repos)
        >>> is_valid
        False
        >>> "logs" in str(errors)
        True
    """
    errors = []

    # Check that all required repository PRNs are present
    for repo_type in REPOSITORY_TYPES:
        prn_key = f"{repo_type}_prn"
        if prn_key not in repositories or not repositories.get(prn_key):
            errors.append(f"Missing {repo_type} repository PRN")

    # For RPM repositories, also check that href is present
    if "rpms_href" not in repositories or not repositories.get("rpms_href"):
        errors.append("Missing rpms repository href")

    # Check that repository references are valid (non-empty strings)
    for repo_key, repo_ref in repositories.items():
        if repo_ref and (not isinstance(repo_ref, str) or not repo_ref.strip()):
            errors.append(f"Invalid repository reference for {repo_key}")

    is_valid = len(errors) == 0
    return is_valid, errors


__all__ = ["validate_repository_setup"]
