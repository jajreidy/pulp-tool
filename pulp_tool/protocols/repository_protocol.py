"""
Repository protocol for type safety.

This module defines protocols for repository operations, enabling
better type checking and abstraction.
"""

from typing import Optional, Protocol

from ..models.repository import RepositoryRefs


class RepositoryProtocol(Protocol):
    """
    Protocol defining the interface for repository operations.

    This protocol enables type checking and abstraction for repository
    management operations without requiring inheritance.
    """

    def setup_repositories(self, build_id: str) -> RepositoryRefs:
        """
        Set up all required repositories for a build.

        Args:
            build_id: Build identifier

        Returns:
            RepositoryRefs containing all repository identifiers
        """
        ...

    def get_distribution_urls(self, build_id: str) -> dict[str, str]:
        """
        Get distribution URLs for all repository types.

        Args:
            build_id: Build identifier

        Returns:
            Dictionary mapping repository types to distribution URLs
        """
        ...

    def create_or_get_repository(self, build_id: str, repo_type: str) -> tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution.

        Args:
            build_id: Build identifier
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts')

        Returns:
            Tuple of (repository_prn, repository_href)
        """
        ...


__all__ = ["RepositoryProtocol"]
