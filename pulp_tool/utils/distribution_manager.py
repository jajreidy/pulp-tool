"""
Distribution URL management for Pulp operations.

This module handles retrieving and constructing distribution URLs.
"""

import logging
from typing import Dict, Optional, Tuple, TYPE_CHECKING

from .constants import REPOSITORY_TYPES
from .validation import sanitize_build_id_for_repository, strip_namespace_from_build_id, validate_build_id

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient


class DistributionManager:
    """
    Manages distribution URL operations for Pulp.

    This class handles retrieving and constructing distribution URLs
    for different repository types.
    """

    def __init__(
        self, pulp_client: "PulpClient", namespace: str, distribution_cache: Optional[Dict[Tuple[str, str], str]] = None
    ) -> None:
        """
        Initialize the distribution manager.

        Args:
            pulp_client: PulpClient instance for API interactions
            namespace: Namespace for multi-tenant content serving
            distribution_cache: Optional shared cache for distribution base paths
        """
        self.client = pulp_client
        self.namespace = namespace
        self._distribution_cache: Dict[Tuple[str, str], str] = (
            distribution_cache if distribution_cache is not None else {}
        )

    def get_distribution_urls(self, build_id: str) -> Dict[str, str]:
        """
        Get distribution URLs for all repository types.

        This method orchestrates the retrieval of distribution URLs
        by delegating to the PulpClient API methods.

        Args:
            build_id: Build ID for naming repositories and distributions

        Returns:
            Dictionary mapping repo_type to distribution URL
        """
        # Check for empty/None build ID before sanitization
        if not build_id or not isinstance(build_id, str) or not build_id.strip():
            raise ValueError(f"Invalid build ID: {build_id}")

        # Sanitize build ID for repository naming first
        sanitized_build_id = sanitize_build_id_for_repository(build_id)

        # Validate sanitized build ID
        if not validate_build_id(sanitized_build_id):
            raise ValueError(f"Invalid build ID: {build_id} (sanitized: {sanitized_build_id})")
        if sanitized_build_id != build_id:
            logging.debug("Sanitized build ID '%s' to '%s' for repository naming", build_id, sanitized_build_id)

        logging.debug("Getting distribution URLs for build: %s", sanitized_build_id)

        # Get distribution URLs directly using the helper's own methods
        distribution_urls = self._get_distribution_urls_impl(sanitized_build_id)

        logging.debug("Retrieved %d distribution URLs", len(distribution_urls))
        return distribution_urls

    def _get_single_distribution_url(self, build_id: str, repo_type: str, base_url: str) -> Optional[str]:
        """
        Get distribution URL for a single repository type.

        Checks cache first, then falls back to API query if needed.
        """
        # Check cache first - avoid API call if we already have the base_path
        cache_key = (build_id, repo_type)
        if cache_key in self._distribution_cache:
            base_path = self._distribution_cache[cache_key]
            # Include namespace in the full URL path for multi-tenant content serving
            distribution_url = f"{base_url}{self.namespace}/{base_path}/"
            logging.info(
                "Using cached distribution for %s: base_path=%s, url=%s", repo_type, base_path, distribution_url
            )
            return distribution_url

        # Cache miss - compute the expected base_path and use it
        # We compute the base_path rather than trusting what's in the API
        # because old distributions might have been created with incorrect paths
        # Strip namespace from build_id to avoid duplication in base_path
        build_name = strip_namespace_from_build_id(build_id)
        base_path = f"{build_name}/{repo_type}"
        # Include namespace in the full URL path for multi-tenant content serving
        distribution_url = f"{base_url}{self.namespace}/{base_path}/"

        # Cache for future use
        self._distribution_cache[cache_key] = base_path

        logging.info(
            "Using computed distribution URL for %s: base_path=%s, url=%s", repo_type, base_path, distribution_url
        )
        return distribution_url

    def _get_distribution_urls_impl(self, build_id: str) -> Dict[str, str]:
        """
        Get distribution URLs for all repository types.

        Args:
            build_id: Base name for the repositories (may include namespace prefix)

        Returns:
            Dictionary mapping repo_type to distribution URL
        """
        distribution_urls = {}
        # Get base_url from client's config
        base_url_str = str(self.client.config["base_url"])
        pulp_content_base_url = f"{base_url_str}/api/pulp-content"
        # Base URL from Pulp content service - distribution's base_path is build_id/repo_type
        base_url = f"{pulp_content_base_url}/"

        for repo_type in REPOSITORY_TYPES:
            url = self._get_single_distribution_url(build_id, repo_type, base_url)
            if url:
                distribution_urls[repo_type] = url

        return distribution_urls


__all__ = ["DistributionManager"]
