"""
Artifact API operations.

This module provides artifact operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulpcore/restapi.html#artifacts
"""

from typing import Any, List, Optional

from ...models.pulp_api import ArtifactResponse
from ..base import BaseResourceMixin


class ArtifactMixin(BaseResourceMixin):
    """Mixin that provides artifact operations for Pulp."""

    def get_artifact(self, href: str) -> ArtifactResponse:
        """
        Get an artifact by href.

        API Endpoint: GET /api/v3/artifacts/{id}/

        Args:
            href: Full artifact href

        Returns:
            ArtifactResponse model

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/artifacts_read
        """
        url = str(self.config["base_url"]) + href
        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        return self._parse_response(response, ArtifactResponse, "get artifact")

    def list_artifacts(self, **query_params: Any) -> tuple[list[ArtifactResponse], Optional[str], Optional[str], int]:
        """
        List artifacts with pagination.

        API Endpoint: GET /api/v3/artifacts/

        Args:
            **query_params: Query parameters (offset, limit, pulp_href__in, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/artifacts_list
        """
        endpoint = "api/v3/artifacts/"
        return self._list_resources(endpoint, ArtifactResponse, **query_params)

    def get_file_locations(self, artifact_hrefs: List[str]) -> List[ArtifactResponse]:
        """
        Get file locations for multiple artifacts.

        API Endpoint: GET /api/v3/artifacts/?pulp_href__in={href1},{href2},...

        This method handles chunking for large lists of artifact hrefs.

        Args:
            artifact_hrefs: List of artifact hrefs to query

        Returns:
            List of ArtifactResponse models with file locations

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/artifacts_list
        """
        endpoint = "api/v3/artifacts/"
        query_params = {"pulp_href__in": ",".join(artifact_hrefs)}

        # Use chunked get if available (from pulp_client)
        if hasattr(self, "_chunked_get"):
            response = self._chunked_get(
                self._url(endpoint),
                params=query_params,
                chunk_param="pulp_href__in",
                chunk_size=20,
                timeout=self.timeout,
                **self.request_params,
            )
            json_data = response.json()
            results = json_data.get("results", [])
            return [ArtifactResponse(**item) for item in results]
        else:
            # Fallback to regular list
            results, _, _, _ = self._list_resources(endpoint, ArtifactResponse, **query_params)
            return results


__all__ = ["ArtifactMixin"]
