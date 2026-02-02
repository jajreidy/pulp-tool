"""
File distribution API operations.

This module provides file-specific distribution operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_file/restapi.html#distributions
"""

from typing import Any, Optional

import httpx

from ...models.pulp_api import DistributionRequest, FileDistributionResponse
from .base import BaseDistributionMixin


class FileDistributionMixin(BaseDistributionMixin):
    """Mixin that provides file distribution operations for Pulp."""

    def create_file_distribution(self, request: DistributionRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create a file distribution.

        API Endpoint: POST /api/v3/distributions/file/file/

        Args:
            request: DistributionRequest model with distribution data

        Returns:
            Tuple of (response, task_href) - task_href is None if operation is synchronous

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/distributions_file_file_create
        """
        endpoint = "api/v3/distributions/file/file/"
        return self.create_distribution(endpoint, request)

    def get_file_distribution(self, name: str) -> FileDistributionResponse:
        """
        Get a file distribution by name.

        API Endpoint: GET /api/v3/distributions/file/file/?name={name}

        Args:
            name: Distribution name

        Returns:
            FileDistributionResponse model

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/distributions_file_file_read
        """
        endpoint = "api/v3/distributions/file/file/"
        response = self._get_resource(endpoint, FileDistributionResponse, name=name)
        return response

    def list_file_distributions(
        self, **query_params: Any
    ) -> tuple[list[FileDistributionResponse], Optional[str], Optional[str], int]:
        """
        List file distributions with pagination.

        API Endpoint: GET /api/v3/distributions/file/file/

        Args:
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/distributions_file_file_list
        """
        endpoint = "api/v3/distributions/file/file/"
        return self._list_resources(endpoint, FileDistributionResponse, **query_params)

    def update_file_distribution(self, href: str, request: DistributionRequest) -> FileDistributionResponse:
        """
        Update a file distribution by href.

        API Endpoint: PATCH /api/v3/distributions/file/file/{id}/

        Args:
            href: Full distribution href
            request: DistributionRequest model with update data (typically just publication)

        Returns:
            FileDistributionResponse model

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/distributions_file_file_partial_update
        """
        return self._update_resource(href, request, FileDistributionResponse, "update file distribution")

    def delete_file_distribution(self, href: str) -> None:
        """
        Delete a file distribution by href.

        API Endpoint: DELETE /api/v3/distributions/file/file/{id}/

        Args:
            href: Full distribution href

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/distributions_file_file_delete
        """
        self._delete_resource(href, "delete file distribution")


__all__ = ["FileDistributionMixin"]
