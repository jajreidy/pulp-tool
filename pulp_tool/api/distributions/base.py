"""
Base distribution mixin providing common distribution operations.

This module provides the base functionality for all distribution types,
following Pulp's API structure.
"""

from typing import Any, Optional

import httpx

from ..base import BaseResourceMixin
from ...models.pulp_api import DistributionRequest, DistributionResponse


class BaseDistributionMixin(BaseResourceMixin):
    """Base mixin providing distribution operations for Pulp."""

    def create_distribution(self, endpoint: str, request: DistributionRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create a distribution.

        Args:
            endpoint: API endpoint for distribution creation (e.g., "api/v3/distributions/rpm/rpm/")
            request: DistributionRequest model with distribution data

        Returns:
            Tuple of (response, task_href) - task_href is None if operation is synchronous
        """
        url = self._url(endpoint)
        data = request.model_dump(exclude_none=True)

        response = self.session.post(url, json=data, timeout=self.timeout, **self.request_params)
        self._check_response(response, "create distribution")

        # Check if response contains a task (for async operations)
        task_href = None
        try:
            json_data = response.json()
            task_href = json_data.get("task")
        except (ValueError, KeyError):
            pass

        return response, task_href

    def get_distribution(self, endpoint: str, name: str) -> DistributionResponse:
        """
        Get a distribution by name.

        Args:
            endpoint: API endpoint for distribution type
            name: Distribution name

        Returns:
            DistributionResponse model
        """
        return self._get_resource(endpoint, DistributionResponse, name=name)

    def list_distributions(
        self, endpoint: str, **query_params: Any
    ) -> tuple[list[DistributionResponse], Optional[str], Optional[str], int]:
        """
        List distributions with pagination.

        Args:
            endpoint: API endpoint for distribution type
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)
        """
        return self._list_resources(endpoint, DistributionResponse, **query_params)

    def update_distribution(self, href: str, request: DistributionRequest) -> DistributionResponse:
        """
        Update a distribution by href.

        Args:
            href: Full distribution href
            request: DistributionRequest model with update data

        Returns:
            DistributionResponse model
        """
        return self._update_resource(href, request, DistributionResponse, "update distribution")

    def delete_distribution(self, href: str) -> None:
        """
        Delete a distribution by href.

        Args:
            href: Full distribution href
        """
        self._delete_resource(href, "delete distribution")


__all__ = ["BaseDistributionMixin"]
