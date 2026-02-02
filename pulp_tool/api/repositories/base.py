"""
Base repository mixin providing common repository operations.

This module provides the base functionality for all repository types,
following Pulp's API structure.
"""

from typing import Any, Optional

import httpx

from ..base import BaseResourceMixin
from ...models.pulp_api import RepositoryRequest, RepositoryResponse


class BaseRepositoryMixin(BaseResourceMixin):
    """Base mixin providing repository operations for Pulp."""

    def create_repository(self, endpoint: str, request: RepositoryRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create a repository.

        Args:
            endpoint: API endpoint for repository creation (e.g., "api/v3/repositories/rpm/rpm/")
            request: RepositoryRequest model with repository data

        Returns:
            Tuple of (response, task_href) - task_href is None if autopublish is False
        """
        url = self._url(endpoint)
        data = request.model_dump(exclude_none=True)

        response = self.session.post(url, json=data, timeout=self.timeout, **self.request_params)
        self._check_response(response, "create repository")

        # Check if response contains a task (for async operations)
        task_href = None
        try:
            json_data = response.json()
            task_href = json_data.get("task")
        except (ValueError, KeyError):
            pass

        return response, task_href

    def get_repository(self, endpoint: str, name: str) -> RepositoryResponse:
        """
        Get a repository by name.

        Args:
            endpoint: API endpoint for repository type
            name: Repository name

        Returns:
            RepositoryResponse model
        """
        return self._get_resource(endpoint, RepositoryResponse, name=name)

    def list_repositories(
        self, endpoint: str, **query_params: Any
    ) -> tuple[list[RepositoryResponse], Optional[str], Optional[str], int]:
        """
        List repositories with pagination.

        Args:
            endpoint: API endpoint for repository type
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)
        """
        return self._list_resources(endpoint, RepositoryResponse, **query_params)

    def update_repository(self, href: str, request: RepositoryRequest) -> RepositoryResponse:
        """
        Update a repository by href.

        Args:
            href: Full repository href
            request: RepositoryRequest model with update data

        Returns:
            RepositoryResponse model
        """
        return self._update_resource(href, request, RepositoryResponse, "update repository")

    def delete_repository(self, href: str) -> None:
        """
        Delete a repository by href.

        Args:
            href: Full repository href
        """
        self._delete_resource(href, "delete repository")


__all__ = ["BaseRepositoryMixin"]
