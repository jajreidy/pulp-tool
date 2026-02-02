"""
File repository API operations.

This module provides file-specific repository operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_file/restapi.html#repositories
"""

from typing import Any, Optional

import httpx

from ...models.pulp_api import FileRepositoryResponse, RepositoryRequest
from .base import BaseRepositoryMixin


class FileRepositoryMixin(BaseRepositoryMixin):
    """Mixin that provides file repository operations for Pulp."""

    def create_file_repository(self, request: RepositoryRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create a file repository.

        API Endpoint: POST /api/v3/repositories/file/file/

        Args:
            request: RepositoryRequest model with repository data

        Returns:
            Tuple of (response, task_href) - task_href is None if autopublish is False

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/repositories_file_file_create
        """
        endpoint = "api/v3/repositories/file/file/"
        return self.create_repository(endpoint, request)

    def get_file_repository(self, name: str) -> FileRepositoryResponse:
        """
        Get a file repository by name.

        API Endpoint: GET /api/v3/repositories/file/file/?name={name}

        Args:
            name: Repository name

        Returns:
            FileRepositoryResponse model

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/repositories_file_file_read
        """
        endpoint = "api/v3/repositories/file/file/"
        response = self._get_resource(endpoint, FileRepositoryResponse, name=name)
        return response

    def list_file_repositories(
        self, **query_params: Any
    ) -> tuple[list[FileRepositoryResponse], Optional[str], Optional[str], int]:
        """
        List file repositories with pagination.

        API Endpoint: GET /api/v3/repositories/file/file/

        Args:
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/repositories_file_file_list
        """
        endpoint = "api/v3/repositories/file/file/"
        return self._list_resources(endpoint, FileRepositoryResponse, **query_params)

    def update_file_repository(self, href: str, request: RepositoryRequest) -> FileRepositoryResponse:
        """
        Update a file repository by href.

        API Endpoint: PATCH /api/v3/repositories/file/file/{id}/

        Args:
            href: Full repository href
            request: RepositoryRequest model with update data

        Returns:
            FileRepositoryResponse model

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/repositories_file_file_partial_update
        """
        return self._update_resource(href, request, FileRepositoryResponse, "update file repository")

    def delete_file_repository(self, href: str) -> None:
        """
        Delete a file repository by href.

        API Endpoint: DELETE /api/v3/repositories/file/file/{id}/

        Args:
            href: Full repository href

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/repositories_file_file_delete
        """
        self._delete_resource(href, "delete file repository")


__all__ = ["FileRepositoryMixin"]
