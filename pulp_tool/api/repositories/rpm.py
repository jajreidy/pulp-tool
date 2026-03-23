"""
RPM repository API operations.

This module provides RPM-specific repository operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_rpm/restapi.html#repositories
"""

from typing import Any, Optional

import httpx

from ...models.pulp_api import RpmRepositoryRequest, RpmRepositoryResponse
from .base import BaseRepositoryMixin


class RpmRepositoryMixin(BaseRepositoryMixin):
    """Mixin that provides RPM repository operations for Pulp."""

    def create_rpm_repository(self, request: RpmRepositoryRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create an RPM repository.

        API Endpoint: POST /api/v3/repositories/rpm/rpm/

        Args:
            request: RpmRepositoryRequest model with repository data

        Returns:
            Tuple of (response, task_href) - task_href is None if autopublish is False

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/repositories_rpm_rpm_create
        """
        endpoint = "api/v3/repositories/rpm/rpm/"
        return self.create_repository(endpoint, request)

    def get_rpm_repository(self, name: str) -> RpmRepositoryResponse:
        """
        Get an RPM repository by name.

        API Endpoint: GET /api/v3/repositories/rpm/rpm/?name={name}

        Args:
            name: Repository name

        Returns:
            RpmRepositoryResponse model

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/repositories_rpm_rpm_read
        """
        endpoint = "api/v3/repositories/rpm/rpm/"
        response = self._get_resource(endpoint, RpmRepositoryResponse, name=name)
        return response

    def fetch_rpm_repository_by_href(self, pulp_href: str) -> RpmRepositoryResponse:
        """
        Fetch an RPM repository by pulp_href (includes latest_version_href).

        API Endpoint: GET {pulp_href}

        Args:
            pulp_href: Full repository href (e.g. /pulp/api/v3/repositories/rpm/rpm/{uuid}/)

        Returns:
            RpmRepositoryResponse model
        """
        url = str(self.config["base_url"]) + pulp_href  # type: ignore[attr-defined]
        response = self.session.get(url, timeout=self.timeout, **self.request_params)  # type: ignore[attr-defined]
        return self._parse_response(response, RpmRepositoryResponse, "get RPM repository by href")

    def list_rpm_repositories(
        self, **query_params: Any
    ) -> tuple[list[RpmRepositoryResponse], Optional[str], Optional[str], int]:
        """
        List RPM repositories with pagination.

        API Endpoint: GET /api/v3/repositories/rpm/rpm/

        Args:
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/repositories_rpm_rpm_list
        """
        endpoint = "api/v3/repositories/rpm/rpm/"
        return self._list_resources(endpoint, RpmRepositoryResponse, **query_params)

    def update_rpm_repository(self, href: str, request: RpmRepositoryRequest) -> RpmRepositoryResponse:
        """
        Update an RPM repository by href.

        API Endpoint: PATCH /api/v3/repositories/rpm/rpm/{id}/

        Args:
            href: Full repository href
            request: RpmRepositoryRequest model with update data

        Returns:
            RpmRepositoryResponse model

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/repositories_rpm_rpm_partial_update
        """
        return self._update_resource(href, request, RpmRepositoryResponse, "update RPM repository")

    def delete_rpm_repository(self, href: str) -> None:
        """
        Delete an RPM repository by href.

        API Endpoint: DELETE /api/v3/repositories/rpm/rpm/{id}/

        Args:
            href: Full repository href

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/repositories_rpm_rpm_delete
        """
        self._delete_resource(href, "delete RPM repository")


__all__ = ["RpmRepositoryMixin"]
