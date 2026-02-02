"""
RPM distribution API operations.

This module provides RPM-specific distribution operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_rpm/restapi.html#distributions
"""

from typing import Any, Optional

import httpx

from ...models.pulp_api import DistributionRequest, RpmDistributionRequest, RpmDistributionResponse
from .base import BaseDistributionMixin


class RpmDistributionMixin(BaseDistributionMixin):
    """Mixin that provides RPM distribution operations for Pulp."""

    def create_rpm_distribution(self, request: RpmDistributionRequest) -> tuple[httpx.Response, Optional[str]]:
        """
        Create an RPM distribution.

        API Endpoint: POST /api/v3/distributions/rpm/rpm/

        Args:
            request: RpmDistributionRequest model with distribution data

        Returns:
            Tuple of (response, task_href) - task_href is None if operation is synchronous

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/distributions_rpm_rpm_create
        """
        endpoint = "api/v3/distributions/rpm/rpm/"
        return self.create_distribution(endpoint, request)

    def get_rpm_distribution(self, name: str) -> RpmDistributionResponse:
        """
        Get an RPM distribution by name.

        API Endpoint: GET /api/v3/distributions/rpm/rpm/?name={name}

        Args:
            name: Distribution name

        Returns:
            RpmDistributionResponse model

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/distributions_rpm_rpm_read
        """
        endpoint = "api/v3/distributions/rpm/rpm/"
        response = self._get_resource(endpoint, RpmDistributionResponse, name=name)
        return response

    def list_rpm_distributions(
        self, **query_params: Any
    ) -> tuple[list[RpmDistributionResponse], Optional[str], Optional[str], int]:
        """
        List RPM distributions with pagination.

        API Endpoint: GET /api/v3/distributions/rpm/rpm/

        Args:
            **query_params: Query parameters (offset, limit, name, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/distributions_rpm_rpm_list
        """
        endpoint = "api/v3/distributions/rpm/rpm/"
        return self._list_resources(endpoint, RpmDistributionResponse, **query_params)

    def update_rpm_distribution(self, href: str, request: DistributionRequest) -> RpmDistributionResponse:
        """
        Update an RPM distribution by href.

        API Endpoint: PATCH /api/v3/distributions/rpm/rpm/{id}/

        Args:
            href: Full distribution href
            request: DistributionRequest model with update data (typically just publication)

        Returns:
            RpmDistributionResponse model

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/distributions_rpm_rpm_partial_update
        """
        return self._update_resource(href, request, RpmDistributionResponse, "update RPM distribution")

    def delete_rpm_distribution(self, href: str) -> None:
        """
        Delete an RPM distribution by href.

        API Endpoint: DELETE /api/v3/distributions/rpm/rpm/{id}/

        Args:
            href: Full distribution href

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/distributions_rpm_rpm_delete
        """
        self._delete_resource(href, "delete RPM distribution")


__all__ = ["RpmDistributionMixin"]
