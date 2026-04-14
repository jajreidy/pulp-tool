"""
Repository and distribution HTTP helpers for :class:`pulp_tool.api.pulp_client.PulpClient`.

Extracted to keep ``pulp_client.py`` smaller; behavior is unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlencode

import httpx

if TYPE_CHECKING:
    from .pulp_client import PulpClient  # pragma: no cover


def get_single_resource_by_name(client: "PulpClient", endpoint: str, name: str) -> httpx.Response:
    """
    GET a single resource by name (uncached implementation body).

    ``PulpClient._get_single_resource`` applies ``@cached_get`` and delegates here.
    """
    url = client._url(f"{endpoint}?")
    url += urlencode({"name": name, "offset": 0, "limit": 1})
    response = client.session.get(url, timeout=client.timeout, **client.request_params)
    if response.status_code != 404:
        client._check_response(response, "get single resource by name")
    return response


def repository_operation(
    client: "PulpClient",
    operation: str,
    repo_type: str,
    *,
    name: Optional[str] = None,
    repository_data: Optional[Any] = None,
    distribution_data: Optional[Any] = None,
    publication: Optional[str] = None,
    distribution_href: Optional[str] = None,
) -> httpx.Response:
    """Perform repository or distribution operations (implementation body for ``PulpClient.repository_operation``)."""
    endpoint_base = f"api/v3/{'repositories' if 'repo' in operation else 'distributions'}/{repo_type}/{repo_type}/"

    if operation == "create_repo":
        if not repository_data:
            raise ValueError("Repository data is required for 'create_repo' operations")
        return client._create_repository(endpoint_base, repository_data)

    if operation == "get_repo":
        if not name:
            raise ValueError("Name is required for 'get_repo' operations")
        return client._get_single_resource(endpoint_base, name)

    if operation == "create_distro":
        if not distribution_data:
            raise ValueError("Distribution data is required for 'create_distro' operations")
        return client._create_distribution(endpoint_base, distribution_data)

    if operation == "get_distro":
        if not name:
            raise ValueError("Name is required for 'get_distro' operations")
        return client._get_single_resource(endpoint_base, name)

    if operation == "update_distro":
        if not distribution_href:
            raise ValueError("Distribution href is required")
        url = str(client.config["base_url"]) + distribution_href
        data = {"publication": publication}
        response = client.session.patch(url, json=data, timeout=client.timeout, **client.request_params)
        client._check_response(response, "update distribution")
        return response

    raise ValueError(f"Unknown operation: {operation}")


__all__ = ["get_single_resource_by_name", "repository_operation"]
