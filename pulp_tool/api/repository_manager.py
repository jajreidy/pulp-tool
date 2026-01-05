"""
Repository management operations for Pulp API.

This module handles creating and managing repositories and distributions.
"""

from typing import Any, Callable, Optional, Protocol, runtime_checkable

import httpx

from ..models.pulp_api import DistributionRequest, RepositoryRequest


@runtime_checkable
class RepositoryManagerMixin(Protocol):
    """Protocol that provides repository and distribution operations for Pulp."""

    # Required attributes
    _url: Callable[[str], str]  # Method that constructs URLs
    session: Any  # httpx.Client
    timeout: int
    request_params: dict
    config: dict

    def _get_single_resource(self, endpoint: str, name: str) -> httpx.Response:
        """Get a single resource."""
        ...  # pragma: no cover - defined in implementation

    def _create_repository(self, endpoint: str, new_repository: RepositoryRequest) -> httpx.Response:
        """
        Create a repository.

        Args:
            endpoint: API endpoint for repository creation
            new_repository: RepositoryRequest model for the repository to create

        Returns:
            Response object from the repository creation request
        """
        url = self._url(endpoint)
        data = new_repository.model_dump(exclude_none=True)

        return self.session.post(url, json=data, timeout=self.timeout, **self.request_params)

    def _create_distribution(self, endpoint: str, new_distribution: DistributionRequest) -> httpx.Response:
        """
        Create a distribution.

        Args:
            endpoint: API endpoint for distribution creation
            new_distribution: DistributionRequest model for the distribution to create

        Returns:
            Response object from the distribution creation request
        """
        url = self._url(endpoint)

        data = new_distribution.model_dump(exclude_none=True)
        return self.session.post(url, json=data, timeout=self.timeout, **self.request_params)

    def repository_operation(
        self,
        operation: str,
        repo_type: str,
        *,
        name: str = None,
        repository_data: Optional[RepositoryRequest] = None,
        distribution_data: Optional[DistributionRequest] = None,
        publication: Optional[str] = None,
        distribution_href: Optional[str] = None,
    ) -> httpx.Response:
        """
        Perform repository or distribution operations.

        Args:
            operation: Operation to perform ('create_repo', 'get_repo',
                      'create_distro', 'get_distro', 'update_distro')
            repo_type: Type of repository/distribution ('rpm' or 'file')
            name: Name of the repository/distribution (for get resource operations)
            repository_data: RepositoryRequest model for the repository to create
            distribution: DistributionRequest model for the distribution to create
            publication: Publication href (for update operations)
            distribution_href: Full href of distribution (for update operations)

        Returns:
            Response object from the operation
        """
        if operation == "create_repo":
            if repository_data is not None:
                endpoint = f"api/v3/repositories/{repo_type}/{repo_type}/"
                return self._create_repository(endpoint, repository_data)
            else:
                raise ValueError("Repository data is required for 'create_repo' operations")
        if operation == "get_repo":
            if name is not None:
                endpoint = f"api/v3/repositories/{repo_type}/{repo_type}/"
                return self._get_single_resource(endpoint, name)
            else:
                raise ValueError("Name is required for 'get_repo' operations")
        if operation == "create_distro":
            if distribution_data is not None:
                endpoint = f"api/v3/distributions/{repo_type}/{repo_type}/"
                return self._create_distribution(endpoint, distribution_data)
            else:
                raise ValueError("Distribution data is required for 'create_distro' operations")
        if operation == "get_distro":
            if name is not None:
                endpoint = f"api/v3/distributions/{repo_type}/{repo_type}/"
                return self._get_single_resource(endpoint, name)
            else:
                raise ValueError("Name is required for 'get_distro' operations")
        if operation == "update_distro":
            if distribution_href is None:
                raise ValueError("Distribution href is required")
            url = str(self.config["base_url"]) + distribution_href
            data = {"publication": publication}
            return self.session.patch(url, json=data, timeout=self.timeout, **self.request_params)

        raise ValueError(f"Unknown operation: {operation}")


__all__ = ["RepositoryManagerMixin"]
