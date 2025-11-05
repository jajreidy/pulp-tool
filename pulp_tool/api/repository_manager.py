"""
Repository management operations for Pulp API.

This module handles creating and managing repositories and distributions.
"""

from typing import Any, Callable, Optional, Protocol, runtime_checkable

import httpx


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

    def _create_repository(self, endpoint: str, name: str) -> httpx.Response:
        """
        Create a repository.

        Args:
            endpoint: API endpoint for repository creation
            name: Name of the repository to create

        Returns:
            Response object from the repository creation request
        """
        url = self._url(endpoint)
        data = {"name": name, "autopublish": True}
        return self.session.post(url, json=data, timeout=self.timeout, **self.request_params)

    def _create_distribution(
        self,
        endpoint: str,
        name: str,
        repository: str,
        *,
        basepath: Optional[str] = None,
        publication: Optional[str] = None,
    ) -> httpx.Response:
        """
        Create a distribution.

        Args:
            endpoint: API endpoint for distribution creation
            name: Name of the distribution to create
            repository: Repository PRN or href to associate with the distribution
            basepath: Base path for the distribution (defaults to name)
            publication: Publication href to associate with the distribution (optional)

        Returns:
            Response object from the distribution creation request
        """
        url = self._url(endpoint)
        if publication:
            data = {
                "name": name,
                "base_path": basepath or name,
                "publication": publication,
            }
        else:
            data = {
                "name": name,
                "repository": repository,
                "base_path": basepath or name,
            }
        return self.session.post(url, json=data, timeout=self.timeout, **self.request_params)

    def repository_operation(
        self,
        operation: str,
        repo_type: str,
        name: str,
        *,
        repository: Optional[str] = None,
        basepath: Optional[str] = None,
        publication: Optional[str] = None,
        distribution_href: Optional[str] = None,
    ) -> httpx.Response:
        """
        Perform repository or distribution operations.

        Args:
            operation: Operation to perform ('create_repo', 'get_repo',
                      'create_distro', 'get_distro', 'update_distro')
            repo_type: Type of repository/distribution ('rpm' or 'file')
            name: Name of the repository/distribution
            repository: Repository PRN or href (for distribution operations)
            basepath: Base path for distribution (for distribution creation)
            publication: Publication href (for distribution operations)
            distribution_href: Full href of distribution (for update operations)

        Returns:
            Response object from the operation
        """
        if operation == "create_repo":
            endpoint = f"api/v3/repositories/{repo_type}/{repo_type}/"
            return self._create_repository(endpoint, name)
        if operation == "get_repo":
            endpoint = f"api/v3/repositories/{repo_type}/{repo_type}/"
            return self._get_single_resource(endpoint, name)
        if operation == "create_distro":
            endpoint = f"api/v3/distributions/{repo_type}/{repo_type}/"
            if repository is None:
                raise ValueError("Repository is required for distribution creation")
            return self._create_distribution(
                endpoint, name, repository, basepath=basepath, publication=publication  # type: ignore[arg-type]
            )
        if operation == "get_distro":
            endpoint = f"api/v3/distributions/{repo_type}/{repo_type}/"
            return self._get_single_resource(endpoint, name)
        if operation == "update_distro":
            if distribution_href is None:
                raise ValueError("Distribution href is required")
            url = str(self.config["base_url"]) + distribution_href
            data = {"publication": publication}
            return self.session.patch(url, json=data, timeout=self.timeout, **self.request_params)

        raise ValueError(f"Unknown operation: {operation}")


__all__ = ["RepositoryManagerMixin"]
