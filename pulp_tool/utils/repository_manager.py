"""
Repository management for Pulp operations.

This module handles repository creation, retrieval, and distribution management.
"""

import asyncio
import logging
import traceback
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

import httpx

from ..models.repository import RepositoryRefs
from ..models.pulp_api import (
    DistributionRequest,
    RepositoryRequest,
)

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

from .constants import (
    API_TYPES,
    REPOSITORY_TYPES,
)
from .validation import (
    strip_namespace_from_build_id,
    sanitize_build_id_for_repository,
    validate_build_id,
    validate_repository_setup,
)


class RepositoryManager:
    """
    Manages repository and distribution operations for Pulp.

    This class handles creating, retrieving, and managing repositories
    and their distributions.
    """

    def __init__(self, pulp_client: "PulpClient", parent_package: Optional[str] = None) -> None:
        """
        Initialize the repository manager.

        Args:
            pulp_client: PulpClient instance for API interactions
            parent_package: Optional parent package name for distribution paths
        """
        self.client = pulp_client
        self.namespace = pulp_client.namespace
        self.parent_package = parent_package
        # Cache for distribution base paths: (build_id, repo_type) -> base_path
        self._distribution_cache: Dict[Tuple[str, str], str] = {}

    def _validate_full_name(self, full_name: str, build_name: str, repo_type: str) -> None:
        """
        Validate that full_name is not empty or whitespace-only.

        This method is extracted to allow patching in tests for coverage.

        Args:
            full_name: The full repository name to validate
            build_name: The build name (for error messages)
            repo_type: The repository type (for error messages)

        Raises:
            ValueError: If full_name is empty or whitespace-only
        """
        if not full_name or not full_name.strip():
            raise ValueError(f"Invalid full_name constructed: build_name={build_name}, repo_type={repo_type}")

    def setup_repositories(self, build_id: str) -> RepositoryRefs:
        """
        Setup all required repositories and return their identifiers.

        This method orchestrates the creation of all necessary repositories
        by delegating to the PulpClient API methods.

        Args:
            build_id: Build ID for naming repositories and distributions

        Returns:
            RepositoryRefs NamedTuple containing all repository PRNs and hrefs
        """
        # Check for empty/None build ID before sanitization
        if not build_id or not isinstance(build_id, str) or not build_id.strip():
            raise ValueError(f"Invalid build ID: {build_id}")

        # Sanitize build ID for repository naming first
        sanitized_build_id = sanitize_build_id_for_repository(build_id)

        # Validate sanitized build ID
        if not validate_build_id(sanitized_build_id):
            raise ValueError(f"Invalid build ID: {build_id} (sanitized: {sanitized_build_id})")
        if sanitized_build_id != build_id:
            logging.debug("Sanitized build ID '%s' to '%s' for repository naming", build_id, sanitized_build_id)

        logging.debug("Setting up repositories for build: %s", sanitized_build_id)

        # Create repositories directly using the helper's own methods
        repositories = self._setup_repositories_impl(sanitized_build_id)

        # Validate the setup
        is_valid, errors = validate_repository_setup(repositories)
        if not is_valid:
            raise RuntimeError(f"Repository setup validation failed: {', '.join(errors)}")

        logging.debug("Repository setup completed successfully")

        # Convert dictionary to NamedTuple for type safety
        return RepositoryRefs(
            rpms_href=repositories.get("rpms_href", ""),
            rpms_prn=repositories.get("rpms_prn", ""),
            logs_href=repositories.get("logs_href", ""),
            logs_prn=repositories.get("logs_prn", ""),
            sbom_href=repositories.get("sbom_href", ""),
            sbom_prn=repositories.get("sbom_prn", ""),
            artifacts_href=repositories.get("artifacts_href", ""),
            artifacts_prn=repositories.get("artifacts_prn", ""),
        )

    def create_or_get_repository(
        self,
        build_id: Optional[str],
        repo_api_type: str,
        new_repository: Optional[RepositoryRequest] = None,
        new_distribution: Optional[DistributionRequest] = None,
    ) -> Tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution of the specified type.

        This method orchestrates the creation/retrieval of repositories
        by delegating to the PulpClient API methods.

        Args:
            build_id: Build ID for naming repositories and distributions
            repo_api_type: Type of repository or API ('rpms', 'logs', 'sbom', 'artifacts', 'rpm','file')
            new_repository: RepositoryRequest model for the repository to create
            new_distribution: DistributionRequest model for the distribution to create

        Returns:
            Tuple of (repository_prn, repository_href) where href is None for file repos
        """
        # Validate repository type
        if repo_api_type not in REPOSITORY_TYPES + API_TYPES:
            raise ValueError(f"Invalid repository or API type: {repo_api_type}")

        if new_repository is not None and new_distribution is not None:
            logging.debug("Creating or getting defined repository: %s", new_repository.name)
            # Create or get repository directly using the helper's own methods
            repository_prn, repository_href = self._create_or_get_repository_impl(
                new_repository, new_distribution, repo_api_type
            )
            logging.debug("Repository operation completed: %s", new_repository.name)

        else:

            # Check for empty/None build ID before sanitization
            if not build_id or not isinstance(build_id, str) or not build_id.strip():
                raise ValueError(f"Invalid build ID: {build_id}")

            # Sanitize build ID for repository naming first
            sanitized_build_id = sanitize_build_id_for_repository(build_id)

            # Validate sanitized build ID
            if not validate_build_id(sanitized_build_id):
                raise ValueError(f"Invalid build ID: {build_id} (sanitized: {sanitized_build_id})")
            if sanitized_build_id != build_id:
                logging.debug("Sanitized build ID '%s' to '%s' for repository naming", build_id, sanitized_build_id)

            logging.debug("Creating or getting repository: %s/%s", sanitized_build_id, repo_api_type)

            build_name = strip_namespace_from_build_id(build_id)
            if not build_name or not build_name.strip():
                raise ValueError(f"Empty build_name after stripping namespace from build_id: {build_id}")
            full_name = f"{build_name}/{repo_api_type}"
            self._validate_full_name(full_name, build_name, repo_api_type)
            new_repo = RepositoryRequest(name=full_name, autopublish=True)
            new_distro = DistributionRequest(name=full_name, base_path=full_name)
            # Validate that base_path was set correctly
            if not new_distro.base_path or not new_distro.base_path.strip():
                raise ValueError(
                    f"DistributionRequest base_path is empty after creation: "
                    f"name={full_name}, base_path={new_distro.base_path}"
                )

            # Create or get repository directly using the helper's own methods
            repository_prn, repository_href = self._create_or_get_repository_impl(
                new_repo, new_distro, repo_api_type, build_id=build_id
            )

            logging.debug("Repository operation completed: %s/%s", sanitized_build_id, repo_api_type)

        return repository_prn, repository_href

    def get_repository_methods(self, repo_type: str) -> Dict[str, Any]:
        """
        Get the appropriate client methods for the repository type.

        Args:
            repo_type: Type of repository ('rpm' or 'file')

        Returns:
            Dictionary mapping method names to their implementations
        """
        return {
            "get": lambda name: self.client.repository_operation("get_repo", repo_type, name=name),
            "create": lambda new_repository: self.client.repository_operation(
                "create_repo", repo_type, repository_data=new_repository
            ),
            "distro": lambda new_distribution: self.client.repository_operation(
                "create_distro", repo_type, distribution_data=new_distribution
            ),
            "get_distro": lambda name: self.client.repository_operation("get_distro", repo_type, name=name),
            "update_distro": lambda distribution_href, publication: self.client.repository_operation(
                "update_distro", repo_type, distribution_href=distribution_href, publication=publication
            ),
            "wait_for_finished_task": self.client.wait_for_finished_task,
        }

    def _parse_repository_response(self, response: httpx.Response, repo_type: str, operation: str) -> Dict[str, Any]:
        """Parse repository response JSON with error handling."""
        try:
            return response.json()
        except ValueError as e:
            logging.error("Failed to parse JSON response for %s repository %s: %s", repo_type, operation, e)
            logging.error("Response content: %s", response.text[:500])
            logging.error("Traceback: %s", traceback.format_exc())
            raise ValueError(f"Invalid JSON response from Pulp API: {e}") from e

    def _get_existing_repository(
        self, methods: Dict[str, Any], full_name: str, repo_type: str
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Check if repository exists and return its details.

        Returns None if repository doesn't exist (404), allowing the caller to create it.
        """
        repository_response = methods["get"](full_name)

        # Handle 404 gracefully - repository doesn't exist, return None to allow creation
        if repository_response.status_code == 404:
            logging.debug("Repository %s not found (404), will create it", full_name)
            return None

        # For other errors, check_response will raise an exception
        self.client.check_response(repository_response, f"check {repo_type} repository")

        response_data = self._parse_repository_response(repository_response, repo_type, "check")

        results = response_data.get("results", [])
        if results:
            logging.warning("Found existing %s repository: %s", repo_type.capitalize(), full_name)
            result = results[0]
            return result["prn"], result.get("pulp_href")

        return None

    def _create_new_repository(
        self, methods: Dict[str, Any], new_repository: RepositoryRequest, repo_type: str
    ) -> Tuple[str, Optional[str]]:
        """Create a new repository and return its details."""
        logging.warning("Creating new %s repository: %s", repo_type.capitalize(), new_repository.name)
        repository_response = methods["create"](new_repository)
        self.client.check_response(repository_response, f"create {repo_type} repository")

        # The create response contains the repository details directly
        response_data = self._parse_repository_response(repository_response, repo_type, "create")

        # Create returns the object directly, not wrapped in results
        if "prn" in response_data:
            # Direct repository object
            return response_data["prn"], response_data.get("pulp_href")
        elif "results" in response_data:
            # Wrapped in results (fallback)
            results = response_data["results"]
            if not results:
                raise ValueError(f"No {repo_type} repository found after creation: {new_repository.name}")
            result = results[0]
            return result["prn"], result.get("pulp_href")
        else:
            raise ValueError(f"Unexpected response format for {repo_type} repository creation: {new_repository.name}")

    def _wait_for_distribution_task(
        self, methods: Dict[str, Any], task_id: str, repo_type: str, build_id: str
    ) -> Optional[str]:
        """
        Wait for distribution creation task to complete and return the base_path.

        Returns:
            The base_path of the created distribution, or None if not found
        """
        task_response = methods["wait_for_finished_task"](task_id)

        # task_response is now a TaskResponse model
        if not task_response.is_successful:
            error_msg = (
                task_response.error.get("description", "Unknown error") if task_response.error else "Unknown error"
            )
            logging.error("Task failed for %s distribution (build_id=%s): %s", repo_type, build_id, error_msg)
            raise ValueError(f"Distribution creation task failed: {error_msg}")

        # Extract the distribution base_path from created resources
        base_path = None
        if task_response.created_resources:
            logging.debug("Distribution creation completed. Created resources:")
            for resource_href in task_response.created_resources:
                logging.debug("  - %s", resource_href)
                # Fetch the distribution details to get the base_path
                try:
                    # Use session.get to make the API request
                    distro_response = self.client.session.get(
                        resource_href, timeout=self.client.timeout, **self.client.request_params
                    )
                    if distro_response.is_success:
                        distro_data = distro_response.json()
                        base_path = distro_data.get("base_path")
                        if base_path:
                            logging.info(
                                "Retrieved base_path from distribution task: %s (build_id=%s, repo_type=%s)",
                                base_path,
                                build_id,
                                repo_type,
                            )
                            break
                except (httpx.HTTPError, ValueError, KeyError) as e:
                    logging.warning("Could not fetch distribution details from %s: %s", resource_href, e)
        else:
            logging.debug("Distribution creation completed for %s %s", repo_type, build_id)

        return base_path

    async def _setup_repositories_impl_async(self, build_id: str) -> Dict[str, str]:
        """
        Async version: Setup all required repositories using asyncio.gather for concurrency.

        This method creates or retrieves all necessary repositories (rpms, logs, sbom, artifacts)
        and their distributions concurrently using async/await for better performance.

        Args:
            build_id: Base name for the repositories

        Returns:
            Dictionary mapping repository types to their PRNs and hrefs
        """
        logging.debug("Setting up repositories async for: %s", build_id)

        repo_types = REPOSITORY_TYPES

        # Use asyncio.gather to run all repository setups concurrently
        # Each operation runs in the event loop without blocking
        async def create_repo(repo_type: str) -> Tuple[str, Tuple[str, Optional[str]]]:
            """Helper to create repository and return type with result."""
            loop = asyncio.get_event_loop()
            build_name = strip_namespace_from_build_id(build_id)
            if not build_name or not build_name.strip():
                raise ValueError(f"Empty build_name after stripping namespace from build_id: {build_id}")
            full_name = f"{build_name}/{repo_type}"
            self._validate_full_name(full_name, build_name, repo_type)
            new_repository = RepositoryRequest(name=full_name, autopublish=True)
            new_distribution = DistributionRequest(name=full_name, base_path=full_name)
            # Validate that base_path was set correctly
            if not new_distribution.base_path or not new_distribution.base_path.strip():
                raise ValueError(
                    f"DistributionRequest base_path is empty after creation: "
                    f"name={full_name}, base_path={new_distribution.base_path}"
                )
            # Run sync method in executor to avoid blocking
            prn, href = await loop.run_in_executor(
                None, self._create_or_get_repository_impl, new_repository, new_distribution, repo_type
            )
            return repo_type, (prn, href)

        try:
            # Gather all repository creation tasks concurrently
            results = await asyncio.gather(*[create_repo(rt) for rt in repo_types])

            # Build result dictionary
            repositories = {}
            for repo_type, (prn, href) in results:
                repositories[f"{repo_type}_prn"] = prn
                if href:  # RPM repositories have href, file repositories don't
                    repositories[f"{repo_type}_href"] = href
                logging.debug("Completed setup for %s repository", repo_type)

            return repositories

        except httpx.HTTPError as e:
            # HTTP errors are already formatted nicely, just re-raise
            error_msg = str(e)
            if "403" in error_msg:
                logging.error(
                    "Authentication failed: You don't have permission to access this Pulp instance. "
                    "Please check your credentials in the Pulp config file."
                )
            elif "401" in error_msg:
                logging.error(
                    "Authentication failed: Invalid credentials. "
                    "Please check your OAuth2 settings in the Pulp config file."
                )
            logging.debug("Failed to setup repositories: %s", error_msg)
            logging.debug("Traceback: %s", traceback.format_exc())
            raise
        except Exception as e:
            logging.error("Failed to setup repositories: %s", e)
            logging.debug("Traceback: %s", traceback.format_exc())
            raise

    def _create_or_get_repository_impl(
        self,
        new_repository: RepositoryRequest,
        new_distribution: DistributionRequest,
        repo_type: str,
        build_id: Optional[str] = None,
    ) -> Tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution of the specified type.

        Args:
            new_repository: RepositoryRequest model for the repository to create
            new_distribution: DistributionRequest model for the distribution to create
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts', 'rpm', 'file')
            build_id: Base name for the repository (may include namespace prefix)

        Returns:
            Tuple of (repository_prn, repository_href) where href is None for file repos
        """
        if repo_type not in API_TYPES:
            api_type = "rpm" if repo_type == "rpms" else "file"
            methods = self.get_repository_methods(api_type)
        else:
            methods = self.get_repository_methods(repo_type)

        # Check if repository already exists
        existing_repo = self._get_existing_repository(methods, new_repository.name, repo_type)
        if existing_repo:
            repository_prn, repository_href = existing_repo
            is_new_repository = False
        else:
            repository_prn, repository_href = self._create_new_repository(methods, new_repository, repo_type)
            is_new_repository = True

        # Create distribution (always create new distribution for new repositories)
        new_distribution.repository = repository_prn
        # Validate base_path is still valid after setting repository
        if not new_distribution.base_path or not new_distribution.base_path.strip():
            raise ValueError(
                f"DistributionRequest base_path is empty before creating {repo_type} distribution: "
                f"name={new_distribution.name}, repository={repository_prn}"
            )
        task_id = self._create_distribution_task(
            methods, new_distribution, repo_type, is_new_repository, build_id=build_id
        )

        # If distribution was created, wait for it to complete and cache the base_path
        if task_id:
            base_path = self._wait_for_distribution_task(methods, task_id, repo_type, build_id or new_repository.name)
            if build_id and base_path:
                # Cache the base_path so we don't need to query it later
                self._distribution_cache[(build_id, repo_type)] = base_path
                logging.debug("Cached distribution base_path for %s/%s: %s", build_id, repo_type, base_path)

        return repository_prn, repository_href

    def _check_existing_distribution(self, methods: Dict[str, Any], full_name: str, repo_type: str) -> bool:
        """Check if distribution already exists by name.

        Returns False if distribution doesn't exist (404), allowing the caller to create it.
        """
        try:
            logging.debug("Checking for existing %s distribution: %s", repo_type, full_name)
            distro_response = methods["get_distro"](full_name)
            logging.debug("Distribution check response status: %s", distro_response.status_code)

            # Handle 404 gracefully - distribution doesn't exist, return False to allow creation
            if distro_response.status_code == 404:
                logging.debug("Distribution %s not found (404), will create it", full_name)
                return False

            # For other errors, check_response will raise an exception
            self.client.check_response(distro_response, f"check {repo_type} distribution")

            response_data = self._parse_repository_response(distro_response, repo_type, "distribution check")
            logging.debug("Distribution check response data: %s", response_data)

            if response_data.get("results"):
                logging.warning("Found existing %s distribution: %s", repo_type.capitalize(), full_name)
                return True

            logging.debug("No existing %s distribution found for: %s", repo_type, full_name)
            return False
        except AttributeError:
            logging.debug("Distribution check method not available for %s, will create", repo_type)
            return False  # Create distribution if check method doesn't exist
        except (httpx.HTTPError, ValueError, KeyError) as e:
            logging.warning("Error checking for existing distribution: %s", e)
            logging.error("Traceback: %s", traceback.format_exc())
            return False  # Continue with creation if check fails

    def _new_distribution_task(self, methods: Dict[str, Any], new_distribution: DistributionRequest, repo_type: str):
        """Create a distribution for a repository and return the task ID.

         Args:
            methods: Dictionary of repository methods
            new_distribution: DistributionRequest model for the distribution to create
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts', 'rpm', 'file')

        Returns:
            Task ID for the new distribution
        """
        # Logging is handled by the caller (_create_distribution_task) to avoid duplicate messages
        distro_response = methods["distro"](new_distribution)
        self.client.check_response(distro_response, f"create {repo_type} distribution")

        response_data = self._parse_repository_response(distro_response, repo_type, "distribution creation")

        return response_data["task"]

    def _create_distribution_task(
        self,
        methods: Dict[str, Any],
        new_distribution: DistributionRequest,
        repo_type: str,
        is_new_repository: bool = False,
        build_id: Optional[str] = None,
    ) -> str:
        """Create a distribution for a repository and return the task ID.

        Args:
            methods: Dictionary of repository methods
            new_distribution: DistributionRequest model for the distribution to create
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts', 'rpm', 'file')
            is_new_repository: If True, always create distribution without checking existence
            build_id: Base name for the repository (may include namespace prefix)

        Returns:
            Task ID if distribution was created, empty string if it already exists
        """
        # For new repositories, always create a distribution without checking
        # For existing repositories, check if distribution already exists
        if not is_new_repository and self._check_existing_distribution(methods, new_distribution.name, repo_type):
            return ""  # Return empty string instead of None to match return type

        # Validate base_path before creating distribution
        base_path_value = getattr(new_distribution, "base_path", None)
        if not base_path_value or not str(base_path_value).strip():
            error_msg = (
                f"Invalid base_path for {repo_type} distribution: "
                f"name={new_distribution.name}, base_path={base_path_value}, "
                f"type={type(base_path_value)}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)
        # Create distribution with namespace/build_id/repo_type basepath
        logging.warning(
            "Creating new %s distribution: %s with basepath: %s",
            repo_type.capitalize(),
            new_distribution.name,
            base_path_value,
        )
        distro_task = self._new_distribution_task(methods, new_distribution, repo_type)

        # Cache the base_path for future URL construction
        if build_id:
            cache_key = (build_id, repo_type)
            self._distribution_cache[cache_key] = new_distribution.base_path

        return distro_task

    def _setup_repositories_impl(self, build_id: str) -> Dict[str, str]:
        """
        Setup all required repositories and return their identifiers.

        This method creates or retrieves all necessary repositories (rpms, logs, sbom, artifacts)
        and their distributions concurrently using async/await for better performance than threads.

        Args:
            build_id: Base name for the repositories

        Returns:
            Dictionary mapping repository types to their PRNs and hrefs:
                - {repo_type}_prn: Repository PRN for each type
                - {repo_type}_href: Repository href for RPM repositories (None for file repos)
        """
        # Run the async version and return results
        return asyncio.run(self._setup_repositories_impl_async(build_id))

    def get_distribution_cache(self) -> Dict[Tuple[str, str], str]:
        """Get the distribution cache for sharing with DistributionManager."""
        return self._distribution_cache


__all__ = ["RepositoryManager"]
