"""
PulpHelper class for managing Pulp repositories and distributions.

This module provides the PulpHelper class which encapsulates repository
and distribution management operations for Pulp.
"""

import asyncio
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

import httpx

from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel
from ..models.context import UploadContext

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient
from .validation import (
    strip_namespace_from_build_id,
    sanitize_build_id_for_repository,
    validate_build_id,
    validate_repository_setup,
)
from .uploads import upload_rpms_logs

# Constants used in this module
REPOSITORY_TYPES = ["rpms", "logs", "sbom", "artifacts"]
SUPPORTED_ARCHITECTURES = ["x86_64", "aarch64", "s390x", "ppc64le"]
REPOSITORY_SETUP_MAX_WORKERS = 4
ARCHITECTURE_THREAD_PREFIX = "process_architectures"


class PulpHelper:
    """
    Helper class for Pulp operations including repositories, distributions, and other functionality.

    This class provides high-level methods for managing Pulp operations,
    delegating API calls to the PulpClient instance.
    """

    def __init__(self, pulp_client: "PulpClient", parent_package: Optional[str] = None) -> None:
        """
        Initialize the helper with a PulpClient instance.

        Args:
            pulp_client: PulpClient instance for API interactions
            parent_package: Optional parent package name for distribution paths
        """
        self.client = pulp_client
        # Get namespace from the client (which reads it from config file)
        self.namespace = pulp_client.namespace
        self.parent_package = parent_package
        # Cache for distribution base paths: (build_id, repo_type) -> base_path
        # This avoids making additional API calls after creating distributions
        self._distribution_cache: Dict[Tuple[str, str], str] = {}

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
        # Validate build ID
        if not validate_build_id(build_id):
            raise ValueError(f"Invalid build ID: {build_id}")

        # Sanitize build ID for repository naming
        sanitized_build_id = sanitize_build_id_for_repository(build_id)
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

    def get_distribution_urls(self, build_id: str) -> Dict[str, str]:
        """
        Get distribution URLs for all repository types.

        This method orchestrates the retrieval of distribution URLs
        by delegating to the PulpClient API methods.

        Args:
            build_id: Build ID for naming repositories and distributions

        Returns:
            Dictionary mapping repo_type to distribution URL
        """
        # Validate build ID
        if not validate_build_id(build_id):
            raise ValueError(f"Invalid build ID: {build_id}")

        # Sanitize build ID for repository naming
        sanitized_build_id = sanitize_build_id_for_repository(build_id)
        if sanitized_build_id != build_id:
            logging.debug("Sanitized build ID '%s' to '%s' for repository naming", build_id, sanitized_build_id)

        logging.debug("Getting distribution URLs for build: %s", sanitized_build_id)

        # Get distribution URLs directly using the helper's own methods
        distribution_urls = self._get_distribution_urls_impl(sanitized_build_id)

        logging.debug("Retrieved %d distribution URLs", len(distribution_urls))
        return distribution_urls

    def create_or_get_repository(self, build_id: str, repo_type: str) -> Tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution of the specified type.

        This method orchestrates the creation/retrieval of repositories
        by delegating to the PulpClient API methods.

        Args:
            build_id: Build ID for naming repositories and distributions
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts')

        Returns:
            Tuple of (repository_prn, repository_href) where href is None for file repos
        """
        # Validate inputs
        if not validate_build_id(build_id):
            raise ValueError(f"Invalid build ID: {build_id}")

        if repo_type not in REPOSITORY_TYPES:
            raise ValueError(f"Invalid repository type: {repo_type}")

        # Sanitize build ID for repository naming
        sanitized_build_id = sanitize_build_id_for_repository(build_id)
        if sanitized_build_id != build_id:
            logging.debug("Sanitized build ID '%s' to '%s' for repository naming", build_id, sanitized_build_id)

        logging.debug("Creating or getting repository: %s/%s", sanitized_build_id, repo_type)

        # Create or get repository directly using the helper's own methods
        repository_prn, repository_href = self._create_or_get_repository_impl(sanitized_build_id, repo_type)

        logging.debug("Repository operation completed: %s/%s", sanitized_build_id, repo_type)
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
            "get": lambda name: self.client.repository_operation("get_repo", repo_type, name),
            "create": lambda name: self.client.repository_operation("create_repo", repo_type, name),
            "distro": lambda name, repository, basepath=None, publication=None: self.client.repository_operation(
                "create_distro", repo_type, name, repository=repository, basepath=basepath, publication=publication
            ),
            "get_distro": lambda name: self.client.repository_operation("get_distro", repo_type, name),
            "update_distro": lambda distribution_href, publication: self.client.repository_operation(
                "update_distro", repo_type, "", distribution_href=distribution_href, publication=publication
            ),
            "wait_for_finished_task": self.client.wait_for_finished_task,
        }

    def _get_single_distribution_url(self, build_id: str, repo_type: str, base_url: str) -> Optional[str]:
        """
        Get distribution URL for a single repository type.

        Checks cache first, then falls back to API query if needed.
        """
        # Check cache first - avoid API call if we already have the base_path
        cache_key = (build_id, repo_type)
        if cache_key in self._distribution_cache:
            base_path = self._distribution_cache[cache_key]
            # Include namespace in the full URL path for multi-tenant content serving
            distribution_url = f"{base_url}{self.namespace}/{base_path}/"
            logging.info(
                "Using cached distribution for %s: base_path=%s, url=%s", repo_type, base_path, distribution_url
            )
            return distribution_url

        # Cache miss - compute the expected base_path and use it
        # We compute the base_path rather than trusting what's in the API
        # because old distributions might have been created with incorrect paths
        # Strip namespace from build_id to avoid duplication in base_path
        build_name = strip_namespace_from_build_id(build_id)
        base_path = f"{build_name}/{repo_type}"
        # Include namespace in the full URL path for multi-tenant content serving
        distribution_url = f"{base_url}{self.namespace}/{base_path}/"

        # Cache for future use
        self._distribution_cache[cache_key] = base_path

        logging.info(
            "Using computed distribution URL for %s: base_path=%s, url=%s", repo_type, base_path, distribution_url
        )
        return distribution_url

    def _get_distribution_urls_impl(self, build_id: str) -> Dict[str, str]:
        """
        Get distribution URLs for all repository types.

        Args:
            build_id: Base name for the repositories (may include namespace prefix)

        Returns:
            Dictionary mapping repo_type to distribution URL
        """
        distribution_urls = {}
        # Get base_url from client's config
        base_url_str = str(self.client.config["base_url"])
        pulp_content_base_url = f"{base_url_str}/api/pulp-content"
        # Base URL from Pulp content service - distribution's base_path is build_id/repo_type
        base_url = f"{pulp_content_base_url}/"

        for repo_type in REPOSITORY_TYPES:
            url = self._get_single_distribution_url(build_id, repo_type, base_url)
            if url:
                distribution_urls[repo_type] = url

        return distribution_urls

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
        """Check if repository exists and return its details."""
        repository_response = methods["get"](full_name)
        self.client.check_response(repository_response, f"check {repo_type} repository")

        response_data = self._parse_repository_response(repository_response, repo_type, "check")

        results = response_data.get("results", [])
        if results:
            logging.warning("Found existing %s repository: %s", repo_type.capitalize(), full_name)
            result = results[0]
            return result["prn"], result.get("pulp_href")

        return None

    def _create_new_repository(
        self, methods: Dict[str, Any], full_name: str, repo_type: str
    ) -> Tuple[str, Optional[str]]:
        """Create a new repository and return its details."""
        logging.warning("Creating new %s repository: %s", repo_type.capitalize(), full_name)
        repository_response = methods["create"](full_name)
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
                raise ValueError(f"No {repo_type} repository found after creation: {full_name}")
            result = results[0]
            return result["prn"], result.get("pulp_href")
        else:
            raise ValueError(f"Unexpected response format for {repo_type} repository creation: {full_name}")

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
            # Run sync method in executor to avoid blocking
            prn, href = await loop.run_in_executor(None, self._create_or_get_repository_impl, build_id, repo_type)
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

    def _create_or_get_repository_impl(self, build_id: str, repo_type: str) -> Tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution of the specified type.

        Args:
            build_id: Base name for the repository (may include namespace prefix)
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts')

        Returns:
            Tuple of (repository_prn, repository_href) where href is None for file repos
        """
        # Strip namespace prefix from build_id for repository naming
        build_name = strip_namespace_from_build_id(build_id)
        full_name = f"{build_name}/{repo_type}"
        api_type = "rpm" if repo_type == "rpms" else "file"
        methods = self.get_repository_methods(api_type)

        # Check if repository already exists
        existing_repo = self._get_existing_repository(methods, full_name, repo_type)
        if existing_repo:
            repository_prn, repository_href = existing_repo
            is_new_repository = False
        else:
            repository_prn, repository_href = self._create_new_repository(methods, full_name, repo_type)
            is_new_repository = True

        # Create distribution (always create new distribution for new repositories)
        task_id = self._create_distribution_task(build_id, repo_type, repository_prn, methods, is_new_repository)

        # If distribution was created, wait for it to complete and cache the base_path
        if task_id:
            base_path = self._wait_for_distribution_task(methods, task_id, repo_type, build_id)
            if base_path:
                # Cache the base_path so we don't need to query it later
                self._distribution_cache[(build_id, repo_type)] = base_path
                logging.debug("Cached distribution base_path for %s/%s: %s", build_id, repo_type, base_path)

        return repository_prn, repository_href

    def _check_existing_distribution(self, methods: Dict[str, Any], full_name: str, repo_type: str) -> bool:
        """Check if distribution already exists by name."""
        try:
            logging.debug("Checking for existing %s distribution: %s", repo_type, full_name)
            distro_response = methods["get_distro"](full_name)
            logging.debug("Distribution check response status: %s", distro_response.status_code)

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

    def _create_distribution_task(
        self,
        build_id: str,
        repo_type: str,
        repository_prn: str,
        methods: Dict[str, Any],
        is_new_repository: bool = False,
    ) -> str:
        """Create a distribution for a repository and return the task ID.

        Args:
            build_id: Base name for the repository (may include namespace prefix)
            repo_type: Type of repository ('rpms', 'logs', 'sbom', 'artifacts')
            repository_prn: PRN of the repository to link to
            methods: Dictionary of repository methods
            is_new_repository: If True, always create distribution without checking existence

        Returns:
            Task ID if distribution was created, empty string if it already exists
        """
        # Strip namespace prefix from build_id for distribution naming
        build_name = strip_namespace_from_build_id(build_id)
        full_name = f"{build_name}/{repo_type}"

        # Distribution base_path must include: build_name/repo_type
        # This creates URLs like: /pulp-content/build_name/repo_type/
        # Example: /pulp-content/jreidy-tenant-libecpg-playground-on-pull-request-xxp48/artifacts/
        # Note: Use build_name (without namespace prefix) to avoid duplication
        basepath = f"{build_name}/{repo_type}"

        # For new repositories, always create a distribution without checking
        # For existing repositories, check if distribution already exists
        if not is_new_repository and self._check_existing_distribution(methods, full_name, repo_type):
            return ""  # Return empty string instead of None to match return type

        # Create distribution with namespace/build_id/repo_type basepath
        logging.warning(
            "Creating new %s distribution: %s with basepath: %s", repo_type.capitalize(), full_name, basepath
        )
        distro_response = methods["distro"](full_name, repository_prn, basepath=basepath)
        self.client.check_response(distro_response, f"create {repo_type} distribution")

        response_data = self._parse_repository_response(distro_response, repo_type, "distribution creation")

        # Cache the base_path for future URL construction
        cache_key = (build_id, repo_type)
        self._distribution_cache[cache_key] = basepath

        return response_data["task"]

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

    def process_architecture_uploads(
        self,
        client: "PulpClient",
        args: UploadContext,
        repositories: RepositoryRefs,
        *,
        date_str: str,
        rpm_href: str,
        results_model: PulpResultsModel,
    ) -> Dict[str, Any]:
        """
        Process uploads for all supported architectures.

        This function processes uploads for all supported architectures in parallel,
        handling RPM and log uploads for each architecture directory found.

        Args:
            client: PulpClient instance for API interactions
            args: Command line arguments
            repositories: Dictionary of repository identifiers
            date_str: Build date string
            rpm_href: RPM repository href for adding content
            results_model: PulpResultsModel to update with upload counts

        Returns:
            Dictionary mapping architecture names to their upload results:
                - {arch}: Dictionary containing uploaded_rpms and existing_rpm_artifacts
        """
        # Find architectures that exist
        existing_archs = []
        for arch in SUPPORTED_ARCHITECTURES:
            arch_path = os.path.join(args.rpm_path, arch)
            if os.path.exists(arch_path):
                existing_archs.append(arch)
            else:
                logging.debug("Skipping %s - path does not exist: %s", arch, arch_path)

        if not existing_archs:
            logging.warning("No architecture directories found in %s", args.rpm_path)
            return {}

        processed_archs = {}

        # Process architectures in parallel for better performance
        with ThreadPoolExecutor(
            thread_name_prefix=ARCHITECTURE_THREAD_PREFIX, max_workers=len(existing_archs)
        ) as executor:
            # Submit all architecture processing tasks
            # Call upload_rpms_logs directly without unnecessary wrapper
            future_to_arch = {
                executor.submit(
                    upload_rpms_logs,
                    os.path.join(args.rpm_path, arch),
                    args,
                    client,
                    arch,
                    rpm_repository_href=rpm_href,
                    file_repository_prn=repositories.logs_prn,
                    date=date_str,
                    results_model=results_model,
                ): arch
                for arch in existing_archs
            }

            # Collect results as they complete
            for future in as_completed(future_to_arch):
                arch = future_to_arch[future]
                try:
                    logging.debug("Processing architecture: %s", arch)
                    result = future.result()
                    processed_archs[arch] = {
                        "uploaded_rpms": result.uploaded_rpms,
                        "created_resources": result.created_resources,
                    }
                    logging.debug(
                        "Completed processing architecture: %s with %d created resources",
                        arch,
                        len(result.created_resources),
                    )
                except Exception as e:
                    logging.error("Failed to process architecture %s: %s", arch, e)
                    logging.error("Traceback: %s", traceback.format_exc())
                    raise

            logging.debug("Processed architectures: %s", ", ".join(processed_archs.keys()))
        return processed_archs

    def process_uploads(self, client: "PulpClient", args: UploadContext, repositories: RepositoryRefs) -> Optional[str]:
        """
        Process all upload operations.

        This function orchestrates the complete upload process including processing
        all architectures, uploading SBOM, and collecting results.

        Args:
            client: PulpClient instance for API interactions
            args: UploadContext with command line arguments (including date_str)
            repositories: RepositoryRefs containing all repository identifiers

        Returns:
            URL of the uploaded results JSON, or None if upload failed
        """
        # Import here to avoid circular import
        from ..upload import upload_sbom, collect_results

        # Ensure RPM repository href exists
        if not repositories.rpms_href:
            raise ValueError("RPM repository href is required but not found")

        # Get date_str from args
        date_str = args.date_str

        # Create unified results model at the start
        results_model = PulpResultsModel(build_id=args.build_id, repositories=repositories)

        # Process each architecture - now updates results_model internally
        processed_uploads = self.process_architecture_uploads(
            client, args, repositories, date_str=date_str, rpm_href=repositories.rpms_href, results_model=results_model
        )

        # Collect all created resources from add_content operations
        created_resources = []
        for upload in processed_uploads.values():
            created_resources.extend(upload.get("created_resources", []))

        # Upload SBOM and capture its created resources - updates results_model internally
        sbom_created_resources = upload_sbom(client, args, repositories.sbom_prn, date_str, results_model)
        created_resources.extend(sbom_created_resources)

        logging.info("Collected %d created resource hrefs from upload operations", len(created_resources))

        # Convert created_resources hrefs into artifact format for extra_artifacts
        extra_artifacts = [{"pulp_href": href} for href in created_resources]
        logging.info("Total artifacts to include in results: %d", len(extra_artifacts))

        # Collect and save results, passing the results_model and all artifacts
        results_json_url = collect_results(client, args, date_str, results_model, extra_artifacts)

        # Summary logging
        total_architectures = len(processed_uploads)
        logging.debug(
            "Upload process completed: %d architectures processed",
            total_architectures,
        )

        return results_json_url


__all__ = ["PulpHelper"]
