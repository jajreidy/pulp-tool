"""
PulpHelper class for managing Pulp repositories and distributions.

This module provides the PulpHelper class which acts as a facade,
delegating to specialized manager classes for repository, distribution,
and upload operations.
"""

from typing import Any, Optional, TYPE_CHECKING

from pulp_tool.models.pulp_api import DistributionRequest, RepositoryRequest

from ..models.context import UploadRpmContext, UploadFilesContext
from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

from .distribution_manager import DistributionManager
from .repository_manager import RepositoryManager
from .upload_orchestrator import UploadOrchestrator


class PulpHelper:
    """
    Helper class for Pulp operations including repositories, distributions, and other functionality.

    This class provides high-level methods for managing Pulp operations,
    delegating to specialized manager classes.
    """

    def __init__(self, pulp_client: "PulpClient", parent_package: Optional[str] = None) -> None:
        """
        Initialize the helper with a PulpClient instance.

        Args:
            pulp_client: PulpClient instance for API interactions
            parent_package: Optional parent package name for distribution paths
        """
        self.client = pulp_client
        self.namespace = pulp_client.namespace
        self.parent_package = parent_package

        # Initialize specialized managers
        self._repository_manager = RepositoryManager(pulp_client, parent_package)
        # Ensure namespace is a string (default to empty string if None)
        namespace_str = self.namespace if isinstance(self.namespace, str) else ""
        self._distribution_manager = DistributionManager(
            pulp_client, namespace_str, self._repository_manager.get_distribution_cache()
        )
        self._upload_orchestrator = UploadOrchestrator()

    def setup_repositories(
        self,
        build_id: str,
        signed_by: Optional[str] = None,
        skip_artifacts_repo: bool = False,
        target_arch_repo: bool = False,
    ) -> RepositoryRefs:
        """
        Setup all required repositories and return their identifiers.

        This method orchestrates the creation of all necessary repositories
        by delegating to the RepositoryManager.

        Args:
            build_id: Build ID for naming repositories and distributions
            signed_by: If set, also create signed repos (rpms-signed, etc.)
            skip_artifacts_repo: If True, do not create artifacts repo (e.g. when saving locally)
            target_arch_repo: If True, skip bulk rpms/rpms-signed; use ensure_rpm_repository_for_arch per arch

        Returns:
            RepositoryRefs NamedTuple containing all repository PRNs and hrefs
        """
        return self._repository_manager.setup_repositories(
            build_id,
            signed_by=signed_by,
            skip_artifacts_repo=skip_artifacts_repo,
            target_arch_repo=target_arch_repo,
        )

    def ensure_rpm_repository_for_arch(self, arch: str) -> str:
        """
        Create or get the RPM repository for an architecture (target-arch-repo mode).

        Returns:
            Repository pulp_href for modify/add_content
        """
        return self._repository_manager.ensure_rpm_repository_for_arch(arch)

    def get_distribution_urls(self, build_id: str, target_arch_repo: bool = False) -> dict[str, str]:
        """
        Get distribution URLs for all repository types.

        This method orchestrates the retrieval of distribution URLs
        by delegating to the DistributionManager.

        Args:
            build_id: Build ID for naming repositories and distributions

        Returns:
            Dictionary mapping repo_type to distribution URL
        """
        return self._distribution_manager.get_distribution_urls(build_id)

    def create_or_get_repository(
        self,
        build_id: Optional[str],
        repo_api_type: str,
        new_repository: Optional[RepositoryRequest] = None,
        new_distribution: Optional[DistributionRequest] = None,
    ) -> tuple[str, Optional[str]]:
        """
        Create or get a repository and distribution of the specified type.

        This method orchestrates the creation/retrieval of repositories
        by delegating to the RepositoryManager.

        Args:
            build_id: Build ID for naming repositories and distributions
            repo_api_type: Type of repository or API ('rpms', 'logs', 'sbom', 'artifacts', 'rpm','file')
            new_repository: RepositoryRequest model for the repository to create
            new_distribution: DistributionRequest model for the distribution to create

        Returns:
            Tuple of (repository_prn, repository_href) where href is None for file repos
        """

        return self._repository_manager.create_or_get_repository(
            build_id, repo_api_type, new_repository, new_distribution
        )

    def process_architecture_uploads(
        self,
        client: "PulpClient",
        args: UploadRpmContext,
        repositories: RepositoryRefs,
        *,
        date_str: str,
        rpm_href: str,
        results_model: PulpResultsModel,
    ) -> dict[str, Any]:
        """
        Process uploads for all supported architectures.

        Delegates to UploadOrchestrator.

        Args:
            client: PulpClient instance for API interactions
            args: Command line arguments
            repositories: Dictionary of repository identifiers
            date_str: Build date string
            rpm_href: RPM repository href for adding content
            results_model: PulpResultsModel to update with upload counts

        Returns:
            Dictionary mapping architecture names to their upload results
        """
        return self._upload_orchestrator.process_architecture_uploads(
            client,
            args,
            repositories,
            date_str=date_str,
            rpm_href=rpm_href,
            results_model=results_model,
            pulp_helper=self,
        )

    def process_uploads(
        self,
        client: "PulpClient",
        args: UploadRpmContext,
        repositories: RepositoryRefs,
        *,
        pulp_helper: Optional["PulpHelper"] = None,
    ) -> Optional[str]:
        """
        Process all upload operations.

        Delegates to UploadOrchestrator.

        Args:
            client: PulpClient instance for API interactions
            args: UploadRpmContext with command line arguments (including date_str)
            repositories: RepositoryRefs containing all repository identifiers
            pulp_helper: Helper instance for per-arch RPM repos when ``target_arch_repo`` is set

        Returns:
            URL of the uploaded results JSON, or None if upload failed
        """
        return self._upload_orchestrator.process_uploads(client, args, repositories, pulp_helper=pulp_helper or self)

    def process_file_uploads(
        self, client: "PulpClient", context: UploadFilesContext, repositories: RepositoryRefs
    ) -> Optional[str]:
        """
        Process upload of individual files to Pulp repositories.

        Delegates to UploadOrchestrator.

        Args:
            client: PulpClient instance for API interactions
            context: UploadFilesContext with file paths and metadata
            repositories: RepositoryRefs containing all repository identifiers

        Returns:
            URL of the uploaded results JSON, or None if upload failed
        """
        return self._upload_orchestrator.process_file_uploads(client, context, repositories)


__all__ = ["PulpHelper"]
