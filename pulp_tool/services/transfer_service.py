"""
Transfer service for high-level transfer operations.

This module provides a service layer that orchestrates transfer operations,
abstracting the complexity of downloading and optionally re-uploading artifacts.
"""

import logging
from typing import Optional, TYPE_CHECKING

from ..models.artifacts import ArtifactData, PulledArtifacts
from ..models.context import TransferContext
from ..models.results import PulpResultsModel

if TYPE_CHECKING:
    from ..api import DistributionClient, PulpClient

from ..transfer import (
    download_artifacts_concurrently,
    generate_transfer_report,
    load_and_validate_artifacts,
    setup_repositories_if_needed,
    upload_downloaded_files_to_pulp,
)


class TransferService:
    """
    High-level service for transfer operations.

    This service provides a clean interface for downloading artifacts
    and optionally re-uploading them to destination repositories.
    """

    def __init__(self) -> None:
        """Initialize the transfer service."""

    def load_artifacts(
        self, context: TransferContext, distribution_client: Optional["DistributionClient"]
    ) -> ArtifactData:
        """
        Load and validate artifact metadata.

        Args:
            context: Transfer context with artifact location
            distribution_client: Optional DistributionClient for remote URLs

        Returns:
            ArtifactData containing validated artifact metadata

        Raises:
            SystemExit: If artifacts cannot be loaded or validated
        """
        logging.info("Loading artifact metadata from: %s", context.artifact_location)
        artifact_data = load_and_validate_artifacts(context, distribution_client)
        logging.info("Successfully loaded artifact metadata")
        return artifact_data

    def download_artifacts(
        self,
        artifact_data: ArtifactData,
        distribution_client: Optional["DistributionClient"],
        context: TransferContext,
        max_workers: int,
    ) -> tuple[PulledArtifacts, int, int]:
        """
        Download artifacts concurrently.

        Args:
            artifact_data: Artifact metadata
            distribution_client: DistributionClient for downloading (required)
            context: Transfer context with filters
            max_workers: Maximum number of concurrent workers

        Returns:
            Tuple of (pulled_artifacts, completed_count, failed_count)
        """
        logging.info("Starting artifact download with %d workers", max_workers)
        download_result = download_artifacts_concurrently(
            artifact_data.artifacts,
            artifact_data.artifact_json.distributions,  # type: ignore[attr-defined]
            distribution_client,
            max_workers,
            context.content_types,
            context.archs,
        )
        logging.info(
            "Download completed: %d succeeded, %d failed",
            download_result.completed,
            download_result.failed,
        )
        return download_result.pulled_artifacts, download_result.completed, download_result.failed

    def upload_artifacts(
        self,
        pulp_client: "PulpClient",
        pulled_artifacts: PulledArtifacts,
        context: TransferContext,
    ) -> Optional[PulpResultsModel]:
        """
        Upload downloaded artifacts to Pulp repositories.

        Args:
            pulp_client: PulpClient instance for destination repositories
            pulled_artifacts: Downloaded artifacts to upload
            context: Transfer context with configuration

        Returns:
            PulpResultsModel containing upload information, or None if upload skipped
        """
        logging.info("Uploading downloaded artifacts to Pulp repositories")
        upload_info = upload_downloaded_files_to_pulp(pulp_client, pulled_artifacts, context)
        logging.info("Upload completed: %d total artifacts uploaded", upload_info.total_uploaded)
        return upload_info

    def setup_destination_repositories(
        self, context: TransferContext, artifact_json: Optional[dict] = None
    ) -> Optional["PulpClient"]:
        """
        Set up destination repositories if configuration is provided.

        Args:
            context: Transfer context with configuration
            artifact_json: Optional artifact metadata

        Returns:
            PulpClient instance if repositories were set up, None otherwise
        """
        if not context.config:
            logging.debug("No Pulp configuration provided, skipping repository setup")
            return None

        logging.info("Setting up destination repositories")
        pulp_client = setup_repositories_if_needed(context, artifact_json)
        if pulp_client:
            logging.info("Destination repositories set up successfully")
        return pulp_client

    def generate_report(
        self,
        pulled_artifacts: PulledArtifacts,
        completed: int,
        failed: int,
        context: TransferContext,
        upload_info: Optional[PulpResultsModel] = None,
    ) -> None:
        """
        Generate and display transfer report.

        Args:
            pulled_artifacts: Downloaded artifacts
            completed: Number of successful downloads
            failed: Number of failed downloads
            context: Transfer context
            upload_info: Optional upload information
        """
        generate_transfer_report(pulled_artifacts, completed, failed, context, upload_info)


__all__ = ["TransferService"]
