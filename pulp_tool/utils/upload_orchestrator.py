"""
Upload workflow orchestration for Pulp operations.

This module handles orchestrating upload workflows including
architecture processing and result collection.
"""

import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..models.context import UploadContext
from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel

from .constants import ARCHITECTURE_THREAD_PREFIX, SUPPORTED_ARCHITECTURES
from .uploads import upload_rpms_logs

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient


class UploadOrchestrator:
    """
    Orchestrates upload workflows for Pulp operations.

    This class handles processing uploads for multiple architectures
    and coordinating the complete upload process.
    """

    def __init__(self) -> None:
        """Initialize the upload orchestrator."""

    def _find_existing_architectures(self, rpm_path: str) -> List[str]:
        """
        Find architectures that have existing directories.

        Args:
            rpm_path: Base path containing architecture subdirectories

        Returns:
            List of architecture names that have existing directories
        """
        existing_archs = []
        for arch in SUPPORTED_ARCHITECTURES:
            arch_path = os.path.join(rpm_path, arch)
            if os.path.exists(arch_path):
                existing_archs.append(arch)
            else:
                logging.debug("Skipping %s - path does not exist: %s", arch, arch_path)
        return existing_archs

    def _submit_architecture_tasks(
        self,
        executor: ThreadPoolExecutor,
        existing_archs: List[str],
        rpm_path: str,
        args: UploadContext,
        client: "PulpClient",
        rpm_href: str,
        logs_prn: str,
        date_str: str,
        results_model: PulpResultsModel,
    ) -> Dict[Any, str]:
        """
        Submit architecture upload tasks to the executor.

        Args:
            executor: ThreadPoolExecutor instance
            existing_archs: List of architecture names to process
            rpm_path: Base path containing architecture subdirectories
            args: Upload context with command arguments
            client: PulpClient instance for API interactions
            rpm_href: RPM repository href for adding content
            logs_prn: Logs repository PRN
            date_str: Build date string
            results_model: PulpResultsModel to update with upload counts

        Returns:
            Dictionary mapping futures to architecture names
        """
        future_to_arch = {}
        for arch in existing_archs:
            arch_path = os.path.join(rpm_path, arch)
            future = executor.submit(
                upload_rpms_logs,
                arch_path,
                args,
                client,
                arch,
                rpm_repository_href=rpm_href,
                file_repository_prn=logs_prn,
                date=date_str,
                results_model=results_model,
            )
            future_to_arch[future] = arch
        return future_to_arch

    def _collect_architecture_results(self, future_to_arch: Dict[Any, str]) -> Dict[str, Any]:
        """
        Collect results from architecture upload futures.

        Args:
            future_to_arch: Dictionary mapping futures to architecture names

        Returns:
            Dictionary mapping architecture names to their upload results

        Raises:
            Exception: If any architecture upload fails
        """
        processed_archs = {}
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
                - {arch}: Dictionary containing uploaded_rpms and created_resources
        """
        # Find architectures that exist
        existing_archs = self._find_existing_architectures(args.rpm_path)

        if not existing_archs:
            logging.warning("No architecture directories found in %s", args.rpm_path)
            return {}

        # Process architectures in parallel for better performance
        with ThreadPoolExecutor(
            thread_name_prefix=ARCHITECTURE_THREAD_PREFIX, max_workers=len(existing_archs)
        ) as executor:
            # Submit all architecture processing tasks
            future_to_arch = self._submit_architecture_tasks(
                executor,
                existing_archs,
                args.rpm_path,
                args,
                client,
                rpm_href,
                repositories.logs_prn,
                date_str,
                results_model,
            )

            # Collect results as they complete
            processed_archs = self._collect_architecture_results(future_to_arch)

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
        from ..services.upload_service import upload_sbom, collect_results

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


__all__ = ["UploadOrchestrator"]
