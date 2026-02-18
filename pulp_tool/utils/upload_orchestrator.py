"""
Upload workflow orchestration for Pulp operations.

This module handles orchestrating upload workflows including
architecture processing and result collection.
"""

import glob
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..models.context import UploadRpmContext, UploadFilesContext
from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel

from .constants import ARCHITECTURE_THREAD_PREFIX, ARCH_DETECT_WARNING_MSG, SUPPORTED_ARCHITECTURES
from .error_handling import handle_generic_error
from .uploads import upload_log, upload_rpms, upload_rpms_logs, create_labels, RPM_FILE_PATTERN
from .validation import validate_file_path
from .artifact_detection import detect_arch_from_filepath, group_rpm_paths_by_arch

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
        args: UploadRpmContext,
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
                handle_generic_error(e, f"process architecture {arch}")
                raise

        logging.debug("Processed architectures: %s", ", ".join(processed_archs.keys()))
        return processed_archs

    def process_architecture_uploads(
        self,
        client: "PulpClient",
        args: UploadRpmContext,
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
        # Ensure rpm_path is set (should be set by CLI, but check for safety)
        if not args.rpm_path:
            logging.warning("rpm_path is not set, cannot process architecture uploads")
            return {}

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

    def process_uploads(
        self, client: "PulpClient", args: UploadRpmContext, repositories: RepositoryRefs
    ) -> Optional[str]:
        """
        Process all upload operations.

        This function orchestrates the complete upload process including processing
        all architectures, uploading SBOM, and collecting results.

        Args:
            client: PulpClient instance for API interactions
            args: UploadRpmContext with command line arguments (including date_str)
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
        created_resources: List[str] = []
        for upload in processed_uploads.values():
            created_resources.extend(upload.get("created_resources", []))

        # Always search the base rpm_path for root-level RPMs (e.g. .src.rpm, .noarch.rpm).
        # OCI/oras layouts often put these in the root while logs live in arch subdirs (e.g. aarch64/).
        if args.rpm_path:
            rpm_glob_path = os.path.join(args.rpm_path, RPM_FILE_PATTERN)
            root_rpm_files = [p for p in glob.glob(rpm_glob_path) if os.path.isfile(p)]
            if root_rpm_files:
                logging.info(
                    "Found %d RPM(s) in base path %s (root-level), uploading by detected architecture",
                    len(root_rpm_files),
                    args.rpm_path,
                )
                rpms_by_arch = group_rpm_paths_by_arch(root_rpm_files)
                for arch, rpm_list in rpms_by_arch.items():
                    logging.info("Uploading %d root-level RPM(s) for architecture %s", len(rpm_list), arch)
                    created_resources.extend(
                        upload_rpms(
                            rpm_list,
                            args,
                            client,
                            arch,
                            rpm_repository_href=repositories.rpms_href,
                            date=date_str,
                            results_model=results_model,
                        )
                    )

        # Upload SBOM and capture its created resources - updates results_model internally
        # Only upload SBOM if sbom_path is provided
        if args.sbom_path:
            sbom_created_resources = upload_sbom(
                client, args, repositories.sbom_prn, date_str, results_model, args.sbom_path
            )
            created_resources.extend(sbom_created_resources)
        else:
            logging.debug("Skipping SBOM upload - no sbom_path provided")

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

    def process_file_uploads(
        self, client: "PulpClient", context: UploadFilesContext, repositories: RepositoryRefs
    ) -> Optional[str]:
        """
        Process upload of individual files to Pulp repositories.

        This function handles uploading RPMs, generic files, logs, and SBOMs
        from individual file paths specified in the context.

        Args:
            client: PulpClient instance for API interactions
            context: UploadFilesContext with file paths and metadata
            repositories: RepositoryRefs containing all repository identifiers

        Returns:
            URL of the uploaded results JSON, or None if upload failed
        """
        # Import here to avoid circular import
        from ..services.upload_service import upload_sbom, collect_results

        # Create unified results model
        results_model = PulpResultsModel(build_id=context.build_id, repositories=repositories)

        # Store created resources from add_content operations
        created_resources = []

        # Upload RPMs
        if context.rpm_files:
            logging.info("Uploading %d RPM file(s)", len(context.rpm_files))
            rpms_by_arch = group_rpm_paths_by_arch(context.rpm_files, explicit_arch=context.arch)

            # Upload RPMs for each architecture
            for arch, rpm_list in rpms_by_arch.items():
                arch_created_resources = upload_rpms(
                    rpm_list,
                    context,
                    client,
                    arch,
                    rpm_repository_href=repositories.rpms_href,
                    date=context.date_str,
                    results_model=results_model,
                )
                created_resources.extend(arch_created_resources)

        # Upload generic files
        if context.file_files:
            logging.info("Uploading %d generic file(s)", len(context.file_files))
            for file_path in context.file_files:
                logging.debug("Uploading file: %s", os.path.basename(file_path))
                labels = create_labels(
                    context.build_id, "", context.namespace, context.parent_package, context.date_str
                )
                validate_file_path(file_path, "File")

                content_upload_response = client.create_file_content(
                    repositories.artifacts_prn, file_path, build_id=context.build_id, pulp_label=labels
                )

                client.check_response(content_upload_response, f"upload file {file_path}")
                task_href = content_upload_response.json()["task"]
                task_response = client.wait_for_finished_task(task_href)
                if task_response.created_resources:
                    created_resources.extend(task_response.created_resources)
                results_model.uploaded_counts.files += 1

        # Upload logs
        if context.log_files:
            logging.info("Uploading %d log file(s)", len(context.log_files))
            for log_path in context.log_files:
                logging.debug("Uploading log: %s", os.path.basename(log_path))
                log_arch = context.arch or detect_arch_from_filepath(log_path)
                if not log_arch:
                    logging.warning(ARCH_DETECT_WARNING_MSG, os.path.basename(log_path))
                    continue

                labels = create_labels(
                    context.build_id, log_arch, context.namespace, context.parent_package, context.date_str
                )

                log_created_resources = upload_log(
                    client, repositories.logs_prn, log_path, build_id=context.build_id, labels=labels, arch=log_arch
                )
                created_resources.extend(log_created_resources)
                results_model.uploaded_counts.logs += 1

        # Upload SBOMs
        if context.sbom_files:
            logging.info("Uploading %d SBOM file(s)", len(context.sbom_files))
            for sbom_path in context.sbom_files:
                logging.debug("Uploading SBOM: %s", os.path.basename(sbom_path))
                sbom_created_resources = upload_sbom(
                    client, context, repositories.sbom_prn, context.date_str, results_model, sbom_path
                )
                created_resources.extend(sbom_created_resources)

        logging.info("Collected %d created resource hrefs from upload operations", len(created_resources))

        # Convert created_resources hrefs into artifact format for extra_artifacts
        extra_artifacts = [{"pulp_href": href} for href in created_resources]
        logging.info("Total artifacts to include in results: %d", len(extra_artifacts))

        # Collect and save results, passing the results_model and all artifacts
        results_json_url = collect_results(client, context, context.date_str, results_model, extra_artifacts)

        return results_json_url


__all__ = ["UploadOrchestrator"]
