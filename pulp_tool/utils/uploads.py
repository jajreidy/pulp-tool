"""
Upload utilities for Pulp operations.

This module provides utilities for uploading RPMs, logs, SBOM files,
and other artifacts to Pulp repositories.
"""

import glob
import logging
import os
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import httpx

from ..models.results import RpmUploadResult, PulpResultsModel
from ..models.context import UploadContext
from .validation import validate_file_path
from .rpm_operations import upload_rpms_parallel

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

# Constants used in this module
RPM_FILE_PATTERN = "*.rpm"
LOG_FILE_PATTERN = "*.log"
DEFAULT_MAX_WORKERS = 4


def create_labels(build_id: str, arch: str, namespace: str, parent_package: str, date: str) -> Dict[str, str]:
    """
    Create standard labels for Pulp content.

    Args:
        build_id: Unique build identifier
        arch: Architecture (e.g., 'x86_64', 'aarch64')
        namespace: Namespace for the content
        parent_package: Parent package name
        date: Build date string

    Returns:
        Dictionary containing standard labels for Pulp content
    """
    labels = {
        "date": date,
        "build_id": build_id,
        "arch": arch,
        "namespace": namespace,
        "parent_package": parent_package,
    }
    return labels


def upload_log(
    client: "PulpClient",
    file_repository_prn: str,
    log_path: str,
    *,
    build_id: str,
    labels: Dict[str, str],
    arch: str,
) -> List[str]:
    """
    Upload a log file to the specified file repository.

    Args:
        client: PulpClient instance for API interactions
        file_repository_prn: File repository PRN for log uploads
        log_path: Path to the log file to upload
        build_id: Build identifier for the log
        labels: Labels to attach to the log content
        arch: Architecture for the log content

    Returns:
        List of created resource hrefs from the upload task
    """
    validate_file_path(log_path, "Log")

    content_upload_response = client.create_file_content(
        file_repository_prn, log_path, build_id=build_id, pulp_label=labels, arch=arch
    )

    client.check_response(content_upload_response, f"upload log {log_path}")
    task_href = content_upload_response.json()["task"]
    task_response = client.wait_for_finished_task(task_href)

    # Return the created resources from the task
    return task_response.created_resources if task_response.created_resources else []


def _upload_logs_sequential(
    client: "PulpClient",
    logs: List[str],
    *,
    file_repository_prn: str,
    build_id: str,
    labels: Dict[str, str],
    arch: str,
) -> None:
    """
    Upload logs sequentially.

    This function uploads log files one by one to avoid overwhelming the server
    with concurrent file uploads.

    Args:
        client: PulpClient instance for API interactions
        logs: List of log file paths to upload
        file_repository_prn: File repository PRN for log uploads
        build_id: Build identifier for the logs
        labels: Labels to attach to the uploaded content
        arch: Architecture for the uploaded logs
    """
    logging.info("Uploading %d log file(s) for %s", len(logs), arch)
    for log in logs:
        logging.warning("Uploading log: %s", os.path.basename(log))
        upload_log(client, file_repository_prn, log, build_id=build_id, labels=labels, arch=arch)


def upload_artifacts_to_repository(
    client: "PulpClient", artifacts: Dict[str, Any], repository_prn: str, file_type: str
) -> Tuple[int, List[str]]:
    """
    Upload artifacts to a specific repository.

    Args:
        client: PulpClient instance for API interactions
        artifacts: Dictionary of artifacts to upload (either Dict[str, Dict] or Dict[str, ArtifactFile])
        repository_prn: Repository PRN to upload to
        file_type: Type of file being uploaded (for logging)

    Returns:
        Tuple of (upload_count, error_list)
    """
    upload_count = 0
    errors = []

    for artifact_name, artifact_info in artifacts.items():
        try:
            logging.debug("Uploading %s: %s", file_type, artifact_name)

            # Support both dict and ArtifactFile objects
            if isinstance(artifact_info, dict):
                file_path = artifact_info["file"]
                labels = artifact_info["labels"]
            else:  # ArtifactFile model
                file_path = artifact_info.file
                labels = artifact_info.labels

            # Upload the file content
            content_response = client.create_file_content(
                repository_prn,
                file_path,
                build_id=labels.get("build_id", "unknown"),
                pulp_label=labels,
                filename=os.path.basename(file_path),
                arch=labels.get("arch", "unknown"),
            )

            # Check if response contains a task or if it's already complete
            response_data = content_response.json()
            if "task" in response_data:
                # Wait for upload to complete
                task_href = response_data["task"]
                client.wait_for_finished_task(task_href)
            else:
                # Response might be immediate success, log it
                logging.debug("File upload completed immediately: %s", artifact_name)
            upload_count += 1
            logging.debug("Successfully uploaded %s: %s", file_type, artifact_name)

        except (httpx.HTTPError, ValueError, FileNotFoundError, KeyError) as e:
            logging.error("Failed to upload %s %s: %s", file_type, artifact_name, e)
            logging.error("Traceback: %s", traceback.format_exc())
            errors.append(f"{file_type} {artifact_name}: {e}")

    return upload_count, errors


def upload_rpms(
    rpms: List[str],
    context: UploadContext,
    client: "PulpClient",
    arch: str,
    *,
    rpm_repository_href: str,
    date: str,
    results_model: PulpResultsModel,
) -> List[str]:
    """
    Upload RPMs for a specific architecture.

    This function handles uploading RPMs in parallel and adding them to the repository.

    Args:
        rpms: List of RPM file paths to upload
        context: Upload context containing build metadata
        client: PulpClient instance for API interactions
        arch: Architecture being processed
        rpm_repository_href: RPM repository href for adding content
        date: Build date string
        results_model: PulpResultsModel to update with upload counts

    Returns:
        List of created resource hrefs from the add_content operation
    """
    if not rpms:
        logging.debug("No new RPMs to upload for %s", arch)
        return []

    logging.info("Uploading %d RPMs for %s", len(rpms), arch)
    labels = create_labels(context.build_id, arch, context.namespace, context.parent_package, date)

    # Upload RPMs in parallel
    rpm_results_artifacts = upload_rpms_parallel(client, rpms, labels, arch)

    # Update upload counts
    results_model.uploaded_counts.rpms += len(rpms)

    # Store created resources from add_content operations
    created_resources = []

    # Add uploaded RPMs to the repository
    if rpm_results_artifacts:
        logging.debug("Adding %s RPM artifacts to repository", len(rpm_results_artifacts))
        rpm_repo_task = client.add_content(rpm_repository_href, rpm_results_artifacts)
        final_task = client.wait_for_finished_task(rpm_repo_task.pulp_href)
        # Capture created resources from the task
        if final_task.created_resources:
            created_resources.extend(final_task.created_resources)
            logging.debug("Captured %d created resources from RPM add_content", len(final_task.created_resources))

    return created_resources


def upload_rpms_logs(
    rpm_path: str,
    context: UploadContext,
    client: "PulpClient",
    arch: str,
    *,
    rpm_repository_href: str,
    file_repository_prn: str,
    date: str,
    results_model: PulpResultsModel,
) -> RpmUploadResult:
    """
    Upload RPMs and logs for a specific architecture.

    This function handles the complete upload process for a single architecture,
    including checking existing RPMs on Pulp, uploading new RPMs, and uploading logs.

    Args:
        rpm_path: Path to directory containing RPM and log files
        context: Upload context containing build metadata
        client: PulpClient instance for API interactions
        arch: Architecture being processed
        rpm_repository_href: RPM repository href for adding content
        file_repository_prn: File repository PRN for log uploads
        date: Build date string
        results_model: PulpResultsModel to update with upload counts

    Returns:
        RpmUploadResult containing uploaded RPMs, existing artifacts, and created resources
    """
    # Find RPM and log files
    rpms = glob.glob(os.path.join(rpm_path, RPM_FILE_PATTERN))
    logs = glob.glob(os.path.join(rpm_path, LOG_FILE_PATTERN))

    if not rpms and not logs:
        logging.debug("No RPMs or logs found in %s", rpm_path)
        return RpmUploadResult()

    logging.info("Processing %s: %d RPMs, %d logs", arch, len(rpms), len(logs))
    labels = create_labels(context.build_id, arch, context.namespace, context.parent_package, date)

    # Store created resources from add_content operations
    created_resources = []

    # Upload RPMs in parallel
    if rpms:
        created_resources = upload_rpms(
            rpms, context, client, arch, rpm_repository_href=rpm_repository_href, date=date, results_model=results_model
        )

    # Upload logs sequentially
    if logs:
        logging.info("Uploading %d logs for %s", len(logs), arch)
        _upload_logs_sequential(
            client, logs, file_repository_prn=file_repository_prn, build_id=context.build_id, labels=labels, arch=arch
        )
        # Update upload counts
        results_model.uploaded_counts.logs += len(logs)
    else:
        logging.debug("No logs to upload for %s", arch)

    return RpmUploadResult(
        uploaded_rpms=rpms,
        created_resources=created_resources,
    )


__all__ = [
    "create_labels",
    "upload_log",
    "upload_artifacts_to_repository",
    "upload_rpms",
    "upload_rpms_logs",
]
