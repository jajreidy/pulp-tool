"""
Upload utilities for Pulp operations.

This module provides utilities for uploading RPMs, logs, SBOM files,
and other artifacts to Pulp repositories.
"""

import glob
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import httpx

from ..models.results import RpmUploadResult, PulpResultsModel
from ..models.context import UploadContext
from .error_handling import handle_generic_error
from .validation import validate_file_path
from .constants import SUPPORTED_ARCHITECTURES
from .rpm_operations import upload_rpms_parallel
from .rpm_overwrite import remove_rpms_matching_local_files_from_repository
from .file_operations import FileRepositoryBatch, FileUploadSpec, upload_files_parallel
from .pulp_tasks import upload_file_content

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

# Constants used in this module
RPM_FILE_PATTERN = "*.rpm"
LOG_FILE_PATTERN = "*.log"


def create_labels(build_id: str, arch: str, namespace: str, parent_package: Optional[str], date: str) -> Dict[str, str]:
    """
    Create standard labels for Pulp content.

    Args:
        build_id: Unique build identifier
        arch: Architecture (e.g., 'x86_64', 'aarch64')
        namespace: Namespace for the content
        parent_package: Optional parent package name (will not be added to labels if None)
        date: Build date string

    Returns:
        Dictionary containing standard labels for Pulp content
    """
    labels = {
        "date": date,
        "build_id": build_id,
        "arch": arch,
        "namespace": namespace,
    }
    if parent_package:
        labels["parent_package"] = parent_package
    return labels


def _record_file_in_results_model(
    client: "PulpClient",
    *,
    results_model: Optional[PulpResultsModel],
    distribution_urls: Optional[Dict[str, str]],
    local_path: str,
    labels: Dict[str, str],
    relative_path: Optional[str],
    arch: str,
    target_arch_repo: bool,
) -> None:
    if results_model is None or distribution_urls is None:
        return
    rel_path = relative_path
    if not rel_path:
        fn = os.path.basename(local_path)
        rel_path = f"{arch}/{fn}" if arch else fn
    client.add_uploaded_artifact_to_results_model(
        results_model,
        local_path=local_path,
        labels=labels,
        is_rpm=False,
        distribution_urls=distribution_urls,
        target_arch_repo=target_arch_repo,
        file_relative_path=rel_path,
    )


def upload_log_phase1(
    client: "PulpClient",
    log_path: str,
    *,
    build_id: str,
    labels: Dict[str, str],
    arch: str,
    file_batch: FileRepositoryBatch,
    results_model: Optional[PulpResultsModel] = None,
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
) -> Optional[str]:
    """
    Phase 1: upload a log file without associating a repository.

    Returns:
        Content href when upload succeeds, otherwise None.
    """
    validate_file_path(log_path, "Log")

    upload_result = upload_file_content(
        client,
        log_path,
        build_id=build_id,
        pulp_label=labels,
        arch=arch,
        operation=f"upload log {log_path}",
    )
    file_batch.add_log(upload_result.content_href)
    _record_file_in_results_model(
        client,
        results_model=results_model,
        distribution_urls=distribution_urls,
        local_path=log_path,
        labels=labels,
        relative_path=upload_result.relative_path,
        arch=arch,
        target_arch_repo=target_arch_repo,
    )
    return upload_result.content_href


def upload_logs_parallel(
    client: "PulpClient",
    logs: List[str],
    *,
    build_id: str,
    labels: Dict[str, str],
    arch: str,
    file_batch: FileRepositoryBatch,
    results_model: Optional[PulpResultsModel] = None,
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
) -> int:
    """Upload log files in parallel (phase 1 only) and append hrefs to the batch."""
    if not logs:
        return 0

    logging.warning("Uploading %d log file(s) for %s", len(logs), arch)
    specs = [
        FileUploadSpec(
            content_or_path=log_path,
            labels=labels,
            local_key=log_path,
            build_id=build_id,
            arch=arch,
            operation=f"upload log {log_path}",
        )
        for log_path in logs
    ]
    for log_path in logs:
        logging.warning("Uploading log for %s: %s", arch, os.path.basename(log_path))

    uploaded = 0
    for local_key, upload_result in upload_files_parallel(client, specs):
        file_batch.add_log(upload_result.content_href)
        _record_file_in_results_model(
            client,
            results_model=results_model,
            distribution_urls=distribution_urls,
            local_path=local_key,
            labels=labels,
            relative_path=upload_result.relative_path,
            arch=arch,
            target_arch_repo=target_arch_repo,
        )
        uploaded += 1
    return uploaded


def upload_artifact_phase1(
    client: "PulpClient",
    file_path: str,
    *,
    build_id: str,
    labels: Dict[str, str],
    file_batch: FileRepositoryBatch,
    results_model: Optional[PulpResultsModel] = None,
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
) -> Optional[str]:
    """Phase 1: upload a generic artifact file without associating a repository."""
    validate_file_path(file_path, "File")
    arch = labels.get("arch") or ""
    upload_result = upload_file_content(
        client,
        file_path,
        build_id=build_id,
        pulp_label=labels,
        arch=arch or None,
        operation=f"upload file {file_path}",
    )
    file_batch.add_artifact(upload_result.content_href)
    _record_file_in_results_model(
        client,
        results_model=results_model,
        distribution_urls=distribution_urls,
        local_path=file_path,
        labels=labels,
        relative_path=upload_result.relative_path,
        arch=arch,
        target_arch_repo=target_arch_repo,
    )
    return upload_result.content_href


def upload_artifacts_to_repository(
    client: "PulpClient",
    artifacts: Dict[str, Any],
    file_batch: FileRepositoryBatch,
    file_type: str,
) -> Tuple[int, List[str]]:
    """
    Upload artifacts (phase 1 only) and append content hrefs to the artifacts batch.

    Returns:
        Tuple of (upload_count, error_list)
    """
    upload_count = 0
    errors = []

    for artifact_name, artifact_info in artifacts.items():
        try:
            logging.warning("Uploading %s: %s", file_type, artifact_name)

            if isinstance(artifact_info, dict):
                file_path = artifact_info["file"]
                labels = artifact_info["labels"]
            else:
                file_path = artifact_info.file
                labels = artifact_info.labels

            upload_result = upload_file_content(
                client,
                file_path,
                build_id=labels.get("build_id", "unknown"),
                pulp_label=labels,
                filename=os.path.basename(file_path),
                arch=labels.get("arch") or None,
                operation=f"upload {file_type} {artifact_name}",
            )
            file_batch.add_artifact(upload_result.content_href)
            upload_count += 1
            logging.debug("Successfully uploaded %s: %s", file_type, artifact_name)

        except (httpx.HTTPError, ValueError, FileNotFoundError, KeyError) as e:
            handle_generic_error(e, f"upload {file_type} {artifact_name}")
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
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
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

    When context has overwrite=True (UploadRpmContext), existing RPM package units in the
    target repository matching local RPM NVRA filenames (and signed_by when set) are removed before upload.

    Returns:
        List of created resource hrefs from the add_content operation
    """
    if not rpms:
        logging.debug("No new RPMs to upload for %s", arch)
        return []

    logging.warning("Uploading %d RPMs for %s", len(rpms), arch)
    labels = create_labels(context.build_id, arch, context.namespace, context.parent_package, date)
    signed_by_val = getattr(context, "signed_by", None)
    if signed_by_val and isinstance(signed_by_val, str) and signed_by_val.strip():
        labels["signed_by"] = signed_by_val.strip()

    if getattr(context, "overwrite", False) is True:
        sb_for_search = (
            signed_by_val.strip()
            if signed_by_val and isinstance(signed_by_val, str) and signed_by_val.strip()
            else None
        )
        remove_rpms_matching_local_files_from_repository(client, rpms, rpm_repository_href, sb_for_search)

    rpm_path_href_pairs = upload_rpms_parallel(client, rpms, labels, arch)
    rpm_results_artifacts = [href for _path, href in rpm_path_href_pairs]

    if distribution_urls is not None:
        for rpm_path, _href in rpm_path_href_pairs:
            client.add_uploaded_artifact_to_results_model(
                results_model,
                local_path=rpm_path,
                labels=labels,
                is_rpm=True,
                distribution_urls=distribution_urls,
                target_arch_repo=target_arch_repo,
            )

    results_model.uploaded_counts.rpms += len(rpms)
    created_resources: List[str] = []

    if rpm_results_artifacts:
        logging.debug("Adding %s RPM artifacts to repository", len(rpm_results_artifacts))
        rpm_repo_task = client.add_content(rpm_repository_href, rpm_results_artifacts)
        final_task = client.wait_for_finished_task(rpm_repo_task.pulp_href)
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
    file_batch: FileRepositoryBatch,
    date: str,
    results_model: PulpResultsModel,
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
) -> RpmUploadResult:
    """
    Upload RPMs and logs for a specific architecture.

    Logs are uploaded in parallel (phase 1) and batched into a shared FileRepositoryBatch.
    """
    rpms = glob.glob(os.path.join(rpm_path, RPM_FILE_PATTERN))
    logs = glob.glob(os.path.join(rpm_path, LOG_FILE_PATTERN))

    if not rpms and not logs:
        logging.debug("No RPMs or logs found in %s", rpm_path)
        return RpmUploadResult()

    if logs and not (results_model.repositories.logs_href or "").strip():
        raise ValueError(
            "Log files are present but logs repository href is empty. "
            "Create the logs repository when uploading logs (do not set skip_logs_repo)."
        )

    logging.warning("Processing %s: %d RPMs, %d logs", arch, len(rpms), len(logs))
    labels = create_labels(context.build_id, arch, context.namespace, context.parent_package, date)
    created_resources: List[str] = []

    if rpms:
        created_resources = upload_rpms(
            rpms,
            context,
            client,
            arch,
            rpm_repository_href=rpm_repository_href,
            date=date,
            results_model=results_model,
            distribution_urls=distribution_urls,
            target_arch_repo=target_arch_repo,
        )

    if logs:
        uploaded_count = upload_logs_parallel(
            client,
            logs,
            build_id=context.build_id,
            labels=labels,
            arch=arch,
            file_batch=file_batch,
            results_model=results_model,
            distribution_urls=distribution_urls,
            target_arch_repo=target_arch_repo,
        )
        results_model.uploaded_counts.logs += uploaded_count
    else:
        logging.debug("No logs to upload for %s", arch)

    return RpmUploadResult(
        uploaded_rpms=rpms,
        created_resources=created_resources,
    )


def rpm_directory_has_log_files(rpm_path: str) -> bool:
    """
    Return True if ``rpm_path`` contains any ``*.log`` under a supported arch subdirectory or at root.
    """
    if not rpm_path or not os.path.isdir(rpm_path):
        return False

    for arch in SUPPORTED_ARCHITECTURES:
        arch_dir = os.path.join(rpm_path, arch)
        if os.path.isdir(arch_dir) and glob.glob(os.path.join(arch_dir, LOG_FILE_PATTERN)):
            return True
    return bool(glob.glob(os.path.join(rpm_path, LOG_FILE_PATTERN)))


__all__ = [
    "create_labels",
    "upload_log_phase1",
    "upload_logs_parallel",
    "upload_artifact_phase1",
    "upload_artifacts_to_repository",
    "upload_rpms",
    "upload_rpms_logs",
    "rpm_directory_has_log_files",
]
