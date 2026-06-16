"""
Upload operations for re-uploading downloaded artifacts to Pulp.

This module handles uploading downloaded artifacts to destination Pulp repositories.
"""

import logging

import httpx

from ..api import PulpClient
from ..utils.error_handling import handle_generic_error
from ..models.context import PullContext
from ..models.results import PulpResultsModel
from ..models.repository import RepositoryRefs
from ..models.artifacts import PulledArtifacts
from ..utils import PulpHelper, determine_build_id, extract_metadata_from_artifacts
from ..utils.file_operations import (
    FileRepositoryBatch,
    FileUploadSpec,
    add_file_content_to_repository,
    upload_files_parallel,
)
from ..utils.rpm_operations import upload_rpms_parallel


def _upload_sboms_and_logs(
    pulp_client: PulpClient,
    pulled_artifacts: PulledArtifacts,
    repositories: RepositoryRefs,
    upload_info: PulpResultsModel,
) -> None:
    """Upload SBOM and log files using two-phase file uploads."""
    file_batch = FileRepositoryBatch()
    specs: list[FileUploadSpec] = []
    sbom_keys: set[str] = set()
    log_keys: set[str] = set()
    errors: list[str] = []

    if pulled_artifacts.sboms:
        sbom_items = list(pulled_artifacts.sboms.items())
        logging.warning("Uploading %d SBOM file(s)", len(sbom_items))
        for name, artifact in sbom_items:
            logging.warning("Uploading SBOM: %s", name)
            sbom_keys.add(name)
            specs.append(
                FileUploadSpec(
                    content_or_path=artifact.file,
                    labels=artifact.labels,
                    local_key=name,
                    build_id=artifact.labels.get("build_id", ""),
                    filename=name,
                    operation=f"upload SBOM {name}",
                )
            )

    if pulled_artifacts.logs:
        log_items = list(pulled_artifacts.logs.items())
        logging.warning("Uploading %d log file(s)", len(log_items))
        for name, artifact in log_items:
            arch = artifact.labels.get("arch", "unknown")
            logging.warning("Uploading log for %s: %s", arch, name)
            log_keys.add(name)
            specs.append(
                FileUploadSpec(
                    content_or_path=artifact.file,
                    labels=artifact.labels,
                    local_key=name,
                    build_id=artifact.labels.get("build_id", ""),
                    filename=name,
                    arch=arch,
                    operation=f"upload log {name}",
                )
            )

    sbom_count = 0
    log_count = 0
    for local_key, upload_result in upload_files_parallel(pulp_client, specs):
        try:
            if local_key in sbom_keys:
                file_batch.add_sbom(upload_result.content_href)
                sbom_count += 1
            elif local_key in log_keys:
                file_batch.add_log(upload_result.content_href)
                log_count += 1
        except Exception as e:
            errors.append(f"{local_key}: {e}")
            logging.error("Failed to record uploaded file %s: %s", local_key, e)

    try:
        if file_batch.sbom:
            add_file_content_to_repository(pulp_client, repositories.sbom_href, file_batch.sbom)
    except Exception as e:
        errors.append(f"SBOM repository modify: {e}")
        logging.error("Failed to add SBOMs to repository: %s", e)

    try:
        if file_batch.logs:
            add_file_content_to_repository(pulp_client, repositories.logs_href, file_batch.logs)
    except Exception as e:
        errors.append(f"logs repository modify: {e}")
        logging.error("Failed to add logs to repository: %s", e)

    upload_info.uploaded_counts.sboms = sbom_count
    upload_info.uploaded_counts.logs = log_count
    upload_info.upload_errors = upload_info.upload_errors + errors


def _upload_rpms_to_repository(
    pulp_client: PulpClient,
    pulled_artifacts: PulledArtifacts,
    repositories: RepositoryRefs,
    upload_info: PulpResultsModel,
) -> None:
    """Upload RPM files to the RPM repository."""
    if not pulled_artifacts.rpms:
        return

    rpm_infos = [
        (artifact_info.file, artifact_info.labels, artifact_info.arch or "noarch")
        for artifact_info in pulled_artifacts.rpms.values()
    ]

    logging.warning("Uploading %d RPM file(s)", len(rpm_infos))
    rpm_pairs = upload_rpms_parallel(pulp_client, rpm_infos)
    rpm_artifacts = [href for _path, href in rpm_pairs]

    if rpm_artifacts:
        logging.debug("Adding %d RPM artifacts to repository", len(rpm_artifacts))
        try:
            add_task = pulp_client.add_content(repositories.rpms_href, rpm_artifacts)
            pulp_client.wait_for_finished_task(add_task.pulp_href)
            upload_info.uploaded_counts.rpms = len(rpm_artifacts)
        except (httpx.HTTPError, ValueError, KeyError) as e:
            handle_generic_error(e, "add RPMs to repository")
            upload_info.add_error(f"RPM repository addition: {e}")


def upload_downloaded_files_to_pulp(
    pulp_client: PulpClient, pulled_artifacts: PulledArtifacts, args: PullContext
) -> PulpResultsModel:
    """Upload downloaded files to the appropriate Pulp repositories."""
    parent_package = extract_metadata_from_artifacts(pulled_artifacts, "parent_package")
    logging.debug("Extracted parent_package from artifacts: %s", parent_package)

    helper = PulpHelper(pulp_client, parent_package=parent_package)
    build_id = determine_build_id(args, pulled_artifacts=pulled_artifacts)  # type: ignore[arg-type]
    repositories = helper.setup_repositories(build_id)

    upload_info = PulpResultsModel(build_id=build_id, repositories=repositories)

    _upload_sboms_and_logs(pulp_client, pulled_artifacts, repositories, upload_info)
    _upload_rpms_to_repository(pulp_client, pulled_artifacts, repositories, upload_info)

    from .reporting import _log_upload_summary

    _log_upload_summary(upload_info)

    return upload_info


__all__ = ["upload_downloaded_files_to_pulp"]
