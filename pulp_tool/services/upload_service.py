"""
Upload service for high-level upload operations.

This module provides a service layer that orchestrates upload operations,
abstracting the complexity of coordinating between repositories, distributions,
and content uploads.

Key Functions:
    - upload_sbom(): Upload SBOM files to repository
    - collect_results(): Gather and upload results JSON
    - _handle_artifact_results(): Process Konflux integration results
    - _handle_sbom_results(): Process SBOM results for Konflux
"""

import json
import logging
import os
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ..models.pulp_api import TaskResponse
from ..models.context import UploadContext, UploadRpmContext
from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel
from ..models.artifacts import ContentData, ExtraArtifactRef, FileInfoMap, FileInfoModel, PulpContentRow

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

from ..utils import PulpHelper, validate_file_path, create_labels
from ..utils.constants import SBOM_EXTENSIONS, SUPPORTED_ARCHITECTURES

# ============================================================================
# Constants
# ============================================================================

RESULTS_JSON_FILENAME = "pulp_results.json"
MAX_LOG_LINE_LENGTH = 114


class UploadService:
    """
    High-level service for upload operations.

    This service provides a clean interface for upload operations,
    coordinating between repositories, distributions, and content uploads.
    """

    def __init__(self, pulp_client: "PulpClient", parent_package: Optional[str] = None) -> None:
        """
        Initialize the upload service.

        Args:
            pulp_client: PulpClient instance for API interactions
            parent_package: Optional parent package name for distribution paths
        """
        self.client = pulp_client
        self.helper = PulpHelper(pulp_client, parent_package=parent_package)

    def setup_repositories(self, build_id: str) -> RepositoryRefs:
        """
        Set up all required repositories for a build.

        Args:
            build_id: Build identifier

        Returns:
            RepositoryRefs containing all repository identifiers
        """
        logging.info("Setting up repositories for build: %s", build_id)
        repositories = self.helper.setup_repositories(build_id)
        logging.info("Repository setup completed")
        return repositories

    def upload_artifacts(self, context: UploadRpmContext, repositories: RepositoryRefs) -> Optional[str]:
        """
        Upload all artifacts (RPMs, logs, SBOMs) and collect results.

        This method orchestrates the complete upload process including:
        - Processing architecture-specific uploads
        - Uploading SBOM files
        - Collecting and uploading results JSON

        Args:
            context: UploadRpmContext with all required parameters
            repositories: Repository references for upload targets

        Returns:
            URL of the uploaded results JSON, or None if upload failed
        """
        logging.info("Starting upload process for build: %s", context.build_id)
        results_json_url = self.helper.process_uploads(self.client, context, repositories, pulp_helper=self.helper)

        if not results_json_url:
            logging.error("Upload completed but results JSON was not created")
            return None

        logging.info("Upload completed successfully. Results JSON URL: %s", results_json_url)
        return results_json_url

    def get_distribution_urls(self, build_id: str) -> dict[str, str]:
        """
        Get distribution URLs for all repository types.

        Args:
            build_id: Build identifier

        Returns:
            Dictionary mapping repository types to distribution URLs
        """
        return self.helper.get_distribution_urls(build_id)


# ============================================================================
# SBOM and Results Functions
# ============================================================================


def upload_sbom(
    client: "PulpClient",
    context: UploadContext,
    sbom_repository_prn: str,
    date: str,
    results_model: PulpResultsModel,
    sbom_path: str,
    *,
    distribution_urls: Optional[Dict[str, str]] = None,
    target_arch_repo: bool = False,
) -> List[str]:
    """
    Upload SBOM file to repository.

    Args:
        client: PulpClient instance for API interactions
        context: Upload context containing metadata
        sbom_repository_prn: SBOM repository PRN
        date: Build date string
        results_model: PulpResultsModel to update with upload counts
        sbom_path: Path to the SBOM file to upload

    Returns:
        List of created resource hrefs from the upload task
    """
    if not os.path.exists(sbom_path):
        logging.error("SBOM file not found: %s", sbom_path)
        return []

    logging.warning("Uploading SBOM: %s", sbom_path)
    labels = create_labels(context.build_id, "", context.namespace, context.parent_package, date)
    validate_file_path(sbom_path, "SBOM")

    content_upload_response = client.create_file_content(
        sbom_repository_prn, sbom_path, build_id=context.build_id, pulp_label=labels
    )

    client.check_response(content_upload_response, f"upload SBOM {sbom_path}")
    task_href = content_upload_response.json()["task"]
    task_response = client.wait_for_finished_task(task_href)
    logging.debug("SBOM uploaded successfully: %s", sbom_path)

    # Update upload counts
    results_model.uploaded_counts.sboms += 1

    if distribution_urls is not None:
        rel_path: Optional[str] = None
        if task_response.result and isinstance(task_response.result, dict):
            rel_path = task_response.result.get("relative_path")
        if not rel_path:
            rel_path = os.path.basename(sbom_path)
        client.add_uploaded_artifact_to_results_model(
            results_model,
            local_path=sbom_path,
            labels=labels,
            is_rpm=False,
            distribution_urls=distribution_urls,
            target_arch_repo=target_arch_repo,
            file_relative_path=rel_path,
        )

    # Return the created resources from the task
    return task_response.created_resources


def _distribution_urls_for_context(helper: PulpHelper, build_id: str, context: UploadContext) -> Dict[str, str]:
    """Resolve distribution URL map for results JSON (per-arch vs signed aggregate RPM base)."""
    return helper.get_distribution_urls_for_upload_context(build_id, context)


def _classify_artifact_from_key(key: str) -> str:
    """
    Classify artifact type from key (path/filename).

    Returns: "rpms", "logs", "sbom", or "artifacts"
    """
    key_lower = key.lower()
    if key_lower.endswith(".rpm"):
        return "rpms"
    if key_lower.endswith(".log"):
        return "logs"
    if "sbom" in key_lower:
        return "sbom"
    for ext in SBOM_EXTENSIONS:
        if key_lower.endswith(ext):
            return "sbom"
    return "artifacts"


def process_uploads_from_results_json(
    client: "PulpClient",
    context: UploadRpmContext,
    repositories: RepositoryRefs,
    *,
    pulp_helper: Optional[PulpHelper] = None,
) -> Optional[str]:
    """
    Upload artifacts from pulp_results.json.

    Reads artifact keys from the JSON, resolves file paths (base_path / key),
    classifies each artifact, and uploads to the appropriate repo.
    When signed_by is set, uses signed repos and adds signed_by pulp_label.

    Args:
        client: PulpClient instance
        context: UploadRpmContext with results_json, files_base_path, signed_by
        repositories: RepositoryRefs (with signed refs when signed_by set)
        pulp_helper: Optional PulpHelper for per-arch RPM repos when ``target_arch_repo`` is set

    Returns:
        URL of the uploaded results JSON, or None if upload failed
    """
    from ..utils.uploads import upload_rpms, upload_log

    helper = pulp_helper or PulpHelper(client, parent_package=context.parent_package)
    distribution_urls = helper.get_distribution_urls_for_upload_context(context.build_id, context)

    if not context.results_json:
        return None

    try:
        with open(context.results_json, encoding="utf-8") as f:
            results_data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logging.error("Failed to read results JSON %s: %s", context.results_json, e)
        raise

    artifacts = results_data.get("artifacts", {})
    if not artifacts:
        logging.info("No artifacts in results JSON, creating minimal results")
        results_model = PulpResultsModel(build_id=context.build_id, repositories=repositories)
        return collect_results(client, context, context.date_str, results_model, extra_artifacts=None)

    base_path = Path(context.files_base_path or os.path.dirname(context.results_json)).resolve()
    use_signed = bool(context.signed_by and context.signed_by.strip())

    # Only RPMs use signed aggregate repo; with target_arch_repo, signed_by is label-only on arch repos
    rpm_href = (
        "" if context.target_arch_repo else (repositories.rpms_signed_href if use_signed else repositories.rpms_href)
    )
    logs_prn = repositories.logs_prn
    sbom_prn = repositories.sbom_prn
    artifacts_prn = repositories.artifacts_prn

    if use_signed and not context.target_arch_repo and not rpm_href and not repositories.rpms_signed_prn:
        logging.error("signed_by set but signed repositories not available")
        raise ValueError("signed_by requires signed repositories")

    results_model = PulpResultsModel(build_id=context.build_id, repositories=repositories)
    created_resources: List[str] = []
    date_str = context.date_str

    # Group artifacts by type for batch processing
    rpms_by_arch: Dict[str, List[str]] = {}
    logs_to_upload: List[Tuple[str, str]] = []  # (path, arch)
    sboms_to_upload: List[str] = []
    artifacts_to_upload: List[Tuple[str, Dict[str, str]]] = []  # (path, labels)

    for key, info in artifacts.items():
        if not isinstance(info, dict):
            logging.warning("Skipping invalid artifact entry: %s", key)
            continue

        file_path = base_path / key
        if not file_path.exists():
            logging.warning("Skipping missing file: %s", file_path)
            continue

        labels = dict(info.get("labels") or {})
        labels.update(
            create_labels(
                context.build_id,
                labels.get("arch", ""),
                context.namespace,
                context.parent_package,
                date_str,
            )
        )
        art_type = _classify_artifact_from_key(key)
        arch = labels.get("arch") or ""

        if art_type == "rpms":
            if not arch:
                # Try to infer from path (e.g. x86_64/pkg.rpm)
                parts = key.replace("\\", "/").split("/")
                for p in parts:
                    if p in SUPPORTED_ARCHITECTURES:
                        arch = p
                        break
                if not arch:
                    arch = "noarch"
            rpms_by_arch.setdefault(arch, []).append(str(file_path))
        elif art_type == "logs":
            if not arch:
                parts = key.replace("\\", "/").split("/")
                for p in parts:
                    if p in SUPPORTED_ARCHITECTURES:
                        arch = p
                        break
                if not arch:
                    arch = "noarch"
            logs_to_upload.append((str(file_path), arch))
        elif art_type == "sbom":
            sboms_to_upload.append(str(file_path))
        else:
            artifacts_to_upload.append((str(file_path), labels))

    # Upload RPMs
    for arch, rpm_list in rpms_by_arch.items():
        arch_href = (
            helper.ensure_rpm_repository_for_arch(context.build_id, arch) if context.target_arch_repo else rpm_href
        )
        created_resources.extend(
            upload_rpms(
                rpm_list,
                context,
                client,
                arch,
                rpm_repository_href=arch_href,
                date=date_str,
                results_model=results_model,
                distribution_urls=distribution_urls,
                target_arch_repo=context.target_arch_repo,
            )
        )

    # Upload logs (never signed)
    for log_path, arch in logs_to_upload:
        logging.warning("Uploading log: %s", os.path.basename(log_path))
        log_labels = create_labels(context.build_id, arch, context.namespace, context.parent_package, date_str)
        log_resources = upload_log(
            client,
            logs_prn,
            log_path,
            build_id=context.build_id,
            labels=log_labels,
            arch=arch,
            results_model=results_model,
            distribution_urls=distribution_urls,
            target_arch_repo=context.target_arch_repo,
        )
        created_resources.extend(log_resources)
        results_model.uploaded_counts.logs += 1

    # Upload SBOMs
    for sbom_path in sboms_to_upload:
        sbom_resources = upload_sbom(
            client,
            context,
            sbom_prn,
            date_str,
            results_model,
            sbom_path,
            distribution_urls=distribution_urls,
            target_arch_repo=context.target_arch_repo,
        )
        created_resources.extend(sbom_resources)

    # Upload generic artifacts
    for file_path, labels in artifacts_to_upload:
        logging.warning("Uploading artifact: %s", os.path.basename(file_path))
        validate_file_path(file_path, "File")
        resp = client.create_file_content(artifacts_prn, file_path, build_id=context.build_id, pulp_label=labels)
        client.check_response(resp, f"upload file {file_path}")
        task_href = resp.json()["task"]
        task_response = client.wait_for_finished_task(task_href)
        if task_response.created_resources:
            created_resources.extend(task_response.created_resources)
        results_model.uploaded_counts.files += 1
        if distribution_urls is not None:
            fn = os.path.basename(file_path)
            arch_part = labels.get("arch") or None
            rel_path = f"{arch_part}/{fn}" if arch_part else fn
            client.add_uploaded_artifact_to_results_model(
                results_model,
                local_path=file_path,
                labels=labels,
                is_rpm=False,
                distribution_urls=distribution_urls,
                target_arch_repo=context.target_arch_repo,
                file_relative_path=rel_path,
            )

    extra_artifacts = [ExtraArtifactRef(pulp_href=href) for href in created_resources]
    return collect_results(client, context, date_str, results_model, extra_artifacts)


def _serialize_results_to_json(results: Dict[str, Any]) -> str:
    """Serialize results to JSON with error handling."""
    try:
        logging.debug("Results data before JSON serialization: %s", results)
        json_content = json.dumps(results, indent=2)
        logging.debug("Successfully created JSON content, length: %d", len(json_content))
        preview = json_content[:500] + "..." if len(json_content) > 500 else json_content
        logging.debug("JSON content preview: %s", preview)
        return json_content
    except (TypeError, ValueError) as e:
        logging.error("Failed to serialize results to JSON: %s", e)
        logging.error("Results data: %s", results)
        logging.error("Traceback: %s", traceback.format_exc())
        # Diagnose which key is causing the issue
        for key, value in results.items():
            try:
                json.dumps(value)
                logging.debug("Key '%s' serializes successfully", key)
            except (TypeError, ValueError) as key_error:
                logging.error("Key '%s' failed to serialize: %s", key, key_error)
        raise


def _save_results_to_folder(folder_path: str, json_content: str, context: UploadContext) -> Optional[Path]:
    """
    Save results JSON to a local folder instead of uploading to Pulp.

    Creates the folder if it does not exist. Writes pulp_results.json to it.
    Also handles sbom_results if configured.

    Args:
        folder_path: Path to output directory
        json_content: Serialized results JSON
        context: Upload context (for sbom_results)

    Returns:
        Path to the saved file, or None on failure
    """
    try:
        output_dir = Path(folder_path).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / RESULTS_JSON_FILENAME
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(json_content)
        logging.info("Results JSON saved to %s (skipped Pulp upload)", output_file)
        if context.sbom_results:
            _handle_sbom_results(None, context, json_content)  # type: ignore[arg-type]
        return output_file
    except (OSError, IOError) as e:
        logging.error("Failed to save results JSON to folder %s: %s", folder_path, e)
        logging.error("Traceback: %s", traceback.format_exc())
        return None


def _upload_and_get_results_url(
    client: "PulpClient", context: UploadContext, artifact_repository_prn: str, json_content: str, date: str
) -> Optional[str]:
    """Upload results JSON and return the distribution URL."""
    # Upload results JSON
    labels = create_labels(context.build_id, "", context.namespace, context.parent_package, date)
    content_upload_response = client.create_file_content(
        artifact_repository_prn,
        json_content,
        build_id=context.build_id,
        pulp_label=labels,
        filename=RESULTS_JSON_FILENAME,
    )

    try:
        client.check_response(content_upload_response, "upload results JSON")
        task_href = content_upload_response.json()["task"]
        task_response = client.wait_for_finished_task(task_href)
        logging.info("Results JSON uploaded successfully")

        # Get results URL and handle artifacts
        results_json_url = _extract_results_url(client, context, task_response)

        if context.artifact_results:
            _handle_artifact_results(client, context, task_response)
        else:
            logging.info("Results JSON available at: %s", results_json_url)

        if context.sbom_results:
            _handle_sbom_results(client, context, json_content)

        return results_json_url

    except Exception as e:
        logging.error("Failed to upload results JSON: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        raise


def _extract_results_url(client: "PulpClient", context: UploadContext, task_response: TaskResponse) -> str:
    """Extract results JSON URL from task response.

    Args:
        client: PulpClient instance
        context: Upload context containing build metadata
        task_response: TaskResponse model from Pulp API

    Returns:
        URL to the results JSON file
    """
    logging.debug("Task response for results JSON: state=%s", task_response.state)

    # Get the distribution URL for artifacts repository
    # Namespace is automatically read from config file via client
    repository_helper = PulpHelper(client, parent_package=context.parent_package)
    distribution_urls = repository_helper.get_distribution_urls(context.build_id)

    logging.debug("Available distribution URLs: %s", list(distribution_urls.keys()))
    for repo_type, url in distribution_urls.items():
        logging.debug("  %s: %s", repo_type, url)

    if "artifacts" not in distribution_urls:
        raise ValueError(f"No distribution URL found for artifacts repository (build_id: {context.build_id})")

    artifacts_dist_url = distribution_urls["artifacts"]
    logging.info("Using artifacts distribution URL: %s", artifacts_dist_url)

    # Get the relative path from the task response
    relative_path = task_response.result.get("relative_path") if task_response.result else None
    if not relative_path:
        raise ValueError("Task response does not contain relative_path in result")

    logging.info("Task response relative_path: %s", relative_path)

    # Construct the URL using the distribution URL and relative path
    # The distribution base_path includes build_id/artifacts
    # The relative_path from task is just the filename (e.g., "pulp_results.json")
    final_url = f"{artifacts_dist_url}{relative_path}"
    logging.info("Final results JSON URL: %s", final_url)

    return final_url


def _gather_and_validate_content(
    client: "PulpClient", context: UploadContext, extra_artifacts: Optional[List[ExtraArtifactRef]]
) -> Optional[ContentData]:
    """
    Gather content data and validate it's not empty.

    Args:
        client: PulpClient instance
        context: Upload context with build_id
        extra_artifacts: Optional list of extra artifacts

    Returns:
        Content data object

    Raises:
        ValueError: If no content found
    """
    logging.info("Collecting results for build ID: %s", context.build_id)
    logging.info("Extra artifacts provided: %d", len(extra_artifacts) if extra_artifacts else 0)

    content_data = client.gather_content_data(context.build_id, extra_artifacts)

    if not content_data.content_results:
        logging.error("No content found for build ID: %s", context.build_id)
        logging.error("This usually means content hasn't been indexed yet or build_id label is missing")
        return None

    logging.info("Successfully gathered %d content items", len(content_data.content_results))
    return content_data


def _build_artifact_map(client: "PulpClient", content_results: List[PulpContentRow]) -> FileInfoMap:
    """
    Build map of artifact hrefs to file information.

    Args:
        client: PulpClient instance
        content_results: List of content results from Pulp

    Returns:
        Dictionary mapping artifact href to FileInfoModel
    """
    logging.info("Building results structure from %d content items", len(content_results))

    # Extract artifact hrefs from content_results
    artifact_hrefs = [
        {"pulp_href": artifact_href}
        for content in content_results
        for artifact_href in (content.artifacts or {}).values()
        if artifact_href and "/artifacts/" in artifact_href
    ]

    logging.info("Extracted %d artifact hrefs to query for file locations", len(artifact_hrefs))

    # Get file locations for valid artifact hrefs
    file_info_map: FileInfoMap = {}
    if artifact_hrefs:
        logging.debug("Querying file locations for artifact hrefs: %s", [a["pulp_href"] for a in artifact_hrefs[:3]])
        file_locations_json = client.get_file_locations(artifact_hrefs).json()["results"]
        # Convert to FileInfoModel instances
        file_info_map = {
            file_info["pulp_href"]: FileInfoModel(
                pulp_href=file_info["pulp_href"],
                file=file_info["file"],
                sha256=file_info.get("sha256"),
                size=file_info.get("size"),
            )
            for file_info in file_locations_json
        }
        logging.info("Retrieved file locations for %d artifacts", len(file_info_map))
    else:
        logging.warning("No artifact hrefs found to query for file locations")

    return file_info_map


def _populate_results_model(
    client: "PulpClient",
    results_model: PulpResultsModel,
    content_results: List[PulpContentRow],
    file_info_map: FileInfoMap,
    context: "UploadContext",
) -> None:
    """
    Populate results model with artifacts from content results.

    Args:
        client: PulpClient instance
        results_model: Model to populate
        content_results: List of content results from Pulp
        file_info_map: Map of artifact hrefs to file information
        context: Upload context for getting distribution URLs
    """
    # Get distribution URLs to construct proper artifact URLs
    repository_helper = PulpHelper(client, parent_package=context.parent_package)
    distribution_urls = _distribution_urls_for_context(repository_helper, context.build_id, context)
    target_arch_repo = bool(getattr(context, "target_arch_repo", False))

    client.build_results_structure(
        results_model,
        content_results,
        file_info_map,
        distribution_urls,
        target_arch_repo=target_arch_repo,
        merge=True,
    )


def _add_distributions_to_results(
    client: "PulpClient", context: UploadContext, results_model: PulpResultsModel
) -> None:
    """
    Add distribution URLs to results model.

    Args:
        client: PulpClient instance
        context: Upload context with configuration
        results_model: Model to add distributions to
    """
    repository_helper = PulpHelper(client, parent_package=context.parent_package)
    distribution_urls = _distribution_urls_for_context(repository_helper, context.build_id, context)

    if distribution_urls:
        for repo_type, url in distribution_urls.items():
            results_model.add_distribution(repo_type, url)
            logging.debug("Distribution URL for %s: %s", repo_type, url)
        logging.info("Added distribution URLs for %d repository types", len(distribution_urls))
    else:
        logging.warning("No distribution URLs found")


def collect_results(
    client: "PulpClient",
    context: UploadContext,
    date: str,
    results_model: PulpResultsModel,
    extra_artifacts: Optional[List[ExtraArtifactRef]] = None,
) -> Optional[str]:
    """
    Collect results and upload JSON directly from memory.

    This function orchestrates gathering content, building results structure,
    and uploading the results JSON to the artifacts repository.

    Args:
        client: PulpClient instance for API interactions
        context: Upload context containing build metadata
        date: Build date string
        results_model: PulpResultsModel to populate with artifacts and distributions
        extra_artifacts: Optional list of extra artifacts to include

    Returns:
        URL of the uploaded results JSON, or None if upload failed
    """
    # Gather and validate content
    content_data = _gather_and_validate_content(client, context, extra_artifacts)

    # If artifact_results is a folder path (no comma), save locally instead of uploading to Pulp.
    # When no content is found, still create a minimal pulp_results.json so downstream steps
    # (e.g. search-by with --results-json) have a file to read.
    if context.artifact_results and "," not in context.artifact_results.strip():
        if content_data:
            file_info_map = _build_artifact_map(client, content_data.content_results)
            _populate_results_model(client, results_model, content_data.content_results, file_info_map, context)
        _add_distributions_to_results(client, context, results_model)
        json_content = _serialize_results_to_json(results_model.to_json_dict())
        output_path = _save_results_to_folder(context.artifact_results.strip(), json_content, context)
        return str(output_path) if output_path else None

    if not content_data:
        if results_model.artifact_count or results_model.distributions:
            logging.info("No gathered content; using incrementally populated results model only")
            _add_distributions_to_results(client, context, results_model)
            json_content = _serialize_results_to_json(results_model.to_json_dict())
            return _upload_and_get_results_url(
                client, context, results_model.repositories.artifacts_prn, json_content, date
            )
        return None

    # Build artifact map
    file_info_map = _build_artifact_map(client, content_data.content_results)

    # Populate results model (merge skips keys already added during upload)
    _populate_results_model(client, results_model, content_data.content_results, file_info_map, context)

    # Add distribution URLs
    _add_distributions_to_results(client, context, results_model)

    # Serialize results
    json_content = _serialize_results_to_json(results_model.to_json_dict())

    return _upload_and_get_results_url(client, context, results_model.repositories.artifacts_prn, json_content, date)


def _find_artifact_content(client: "PulpClient", task_response: TaskResponse) -> Optional[Tuple[str, str]]:
    """
    Find artifact content from task response and get file and sha256 from artifacts API.

    Args:
        client: PulpClient instance
        task_response: TaskResponse from upload operation

    Returns:
        Tuple of (file, sha256) from artifacts API, or None if not found
    """
    logging.debug("Task response: state=%s, created_resources=%s", task_response.state, task_response.created_resources)

    # Find the created content
    artifact_href = next((a for a in task_response.created_resources if "content" in a), None)
    if not artifact_href:
        logging.error("No content artifact found in task response")
        return None

    content_resp = client.find_content("href", artifact_href).json()["results"]
    if not content_resp:
        logging.error("No content found for href: %s", artifact_href)
        return None

    # Extract artifact dict, filtering out non-artifact hrefs
    artifacts_dict = content_resp[0]["artifacts"]

    # Extract all artifact hrefs from the dictionary (not just the first one)
    if isinstance(artifacts_dict, dict):
        artifact_hrefs = [href for href in artifacts_dict.values() if href and "/artifacts/" in str(href)]
    else:
        artifact_hrefs = [artifacts_dict] if artifacts_dict else []

    if not artifact_hrefs:
        logging.error("No artifact hrefs found in content response")
        return None

    # Get file and sha256 from artifacts API
    # Format as list of dicts with pulp_href keys as expected by get_file_locations
    artifact_hrefs_formatted = [{"pulp_href": href} for href in artifact_hrefs]
    artifact_response = client.get_file_locations(artifact_hrefs_formatted).json()["results"][0]
    file_value = artifact_response.get("file", "")
    sha256_value = artifact_response.get("sha256", "")

    if not file_value:
        logging.error("No file value found in artifact response")
        return None

    if not sha256_value:
        logging.error("No sha256 value found in artifact response")
        return None

    return (file_value, sha256_value)


def _parse_oci_reference(oci_reference: str) -> Tuple[str, str]:
    """
    Parse OCI reference into URL and digest parts.

    Args:
        oci_reference: Full OCI reference (e.g., "registry/repo@sha256:hash" or "registry/repo")

    Returns:
        Tuple of (image_url, digest). If no digest is present, digest will be empty string.

    Example:
        >>> _parse_oci_reference("quay.io/org/repo@sha256:abc123")
        ('quay.io/org/repo', 'sha256:abc123')
        >>> _parse_oci_reference("quay.io/org/repo")
        ('quay.io/org/repo', '')
    """
    if "@" in oci_reference:
        image_url, digest = oci_reference.rsplit("@", 1)
        logging.debug("Parsed OCI reference: URL=%s, digest=%s", image_url, digest)
        return image_url, digest
    else:
        # No digest in reference, return URL as-is with empty digest
        logging.debug("OCI reference has no digest: %s", oci_reference)
        return oci_reference, ""


def _write_konflux_results(image_url: str, digest: str, url_path: str, digest_path: str) -> None:
    """
    Write Konflux result files.

    Args:
        image_url: Image URL without digest
        digest: Image digest
        url_path: Path to write URL file
        digest_path: Path to write digest file
    """
    with open(url_path, "w", encoding="utf-8") as f:
        f.write(image_url)

    with open(digest_path, "w", encoding="utf-8") as f:
        f.write(digest)

    logging.info("Artifact results written to %s and %s", url_path, digest_path)
    logging.debug("Image URL: %s", image_url)
    logging.debug("Image digest: %s", digest)


def _handle_artifact_results(client: "PulpClient", context: UploadContext, task_response: TaskResponse) -> None:
    """
    Handle artifact results for Konflux integration.

    Processes task response to extract artifact information and writes
    results to files specified in artifact_results argument.

    Args:
        client: PulpClient instance for API interactions
        context: Upload context containing artifact_results path
        task_response: TaskResponse model from the upload task
    """
    # Get distribution URL for artifacts repository
    repository_helper = PulpHelper(client, parent_package=context.parent_package)
    distribution_urls = repository_helper.get_distribution_urls(context.build_id)

    if "artifacts" not in distribution_urls:
        logging.error("No distribution URL found for artifacts repository (build_id: %s)", context.build_id)
        return

    artifacts_dist_url = distribution_urls["artifacts"]

    # Get the relative path from the task response
    relative_path = task_response.result.get("relative_path") if task_response.result else None
    if not relative_path:
        logging.error("Task response does not contain relative_path in result")
        return

    # Construct the distribution URL using the distribution URL and relative path
    # The distribution base_path includes build_id/artifacts
    # The relative_path from task is just the filename (e.g., "artifact.tar.gz")
    distribution_file_url = f"{artifacts_dist_url}{relative_path}"

    # Check if artifact_results is set
    if not context.artifact_results:
        logging.debug("No artifact_results path configured, skipping artifact results handling")
        return

    # Parse paths from context
    try:
        image_url_path, image_digest_path = context.artifact_results.split(",")
    except ValueError as e:
        logging.error("Invalid artifact_results format: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        return

    # Parse OCI reference from the distribution URL
    try:
        image_url, digest = _parse_oci_reference(distribution_file_url)
    except ValueError as e:
        logging.error("Failed to parse OCI reference: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
        return

    # Write results
    _write_konflux_results(image_url, digest, image_url_path, image_digest_path)


def _handle_sbom_results(
    client: "PulpClient", context: UploadContext, json_content: str
) -> None:  # pylint: disable=unused-argument
    """
    Handle SBOM results for Konflux integration.

    This function extracts SBOM information from the results JSON and writes
    the SBOM URL to a file. The URL from the results JSON already contains
    the full reference with digest if applicable.

    Args:
        client: PulpClient instance for API interactions (reserved for future use)
        context: Upload context containing sbom_results path
        json_content: The serialized results JSON content
    """
    try:
        # Parse the results JSON
        results = json.loads(json_content)

        # Find SBOM file(s) in artifacts
        sbom_file = None
        sbom_url = None

        for artifact_name, artifact_info in results.get("artifacts", {}).items():
            # Look for SBOM files (typically .json or .spdx files in the SBOM repo)
            if any(artifact_name.endswith(ext) for ext in [".json", ".spdx", ".spdx.json"]):
                # Check if this artifact has labels indicating it's from sbom repo
                labels = artifact_info.get("labels", {})
                # SBOM files typically won't have arch label (unlike RPMs)
                if not labels.get("arch"):
                    sbom_file = artifact_name
                    sbom_url = artifact_info.get("url", "")
                    break

        if not sbom_url:
            logging.info("No SBOM file found in results JSON (this is normal if no SBOM was uploaded)")
            return

        # Check if sbom_results is set
        if not context.sbom_results:
            logging.debug("No sbom_results path configured, skipping SBOM results file write")
            return

        # Write SBOM URL to file
        # The URL already contains the complete reference (including digest if applicable)
        with open(context.sbom_results, "w", encoding="utf-8") as f:
            f.write(sbom_url)

        logging.info("SBOM results written to %s: %s", context.sbom_results, sbom_file)
        logging.debug("SBOM URL: %s", sbom_url)

    except (ValueError, KeyError) as e:
        logging.error("Failed to process SBOM results: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())
    except IOError as e:
        logging.error("Failed to write SBOM results file: %s", e)
        logging.error("Traceback: %s", traceback.format_exc())


__all__ = [
    "UploadService",
    "upload_sbom",
    "collect_results",
    "_serialize_results_to_json",
    "_save_results_to_folder",
    "_upload_and_get_results_url",
    "_extract_results_url",
    "_handle_artifact_results",
    "_handle_sbom_results",
]
