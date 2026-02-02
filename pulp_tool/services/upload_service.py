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
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from ..models.pulp_api import TaskResponse
from ..models.context import UploadContext, UploadRpmContext
from ..models.repository import RepositoryRefs
from ..models.results import PulpResultsModel
from ..models.artifacts import FileInfoModel

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient

from ..utils import PulpHelper, validate_file_path, create_labels

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
        results_json_url = self.helper.process_uploads(self.client, context, repositories)

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

    logging.info("Uploading SBOM: %s", sbom_path)
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

    # Return the created resources from the task
    return task_response.created_resources


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
    client: "PulpClient", context: UploadContext, extra_artifacts: Optional[List[Dict[str, str]]]
) -> Any:
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


def _build_artifact_map(client: "PulpClient", content_results: List[Dict[str, Any]]) -> Dict[str, FileInfoModel]:
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
        for artifact_href in content.get("artifacts", {}).values()
        if artifact_href and "/artifacts/" in artifact_href
    ]

    logging.info("Extracted %d artifact hrefs to query for file locations", len(artifact_hrefs))

    # Get file locations for valid artifact hrefs
    file_info_map: Dict[str, FileInfoModel] = {}
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
    content_results: list,
    file_info_map: Dict[str, FileInfoModel],
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
    distribution_urls = repository_helper.get_distribution_urls(context.build_id)

    client.build_results_structure(results_model, content_results, file_info_map, distribution_urls)


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
    distribution_urls = repository_helper.get_distribution_urls(context.build_id)

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
    extra_artifacts: Optional[List[Dict[str, str]]] = None,
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
    if not content_data:
        return None

    # Build artifact map
    file_info_map = _build_artifact_map(client, content_data.content_results)

    # Populate results model
    _populate_results_model(client, results_model, content_data.content_results, file_info_map, context)

    # Add distribution URLs
    _add_distributions_to_results(client, context, results_model)

    # Serialize and upload
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
    "_upload_and_get_results_url",
    "_extract_results_url",
    "_handle_artifact_results",
    "_handle_sbom_results",
]
