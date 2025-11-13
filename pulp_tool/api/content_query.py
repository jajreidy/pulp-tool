"""
Content query operations for Pulp API.

This module handles searching for and retrieving content information from Pulp
with optimized caching and single-pass processing.
"""

import json
import logging
import traceback
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

# Third-party imports
import httpx

from ..models.artifacts import ContentData, FileInfoModel
from ..models.results import PulpResultsModel


@lru_cache(maxsize=256)
def _get_content_type_from_href(pulp_href: str) -> str:
    """
    Determine content type from pulp_href (cached for performance).

    Args:
        pulp_href: The Pulp href path

    Returns:
        Content type string (e.g., "rpm.package", "file.file", "unknown")
    """
    if "/rpm/packages/" in pulp_href:
        return "rpm.package"
    elif "/file/files/" in pulp_href:
        return "file.file"
    return "unknown"


@runtime_checkable
class ContentQueryMixin(Protocol):
    """Protocol that provides content search and data retrieval operations for Pulp."""

    # Required attributes
    _url: Callable[[str], str]  # Method that constructs URLs
    session: Any  # httpx.Client
    timeout: int
    request_params: dict
    config: Dict[str, Any]  # Configuration dictionary

    def _chunked_get(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        """Chunked GET request."""
        ...  # pragma: no cover - defined in implementation

    def find_content(self, search_type: str, search_value: str) -> httpx.Response:
        """
        Find content by various criteria.

        Args:
            search_type: Type of search ('build_id' or 'href')
            search_value: Value to search for

        Returns:
            Response object containing content matching the search criteria
        """
        if search_type == "build_id":
            url = self._url(f"api/v3/content/?pulp_label_select=build_id~{search_value}")
        elif search_type == "href":
            url = self._url(f"api/v3/content/?pulp_href__in={search_value}")
        else:
            raise ValueError(f"Unknown search type: {search_type}")

        return self.session.get(url, timeout=self.timeout, **self.request_params)

    def get_file_locations(self, artifacts: List[Dict[str, str]]) -> httpx.Response:
        """
        Get file locations for artifacts using the Pulp artifacts API.

        Args:
            artifacts: List of artifact dictionaries containing hrefs

        Returns:
            Response object containing file location information
        """
        hrefs = [list(artifact.values())[0] for artifact in artifacts]
        url = self._url("api/v3/artifacts/")
        params = {"pulp_href__in": ",".join(hrefs)}

        logging.debug("Querying %d artifacts from Pulp", len(hrefs))

        return self._chunked_get(
            url, params=params, chunk_param="pulp_href__in", timeout=self.timeout, chunk_size=20, **self.request_params
        )

    def get_rpm_by_pkgIDs(self, pkg_ids: List[str]) -> httpx.Response:
        """
        Get RPMs by package IDs.

        Args:
            pkg_ids: List of package IDs (checksums) to search for

        Returns:
            Response object containing RPM information for matching package IDs
        """
        url = self._url("api/v3/content/rpm/packages/")
        params = {"pkgId__in": ",".join(pkg_ids)}
        return self._chunked_get(
            url, params=params, chunk_param="pkgId__in", timeout=self.timeout, **self.request_params
        )

    async def async_get_rpm_by_pkgIDs(self, pkg_ids: List[str]) -> httpx.Response:
        """
        Get RPMs by package IDs asynchronously.

        Args:
            pkg_ids: List of package IDs (checksums) to search for

        Returns:
            Response object containing RPM information for matching package IDs
        """
        url = self._url("api/v3/content/rpm/packages/")
        params = {"pkgId__in": ",".join(pkg_ids)}
        # Use async_get for async HTTP request
        return await self.async_get(url, params=params)

    def gather_content_data(self, build_id: str, extra_artifacts: Optional[List[Dict[str, str]]] = None) -> ContentData:
        """
        Gather content data and artifacts for a build ID.

        Args:
            build_id: Build identifier
            extra_artifacts: Optional extra artifacts to include (from created_resources)

        Returns:
            ContentData containing content results and artifacts
        """
        content_results = []
        artifacts: List[Dict[str, Any]] = []

        # Always use bulk query by build_id for efficiency
        # This gets all content in a single API call instead of N individual calls
        if extra_artifacts:
            logging.info("Found %d created resources, querying all content by build_id", len(extra_artifacts))
        else:
            logging.debug("Searching for content by build_id")

        try:
            resp = self.find_content("build_id", build_id)
            resp_json = resp.json()
            content_results = resp_json["results"]
        except Exception as e:
            logging.error("Failed to get content by build ID: %s", e)
            logging.error("Response text: %s", resp.text if "resp" in locals() else "No response")
            logging.error("Traceback: %s", traceback.format_exc())
            raise

        # If no results from build_id query and we have extra_artifacts, try querying by href
        # This handles the case where content hasn't been indexed yet
        if not content_results and extra_artifacts:
            logging.warning(
                "No content found by build_id, trying direct href query for %d artifacts", len(extra_artifacts)
            )
            try:
                # Extract content hrefs from extra_artifacts
                # Note: extra_artifacts contains content hrefs (not artifact hrefs)
                href_list = [
                    artifact.get("pulp_href", "") for artifact in extra_artifacts if artifact.get("pulp_href", "")
                ]
                if href_list:
                    href_query = ",".join(href_list)
                    resp = self.find_content("href", href_query)
                    resp_json = resp.json()
                    content_results = resp_json["results"]
                    logging.info("Found %d content items by href query", len(content_results))
            except Exception as e:
                logging.error("Failed to get content by href: %s", e)
                logging.error("Traceback: %s", traceback.format_exc())
                # Don't raise, just continue with empty results

        if not content_results:
            logging.warning("No content found for build ID: %s", build_id)
            return ContentData()

        logging.info("Found %d content items for build_id: %s", len(content_results), build_id)

        # Log details about what content was found
        if content_results:
            logging.info("Content types found:")
            for idx, result in enumerate(content_results):
                pulp_href = result.get("pulp_href", "")
                content_type = _get_content_type_from_href(pulp_href)

                # Get relative paths from artifacts dict
                artifacts_dict = result.get("artifacts", {})
                if artifacts_dict:
                    relative_paths = list(artifacts_dict.keys())
                    logging.info("  - %s: %s", content_type, ", ".join(relative_paths))
                else:
                    logging.info("  - %s: no artifacts", content_type)

                # Log full structure for first item to help with debugging
                if idx == 0:
                    logging.debug("First content item full structure: %s", json.dumps(result, indent=2, default=str))

        # Extract artifacts from content results
        # Content structure has "artifacts" (plural) field which is a dict: {relative_path: artifact_href}
        artifacts = []
        for result in content_results:
            artifacts_dict = result.get("artifacts", {})
            if artifacts_dict:
                # Extract all artifact hrefs from the dict values
                for artifact_href in artifacts_dict.values():
                    if artifact_href:
                        artifacts.append({"artifact": artifact_href})

        logging.info("Extracted %d artifact hrefs from content results", len(artifacts))
        return ContentData(content_results=content_results, artifacts=artifacts)

    def build_results_structure(
        self,
        results_model: PulpResultsModel,
        content_results: List[Dict[str, Any]],
        file_info_map: Dict[str, FileInfoModel],
    ) -> PulpResultsModel:
        """
        Build the results structure from content and file info using optimized single-pass processing.

        Args:
            results_model: PulpResultsModel to populate with artifacts
            content_results: Content data from Pulp
            file_info_map: Mapping of artifact hrefs to file info models

        Returns:
            Populated PulpResultsModel
        """
        logging.info("Building results structure:")
        logging.info("  - Content items: %d", len(content_results))
        logging.info("  - File info entries: %d", len(file_info_map))

        # Track statistics for logging
        missing_artifacts = 0
        missing_file_info = 0

        for idx, content in enumerate(content_results):
            labels = content.get("pulp_labels", {})
            build_id = labels.get("build_id", "")
            pulp_href = content.get("pulp_href", "unknown")

            # Content structure has "artifacts" (plural) field which is a dict: {relative_path: artifact_href}
            artifacts_dict = content.get("artifacts", {})

            if not artifacts_dict:
                missing_artifacts += 1
                # Only log details for the first few items to avoid spam
                if idx < 3:
                    logging.warning(
                        "Content item %d structure (no artifacts field). Available fields: %s",
                        idx,
                        list(content.keys()),
                    )
                    logging.debug("Full content: %s", json.dumps(content, indent=2, default=str))
                continue

            # Determine content type once per content item (cached via lru_cache)
            pulp_type = _get_content_type_from_href(pulp_href)
            is_rpm = "rpm" in pulp_type.lower()

            # Process all artifacts in a single pass
            for relative_path, artifact_href in artifacts_dict.items():
                # Skip invalid artifact hrefs
                if not artifact_href or "/artifacts/" not in artifact_href:
                    continue

                # Get file info
                file_info = file_info_map.get(artifact_href)
                if not file_info:
                    missing_file_info += 1
                    if missing_file_info <= 3:  # Only log first few
                        logging.warning("No file info found for artifact href: %s", artifact_href)
                    continue

                # Construct artifact key based on content type (optimized logic)
                if is_rpm:
                    # RPM content - use just the filename as the key
                    artifact_key = relative_path
                else:
                    # File content (logs, SBOM, etc.) - use build_id/relative_path
                    artifact_key = f"{build_id}/{relative_path}" if build_id else relative_path
                    if not build_id and missing_file_info <= 1:
                        logging.warning(
                            "No build_id in labels for file content, using relative_path only: %s", relative_path
                        )

                # Add artifact to results model
                results_model.add_artifact(
                    key=artifact_key, url=file_info.file, sha256=file_info.sha256 or "", labels=labels
                )

        # Log summary statistics
        logging.info("Final results: %d artifacts processed", results_model.artifact_count)
        if missing_artifacts > 0:
            logging.warning("Content items without artifacts field: %d", missing_artifacts)
        if missing_file_info > 3:
            logging.warning("Missing file info for %d artifacts", missing_file_info)

        return results_model


__all__ = ["ContentQueryMixin"]
