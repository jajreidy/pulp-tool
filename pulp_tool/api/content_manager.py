"""Content management operations for Pulp API.

This module handles uploading content and creating artifacts in Pulp.
"""

import json
import logging
import os
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Union

import httpx

from ..models.pulp_api import TaskResponse
from ..utils import validate_file_path


class ContentManagerMixin:
    """Mixin that provides content upload and creation operations for Pulp."""

    def upload_content(self, file_path: str, labels: Dict[str, str], *, file_type: str, arch: str = None) -> str:
        """
        Generic file upload function with validation and error handling.

        Args:
            file_path: Path to the file to upload
            labels: Labels to attach to the uploaded content
            file_type: Type of file (e.g., 'rpm', 'file') - determines upload method
            arch: Architecture for the uploaded content (required for RPM uploads)

        Returns:
            Pulp href of the uploaded content

        Raises:
            FileNotFoundError: If the file does not exist
            PermissionError: If the file cannot be read
            ValueError: If the file is empty or arch is missing for RPMs
        """
        # Validate file before upload
        validate_file_path(file_path, file_type)

        try:
            # Call the appropriate upload method based on file_type
            if file_type.lower() == "rpm":
                if not arch:
                    raise ValueError("arch parameter is required for RPM uploads")
                # Handle RPM upload directly
                url = self._url("api/v3/content/rpm/packages/upload/")
                with open(file_path, "rb") as fp:
                    file_name = os.path.basename(file_path)
                    build_id = labels.get("build_id", "")

                    # Build relative_path for RPMs
                    # RPMs use only the filename as the relative_path (no build_id, no arch prefix)
                    # The distribution base_path contains namespace/parent_package/rpms
                    relative_path = file_name

                    data = {
                        "pulp_labels": json.dumps(labels),
                        "relative_path": relative_path,
                    }
                    files = {"file": fp}

                    # Log upload attempt details for debugging
                    logging.debug("Attempting RPM upload:")
                    logging.debug("  URL: %s", url)
                    logging.debug("  File: %s", file_name)
                    logging.debug("  Relative Path: %s", relative_path)
                    logging.debug("  Build ID: %s", build_id)
                    logging.debug("  Arch: %s", arch)
                    logging.debug("  Labels: %s", labels)

                    response = self.session.post(
                        url, data=data, files=files, timeout=self.timeout, **self.request_params
                    )
            else:
                # For non-RPM files, use create_file_content
                response = self.create_file_content(
                    "", file_path, build_id=labels.get("build_id", ""), pulp_label=labels, arch=arch
                )

            # Include filename in operation for better error context
            operation_context = f"upload {file_type} ({os.path.basename(file_path)})"
            self._check_response(response, operation_context)
            return response.json()["pulp_href"]

        except httpx.HTTPError as e:
            logging.error("Request failed for %s %s: %s", file_type, file_path, e)
            logging.error("Traceback: %s", traceback.format_exc())
            raise
        except Exception as e:
            logging.error("Unexpected error uploading %s %s: %s", file_type, file_path, e)
            logging.error("Traceback: %s", traceback.format_exc())
            raise

    @staticmethod
    def _build_file_relative_path(filename: str, arch: Optional[str] = None) -> str:
        """
        Build relative path for file content based on architecture.

        Args:
            filename: Name of the file
            arch: Optional architecture to include in path

        Returns:
            Relative path string (e.g., "x86_64/file.log" or "file.json")
        """
        return f"{arch}/{filename}" if arch else filename

    def create_file_content(
        self,
        repository: str,
        content_or_path: Union[str, Path],
        *,
        build_id: str,
        pulp_label: Dict[str, str],
        filename: Optional[str] = None,
        arch: Optional[str] = None,
    ) -> httpx.Response:
        """
        Create content for a file artifact from either a file path or in-memory content.

        Args:
            repository: Repository PRN
            content_or_path: Either a file path (str/Path) or in-memory content (str)
            build_id: Build identifier for relative path
            pulp_label: Labels to attach to the content
            filename: Optional filename for in-memory content
                     (required when content_or_path is string content)
            arch: Optional architecture to include in relative path

        Returns:
            Response object from the API call

        Note:
            Namespace/domain is already included in the URL path, so it should
            not be duplicated in the relative_path.
        """
        url = self._url("api/v3/content/file/files/")
        data = {"repository": repository, "pulp_labels": json.dumps(pulp_label)}

        # Determine if content_or_path is a file path or in-memory content
        if isinstance(content_or_path, (str, Path)) and os.path.exists(str(content_or_path)):
            # File path - read from file
            file_path = Path(content_or_path)
            file_name = file_path.name
            data["relative_path"] = self._build_file_relative_path(file_name, arch)

            with open(file_path, "rb") as fp:
                files = {"file": fp}
                return self.session.post(url, data=data, files=files, timeout=self.timeout, **self.request_params)
        else:
            # In-memory content
            if not filename:
                raise ValueError("filename is required when providing in-memory content")

            content = str(content_or_path)
            data["relative_path"] = self._build_file_relative_path(filename, arch)

            files = {"file": (filename, content, "application/json")}  # type: ignore[dict-item]
            return self.session.post(url, data=data, files=files, timeout=self.timeout, **self.request_params)

    def add_content(self, repository: str, artifacts: List[str]) -> TaskResponse:
        """
        Add a list of artifacts to a repository.

        Args:
            repository: Repository href to add content to
            artifacts: List of artifact hrefs to add to the repository

        Returns:
            TaskResponse model from add content operation
        """
        modify_path = os.path.join(repository, "modify/")
        url = str(self.config["base_url"]) + modify_path
        data = {"add_content_units": artifacts}
        response = self.session.post(url, json=data, timeout=self.timeout, **self.request_params)
        response.raise_for_status()
        task_href = response.json()["task"]
        # Return the task response directly
        return self._get_task(task_href)


__all__ = ["ContentManagerMixin"]
