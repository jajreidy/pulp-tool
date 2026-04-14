"""
File content API operations.

This module provides file content operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_file/restapi.html#content-files
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import httpx

from ...models.pulp_api import FileResponse
from ..base import BaseResourceMixin


class FileContentMixin(BaseResourceMixin):
    """Mixin that provides file content operations for Pulp."""

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
        Create file content from either a file path or in-memory content.

        API Endpoint: POST /api/v3/content/file/files/

        Args:
            repository: Repository PRN
            content_or_path: Either a file path (str/Path) or in-memory content (str)
            build_id: Build identifier for relative path
            pulp_label: Labels to attach to the content
            filename: Optional filename for in-memory content (required when content_or_path is string content)
            arch: Optional architecture to include in relative path

        Returns:
            Response object from the API call

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/content_file_files_create
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
                response = self.session.post(url, data=data, files=files, timeout=self.timeout, **self.request_params)
        else:
            # In-memory content
            if not filename:
                raise ValueError("filename is required when providing in-memory content")

            content = str(content_or_path)
            data["relative_path"] = self._build_file_relative_path(filename, arch)

            files = {"file": (filename, content, "application/json")}  # type: ignore[dict-item]
            response = self.session.post(url, data=data, files=files, timeout=self.timeout, **self.request_params)

        self._check_response(response, "create file content")  # type: ignore[attr-defined]
        return response

    def get_file_content(self, href: str) -> FileResponse:
        """
        Get file content by href.

        API Endpoint: GET /api/v3/content/file/files/{id}/

        Args:
            href: Full file content href

        Returns:
            FileResponse model

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/content_file_files_read
        """
        url = str(self.config["base_url"]) + href
        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        return self._parse_response(response, FileResponse, "get file content")

    def list_file_content(self, **query_params: Any) -> tuple[list[FileResponse], Optional[str], Optional[str], int]:
        """
        List file content with pagination.

        API Endpoint: GET /api/v3/content/file/files/

        Args:
            **query_params: Query parameters (offset, limit, pulp_label_select, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_file/restapi.html#operation/content_file_files_list
        """
        endpoint = "api/v3/content/file/files/"
        return self._list_resources(endpoint, FileResponse, **query_params)

    def find_content_by_build_id(self, build_id: str) -> List[FileResponse]:
        """
        Find file content by build_id label.

        API Endpoint: GET /api/v3/content/?pulp_label_select=build_id~{build_id}

        Args:
            build_id: Build ID to search for

        Returns:
            List of FileResponse models matching the build_id
        """
        endpoint = "api/v3/content/"
        query_params = {"pulp_label_select": f"build_id~{build_id}"}
        results, _, _, _ = self._list_resources(endpoint, FileResponse, **query_params)
        return results

    def find_content_by_hrefs(self, hrefs: List[str]) -> List[FileResponse]:
        """
        Find file content by hrefs.

        API Endpoint: GET /api/v3/content/?pulp_href__in={href1},{href2},...

        Args:
            hrefs: List of content hrefs to retrieve

        Returns:
            List of FileResponse models
        """
        endpoint = "api/v3/content/"
        query_params = {"pulp_href__in": ",".join(hrefs)}
        results, _, _, _ = self._list_resources(endpoint, FileResponse, **query_params)
        return results

    def modify_repository_content(
        self,
        repository: str,
        *,
        add_content_units: Optional[List[str]] = None,
        remove_content_units: Optional[List[str]] = None,
    ) -> Any:
        """
        Add and/or remove content units on a repository (modify endpoint).

        API Endpoint: POST /api/v3/repositories/{type}/{type}/{id}/modify/

        Args:
            repository: Repository href
            add_content_units: Optional list of content hrefs to add
            remove_content_units: Optional list of content hrefs to remove

        Returns:
            TaskResponse model from the modify operation

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/repositories_modify
        """
        import os

        from ...models.pulp_api import TaskResponse

        adds = list(add_content_units or [])
        removes = list(remove_content_units or [])
        if not adds and not removes:
            raise ValueError(
                "modify_repository_content requires at least one of add_content_units or remove_content_units"
            )

        modify_path = os.path.join(repository, "modify/")
        url = str(self.config["base_url"]) + modify_path  # type: ignore[attr-defined]
        data: Dict[str, List[str]] = {}
        if adds:
            data["add_content_units"] = adds
        if removes:
            data["remove_content_units"] = removes
        response = self.session.post(  # type: ignore[attr-defined]
            url, json=data, timeout=self.timeout, **self.request_params  # type: ignore[attr-defined]
        )
        self._check_response(response, "modify repository content")  # type: ignore[attr-defined]
        task_href = response.json()["task"]
        if hasattr(self, "get_task"):
            return self.get_task(task_href)  # type: ignore[attr-defined]
        task_url = str(self.config["base_url"]) + task_href  # type: ignore[attr-defined]
        task_response = self.session.get(  # type: ignore[attr-defined]
            task_url, timeout=self.timeout, **self.request_params  # type: ignore[attr-defined]
        )
        return self._parse_response(task_response, TaskResponse, "get task")  # type: ignore[attr-defined]

    def add_content(self, repository: str, artifacts: List[str]) -> Any:
        """
        Add a list of artifacts to a repository.

        API Endpoint: POST /api/v3/repositories/{type}/{type}/{id}/modify/

        Args:
            repository: Repository href to add content to
            artifacts: List of artifact hrefs to add to the repository

        Returns:
            TaskResponse model from add content operation

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/repositories_modify
        """
        return self.modify_repository_content(repository, add_content_units=artifacts)


__all__ = ["FileContentMixin"]
