"""
RPM package content API operations.

This module provides RPM package content operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulp_rpm/restapi.html#content-packages
"""

import json
import os
from typing import Any, Dict, List, Optional

import httpx

from ...models.pulp_api import RpmPackageResponse
from ..base import BaseResourceMixin


class RpmPackageContentMixin(BaseResourceMixin):
    """Mixin that provides RPM package content operations for Pulp."""

    def upload_rpm_package(
        self,
        file_path: str,
        labels: Dict[str, str],
        *,
        arch: str,
        relative_path: Optional[str] = None,
    ) -> httpx.Response:
        """
        Upload an RPM package.

        API Endpoint: POST /api/v3/content/rpm/packages/upload/

        Args:
            file_path: Path to the RPM file to upload
            labels: Labels to attach to the uploaded content
            arch: Architecture for the uploaded content (required)
            relative_path: Optional relative path (defaults to filename)

        Returns:
            Response object containing task or content href

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/content_rpm_packages_create
        """
        url = self._url("api/v3/content/rpm/packages/upload/")

        if relative_path is None:
            relative_path = os.path.basename(file_path)

        data = {
            "pulp_labels": json.dumps(labels),
            "relative_path": relative_path,
        }

        with open(file_path, "rb") as fp:
            files = {"file": fp}
            response = self.session.post(url, data=data, files=files, timeout=self.timeout, **self.request_params)

        self._check_response(response, "upload RPM package")
        return response

    def get_rpm_package(self, href: str) -> RpmPackageResponse:
        """
        Get an RPM package by href.

        API Endpoint: GET /api/v3/content/rpm/packages/{id}/

        Args:
            href: Full package href

        Returns:
            RpmPackageResponse model

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/content_rpm_packages_read
        """
        url = str(self.config["base_url"]) + href
        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        return self._parse_response(response, RpmPackageResponse, "get RPM package")

    def list_rpm_packages(
        self, **query_params: Any
    ) -> tuple[list[RpmPackageResponse], Optional[str], Optional[str], int]:
        """
        List RPM packages with pagination.

        API Endpoint: GET /api/v3/content/rpm/packages/

        Args:
            **query_params: Query parameters (offset, limit, pkgId__in, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/content_rpm_packages_list
        """
        endpoint = "api/v3/content/rpm/packages/"
        return self._list_resources(endpoint, RpmPackageResponse, **query_params)

    def get_rpm_by_pkg_ids(self, pkg_ids: List[str]) -> List[RpmPackageResponse]:
        """
        Get RPM packages by package IDs (checksums).

        API Endpoint: GET /api/v3/content/rpm/packages/?pkgId__in={id1},{id2},...

        Args:
            pkg_ids: List of package IDs (checksums) to search for

        Returns:
            List of RpmPackageResponse models

        Reference:
            https://docs.pulpproject.org/pulp_rpm/restapi.html#operation/content_rpm_packages_list
        """
        endpoint = "api/v3/content/rpm/packages/"
        query_params = {"pkgId__in": ",".join(pkg_ids)}
        results, _, _, _ = self._list_resources(endpoint, RpmPackageResponse, **query_params)
        return results


__all__ = ["RpmPackageContentMixin"]
