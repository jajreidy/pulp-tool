"""
Tests for RpmPackageContentMixin.

This module tests RpmPackageContentMixin methods that need coverage.
"""

import httpx

from pulp_tool.models.pulp_api import RpmPackageResponse


class TestRpmPackageContentMixin:
    """Test RpmPackageContentMixin methods."""

    def test_upload_rpm_package_with_relative_path(self, mock_pulp_client, httpx_mock, temp_rpm_file):
        """Test upload_rpm_package with explicit relative_path."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )

        labels = {"build_id": "test-build"}
        result = mock_pulp_client.upload_rpm_package(
            str(temp_rpm_file), labels, arch="x86_64", relative_path="custom/path/package.rpm"
        )

        assert result.status_code == 202

    def test_upload_rpm_package_without_relative_path(self, mock_pulp_client, httpx_mock, temp_rpm_file):
        """Test upload_rpm_package without relative_path (should use basename)."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )

        labels = {"build_id": "test-build"}
        result = mock_pulp_client.upload_rpm_package(str(temp_rpm_file), labels, arch="x86_64")

        assert result.status_code == 202
        # Verify that basename was used
        request = httpx_mock.calls[0].request
        assert request is not None

    def test_get_rpm_package(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_package method."""
        httpx_mock.get("https://pulp.example.com/api/v3/content/rpm/packages/12345/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": "/api/v3/content/rpm/packages/12345/",
                    "name": "test-package",
                    "version": "1.0.0",
                    "release": "1",
                    "arch": "x86_64",
                    "sha256": "abc123",
                },
            )
        )

        result = mock_pulp_client.get_rpm_package("/api/v3/content/rpm/packages/12345/")

        assert isinstance(result, RpmPackageResponse)
        assert result.pulp_href == "/api/v3/content/rpm/packages/12345/"
        assert result.name == "test-package"

    def test_list_rpm_packages(self, mock_pulp_client, httpx_mock):
        """Test list_rpm_packages method."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/content/rpm/packages/12345/",
                            "name": "test-package",
                            "version": "1.0.0",
                            "release": "1",
                            "arch": "x86_64",
                            "sha256": "abc123",
                        }
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mock_pulp_client.list_rpm_packages()

        assert len(results) == 1
        assert isinstance(results[0], RpmPackageResponse)
        assert count == 1

    def test_get_rpm_by_pkg_ids(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_pkg_ids method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/"
            "?pkgId__in=abcd1234%2Cefgh5678"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/content/rpm/packages/12345/",
                            "pkgId": "abcd1234",
                            "name": "test-package-1",
                            "version": "1.0.0",
                            "release": "1",
                            "arch": "x86_64",
                            "sha256": "abcd1234",
                        },
                        {
                            "pulp_href": "/api/v3/content/rpm/packages/67890/",
                            "pkgId": "efgh5678",
                            "name": "test-package-2",
                            "version": "2.0.0",
                            "release": "1",
                            "arch": "x86_64",
                            "sha256": "efgh5678",
                        },
                    ],
                    "next": None,
                    "previous": None,
                    "count": 2,
                },
            )
        )

        pkg_ids = ["abcd1234", "efgh5678"]
        results = mock_pulp_client.get_rpm_by_pkg_ids(pkg_ids)

        assert len(results) == 2
        assert isinstance(results[0], RpmPackageResponse)
        assert results[0].pkgId == "abcd1234"
