"""Tests for extended timeout on multipart content uploads."""

from unittest.mock import patch

import httpx

from pulp_tool.utils.constants import UPLOAD_CONTENT_TIMEOUT


class TestUploadContentTimeout:
    """Multipart uploads use UPLOAD_CONTENT_TIMEOUT, not DEFAULT_TIMEOUT."""

    def test_upload_content_timeout_property(self, mock_pulp_client) -> None:
        assert mock_pulp_client.upload_content_timeout == UPLOAD_CONTENT_TIMEOUT

    def test_upload_content_rpm_uses_upload_timeout(self, mock_pulp_client, temp_rpm_file, httpx_mock) -> None:
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            mock_pulp_client.upload_content(temp_rpm_file, labels, file_type="RPM", arch="x86_64")
        request = httpx_mock.calls[0].request
        assert request.extensions["timeout"]["write"] == UPLOAD_CONTENT_TIMEOUT

    def test_upload_rpm_package_uses_upload_timeout(self, mock_pulp_client, httpx_mock, temp_rpm_file) -> None:
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )
        labels = {"build_id": "test-build"}
        mock_pulp_client.upload_rpm_package(str(temp_rpm_file), labels, arch="x86_64")
        request = httpx_mock.calls[0].request
        assert request.extensions["timeout"]["write"] == UPLOAD_CONTENT_TIMEOUT

    def test_create_file_content_from_file_uses_upload_timeout(self, mock_pulp_client, temp_file, httpx_mock) -> None:
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )
        labels = {"build_id": "test-build"}
        mock_pulp_client.create_file_content("test-repo", temp_file, build_id="test-build", pulp_label=labels)
        request = httpx_mock.calls[0].request
        assert request.extensions["timeout"]["write"] == UPLOAD_CONTENT_TIMEOUT
