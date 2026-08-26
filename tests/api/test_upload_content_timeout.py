"""Tests for extended timeout on multipart content uploads."""

from unittest.mock import MagicMock, patch

import httpx

from pulp_tool.api import PulpClient
from pulp_tool.utils.constants import (
    DEFAULT_TIMEOUT,
    LARGE_UPLOAD_MIN_SIZE_BYTES,
    UPLOAD_CONTENT_TIMEOUT,
)


class TestUploadContentTimeout:
    """Multipart uploads use UPLOAD_CONTENT_TIMEOUT, not DEFAULT_TIMEOUT."""

    def test_upload_content_timeout_property(self, mock_pulp_client) -> None:
        assert mock_pulp_client.upload_content_timeout == UPLOAD_CONTENT_TIMEOUT

    def test_upload_content_timeout_exceeds_default_api_timeout(self) -> None:
        assert UPLOAD_CONTENT_TIMEOUT > DEFAULT_TIMEOUT
        assert UPLOAD_CONTENT_TIMEOUT >= 1800

    def test_large_upload_minimum_size_constant(self) -> None:
        assert LARGE_UPLOAD_MIN_SIZE_BYTES == 300 * 1024 * 1024

    def test_upload_content_rpm_uses_upload_timeout(self, mock_pulp_client, temp_rpm_file, httpx_mock) -> None:
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            mock_pulp_client.upload_content(temp_rpm_file, labels, file_type="RPM", arch="x86_64")
        request = httpx_mock.calls[0].request
        assert request.extensions["timeout"]["write"] == UPLOAD_CONTENT_TIMEOUT

    def test_upload_content_300mb_plus_file_uses_upload_timeout(self, mock_config, tmp_path) -> None:
        """Regression: production failures involved 300 MiB+ RPM multipart uploads."""
        large_rpm = tmp_path / "cmake-debuginfo-sized.rpm"
        large_rpm.write_bytes(b"RPM")
        with large_rpm.open("r+b") as handle:
            handle.truncate(LARGE_UPLOAD_MIN_SIZE_BYTES + 1)

        client = PulpClient(mock_config)
        labels = {"build_id": "test-build", "arch": "x86_64"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"pulp_href": "/pulp/api/v3/content/rpm/packages/1/"}
        mock_response.status_code = 201

        with (
            patch("pulp_tool.utils.validation.file.validate_file_path"),
            patch.object(client.session, "post", return_value=mock_response) as mock_post,
            patch.object(client, "_check_response"),
        ):
            href = client.upload_content(str(large_rpm), labels, file_type="rpm", arch="x86_64")

        assert href == "/pulp/api/v3/content/rpm/packages/1/"
        assert large_rpm.stat().st_size > LARGE_UPLOAD_MIN_SIZE_BYTES
        assert mock_post.call_args.kwargs["timeout"] == UPLOAD_CONTENT_TIMEOUT

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
