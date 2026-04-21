"""Tests for pulp_upload.py module."""

import re
from unittest.mock import Mock, patch, mock_open
import httpx
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.models.context import UploadContext, UploadRpmContext
from pulp_tool.services.upload_service import _distribution_urls_for_context, _handle_artifact_results


class TestHandleArtifactResults:
    """Test _handle_artifact_results function."""

    def test_handle_artifact_results_success(self, mock_pulp_client, httpx_mock, tmp_path) -> None:
        """Test successful artifact results handling."""
        httpx_mock.get(re.compile(".*/content/\\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]})
        )
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results=f"{url_path},{digest_path}",
        )
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/content/"],
            result={"relative_path": "test.txt"},
        )
        with patch("builtins.open", mock_open()) as mock_file:
            _handle_artifact_results(mock_pulp_client, context, task_response)
            assert mock_file.call_count == 2

    def test_handle_artifact_results_no_content(self, mock_pulp_client, tmp_path) -> None:
        """Test artifact results handling with no content."""
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results=f"{url_path},{digest_path}",
        )
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/other/"],
            result={"relative_path": "test.txt"},
        )
        _handle_artifact_results(mock_pulp_client, context, task_response)

    def test_handle_artifact_results_invalid_format(self, mock_pulp_client, httpx_mock) -> None:
        """Test artifact results handling with invalid format."""
        httpx_mock.get(re.compile(".*/content/\\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt", "sha256": "abc123"}]})
        )
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results="invalid_format",
        )
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/content/"],
            result={"relative_path": "test.txt"},
        )
        _handle_artifact_results(mock_pulp_client, context, task_response)

    def test_handle_artifact_results_no_distribution_url(self, mock_pulp_client, httpx_mock, tmp_path) -> None:
        """Test artifact results handling when no distribution URL found."""
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"
        with patch("pulp_tool.services.upload_collect.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {}
            mock_helper_class.return_value = mock_helper
            context = UploadContext(
                build_id="test-build",
                date_str="2024-01-01",
                namespace="test-namespace",
                parent_package="test-package",
                artifact_results=f"{url_path},{digest_path}",
            )
            task_response = TaskResponse(
                pulp_href="/api/v3/tasks/123/",
                state="completed",
                created_resources=["/test/content/"],
                result={"relative_path": "test.txt"},
            )
            with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
                _handle_artifact_results(mock_pulp_client, context, task_response)
                mock_logging.error.assert_called()

    def test_handle_artifact_results_no_relative_path(self, mock_pulp_client, httpx_mock, tmp_path) -> None:
        """Test artifact results handling when task response has no relative_path."""
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"
        with patch("pulp_tool.services.upload_collect.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {"artifacts": "https://example.com/artifacts/"}
            mock_helper_class.return_value = mock_helper
            context = UploadContext(
                build_id="test-build",
                date_str="2024-01-01",
                namespace="test-namespace",
                parent_package="test-package",
                artifact_results=f"{url_path},{digest_path}",
            )
            task_response = TaskResponse(
                pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/test/content/"], result={}
            )
            with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
                _handle_artifact_results(mock_pulp_client, context, task_response)
                mock_logging.error.assert_called()


class TestFindArtifactContent:
    """Test _find_artifact_content function."""

    def test_find_artifact_content_no_artifacts_dict(self, mock_pulp_client) -> None:
        """Test _find_artifact_content when artifacts_dict is empty."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_response = Mock(spec=httpx.Response)
        mock_response.json.return_value = {"results": [{"artifacts": {}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_response)
        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_non_dict_artifacts(self, mock_pulp_client) -> None:
        """Test _find_artifact_content when artifacts is not a dict."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_response = Mock(spec=httpx.Response)
        mock_response.json.return_value = {"results": [{"artifacts": None}]}
        mock_pulp_client.find_content = Mock(return_value=mock_response)
        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_no_file_value(self, mock_pulp_client, httpx_mock) -> None:
        """Test _find_artifact_content when artifact response has no file value."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"sha256": "abc123"}]}
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)
        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_no_sha256_value(self, mock_pulp_client, httpx_mock) -> None:
        """Test _find_artifact_content when artifact response has no sha256 value."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"file": "test.txt@sha256:abc123"}]}
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)
        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_success(self, mock_pulp_client, httpx_mock) -> None:
        """Test _find_artifact_content successful path."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]}
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)
        result = _find_artifact_content(mock_pulp_client, task_response)
        assert result is not None
        assert result[0] == "test.txt@sha256:abc123"
        assert result[1] == "abc123"

    def test_find_artifact_content_bare_list_json(self, mock_pulp_client, httpx_mock) -> None:
        """find_content JSON may be a list of content objects instead of paginated dict."""
        from pulp_tool.services.upload_collect import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/api/v3/content/file/files/12345/"]
        )
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]}
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)
        result = _find_artifact_content(mock_pulp_client, task_response)
        assert result == ("test.txt@sha256:abc123", "abc123")


class TestParseOciReference:
    """Test _parse_oci_reference function."""

    def test_parse_oci_reference_with_digest(self) -> None:
        """Test _parse_oci_reference with digest."""
        from pulp_tool.services.upload_collect import _parse_oci_reference

        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            image_url, digest = _parse_oci_reference("quay.io/org/repo@sha256:abc123")
            assert image_url == "quay.io/org/repo"
            assert digest == "sha256:abc123"
            mock_logging.debug.assert_called()

    def test_parse_oci_reference_without_digest(self) -> None:
        """Test _parse_oci_reference without digest."""
        from pulp_tool.services.upload_collect import _parse_oci_reference

        with patch("pulp_tool.services.upload_collect.logging") as mock_logging:
            image_url, digest = _parse_oci_reference("quay.io/org/repo")
            assert image_url == "quay.io/org/repo"
            assert digest == ""
            mock_logging.debug.assert_called()


class TestDistributionUrlsForContext:
    """Tests for _distribution_urls_for_context helper."""

    def test_signed_by_requests_include_signed_rpm_distro(self) -> None:
        """Non-empty signed_by is passed through get_distribution_urls_for_upload_context."""
        helper = Mock()
        helper.get_distribution_urls_for_upload_context.return_value = {"rpms": "https://example.com/rpms/"}
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package=None,
            signed_by=" gpg-key ",
            target_arch_repo=False,
        )
        result = _distribution_urls_for_context(helper, "test-build", context)
        assert result == {"rpms": "https://example.com/rpms/"}
        helper.get_distribution_urls_for_upload_context.assert_called_once_with("test-build", context)
