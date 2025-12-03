"""Tests for pulp_upload.py module."""

import pytest
import httpx
from httpx import HTTPError
import re
from unittest.mock import Mock, patch, mock_open
import json

from pulp_tool.services.upload_service import (
    upload_sbom,
    _serialize_results_to_json,
    _upload_and_get_results_url,
    _extract_results_url,
    _handle_artifact_results,
    _handle_sbom_results,
    collect_results,
)
from pulp_tool.models import PulpResultsModel, RepositoryRefs
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.models.context import UploadContext
import logging

# CLI imports removed - Click testing done in test_cli.py


class TestUploadSbom:
    """Test upload_sbom function."""

    def test_upload_sbom_success(self, mock_pulp_client, httpx_mock):
        """Test successful SBOM upload."""
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(200, json={"task": "/api/v3/tasks/123/"})
        )
        httpx_mock.get(re.compile(r".*/tasks/123/")).mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed"})
        )

        args = Mock()
        args.sbom_path = "/tmp/test.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"

        # Create results model
        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="test sbom content")),
        ):

            upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model)

    def test_upload_sbom_file_not_found(self, mock_pulp_client):
        """Test upload_sbom with file not found."""
        args = Mock()
        args.sbom_path = "/tmp/nonexistent.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"

        # Create results model
        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        with patch("os.path.exists", return_value=False):
            upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model)

    def test_upload_sbom_upload_error(self, mock_pulp_client, httpx_mock):
        """Test upload_sbom with upload error."""
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        args = Mock()
        args.sbom_path = "/tmp/test.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"

        # Create results model
        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="test sbom content")),
        ):

            with pytest.raises(HTTPError):
                upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model)


class TestSerializeResultsToJson:
    """Test _serialize_results_to_json function."""

    def test_serialize_results_to_json_success(self):
        """Test successful JSON serialization."""
        results = {"content": "test", "number": 123}

        json_content = _serialize_results_to_json(results)

        assert isinstance(json_content, str)
        parsed = json.loads(json_content)
        assert parsed == results

    def test_serialize_results_to_json_error(self):
        """Test JSON serialization with error."""

        # Create an object that can't be serialized
        class Unserializable:
            pass

        results = {"content": "test", "unserializable": Unserializable()}

        with pytest.raises((TypeError, ValueError)):
            _serialize_results_to_json(results)


class TestUploadAndGetResultsUrl:
    """Test _upload_and_get_results_url function."""

    def test_upload_and_get_results_url_error(self, mock_pulp_client, httpx_mock):
        """Test results upload with error."""
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        args = Mock()
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"

        with patch("pulp_tool.utils.create_labels", return_value={"build_id": "test-build"}):
            with pytest.raises(Exception):
                _upload_and_get_results_url(mock_pulp_client, args, "test-repo", "test json content", "2024-01-01")


class TestExtractResultsUrl:
    """Test _extract_results_url function."""

    def test_extract_results_url_success(self, mock_pulp_client):
        """Test successful results URL extraction."""
        args = Mock()
        args.build_id = "test-build"

        # Now task_response is a TaskResponse model, not a Mock
        # relative_path should just be the filename, not the full path
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            result={"relative_path": "pulp_results.json"},
        )

        # Mock PulpHelper and its get_distribution_urls method
        with patch("pulp_tool.services.upload_service.PulpHelper") as MockPulpHelper:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {
                "artifacts": "https://pulp-content.example.com/test-domain/test-build/artifacts/"
            }
            MockPulpHelper.return_value = mock_helper

            result = _extract_results_url(mock_pulp_client, args, task_response)

            assert result == "https://pulp-content.example.com/test-domain/test-build/artifacts/pulp_results.json"
            mock_helper.get_distribution_urls.assert_called_once_with("test-build")


class TestCollectResults:
    """Test collect_results function."""

    def test_collect_results_calls_add_distributions(self, mock_pulp_client, httpx_mock):
        """Test that collect_results calls _add_distributions_to_results."""
        # Mock HTTP responses for content gathering
        httpx_mock.get(re.compile(r".*/content/rpm/packages/\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": []})
        )
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(200, json={"task": "/api/v3/tasks/123/"})
        )
        httpx_mock.get(re.compile(r".*/tasks/123/")).mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed"})
        )

        # Create context
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
        )

        # Create results model with repositories
        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock get_distribution_urls to return URLs
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {
                "rpms": "https://pulp.example.com/rpms/",
                "logs": "https://pulp.example.com/logs/",
            }
            mock_helper_class.return_value = mock_helper

            # Call collect_results
            with patch("pulp_tool.services.upload_service._gather_and_validate_content") as mock_gather:
                # Mock gather to return minimal content
                mock_gather.return_value = Mock(
                    content_results=[],
                    file_results=[],
                    log_results=[],
                    sbom_results=[],
                )

                with patch("pulp_tool.services.upload_service._build_artifact_map", return_value={}):
                    with patch("pulp_tool.services.upload_service._populate_results_model"):
                        # Mock build_results_structure to return the results_model (modifies in place)
                        with patch.object(mock_pulp_client, "build_results_structure", return_value=results_model):
                            with patch(
                                "pulp_tool.services.upload_service._serialize_results_to_json",
                                return_value='{"test": "json"}',
                            ):
                                with patch(
                                    "pulp_tool.services.upload_service._upload_and_get_results_url",
                                    return_value="https://example.com/results.json",
                                ):
                                    result = collect_results(mock_pulp_client, context, "2024-01-01", results_model)

            # Verify PulpHelper was called with parent_package
            mock_helper_class.assert_called_once_with(mock_pulp_client, parent_package="test-pkg")
            # Verify get_distribution_urls was called
            mock_helper.get_distribution_urls.assert_called_once_with("test-build")
            assert result == "https://example.com/results.json"


class TestHandleArtifactResults:
    """Test _handle_artifact_results function."""

    def test_handle_artifact_results_success(self, mock_pulp_client, httpx_mock):
        """Test successful artifact results handling."""
        httpx_mock.get(re.compile(r".*/content/\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(r".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]})
        )

        args = Mock()
        args.artifact_results = "url_path,digest_path"

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/test/content/"]
        )

        with patch("builtins.open", mock_open()) as mock_file:
            _handle_artifact_results(mock_pulp_client, args, task_response)

            assert mock_file.call_count == 2

    def test_handle_artifact_results_no_content(self, mock_pulp_client):
        """Test artifact results handling with no content."""
        args = Mock()
        args.artifact_results = "url_path,digest_path"

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/test/other/"]
        )

        _handle_artifact_results(mock_pulp_client, args, task_response)

    def test_handle_artifact_results_invalid_format(self, mock_pulp_client, httpx_mock):
        """Test artifact results handling with invalid format."""
        httpx_mock.get(re.compile(r".*/content/\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(r".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt", "sha256": "abc123"}]})
        )

        args = Mock()
        args.artifact_results = "invalid_format"

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/", state="completed", created_resources=["/test/content/"]
        )

        _handle_artifact_results(mock_pulp_client, args, task_response)


class TestHandleSbomResults:
    """Test _handle_sbom_results function."""

    def test_handle_sbom_results_success(self, tmp_path):
        """Test successful SBOM results writing."""

        # Create mock results JSON with SBOM
        # The URL already contains the full reference with digest
        results_json = {
            "artifacts": {
                "test-sbom.spdx.json": {
                    "labels": {"build_id": "test-build", "namespace": "test-ns"},
                    "url": (
                        "https://pulp.example.com/pulp/content/test-build/sbom/"
                        "test-sbom.spdx.json@sha256:abc123def456789"
                    ),
                    "sha256": "abc123def456789",
                },
                "test-package.rpm": {
                    "labels": {"build_id": "test-build", "arch": "x86_64"},
                    "url": "https://pulp.example.com/pulp/content/test-build/rpms/test-package.rpm",
                    "sha256": "rpm123456",
                },
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        # Create proper UploadContext instead of Namespace
        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )

        # Mock client (not actually used in this function)
        mock_client = Mock()

        _handle_sbom_results(mock_client, args, json_content)

        # Verify the file was created with correct content
        assert sbom_file.exists()
        content = sbom_file.read_text()
        expected = "https://pulp.example.com/pulp/content/test-build/sbom/" "test-sbom.spdx.json@sha256:abc123def456789"
        assert content == expected

    def test_handle_sbom_results_no_sbom_found(self, tmp_path, caplog):
        """Test handling when no SBOM is found."""
        # Create mock results JSON without SBOM
        results_json = {
            "artifacts": {
                "test-package.rpm": {
                    "labels": {"build_id": "test-build", "arch": "x86_64"},
                    "url": "https://pulp.example.com/pulp/content/test-build/rpms/test-package.rpm",
                    "sha256": "rpm123456",
                }
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )
        mock_client = Mock()

        # Capture INFO level logs since the message is now at INFO level
        with caplog.at_level(logging.INFO):
            _handle_sbom_results(mock_client, args, json_content)

        # File should not be created
        assert not sbom_file.exists()
        assert "No SBOM file found" in caplog.text

    def test_handle_sbom_results_json_file_without_arch(self, tmp_path):
        """Test SBOM detection with .json extension (no arch label)."""

        # Create mock results JSON with .json file (SBOM) without arch
        # The URL already contains the full reference with digest
        results_json = {
            "artifacts": {
                "cyclonedx.json": {
                    "labels": {"build_id": "test-build", "namespace": "test-ns"},
                    "url": "https://pulp.example.com/pulp/content/test-build/sbom/cyclonedx.json@sha256:def789abc123",
                    "sha256": "def789abc123",
                }
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )
        mock_client = Mock()

        _handle_sbom_results(mock_client, args, json_content)

        # Verify the file was created with correct content
        assert sbom_file.exists()
        content = sbom_file.read_text()
        expected = "https://pulp.example.com/pulp/content/test-build/sbom/cyclonedx.json@sha256:def789abc123"
        assert content == expected
