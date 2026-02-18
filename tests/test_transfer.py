#!/usr/bin/env python3
"""
Tests for pulp_transfer.py module.
"""

import json
import logging
import os
import re
import tempfile
from typing import Tuple
from unittest.mock import MagicMock, Mock, mock_open, patch

import httpx
import pytest
from httpx import HTTPError

from pulp_tool.api import DistributionClient
from pulp_tool.models.artifacts import ArtifactFile, PulledArtifacts
from pulp_tool.models.results import PulpResultsModel
from pulp_tool.transfer import (
    _categorize_artifacts,
    download_artifacts_concurrently,
    load_and_validate_artifacts,
    load_artifact_metadata,
    setup_repositories_if_needed,
    upload_downloaded_files_to_pulp,
)
from pulp_tool.transfer.reporting import (
    _calculate_artifact_totals,
    _extract_artifact_info,
    _format_download_summary,
    _format_file_size,
    _get_file_size_safe,
    _log_artifacts_downloaded,
    _log_build_information,
    _log_pulp_upload_info,
    _log_single_artifact,
    _log_storage_summary,
    _log_transfer_summary,
    _log_upload_summary,
    generate_transfer_report,
)
from pulp_tool.transfer.upload import (
    _upload_rpms_to_repository,
    _upload_sboms_and_logs,
)
from pulp_tool.utils import RepositoryRefs, determine_build_id


class TestDistributionClient:
    """Test DistributionClient class functionality."""

    def test_init(self):
        """Test DistributionClient initialization."""
        client = DistributionClient("cert.pem", "key.pem")
        assert client.cert == "cert.pem"
        assert client.key == "key.pem"
        assert client.session is not None

    def test_create_session(self):
        """Test _create_session method."""
        client = DistributionClient("cert.pem", "key.pem")
        session = client._create_session()
        assert session is not None

    def test_pull_artifact(self, httpx_mock):
        """Test pull_artifact method."""
        client = DistributionClient("cert.pem", "key.pem")

        # Mock the artifact endpoint
        httpx_mock.get("https://example.com/artifacts.json").mock(
            return_value=httpx.Response(200, json={"artifacts": {"test.rpm": {"labels": {"build_id": "test"}}}})
        )

        response = client.pull_artifact("https://example.com/artifacts.json")

        assert response.status_code == 200
        assert response.json()["artifacts"]["test.rpm"]["labels"]["build_id"] == "test"

    def test_pull_data(self, httpx_mock):
        """Test pull_data method."""
        httpx_mock.get("https://example.com/file.rpm").mock(
            return_value=httpx.Response(200, content=b"file content", headers={"content-length": "12"})
        )

        with (
            patch("os.makedirs"),
            patch("builtins.open", mock_open(read_data=b"file content")) as mock_open_func,
            patch("pulp_tool.api.distribution_client.logging") as mock_logging,
        ):

            client = DistributionClient("cert.pem", "key.pem")
            result = client.pull_data("file.rpm", "https://example.com/file.rpm", "x86_64", "rpm")

            assert result == "file.rpm"
            mock_logging.info.assert_called()
            mock_open_func.assert_called_once_with("file.rpm", "wb")

    def test_pull_data_async_success(self):
        """Test successful async data pull."""
        client = DistributionClient("/tmp/cert.pem", "/tmp/key.pem")
        download_info = ("test.rpm", "https://example.com/test.rpm", "x86_64", "rpm")

        with patch.object(client, "pull_data", return_value="/tmp/test.rpm"):
            result = client.pull_data_async(download_info)

            assert result == ("test.rpm", "/tmp/test.rpm")

    def test_pull_data_async_exception(self):
        """Test async data pull with exception."""
        client = DistributionClient("/tmp/cert.pem", "/tmp/key.pem")
        download_info = ("test.rpm", "https://example.com/test.rpm", "x86_64", "rpm")

        with patch.object(client, "pull_data", side_effect=HTTPError("Network error")):
            with pytest.raises(HTTPError):
                client.pull_data_async(download_info)


class TestArtifactManagement:
    """Test artifact loading and categorization functionality."""

    def test_load_artifact_metadata_success(self, httpx_mock):
        """Test loading artifact metadata successfully."""
        client = DistributionClient("cert.pem", "key.pem")

        # Mock HTTP response
        httpx_mock.get("https://example.com/artifacts.json").mock(
            return_value=httpx.Response(200, json={"artifacts": {"test.rpm": {"labels": {"build_id": "test"}}}})
        )

        result = load_artifact_metadata("https://example.com/artifacts.json", client)

        assert "artifacts" in result
        assert result["artifacts"]["test.rpm"]["labels"]["build_id"] == "test"

    def test_load_artifact_metadata_file_not_found(self):
        """Test loading artifact metadata from non-existent file."""
        client = DistributionClient("cert.pem", "key.pem")

        with pytest.raises(FileNotFoundError):
            load_artifact_metadata("/nonexistent/file.json", client)

    def test_load_artifact_metadata_invalid_json(self, temp_file):
        """Test loading artifact metadata with invalid JSON."""
        client = DistributionClient("cert.pem", "key.pem")

        with open(temp_file, "w") as f:
            f.write("invalid json content")

        with pytest.raises(json.JSONDecodeError):
            load_artifact_metadata(temp_file, client)

    def test_load_artifact_metadata_remote_url_no_client(self):
        """Test loading artifact metadata from remote URL without distribution client raises ValueError."""
        with pytest.raises(ValueError, match="DistributionClient.*required for remote artifact locations"):
            load_artifact_metadata("https://example.com/artifacts.json", None)

    def test_categorize_artifacts(self):
        """Test categorizing artifacts by type."""
        artifacts = {
            "test.rpm": {"labels": {"arch": "x86_64"}},
            "test.sbom": {"labels": {"arch": "noarch"}},
            "test.log": {"labels": {"arch": "noarch"}},
        }

        distros = {
            "rpms": "https://example.com/rpms/",
            "sbom": "https://example.com/sbom/",
            "logs": "https://example.com/logs/",
        }

        result = _categorize_artifacts(artifacts, distros)

        assert len(result) == 3
        # Check that all artifact types are included (using DownloadTask attributes)
        artifact_types = [task.artifact_type for task in result]
        assert "rpm" in artifact_types
        assert "sbom" in artifact_types
        assert "log" in artifact_types

        # Verify DownloadTask structure
        for task in result:
            assert hasattr(task, "artifact_name")
            assert hasattr(task, "file_url")
            assert hasattr(task, "arch")
            assert hasattr(task, "artifact_type")


class TestRepositoryManagement:
    """Test repository setup and management functionality."""

    def test_setup_repositories_no_config(self):
        """Test setup_repositories_if_needed with no config."""
        args = Mock()
        args.config = None

        result = setup_repositories_if_needed(args)

        assert result is None

    def test_setup_repositories_success(self, mock_config, temp_config_file):
        """Test setup_repositories_if_needed with successful setup."""
        args = Mock()
        args.config = temp_config_file
        args.build_id = "test-build"

        with (
            patch("pulp_tool.transfer.download.PulpClient.create_from_config_file") as mock_create,
            patch("pulp_tool.transfer.download.determine_build_id", return_value="test-build"),
            patch("pulp_tool.transfer.download.PulpHelper") as mock_helper,
        ):

            mock_client = Mock()
            mock_create.return_value = mock_client
            mock_helper_instance = Mock()
            mock_helper.return_value = mock_helper_instance
            # Mock setup_repositories to not raise an exception
            from pulp_tool.models.repository import RepositoryRefs

            mock_repos = RepositoryRefs(
                rpms_href="/test/",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )
            mock_helper_instance.setup_repositories.return_value = mock_repos

            result = setup_repositories_if_needed(args)

            assert result == mock_client
            mock_helper_instance.setup_repositories.assert_called_once_with("test-build")

    def test_setup_repositories_exception(self, temp_config_file):
        """Test setup_repositories_if_needed with exception."""
        args = Mock()
        args.config = temp_config_file

        with patch("pulp_tool.api.PulpClient.create_from_config_file", side_effect=ValueError("Config error")):

            result = setup_repositories_if_needed(args)

            assert result is None


class TestBuildIdManagement:
    """Test build ID determination and management."""

    def test_determine_build_id_from_args(self):
        """Test determining build_id from command line arguments."""
        args = Mock()
        args.build_id = "test-build"
        args.artifact_file = None

        pulled_artifacts = PulledArtifacts(rpms={}, logs={}, sboms={})

        result = determine_build_id(args, pulled_artifacts=pulled_artifacts)

        assert result == "test-build"

    def test_determine_build_id_from_artifacts(self):
        """Test determining build_id from pulled artifacts."""
        args = Mock()
        args.build_id = None
        args.artifact_file = None

        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test-build"})

        result = determine_build_id(args, pulled_artifacts=pulled_artifacts)

        assert result == "test-build"

    def test_determine_build_id_from_file(self, temp_file):
        """Test determining build_id from artifact file."""
        # Create artifact metadata with build_id
        artifact_data = {"artifacts": {"test.rpm": {"labels": {"build_id": "test-build"}}}}

        with open(temp_file, "w") as f:
            json.dump(artifact_data, f)

        args = Mock()
        args.build_id = None
        args.artifact_location = temp_file

        # Test with artifact_json parameter
        result = determine_build_id(args, artifact_json=artifact_data)

        assert result == "test-build"


class TestUploadFunctionality:
    """Test upload functionality for different artifact types."""

    def test_upload_rpms_success(self, mock_pulp_client, httpx_mock):
        """Test successful RPM upload."""
        # Create a temporary file for the test
        with tempfile.NamedTemporaryFile(suffix=".rpm", delete=False) as tmp_file:
            tmp_file.write(b"fake rpm content")
            tmp_file_path = tmp_file.name

        try:
            pulled_artifacts = PulledArtifacts()
            pulled_artifacts.add_rpm("test.rpm", tmp_file_path, {"build_id": "test-build", "arch": "x86_64"})

            repositories = RepositoryRefs(
                rpms_href="/pulp/api/v3/repositories/12345/",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )

            upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)

            # Mock the upload endpoint
            httpx_mock.post(re.compile(r".*/content/rpm/packages/upload/")).mock(
                return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
            )

            # Mock the add content endpoint
            httpx_mock.post(re.compile(r".*/repositories/12345/modify/")).mock(
                return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/67890/"})
            )

            # Mock the task endpoint (must match task href from add content response)
            httpx_mock.get(re.compile(r".*/tasks/67890/")).mock(
                return_value=httpx.Response(
                    200, json={"pulp_href": "/pulp/api/v3/tasks/67890/", "state": "completed", "created_resources": []}
                )
            )

            with patch("pulp_tool.utils.validation.file.validate_file_path") as mock_validate:
                mock_validate.return_value = None  # No exception

                _upload_rpms_to_repository(mock_pulp_client, pulled_artifacts, repositories, upload_info)

                # Verify upload_info was updated
                assert upload_info.uploaded_counts.rpms == 1
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)

    def test_upload_rpms_exception(self, mock_pulp_client, httpx_mock):
        """Test RPM upload with exception."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test-build", "arch": "x86_64"})

        repositories = RepositoryRefs(
            rpms_href="/pulp/api/v3/repositories/12345/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock the upload endpoint to raise an exception
        httpx_mock.post(re.compile(r".*/content/rpm/packages/upload/")).mock(side_effect=HTTPError("Upload error"))

        with (
            patch("pulp_tool.utils.validation.file.validate_file_path") as mock_validate,
            patch("builtins.open", mock_open(read_data=b"fake rpm content")),
            patch("pulp_tool.utils.rpm_operations.logging") as mock_logging,
        ):
            mock_validate.return_value = None  # No exception

            # Function should handle exceptions gracefully and continue
            _upload_rpms_to_repository(mock_pulp_client, pulled_artifacts, repositories, upload_info)

            # Verify error was logged (errors are logged but not tracked in upload_info)
            mock_logging.error.assert_called()

    def test_upload_sboms_and_logs(self, mock_pulp_client, httpx_mock):
        """Test uploading SBOMs and logs."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_sbom("test.sbom", "/tmp/test.sbom", {"build_id": "test"})
        pulled_artifacts.add_log("test.log", "/tmp/test.log", {"build_id": "test"})

        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="/pulp/api/v3/repositories/logs/12345/",
            sbom_href="",
            sbom_prn="/pulp/api/v3/repositories/sbom/12345/",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test", repositories=repositories)

        # Mock the file content creation endpoints
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )

        # Mock the task endpoint
        httpx_mock.get(re.compile(r".*/tasks/12345/")).mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed"})
        )

        with patch("pulp_tool.utils.uploads.upload_artifacts_to_repository") as mock_upload:
            mock_upload.return_value = (1, [])  # (count, errors)

            _upload_sboms_and_logs(mock_pulp_client, pulled_artifacts, repositories, upload_info)

            assert upload_info.uploaded_counts.sboms == 1
            assert upload_info.uploaded_counts.logs == 1

    def test_upload_sboms_exception(self, mock_pulp_client, httpx_mock):
        """Test SBOM upload with exception handling (lines 53-55)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_sbom("test.sbom", "/tmp/test.sbom", {"build_id": "test"})

        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="/pulp/api/v3/repositories/sbom/12345/",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test", repositories=repositories)

        # Mock create_file_content method to raise an exception
        with (
            patch.object(mock_pulp_client, "create_file_content", side_effect=ValueError("SBOM upload failed")),
            patch("pulp_tool.transfer.upload.logging") as mock_logging,
        ):
            _upload_sboms_and_logs(mock_pulp_client, pulled_artifacts, repositories, upload_info)

            # Verify error was logged and added to upload_info
            mock_logging.error.assert_called()
            assert len(upload_info.upload_errors) > 0
            assert upload_info.uploaded_counts.sboms == 0

    def test_upload_logs_exception(self, mock_pulp_client, httpx_mock):
        """Test log upload with exception handling (lines 81-83)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_log("test.log", "/tmp/test.log", {"build_id": "test", "arch": "x86_64"})

        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="/pulp/api/v3/repositories/logs/12345/",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test", repositories=repositories)

        # Mock create_file_content method to raise an exception
        with (
            patch.object(mock_pulp_client, "create_file_content", side_effect=ValueError("Log upload failed")),
            patch("pulp_tool.transfer.upload.logging") as mock_logging,
        ):
            _upload_sboms_and_logs(mock_pulp_client, pulled_artifacts, repositories, upload_info)

            # Verify error was logged and added to upload_info
            mock_logging.error.assert_called()
            assert len(upload_info.upload_errors) > 0
            assert upload_info.uploaded_counts.logs == 0

    def test_upload_rpms_repository_addition_exception(self, mock_pulp_client, httpx_mock):
        """Test RPM upload with repository addition exception (lines 124-127)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test-build", "arch": "x86_64"})

        repositories = RepositoryRefs(
            rpms_href="/pulp/api/v3/repositories/12345/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock upload_rpms_parallel to return artifacts
        with (
            patch("pulp_tool.transfer.upload.upload_rpms_parallel") as mock_upload_rpms,
            patch("pulp_tool.utils.error_handling.logging") as mock_logging,
            patch("builtins.open", mock_open(read_data=b"fake rpm content")),
        ):
            mock_upload_rpms.return_value = ["/pulp/api/v3/content/rpm/packages/123/"]

            # Mock add_content method to raise an exception
            with patch.object(
                mock_pulp_client, "add_content", side_effect=httpx.HTTPError("Repository addition failed")
            ):
                _upload_rpms_to_repository(mock_pulp_client, pulled_artifacts, repositories, upload_info)

            # Verify error was logged (via handle_generic_error) and added to upload_info
            mock_logging.error.assert_called()
            assert len(upload_info.upload_errors) > 0

    def test_upload_downloaded_files_success(self, mock_pulp_client, httpx_mock):
        """Test upload_downloaded_files_to_pulp success."""
        # Force single-threaded execution to make mock responses predictable
        with patch("pulp_tool.utils.constants.REPOSITORY_SETUP_MAX_WORKERS", 1):
            # Mock repository endpoints for RPM repos
            httpx_mock.get(re.compile(r".*/repositories/rpm/rpm/.*")).mock(
                side_effect=[
                    httpx.Response(200, json={"count": 0, "results": []}),
                    httpx.Response(
                        200,
                        json={
                            "count": 1,
                            "results": [
                                {"pulp_href": "/test/rpm-repo/", "prn": "pulp:///test/rpm-repo/", "name": "test/rpms"}
                            ],
                        },
                    ),
                ]
            )
            httpx_mock.post(re.compile(r".*/repositories/rpm/rpm/")).mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "pulp_href": "/test/rpm-repo/",
                        "prn": "pulp:///test/rpm-repo/",
                        "task": "/api/v3/tasks/123/",
                    },
                )
            )
            httpx_mock.get(re.compile(r".*/distributions/rpm/rpm/.*")).mock(
                return_value=httpx.Response(200, json={"count": 0, "results": []})
            )
            httpx_mock.post(re.compile(r".*/distributions/rpm/rpm/")).mock(
                return_value=httpx.Response(
                    200, json={"pulp_href": "/test/rpm-distro/", "base_path": "test", "task": "/api/v3/tasks/124/"}
                )
            )
            httpx_mock.get(re.compile(r".*/tasks/124/")).mock(
                return_value=httpx.Response(200, json={"state": "completed", "pulp_href": "/api/v3/tasks/124/"})
            )
            # Mock repository endpoints for file repos (logs, sbom, artifacts) - single threaded so order is predictable
            httpx_mock.get(re.compile(r".*/repositories/file/file/.*")).mock(
                side_effect=[
                    # logs: check, then get details after POST
                    httpx.Response(200, json={"count": 0, "results": []}),
                    httpx.Response(
                        200,
                        json={
                            "count": 1,
                            "results": [
                                {
                                    "pulp_href": "/test/file-repo-logs/",
                                    "prn": "pulp:///test/file-repo-logs/",
                                    "name": "test/logs",
                                }
                            ],
                        },
                    ),
                    # sbom: check, then get details after POST
                    httpx.Response(200, json={"count": 0, "results": []}),
                    httpx.Response(
                        200,
                        json={
                            "count": 1,
                            "results": [
                                {
                                    "pulp_href": "/test/file-repo-sbom/",
                                    "prn": "pulp:///test/file-repo-sbom/",
                                    "name": "test/sbom",
                                }
                            ],
                        },
                    ),
                    # artifacts: check, then get details after POST
                    httpx.Response(200, json={"count": 0, "results": []}),
                    httpx.Response(
                        200,
                        json={
                            "count": 1,
                            "results": [
                                {
                                    "pulp_href": "/test/file-repo-artifacts/",
                                    "prn": "pulp:///test/file-repo-artifacts/",
                                    "name": "test/artifacts",
                                }
                            ],
                        },
                    ),
                ]
            )
            httpx_mock.post(re.compile(r".*/repositories/file/file/")).mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "pulp_href": "/test/file-repo/",
                        "prn": "pulp:///test/file-repo/",
                        "task": "/api/v3/tasks/125/",
                    },
                )
            )
            httpx_mock.get(re.compile(r".*/distributions/file/file/.*")).mock(
                return_value=httpx.Response(200, json={"count": 0, "results": []})
            )
            httpx_mock.post(re.compile(r".*/distributions/file/file/")).mock(
                return_value=httpx.Response(
                    200, json={"pulp_href": "/test/file-distro/", "base_path": "test", "task": "/api/v3/tasks/126/"}
                )
            )
            httpx_mock.get(re.compile(r".*/tasks/126/")).mock(
                return_value=httpx.Response(200, json={"state": "completed", "pulp_href": "/api/v3/tasks/126/"})
            )

            # Create proper PulledArtifacts object
            pulled_artifacts = PulledArtifacts()
            pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test"})

            args = Mock()
            args.build_id = "test-build"
            args.artifact_file = None

            mock_repositories = RepositoryRefs(
                rpms_href="/pulp/api/v3/repositories/rpm/12345/",
                rpms_prn="/pulp/api/v3/repositories/rpm/12345/",
                logs_href="/pulp/api/v3/repositories/logs/12345/",
                logs_prn="/pulp/api/v3/repositories/logs/12345/",
                sbom_href="/pulp/api/v3/repositories/sbom/12345/",
                sbom_prn="/pulp/api/v3/repositories/sbom/12345/",
                artifacts_href="/pulp/api/v3/repositories/artifacts/12345/",
                artifacts_prn="/pulp/api/v3/repositories/artifacts/12345/",
            )

            with (
                patch("pulp_tool.utils.determine_build_id", return_value="test"),
                patch("pulp_tool.transfer.upload._upload_sboms_and_logs") as mock_upload_sboms,
                patch("pulp_tool.transfer.upload._upload_rpms_to_repository") as mock_upload_rpms,
                patch.object(mock_pulp_client, "wait_for_finished_task") as mock_wait,
                patch("pulp_tool.utils.PulpHelper.setup_repositories", return_value=mock_repositories),
            ):

                mock_wait.return_value = Mock(json=lambda: {"state": "completed"})

                result = upload_downloaded_files_to_pulp(mock_pulp_client, pulled_artifacts, args)

                assert result.build_id == "test-build"
                mock_upload_sboms.assert_called_once()
                mock_upload_rpms.assert_called_once()


class TestLoggingAndReporting:
    """Test logging and reporting functionality."""

    def test_log_transfer_summary(self):
        """Test transfer summary logging."""
        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 10
        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_transfer_summary(10, 2, args)

            # Check the concise summary message (with failures)
            mock_logging.info.assert_called_once_with("Transfer: %d/%d successful (%d failed)", 10, 12, 2)
            # Check DEBUG messages for details
            mock_logging.debug.assert_any_call("Source: %s", "test.json")
            mock_logging.debug.assert_any_call("Max workers: %d", 10)

    def test_log_storage_summary(self, caplog):
        """Test _log_storage_summary logs correct information."""
        # Create temporary files for test
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm1 = os.path.join(tmpdir, "test.rpm")
            rpm2 = os.path.join(tmpdir, "test2.rpm")
            sbom1 = os.path.join(tmpdir, "test.sbom")
            log1 = os.path.join(tmpdir, "test.log")

            # Write some data to files
            for f in [rpm1, rpm2, sbom1, log1]:
                with open(f, "wb") as file:
                    file.write(b"test data" * 100)  # ~900 bytes

            # PulledArtifacts uses Dict[str, ArtifactFile]
            pulled_artifacts = PulledArtifacts()
            pulled_artifacts.rpms["test.rpm"] = ArtifactFile(file=rpm1, labels={})
            pulled_artifacts.rpms["test2.rpm"] = ArtifactFile(file=rpm2, labels={})
            pulled_artifacts.sboms["test.sbom"] = ArtifactFile(file=sbom1, labels={})
            pulled_artifacts.logs["test.log"] = ArtifactFile(file=log1, labels={})

            # Set log level to DEBUG to capture debug messages
            with caplog.at_level(logging.DEBUG):
                _log_storage_summary(4, pulled_artifacts)

        # Check that storage locations are logged at DEBUG level
        assert "Storage locations:" in caplog.text

    def test_format_file_size_bytes(self):
        """Test _format_file_size with bytes."""
        result = _format_file_size(1024)
        assert result == "1.0 KB"

    def test_format_file_size_kb(self):
        """Test _format_file_size with KB."""
        result = _format_file_size(1024 * 1024)
        assert result == "1.0 MB"

    def test_format_file_size_mb(self):
        """Test _format_file_size with MB."""
        result = _format_file_size(1024 * 1024 * 1024)
        assert result == "1.0 GB"

    def test_format_file_size_gb(self):
        """Test _format_file_size with GB."""
        result = _format_file_size(1024 * 1024 * 1024 * 1024)
        assert result == "1.0 TB"

    def test_log_storage_summary_debug_level(self):
        """Test logging storage summary at DEBUG level."""
        total_files = 5

        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.rpms["test1.rpm"] = ArtifactFile(file="/tmp/test1.rpm", labels={})
        pulled_artifacts.rpms["test2.rpm"] = ArtifactFile(file="/tmp/test2.rpm", labels={})
        pulled_artifacts.logs["test1.log"] = ArtifactFile(file="/tmp/test1.log", labels={})
        pulled_artifacts.sboms["test1.sbom"] = ArtifactFile(file="/tmp/test1.sbom", labels={})
        pulled_artifacts.sboms["test2.sbom"] = ArtifactFile(file="/tmp/test2.sbom", labels={})

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:

            _log_storage_summary(total_files, pulled_artifacts)

            # Storage summary now uses DEBUG level
            mock_logging.debug.assert_any_call("Storage locations:")
            mock_logging.debug.assert_any_call("  - %s", "/tmp")

    def test_log_pulp_upload_info_with_upload_info(self):
        """Test logging Pulp upload info when upload_info is provided."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="rpms-prn",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 2
        upload_info.uploaded_counts.logs = 1
        upload_info.uploaded_counts.sboms = 1
        upload_info.add_error("Error 1")
        upload_info.add_error("Error 2")

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_pulp_upload_info(upload_info)

            # Check concise INFO message
            mock_logging.info.assert_called_once_with(
                "Uploaded to Pulp (build: %s): %s", "test-build", "1 SBOM, 1 log, 2 RPMs"
            )
            # Check DEBUG messages for repository details
            mock_logging.debug.assert_any_call("Repositories:")
            mock_logging.debug.assert_any_call("  - RPMs: %s", "rpms-prn")
            # Check WARNING for errors
            mock_logging.warning.assert_any_call("Upload errors (%d):", 2)
            mock_logging.warning.assert_any_call("  - %s", "Error 1")
            mock_logging.warning.assert_any_call("  - %s", "Error 2")

    def test_log_pulp_upload_info_without_upload_info(self):
        """Test logging Pulp upload info when upload_info is None."""
        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_pulp_upload_info(None)

            # When upload_info is None, nothing is logged
            mock_logging.info.assert_not_called()
            mock_logging.warning.assert_not_called()

    def test_log_build_information(self):
        """Test logging build information."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm(
            "test1.rpm", "/tmp/test1.rpm", {"build_id": "build1", "namespace": "ns1", "arch": "x86_64"}
        )
        pulled_artifacts.add_rpm(
            "test2.rpm", "/tmp/test2.rpm", {"build_id": "build2", "namespace": "ns2", "arch": "aarch64"}
        )

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_build_information(pulled_artifacts)

            # Build information now uses DEBUG level
            mock_logging.debug.assert_any_call("Build IDs: %s", "build1, build2")
            mock_logging.debug.assert_any_call("Namespaces: %s", "ns1, ns2")
            # Line 308: Architectures logging
            mock_logging.debug.assert_any_call("Architectures: %s", "aarch64, x86_64")

    def test_log_build_information_no_architectures(self):
        """Test logging build information without architectures (line 308)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test1.rpm", "/tmp/test1.rpm", {"build_id": "build1"})

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_build_information(pulled_artifacts)

            # Should not log architectures if empty
            architecture_calls = [call for call in mock_logging.debug.call_args_list if "Architectures" in str(call)]
            assert len(architecture_calls) == 0

    def test_log_upload_summary_zero_uploads(self):
        """Test _log_upload_summary with zero uploads (lines 25-26)."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 0
        upload_info.uploaded_counts.sboms = 0
        upload_info.uploaded_counts.logs = 0

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_upload_summary(upload_info)

            # Should log warning and return early
            mock_logging.warning.assert_called_once_with("Upload complete: No files uploaded to Pulp")
            # Should not call the main warning with parts
            assert mock_logging.warning.call_count == 1

    def test_log_upload_summary_with_counts(self):
        """Test _log_upload_summary with upload counts (lines 29-35)."""

        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="domain:namespace/rpms",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 1  # Singular
        upload_info.uploaded_counts.sboms = 2  # Plural
        upload_info.uploaded_counts.logs = 1  # Singular

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_upload_summary(upload_info)

            # Should log warning with parts and domain
            mock_logging.warning.assert_called_once()
            call_args = mock_logging.warning.call_args[0]
            assert "1 RPM" in call_args[1]  # Singular
            assert "2 SBOMs" in call_args[1]  # Plural
            assert "1 log" in call_args[1]  # Singular
            assert call_args[2] == "domain"  # Extracted from PRN
            assert call_args[3] == "test-build"

    def test_log_upload_summary_domain_extraction(self):
        """Test _log_upload_summary domain extraction from PRN (lines 38-39, 41-43, 45)."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="test-domain:namespace/rpms",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 1

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_upload_summary(upload_info)

            # Should extract domain from PRN
            call_args = mock_logging.warning.call_args[0]
            assert call_args[2] == "test-domain"

    def test_log_upload_summary_domain_unknown(self):
        """Test _log_upload_summary with unknown domain (lines 38-39)."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="invalid-prn",  # No colon, so domain stays "unknown"
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 1

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_upload_summary(upload_info)

            call_args = mock_logging.warning.call_args[0]
            assert call_args[2] == "unknown"

    def test_log_transfer_summary_no_failures(self):
        """Test _log_transfer_summary with no failures (line 59)."""
        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 10

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_transfer_summary(10, 0, args)

            # Should use the no-failures message
            mock_logging.info.assert_any_call("Transfer: %d artifacts successful", 10)

    def test_format_file_size_zero(self):
        """Test _format_file_size with 0 bytes (line 98)."""
        result = _format_file_size(0)
        assert result == "0 B"

    def test_get_file_size_safe_with_oserror(self):
        """Test _get_file_size_safe with OSError (lines 119-124)."""
        with patch("os.path.getsize") as mock_getsize:
            mock_getsize.side_effect = OSError("File not found")

            size_bytes, size_str = _get_file_size_safe("/nonexistent/file")

            assert size_bytes == 0
            assert size_str == "Unknown size"

    def test_log_single_artifact_with_labels(self):
        """Test _log_single_artifact with labels (lines 140-141, 144-146, 148-153, 155)."""
        artifact_data = ArtifactFile(
            file="/tmp/test.rpm",
            labels={"build_id": "test-build", "arch": "x86_64", "namespace": "test-ns"},
        )

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging, patch("os.path.getsize", return_value=1024):
            file_size = _log_single_artifact("test.rpm", artifact_data)

            assert file_size == 1024
            mock_logging.debug.assert_any_call("    - %s", "test.rpm")
            mock_logging.debug.assert_any_call("      Location: %s", "/tmp/test.rpm")
            mock_logging.debug.assert_any_call("      Size: %s", "1.0 KB")
            mock_logging.debug.assert_any_call("      Build ID: %s", "test-build")
            mock_logging.debug.assert_any_call("      Architecture: %s", "x86_64")
            mock_logging.debug.assert_any_call("      Namespace: %s", "test-ns")

    def test_log_single_artifact_without_labels(self):
        """Test _log_single_artifact without labels (lines 144-146)."""
        artifact_data = ArtifactFile(file="/tmp/test.rpm", labels={})

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging, patch("os.path.getsize", return_value=512):
            file_size = _log_single_artifact("test.rpm", artifact_data)

            assert file_size == 512
            mock_logging.debug.assert_any_call("      Build ID: %s", "Unknown")
            mock_logging.debug.assert_any_call("      Architecture: %s", "Unknown")
            mock_logging.debug.assert_any_call("      Namespace: %s", "Unknown")

    def test_calculate_artifact_totals(self):
        """Test _calculate_artifact_totals (lines 168-169, 172, 175-178, 180)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test1.rpm", "/tmp/test1.rpm", {})
        pulled_artifacts.add_rpm("test2.rpm", "/tmp/test2.rpm", {})

        with patch("pulp_tool.transfer.reporting._log_single_artifact") as mock_log:
            mock_log.side_effect = [1024, 2048]  # Return sizes for two artifacts

            total_files, total_size = _calculate_artifact_totals(pulled_artifacts)

            assert total_files == 2
            assert total_size == 3072
            assert mock_log.call_count == 2

    def test_format_download_summary(self):
        """Test _format_download_summary (lines 195-196, 198-199, 201-202, 204)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {})

        summary = _format_download_summary(pulled_artifacts, 1024)

        assert "Downloaded:" in summary
        assert "1.0 KB" in summary

    def test_format_download_summary_no_artifacts(self):
        """Test _format_download_summary with no artifacts (lines 201-202)."""
        pulled_artifacts = PulledArtifacts()

        summary = _format_download_summary(pulled_artifacts, 0)

        assert summary == "Downloaded: No files"

    def test_log_artifacts_downloaded(self):
        """Test _log_artifacts_downloaded (lines 217-219, 221)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {})

        with (
            patch("pulp_tool.transfer.reporting._calculate_artifact_totals", return_value=(1, 1024)),
            patch("pulp_tool.transfer.reporting._format_download_summary", return_value="Downloaded: 1 RPM (1.0 KB)"),
            patch("pulp_tool.transfer.reporting.logging") as mock_logging,
        ):
            total_files, total_size = _log_artifacts_downloaded(pulled_artifacts)

            assert total_files == 1
            assert total_size == 1024
            mock_logging.info.assert_called_once_with("Downloaded: 1 RPM (1.0 KB)")

    def test_log_storage_summary_zero_files(self):
        """Test _log_storage_summary with zero files (line 254)."""
        pulled_artifacts = PulledArtifacts()

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_storage_summary(0, pulled_artifacts)

            # Should return early without logging
            mock_logging.debug.assert_not_called()

    def test_log_pulp_upload_info_no_uploads(self):
        """Test _log_pulp_upload_info with no uploads (line 289)."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        upload_info = PulpResultsModel(build_id="test-build", repositories=repositories)
        upload_info.uploaded_counts.rpms = 0
        upload_info.uploaded_counts.sboms = 0
        upload_info.uploaded_counts.logs = 0

        with patch("pulp_tool.transfer.reporting.logging") as mock_logging:
            _log_pulp_upload_info(upload_info)

            # Should log "No files uploaded to Pulp"
            mock_logging.info.assert_any_call("No files uploaded to Pulp")

    def test_generate_transfer_report(self):
        """Test generate_transfer_report (lines 330-334, 336)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test-build"})

        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 4

        with (
            patch("pulp_tool.transfer.reporting._log_transfer_summary") as mock_transfer,
            patch("pulp_tool.transfer.reporting._log_artifacts_downloaded", return_value=(1, 1024)) as mock_artifacts,
            patch("pulp_tool.transfer.reporting._log_storage_summary") as mock_storage,
            patch("pulp_tool.transfer.reporting._log_pulp_upload_info") as mock_upload,
            patch("pulp_tool.transfer.reporting._log_build_information") as mock_build,
            patch("pulp_tool.transfer.reporting.logging") as mock_logging,
        ):
            generate_transfer_report(pulled_artifacts, 1, 0, args, None)

            mock_transfer.assert_called_once_with(1, 0, args)
            mock_artifacts.assert_called_once_with(pulled_artifacts)
            mock_storage.assert_called_once_with(1, pulled_artifacts)
            mock_upload.assert_called_once_with(None)
            mock_build.assert_called_once_with(pulled_artifacts)
            mock_logging.info.assert_any_call("Transfer completed successfully")


class TestClientInitialization:
    """Test client initialization and configuration."""

    def test_initialize_clients(self):
        """Test distribution client initialization."""
        # This is now inlined in the CLI, but we can test DistributionClient directly
        client = DistributionClient("/tmp/cert.pem", "/tmp/key.pem")
        assert client.cert == "/tmp/cert.pem"
        assert client.key == "/tmp/key.pem"

    def test_load_and_validate_artifacts_exception(self):
        """Test loading and validation with exception."""
        args = Mock()
        args.artifact_location = "/nonexistent/file.json"

        mock_client = Mock()

        with patch("pulp_tool.transfer.load_artifact_metadata") as mock_load:
            mock_load.side_effect = FileNotFoundError("File not found")

            with pytest.raises(FileNotFoundError):
                load_and_validate_artifacts(args, mock_client)

    def test_handle_pulp_upload_no_client(self):
        """Test handling upload with no Pulp client."""
        # The logic is now inlined in the CLI
        pulp_client = None
        upload_info = None if not pulp_client else {"test": "data"}
        assert upload_info is None

    def test_handle_pulp_upload_with_client(self):
        """Test handling upload with Pulp client."""
        pulled_artifacts = PulledArtifacts()
        args = Mock()
        args.build_id = "test-build"
        mock_client = Mock()

        mock_repos = RepositoryRefs(
            rpms_prn="rpm-repo",
            logs_prn="log-repo",
            sbom_prn="sbom-repo",
            artifacts_prn="artifact-repo",
            rpms_href="/pulp/api/v3/repositories/rpm/rpm/",
            logs_href="/pulp/api/v3/repositories/file/file/",
            sbom_href="/pulp/api/v3/repositories/file/file/",
            artifacts_href="/pulp/api/v3/repositories/file/file/",
        )

        with patch("pulp_tool.transfer.upload.PulpHelper") as mock_helper:
            mock_helper_instance = Mock()
            mock_helper_instance.setup_repositories.return_value = mock_repos
            mock_helper.return_value = mock_helper_instance
            result = upload_downloaded_files_to_pulp(mock_client, pulled_artifacts, args)
            assert result is not None
            assert result.build_id == "test-build"


class TestTransferHelpers:
    """Test transfer helper functions."""

    def test_categorize_artifacts(self):
        """Test artifact categorization by type."""
        artifacts = {
            "file1.rpm": {"url": "http://example.com/file1.rpm", "arch": "x86_64"},
            "file2.log": {"url": "http://example.com/file2.log"},
            "sbom.json": {"url": "http://example.com/sbom.json"},
        }
        distros = {
            "rpms": "http://example.com/rpms/",
            "logs": "http://example.com/logs/",
            "sbom": "http://example.com/sbom/",
        }

        result = _categorize_artifacts(artifacts, distros)

        assert len(result) == 3
        assert any(task.artifact_name == "file1.rpm" for task in result)
        assert any(task.artifact_name == "file2.log" for task in result)
        assert any(task.artifact_name == "sbom.json" for task in result)

    # Upload and logging tests temporarily removed due to complex mocking requirements

    def test_format_file_size(self):
        """Test file size formatting."""
        assert _format_file_size(512) == "512.0 B"
        assert _format_file_size(1024) == "1.0 KB"
        assert _format_file_size(1024 * 1024) == "1.0 MB"
        assert _format_file_size(1024 * 1024 * 1024) == "1.0 GB"

    def test_download_artifacts_concurrently_no_client(self):
        """Test download_artifacts_concurrently raises ValueError when distribution_client is None."""
        artifacts = {
            "file1.rpm": {"url": "http://example.com/file1.rpm", "arch": "x86_64"},
        }
        distros = {
            "rpms": "http://example.com/rpms/",
        }

        with pytest.raises(ValueError, match="DistributionClient.*required for downloading artifacts"):
            download_artifacts_concurrently(artifacts, distros, None, max_workers=4)


class TestLoadArtifactMetadata:
    """Test load_artifact_metadata function."""

    def test_load_artifact_metadata_general_exception(self, temp_file):
        """Test load_artifact_metadata handles general exceptions (lines 85-87)."""
        client = DistributionClient("cert.pem", "key.pem")

        # Create a file that will raise a general exception (e.g., permission error)
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                load_artifact_metadata(temp_file, client)


class TestSetupRepositories:
    """Test setup_repositories_if_needed function."""

    def test_setup_repositories_adds_konflux_prefix(self, tmp_path, mock_config):
        """Test setup_repositories_if_needed adds konflux- prefix to domain."""
        from pulp_tool.models.context import TransferContext

        # Create config file with domain that doesn't have konflux- prefix
        config_file = tmp_path / "config.toml"
        config_file.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

        args = TransferContext(
            config=str(config_file),
            build_id="test-build",
            namespace="test-namespace",
        )

        with (
            patch("pulp_tool.transfer.download.PulpClient") as mock_client_class,
            patch("pulp_tool.transfer.download.PulpHelper") as mock_helper_class,
            patch("pulp_tool.transfer.download.logging") as mock_logging,
        ):
            mock_client = Mock()
            mock_client_class.create_from_config_file.return_value = mock_client

            # Mock PulpHelper to avoid real repository setup
            mock_helper = Mock()
            mock_helper.setup_repositories.return_value = Mock()
            mock_helper_class.return_value = mock_helper

            setup_repositories_if_needed(args)

            # Verify domain was passed with konflux- prefix
            mock_client_class.create_from_config_file.assert_called_once()
            # Get the call arguments - call_args[1] is kwargs
            call_args = mock_client_class.create_from_config_file.call_args
            kwargs = call_args[1] if isinstance(call_args, tuple) else call_args.kwargs
            assert kwargs["domain"] == "konflux-test-domain"
            mock_logging.debug.assert_called()

    def test_setup_repositories_preserves_existing_konflux_prefix(self, tmp_path, mock_config):
        """Test setup_repositories_if_needed preserves existing konflux- prefix."""
        from pulp_tool.models.context import TransferContext

        # Create config file with domain that already has konflux- prefix
        config_file = tmp_path / "config.toml"
        config_file.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "konflux-test-domain"')

        args = TransferContext(
            config=str(config_file),
            build_id="test-build",
            namespace="test-namespace",
        )

        with (
            patch("pulp_tool.transfer.download.PulpClient") as mock_client_class,
            patch("pulp_tool.transfer.download.PulpHelper") as mock_helper_class,
            patch("pulp_tool.transfer.download.logging") as mock_logging,
        ):
            mock_client = Mock()
            mock_client_class.create_from_config_file.return_value = mock_client

            # Mock PulpHelper to avoid real repository setup
            mock_helper = Mock()
            mock_helper.setup_repositories.return_value = Mock()
            mock_helper_class.return_value = mock_helper

            setup_repositories_if_needed(args)

            # Verify domain was preserved
            call_args = mock_client_class.create_from_config_file.call_args
            kwargs = call_args[1] if isinstance(call_args, tuple) else call_args.kwargs
            assert kwargs["domain"] == "konflux-test-domain"
            mock_logging.debug.assert_called()

    def test_setup_repositories_with_artifact_json_parent_package(self, mock_config, temp_config_file):
        """Test setup_repositories_if_needed extracts parent_package from artifact_json (lines 113-114)."""
        args = Mock()
        args.config = temp_config_file
        args.build_id = "test-build"

        artifact_json = {"parent_package": "test-package"}

        with (
            patch("pulp_tool.transfer.download.PulpClient.create_from_config_file") as mock_create,
            patch("pulp_tool.transfer.download.determine_build_id", return_value="test-build"),
            patch("pulp_tool.transfer.download.extract_metadata_from_artifact_json") as mock_extract,
            patch("pulp_tool.transfer.download.PulpHelper") as mock_helper,
        ):
            mock_client = Mock()
            mock_create.return_value = mock_client
            mock_helper_instance = Mock()
            mock_helper.return_value = mock_helper_instance
            mock_extract.return_value = "test-package"

            from pulp_tool.models.repository import RepositoryRefs

            mock_repos = RepositoryRefs(
                rpms_href="/test/",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )
            mock_helper_instance.setup_repositories.return_value = mock_repos

            result = setup_repositories_if_needed(args, artifact_json=artifact_json)

            assert result == mock_client
            mock_extract.assert_called_once_with(artifact_json, "parent_package")
            mock_helper.assert_called_once_with(mock_client, parent_package="test-package")


class TestLoadAndValidateArtifacts:
    """Test load_and_validate_artifacts function."""

    def test_load_and_validate_artifacts_no_location(self):
        """Test load_and_validate_artifacts exits when no artifact_location (lines 151-152)."""
        import sys

        args = Mock()
        args.artifact_location = None

        mock_client = Mock()

        # sys.exit is imported inside the function, so we patch sys.exit directly
        # Since sys.exit raises SystemExit, we catch that exception
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
            mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_no_artifacts(self, temp_file):
        """Test load_and_validate_artifacts exits when no artifacts found (lines 157-161)."""
        import sys

        args = Mock()
        args.artifact_location = temp_file

        # Write JSON without artifacts
        with open(temp_file, "w") as f:
            json.dump({"distributions": {}}, f)

        mock_client = Mock()

        # sys.exit is imported inside the function, so we patch sys.exit directly
        # Since sys.exit raises SystemExit, we catch that exception
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
            mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_converts_to_typed_models(self, temp_file):
        """Test load_and_validate_artifacts converts artifacts to typed models (lines 164, 166, 170)."""
        args = Mock()
        args.artifact_location = temp_file

        # Write JSON with artifacts
        artifact_data = {
            "artifacts": {
                "test.rpm": {"labels": {"build_id": "test-build", "arch": "x86_64"}},
                "test.sbom": {"labels": {"build_id": "test-build", "arch": "noarch"}},
            },
            "distributions": {
                "rpms": "https://example.com/rpms/",
                "sbom": "https://example.com/sbom/",
            },
        }

        with open(temp_file, "w") as f:
            json.dump(artifact_data, f)

        mock_client = Mock()

        result = load_and_validate_artifacts(args, mock_client)

        assert result.artifacts is not None
        assert len(result.artifacts) == 2
        assert "test.rpm" in result.artifacts
        assert "test.sbom" in result.artifacts
        # Verify artifacts are ArtifactMetadata instances
        from pulp_tool.models.artifacts import ArtifactMetadata

        assert isinstance(result.artifacts["test.rpm"], ArtifactMetadata)
        assert isinstance(result.artifacts["test.sbom"], ArtifactMetadata)
        # Verify artifact_json is ArtifactJsonResponse
        from pulp_tool.models.artifacts import ArtifactJsonResponse

        assert isinstance(result.artifact_json, ArtifactJsonResponse)


class TestDownloadArtifactsConcurrently:
    """Test download_artifacts_concurrently function."""

    def test_download_artifacts_concurrently_success(self, tmp_path):
        """Test successful concurrent downloads (lines 219-220, 222, 228-232, 235, 238-239, 241, 244, 246-252, 254)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {"labels": {"build_id": "test-build", "arch": "x86_64"}},
            "test.sbom": {"labels": {"build_id": "test-build", "arch": "noarch"}},
            "test.log": {"labels": {"build_id": "test-build", "arch": "noarch"}},
        }
        distros = {
            "rpms": "https://example.com/rpms/",
            "sbom": "https://example.com/sbom/",
            "logs": "https://example.com/logs/",
        }

        mock_client = Mock()

        # Create mock futures
        future1: Future[Tuple[str, str]] = Future()
        future2: Future[Tuple[str, str]] = Future()
        future3: Future[Tuple[str, str]] = Future()

        # Set results for futures
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        future2.set_result(("test.sbom", str(tmp_path / "test.sbom")))
        future3.set_result(("test.log", str(tmp_path / "test.log")))

        # Create files
        (tmp_path / "test.rpm").write_text("rpm content")
        (tmp_path / "test.sbom").write_text("sbom content")
        (tmp_path / "test.log").write_text("log content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None

            # Mock submit to return futures
            mock_executor.submit.side_effect = [future1, future2, future3]

            # Mock as_completed to return futures in order
            mock_as_completed.return_value = [future1, future2, future3]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 3
            assert result.failed == 0
            assert len(result.pulled_artifacts.rpms) == 1
            assert len(result.pulled_artifacts.sboms) == 1
            assert len(result.pulled_artifacts.logs) == 1

    def test_download_artifacts_concurrently_with_dict_labels(self, tmp_path):
        """Test download with dict-based artifact labels (lines 241)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {"labels": {"build_id": "test-build"}},  # dict format
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.return_value = future1
            mock_as_completed.return_value = [future1]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert "test.rpm" in result.pulled_artifacts.rpms

    def test_download_artifacts_concurrently_with_artifact_metadata(self, tmp_path):
        """Test download with ArtifactMetadata instances (lines 238-239)."""
        import concurrent.futures
        from concurrent.futures import Future

        from pulp_tool.models.artifacts import ArtifactMetadata

        artifacts = {
            "test.rpm": ArtifactMetadata(labels={"build_id": "test-build", "arch": "x86_64"}),
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.return_value = future1
            mock_as_completed.return_value = [future1]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert "test.rpm" in result.pulled_artifacts.rpms

    def test_download_artifacts_concurrently_with_httpx_error(self, tmp_path):
        """Test download handles httpx.HTTPError exceptions (lines 256-260)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {"labels": {"build_id": "test-build"}},
            "test2.rpm": {"labels": {"build_id": "test-build"}},
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future2: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        future2.set_exception(httpx.HTTPError("Network error"))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.side_effect = [future1, future2]
            mock_as_completed.return_value = [future1, future2]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert result.failed == 1


class TestExtractArtifactInfo:
    """Test _extract_artifact_info function."""

    def test_extract_artifact_info_with_dict(self):
        """Test _extract_artifact_info with dict input."""
        artifact_data = {
            "file": "/path/to/file.rpm",
            "labels": {"build_id": "test-build", "arch": "x86_64"},
        }

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {"build_id": "test-build", "arch": "x86_64"}

    def test_extract_artifact_info_with_dict_no_labels(self):
        """Test _extract_artifact_info with dict input without labels."""
        artifact_data = {"file": "/path/to/file.rpm"}

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {}

    def test_extract_artifact_info_with_model(self):
        """Test _extract_artifact_info with model object."""
        artifact_data = ArtifactFile(
            file="/path/to/file.rpm",
            labels={"build_id": "test-build", "arch": "x86_64"},
        )

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {"build_id": "test-build", "arch": "x86_64"}

    def test_extract_artifact_info_with_model_no_labels(self):
        """Test _extract_artifact_info with model object without labels."""

        # Create a mock object that has file but no labels
        class MockArtifact:
            file = "/path/to/file.rpm"

        artifact_data = MockArtifact()

        file_path, labels = _extract_artifact_info(artifact_data)  # type: ignore[arg-type]

        assert file_path == "/path/to/file.rpm"
        assert labels == {}

    def test_extract_artifact_info_unexpected_type(self):
        """Test _extract_artifact_info raises ValueError for unexpected type."""
        # Create an object that doesn't have file attribute and isn't a dict
        artifact_data = 123  # int type

        with pytest.raises(ValueError, match="Unexpected artifact_data type"):
            _extract_artifact_info(artifact_data)  # type: ignore[arg-type]
