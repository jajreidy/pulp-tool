#!/usr/bin/env python3
"""
Tests for pulp_tool.pull module.
"""

import logging
import os
import re
import tempfile
from unittest.mock import Mock, mock_open, patch

import httpx
from httpx import HTTPError

from pulp_tool.models.artifacts import ArtifactFile, PulledArtifacts
from pulp_tool.models.results import PulpResultsModel
from pulp_tool.pull import upload_downloaded_files_to_pulp
from pulp_tool.pull.reporting import (
    _calculate_artifact_totals,
    _format_download_summary,
    _format_file_size,
    _get_file_size_safe,
    _log_artifacts_downloaded,
    _log_build_information,
    _log_pulp_upload_info,
    _log_single_artifact,
    _log_storage_summary,
    _log_pull_summary,
    _log_upload_summary,
    generate_pull_report,
)
from pulp_tool.pull.upload import (
    _upload_rpms_to_repository,
    _upload_sboms_and_logs,
)
from pulp_tool.utils import RepositoryRefs


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
            patch("pulp_tool.pull.upload.logging") as mock_logging,
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
            patch("pulp_tool.pull.upload.logging") as mock_logging,
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
            patch("pulp_tool.pull.upload.upload_rpms_parallel") as mock_upload_rpms,
            patch("pulp_tool.utils.error_handling.logging") as mock_logging,
            patch("builtins.open", mock_open(read_data=b"fake rpm content")),
        ):
            mock_upload_rpms.return_value = [
                ("/fake/path.rpm", "/pulp/api/v3/content/rpm/packages/123/"),
            ]

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
                patch("pulp_tool.pull.upload._upload_sboms_and_logs") as mock_upload_sboms,
                patch("pulp_tool.pull.upload._upload_rpms_to_repository") as mock_upload_rpms,
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

    def test_log_pull_summary(self):
        """Test pull summary logging."""
        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 10
        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
            _log_pull_summary(10, 2, args)

            # Check the concise summary message (with failures)
            mock_logging.info.assert_called_once_with("Pull: %d/%d successful (%d failed)", 10, 12, 2)
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:

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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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
        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
            _log_upload_summary(upload_info)

            call_args = mock_logging.warning.call_args[0]
            assert call_args[2] == "unknown"

    def test_log_pull_summary_no_failures(self):
        """Test _log_pull_summary with no failures (line 59)."""
        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 10

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
            _log_pull_summary(10, 0, args)

            # Should use the no-failures message
            mock_logging.info.assert_any_call("Pull: %d artifacts successful", 10)

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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging, patch("os.path.getsize", return_value=1024):
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging, patch("os.path.getsize", return_value=512):
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

        with patch("pulp_tool.pull.reporting._log_single_artifact") as mock_log:
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
            patch("pulp_tool.pull.reporting._calculate_artifact_totals", return_value=(1, 1024)),
            patch("pulp_tool.pull.reporting._format_download_summary", return_value="Downloaded: 1 RPM (1.0 KB)"),
            patch("pulp_tool.pull.reporting.logging") as mock_logging,
        ):
            total_files, total_size = _log_artifacts_downloaded(pulled_artifacts)

            assert total_files == 1
            assert total_size == 1024
            mock_logging.info.assert_called_once_with("Downloaded: 1 RPM (1.0 KB)")

    def test_log_storage_summary_zero_files(self):
        """Test _log_storage_summary with zero files (line 254)."""
        pulled_artifacts = PulledArtifacts()

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
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

        with patch("pulp_tool.pull.reporting.logging") as mock_logging:
            _log_pulp_upload_info(upload_info)

            # Should log "No files uploaded to Pulp"
            mock_logging.info.assert_any_call("No files uploaded to Pulp")

    def test_generate_pull_report(self):
        """Test generate_pull_report (lines 330-334, 336)."""
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "test-build"})

        args = Mock()
        args.artifact_location = "test.json"
        args.max_workers = 4

        with (
            patch("pulp_tool.pull.reporting._log_pull_summary") as mock_transfer,
            patch("pulp_tool.pull.reporting._log_artifacts_downloaded", return_value=(1, 1024)) as mock_artifacts,
            patch("pulp_tool.pull.reporting._log_storage_summary") as mock_storage,
            patch("pulp_tool.pull.reporting._log_pulp_upload_info") as mock_upload,
            patch("pulp_tool.pull.reporting._log_build_information") as mock_build,
            patch("pulp_tool.pull.reporting.logging") as mock_logging,
        ):
            generate_pull_report(pulled_artifacts, 1, 0, args, None)

            mock_transfer.assert_called_once_with(1, 0, args)
            mock_artifacts.assert_called_once_with(pulled_artifacts)
            mock_storage.assert_called_once_with(1, pulled_artifacts)
            mock_upload.assert_called_once_with(None)
            mock_build.assert_called_once_with(pulled_artifacts)
            mock_logging.info.assert_any_call("Pull completed successfully")
