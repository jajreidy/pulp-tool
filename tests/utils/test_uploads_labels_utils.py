"""
Tests for upload utilities.

This module tests upload operations including label creation,
log uploads, and artifact uploads to repositories.
"""

import os
from unittest.mock import patch
from httpx import HTTPError
from pulp_tool.utils import create_labels, upload_log_phase1, upload_artifacts_to_repository
from pulp_tool.utils.file_operations import FileRepositoryBatch
from pulp_tool.utils.pulp_tasks import FileContentUploadResult


class TestLabelUtilities:
    """Test label utility functions."""

    def test_create_labels(self) -> None:
        """Test create_labels function."""
        labels = create_labels(
            build_id="test-build-123",
            arch="x86_64",
            namespace="test-namespace",
            parent_package="test-package",
            date="2024-01-01 12:00:00",
        )
        expected = {
            "date": "2024-01-01 12:00:00",
            "build_id": "test-build-123",
            "arch": "x86_64",
            "namespace": "test-namespace",
            "parent_package": "test-package",
        }
        assert labels == expected

    def test_create_labels_without_parent_package(self) -> None:
        """Test create_labels function without parent_package."""
        labels = create_labels(
            build_id="test-build-123",
            arch="x86_64",
            namespace="test-namespace",
            parent_package=None,
            date="2024-01-01 12:00:00",
        )
        expected = {
            "date": "2024-01-01 12:00:00",
            "build_id": "test-build-123",
            "arch": "x86_64",
            "namespace": "test-namespace",
        }
        assert labels == expected
        assert "parent_package" not in labels


class TestUploadUtilities:
    """Test upload utility functions."""

    def test_upload_log_phase1(self, mock_pulp_client, temp_file) -> None:
        """Test upload_log_phase1 appends href to batch."""
        file_batch = FileRepositoryBatch()
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with patch(
            "pulp_tool.utils.uploads.upload_file_content",
            return_value=FileContentUploadResult(content_href="/content/file/1/", relative_path="x86_64/log"),
        ):
            href = upload_log_phase1(
                mock_pulp_client,
                temp_file,
                build_id="test-build",
                labels=labels,
                arch="x86_64",
                file_batch=file_batch,
            )
        assert href == "/content/file/1/"
        assert file_batch.logs == ["/content/file/1/"]

    def test_upload_log_incremental_uses_relative_path(self, mock_pulp_client, temp_file) -> None:
        """results_model + distribution_urls: relative_path from upload result."""
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs

        file_batch = FileRepositoryBatch()
        repos = RepositoryRefs(
            rpms_href="/r",
            rpms_prn="",
            logs_href="/l",
            logs_prn="",
            sbom_href="/s",
            sbom_prn="",
            artifacts_href="/a",
            artifacts_prn="",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repos)
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with (
            patch(
                "pulp_tool.utils.uploads.upload_file_content",
                return_value=FileContentUploadResult(content_href="/c/2/", relative_path="logs/x86_64/out/build.log"),
            ),
            patch.object(mock_pulp_client, "add_uploaded_artifact_to_results_model") as mock_add,
        ):
            upload_log_phase1(
                mock_pulp_client,
                temp_file,
                build_id="test-build",
                labels=labels,
                arch="x86_64",
                file_batch=file_batch,
                results_model=results_model,
                distribution_urls={"logs": "https://example.com/logs/"},
            )
        mock_add.assert_called_once()
        assert mock_add.call_args.kwargs["file_relative_path"] == "logs/x86_64/out/build.log"

    def test_upload_log_incremental_falls_back_arch_basename(self, mock_pulp_client, temp_file) -> None:
        """When upload result has no relative_path, use arch/basename."""
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs

        file_batch = FileRepositoryBatch()
        repos = RepositoryRefs(
            rpms_href="/r",
            rpms_prn="",
            logs_href="/l",
            logs_prn="",
            sbom_href="/s",
            sbom_prn="",
            artifacts_href="/a",
            artifacts_prn="",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repos)
        labels = {"build_id": "test-build", "arch": "s390x"}
        with (
            patch(
                "pulp_tool.utils.uploads.upload_file_content",
                return_value=FileContentUploadResult(content_href="/c/3/", relative_path=None),
            ),
            patch.object(mock_pulp_client, "add_uploaded_artifact_to_results_model") as mock_add,
        ):
            upload_log_phase1(
                mock_pulp_client,
                temp_file,
                build_id="test-build",
                labels=labels,
                arch="s390x",
                file_batch=file_batch,
                results_model=results_model,
                distribution_urls={"logs": "https://example.com/logs/"},
            )
        mock_add.assert_called_once()
        assert mock_add.call_args.kwargs["file_relative_path"] == f"s390x/{os.path.basename(temp_file)}"

    def test_upload_logs_parallel_empty(self, mock_pulp_client) -> None:
        from pulp_tool.utils.uploads import upload_logs_parallel

        file_batch = FileRepositoryBatch()
        assert (
            upload_logs_parallel(mock_pulp_client, [], build_id="b", labels={}, arch="x86_64", file_batch=file_batch)
            == 0
        )

    def test_upload_logs_parallel_success(self, mock_pulp_client, temp_file) -> None:
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs
        from pulp_tool.utils.uploads import upload_logs_parallel

        file_batch = FileRepositoryBatch()
        repos = RepositoryRefs(
            rpms_href="/r",
            rpms_prn="",
            logs_href="/l",
            logs_prn="",
            sbom_href="/s",
            sbom_prn="",
            artifacts_href="/a",
            artifacts_prn="",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repos)
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with (
            patch(
                "pulp_tool.utils.uploads.upload_files_parallel",
                return_value=[(temp_file, FileContentUploadResult(content_href="/c/1/", relative_path="x86_64/log"))],
            ),
            patch.object(mock_pulp_client, "add_uploaded_artifact_to_results_model"),
        ):
            count = upload_logs_parallel(
                mock_pulp_client,
                [temp_file],
                build_id="test-build",
                labels=labels,
                arch="x86_64",
                file_batch=file_batch,
                results_model=results_model,
                distribution_urls={"logs": "https://example.com/logs/"},
            )
        assert count == 1
        assert file_batch.logs == ["/c/1/"]

    def test_upload_artifact_phase1(self, mock_pulp_client, temp_file) -> None:
        from pulp_tool.utils.uploads import upload_artifact_phase1

        file_batch = FileRepositoryBatch()
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with patch(
            "pulp_tool.utils.uploads.upload_file_content",
            return_value=FileContentUploadResult(content_href="/content/file/9/", relative_path="x86_64/artifact"),
        ):
            href = upload_artifact_phase1(
                mock_pulp_client,
                temp_file,
                build_id="test-build",
                labels=labels,
                file_batch=file_batch,
            )
        assert href == "/content/file/9/"
        assert file_batch.artifacts == ["/content/file/9/"]

    def test_upload_artifacts_to_repository(self, mock_pulp_client, mock_pulled_artifacts) -> None:
        """Test upload_artifacts_to_repository phase 1 only."""
        file_batch = FileRepositoryBatch()
        with patch(
            "pulp_tool.utils.uploads.upload_file_content",
            return_value=FileContentUploadResult(content_href="/content/file/1/"),
        ):
            upload_count, errors = upload_artifacts_to_repository(
                mock_pulp_client, mock_pulled_artifacts.rpms, file_batch, "RPM"
            )
        assert upload_count == 1
        assert len(errors) == 0
        assert file_batch.artifacts == ["/content/file/1/"]

    def test_upload_artifacts_to_repository_error(self, mock_pulp_client) -> None:
        """Test upload_artifacts_to_repository function with error."""
        file_batch = FileRepositoryBatch()
        with patch("pulp_tool.utils.uploads.upload_file_content", side_effect=HTTPError("Upload failed")):
            artifacts = {"test-file": {"file": "/path/to/file", "labels": {"build_id": "test-build"}}}
            upload_count, errors = upload_artifacts_to_repository(mock_pulp_client, artifacts, file_batch, "File")
        assert upload_count == 0
        assert len(errors) == 1
        assert "Upload failed" in errors[0]
