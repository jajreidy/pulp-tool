"""
Tests for upload utilities.

This module tests upload operations including label creation,
log uploads, and artifact uploads to repositories.
"""

from unittest.mock import Mock, patch
from httpx import HTTPError

from pulp_tool.utils import (
    create_labels,
    upload_log,
    upload_artifacts_to_repository,
    upload_rpms,
)


class TestLabelUtilities:
    """Test label utility functions."""

    def test_create_labels(self):
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


class TestUploadUtilities:
    """Test upload utility functions."""

    def test_upload_log(self, mock_pulp_client, temp_file):
        """Test upload_log function."""
        mock_response = Mock()
        mock_response.json.return_value = {"task": "/pulp/api/v3/tasks/12345/"}

        mock_task_response = Mock()
        mock_task_response.created_resources = ["/content/file/1/"]

        mock_pulp_client.create_file_content = Mock()
        mock_pulp_client.create_file_content.return_value = mock_response
        mock_pulp_client.wait_for_finished_task = Mock()
        mock_pulp_client.wait_for_finished_task.return_value = mock_task_response

        labels = {"build_id": "test-build", "arch": "x86_64"}

        created_resources = upload_log(
            mock_pulp_client, "test-repo", temp_file, build_id="test-build", labels=labels, arch="x86_64"
        )

        mock_pulp_client.create_file_content.assert_called_once()
        mock_pulp_client.wait_for_finished_task.assert_called_once()
        assert created_resources == ["/content/file/1/"]

    def test_upload_artifacts_to_repository(self, mock_pulp_client, mock_pulled_artifacts):
        """Test upload_artifacts_to_repository function."""
        mock_response = Mock()
        mock_response.json.return_value = {"task": "/pulp/api/v3/tasks/12345/"}

        mock_pulp_client.create_file_content = Mock()
        mock_pulp_client.create_file_content.return_value = mock_response
        mock_pulp_client.wait_for_finished_task = Mock()
        mock_pulp_client.wait_for_finished_task.return_value = mock_response

        upload_count, errors = upload_artifacts_to_repository(
            mock_pulp_client, mock_pulled_artifacts.rpms, "test-repo", "RPM"
        )

        assert upload_count == 1
        assert len(errors) == 0
        mock_pulp_client.create_file_content.assert_called_once()

    def test_upload_artifacts_to_repository_error(self, mock_pulp_client):
        """Test upload_artifacts_to_repository function with error."""
        mock_pulp_client.create_file_content = Mock()
        mock_pulp_client.create_file_content.side_effect = HTTPError("Upload failed")

        artifacts = {"test-file": {"file": "/path/to/file", "labels": {"build_id": "test-build"}}}

        upload_count, errors = upload_artifacts_to_repository(mock_pulp_client, artifacts, "test-repo", "File")

        assert upload_count == 0
        assert len(errors) == 1
        assert "Upload failed" in errors[0]

    def test_upload_artifacts_immediate_success(self, mock_pulp_client):
        """Test upload_artifacts_to_repository with immediate success."""
        mock_response = Mock()
        # Response without a 'task' key indicates immediate success
        mock_response.json.return_value = {"status": "success"}

        mock_pulp_client.create_file_content = Mock()
        mock_pulp_client.create_file_content.return_value = mock_response

        artifacts = {"test-file": {"file": "/path/to/file", "labels": {"build_id": "test-build"}}}

        upload_count, errors = upload_artifacts_to_repository(mock_pulp_client, artifacts, "test-repo", "File")

        assert upload_count == 1
        assert len(errors) == 0


class TestUploadRpms:
    """Test upload_rpms function."""

    def test_upload_rpms_empty_list(self, mock_pulp_client):
        """Test upload_rpms with empty RPM list (lines 208-209)."""
        from pulp_tool.models.context import UploadRpmContext
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        with patch("pulp_tool.utils.uploads.logging") as mock_logging:
            result = upload_rpms(
                [],
                context,
                mock_pulp_client,
                "x86_64",
                rpm_repository_href="/test/rpm-href",
                date="2024-01-01 00:00:00",
                results_model=results_model,
            )

            assert result == []
            mock_logging.debug.assert_called_with("No new RPMs to upload for %s", "x86_64")

    def test_upload_rpms_with_created_resources(self, mock_pulp_client):
        """Test upload_rpms with created resources (lines 225-227, 229-231)."""
        from pulp_tool.models.context import UploadRpmContext
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs
        from pulp_tool.models.pulp_api import TaskResponse

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock upload_rpms_parallel to return artifacts
        mock_artifacts = ["/rpm/artifact/1", "/rpm/artifact/2"]

        # Mock add_content to return a task
        mock_task_response = TaskResponse(
            pulp_href="/tasks/123/",
            state="completed",
            created_resources=["/resource/1", "/resource/2"],
        )
        mock_repo_task = TaskResponse(pulp_href="/tasks/123/", state="pending", created_resources=[])

        with (
            patch("pulp_tool.utils.uploads.upload_rpms_parallel", return_value=mock_artifacts),
            patch.object(mock_pulp_client, "add_content", return_value=mock_repo_task),
            patch.object(mock_pulp_client, "wait_for_finished_task", return_value=mock_task_response),
            patch("pulp_tool.utils.uploads.logging") as mock_logging,
        ):
            result = upload_rpms(
                ["/path/to/package.rpm"],
                context,
                mock_pulp_client,
                "x86_64",
                rpm_repository_href="/test/rpm-href",
                date="2024-01-01 00:00:00",
                results_model=results_model,
            )

            assert result == ["/resource/1", "/resource/2"]
            assert results_model.uploaded_counts.rpms == 1
            mock_pulp_client.add_content.assert_called_once_with("/test/rpm-href", mock_artifacts)
            mock_logging.debug.assert_any_call("Adding %s RPM artifacts to repository", 2)
            mock_logging.debug.assert_any_call("Captured %d created resources from RPM add_content", 2)

    def test_upload_rpms_no_created_resources(self, mock_pulp_client):
        """Test upload_rpms without created resources (lines 225-227, but not 229-231)."""
        from pulp_tool.models.context import UploadRpmContext
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs
        from pulp_tool.models.pulp_api import TaskResponse

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock upload_rpms_parallel to return artifacts
        mock_artifacts = ["/rpm/artifact/1"]

        # Mock add_content to return a task with no created_resources
        mock_task_response = TaskResponse(
            pulp_href="/tasks/123/",
            state="completed",
            created_resources=[],
        )
        mock_repo_task = TaskResponse(pulp_href="/tasks/123/", state="pending", created_resources=[])

        with (
            patch("pulp_tool.utils.uploads.upload_rpms_parallel", return_value=mock_artifacts),
            patch.object(mock_pulp_client, "add_content", return_value=mock_repo_task),
            patch.object(mock_pulp_client, "wait_for_finished_task", return_value=mock_task_response),
            patch("pulp_tool.utils.uploads.logging") as mock_logging,
        ):
            result = upload_rpms(
                ["/path/to/package.rpm"],
                context,
                mock_pulp_client,
                "x86_64",
                rpm_repository_href="/test/rpm-href",
                date="2024-01-01 00:00:00",
                results_model=results_model,
            )

            assert result == []
            assert results_model.uploaded_counts.rpms == 1
            mock_pulp_client.add_content.assert_called_once_with("/test/rpm-href", mock_artifacts)
            # Should not log "Captured X created resources" since created_resources is None
            debug_calls = [str(call) for call in mock_logging.debug.call_args_list]
            assert not any("Captured" in call for call in debug_calls)

    def test_upload_rpms_empty_artifacts(self, mock_pulp_client):
        """Test upload_rpms with empty rpm_results_artifacts (not hitting lines 225-227)."""
        from pulp_tool.models.context import UploadRpmContext
        from pulp_tool.models.results import PulpResultsModel, RepositoryRefs

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        # Mock upload_rpms_parallel to return empty list
        with (
            patch("pulp_tool.utils.uploads.upload_rpms_parallel", return_value=[]),
            patch.object(mock_pulp_client, "add_content") as mock_add_content,
        ):
            result = upload_rpms(
                ["/path/to/package.rpm"],
                context,
                mock_pulp_client,
                "x86_64",
                rpm_repository_href="/test/rpm-href",
                date="2024-01-01 00:00:00",
                results_model=results_model,
            )

            assert result == []
            assert results_model.uploaded_counts.rpms == 1
            # Should not call add_content when artifacts list is empty
            mock_add_content.assert_not_called()
