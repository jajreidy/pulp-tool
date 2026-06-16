"""Tests for pulp_upload.py module."""

import re
from unittest.mock import Mock, patch, mock_open
import httpx
import pytest
from httpx import HTTPError
from pulp_tool.models import PulpResultsModel, RepositoryRefs
from pulp_tool.models.context import UploadRpmContext
from pulp_tool.services.upload_service import upload_sbom
from pulp_tool.utils.file_operations import FileRepositoryBatch
from pulp_tool.utils.pulp_tasks import FileContentUploadResult


class TestUploadSbom:
    """Test upload_sbom function."""

    def test_upload_sbom_success(self, mock_pulp_client, httpx_mock) -> None:
        """Test successful SBOM upload phase 1."""
        httpx_mock.post(re.compile(".*/content/file/files/")).mock(
            return_value=httpx.Response(202, json={"pulp_href": "/content/file/1/"})
        )
        args = Mock()
        args.sbom_path = "/tmp/test.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"
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
        file_batch = FileRepositoryBatch()
        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="test sbom content")),
        ):
            upload_sbom(mock_pulp_client, args, "2024-01-01", results_model, args.sbom_path, file_batch)
        assert file_batch.sbom == ["/content/file/1/"]

    def test_upload_sbom_empty_repository_href_raises(self, mock_pulp_client) -> None:
        """SBOM upload requires a non-empty repository href."""
        args = Mock()
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"
        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)
        file_batch = FileRepositoryBatch()
        with patch("os.path.exists", return_value=True), patch("pulp_tool.services.upload_service.validate_file_path"):
            with pytest.raises(ValueError, match="SBOM repository href is empty"):
                upload_sbom(mock_pulp_client, args, "2024-01-01", results_model, "/tmp/x.json", file_batch)

    def test_upload_sbom_no_signed_by_label(self, mock_pulp_client) -> None:
        """Test upload_sbom does not add signed_by (SBOMs are never signed)."""
        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/rpms",
            sbom_path="/tmp/sbom.json",
            signed_by="key-123",
        )
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
        file_batch = FileRepositoryBatch()
        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("builtins.open", mock_open(read_data="{}")),
            patch(
                "pulp_tool.services.upload_service.upload_file_content",
                return_value=FileContentUploadResult(content_href="/c/1/"),
            ) as mock_upload,
        ):
            upload_sbom(mock_pulp_client, args, "2024-01-01", results_model, "/tmp/sbom.json", file_batch)
        assert "signed_by" not in mock_upload.call_args.kwargs["pulp_label"]

    def test_upload_sbom_distribution_urls_uses_relative_path(self, mock_pulp_client) -> None:
        """When upload result has relative_path, use it for incremental results."""
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/rpms",
            sbom_path="/tmp/sbom.json",
        )
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
        file_batch = FileRepositoryBatch()
        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="{}")),
            patch(
                "pulp_tool.services.upload_service.upload_file_content",
                return_value=FileContentUploadResult(content_href="/c/1/", relative_path="publish/foo/sbom.json"),
            ),
            patch.object(mock_pulp_client, "add_uploaded_artifact_to_results_model") as mock_add,
        ):
            upload_sbom(
                mock_pulp_client,
                context,
                "2024-01-01",
                results_model,
                "/tmp/sbom.json",
                file_batch,
                distribution_urls={"sbom": "https://example.com/sbom/"},
            )
        mock_add.assert_called_once()
        assert mock_add.call_args.kwargs["file_relative_path"] == "publish/foo/sbom.json"

    def test_upload_sbom_file_not_found(self, mock_pulp_client) -> None:
        """Test upload_sbom with file not found."""
        args = Mock()
        args.sbom_path = "/tmp/nonexistent.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"
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
        file_batch = FileRepositoryBatch()
        with patch("os.path.exists", return_value=False):
            upload_sbom(mock_pulp_client, args, "2024-01-01", results_model, args.sbom_path, file_batch)
        assert file_batch.sbom == []

    def test_upload_sbom_upload_error(self, mock_pulp_client, httpx_mock) -> None:
        """Test upload_sbom with upload error."""
        httpx_mock.post(re.compile(".*/content/file/files/")).mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )
        args = Mock()
        args.sbom_path = "/tmp/test.json"
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"
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
        file_batch = FileRepositoryBatch()
        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="test sbom content")),
        ):
            with pytest.raises(HTTPError):
                upload_sbom(mock_pulp_client, args, "2024-01-01", results_model, args.sbom_path, file_batch)
