"""Tests for pulp_upload.py module."""

import pytest
import httpx
from httpx import HTTPError
import re
from unittest.mock import Mock, patch, mock_open
import json

from pulp_tool.services.upload_service import (
    upload_sbom,
    scan_results_json_for_log_and_sbom_keys,
    _build_artifact_map,
    _serialize_results_to_json,
    _save_results_to_folder,
    _upload_and_get_results_url,
    _extract_results_url,
    _handle_artifact_results,
    _handle_sbom_results,
    collect_results,
    process_uploads_from_results_json,
    _classify_artifact_from_key,
    _populate_results_model,
    _add_distributions_to_results,
    _distribution_urls_for_context,
)
from pulp_tool.models import PulpResultsModel, RepositoryRefs
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.models.context import UploadRpmContext, UploadContext
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

            upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model, args.sbom_path)

    def test_upload_sbom_empty_repository_prn_raises(self, mock_pulp_client):
        """SBOM upload requires a non-empty repository PRN."""
        args = Mock()
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

        with patch("os.path.exists", return_value=True), patch("pulp_tool.services.upload_service.validate_file_path"):
            with pytest.raises(ValueError, match="SBOM repository PRN is empty"):
                upload_sbom(mock_pulp_client, args, "", "2024-01-01", results_model, "/tmp/x.json")

    def test_upload_sbom_no_signed_by_label(self, mock_pulp_client):
        """Test upload_sbom does not add signed_by (SBOMs are never signed)."""
        mock_resp = Mock()
        mock_resp.json.return_value = {"task": "/api/v3/tasks/123/"}
        mock_task = Mock()
        mock_task.created_resources = ["/content/1/"]
        mock_pulp_client.create_file_content = Mock(return_value=mock_resp)
        mock_pulp_client.wait_for_finished_task = Mock(return_value=mock_task)
        mock_pulp_client.check_response = Mock()

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

        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("builtins.open", mock_open(read_data="{}")),
        ):
            upload_sbom(mock_pulp_client, args, "sbom-prn", "2024-01-01", results_model, "/tmp/sbom.json")

        call_kw = mock_pulp_client.create_file_content.call_args[1]
        assert "signed_by" not in call_kw["pulp_label"]

    def test_upload_sbom_distribution_urls_uses_task_result_relative_path(self, mock_pulp_client):
        """When task.result has relative_path, use it for incremental results (line 168)."""
        mock_resp = Mock()
        mock_resp.json.return_value = {"task": "/api/v3/tasks/123/"}
        mock_task = Mock()
        mock_task.created_resources = ["/content/file/1/"]
        mock_task.result = {"relative_path": "publish/foo/sbom.json"}
        mock_pulp_client.create_file_content = Mock(return_value=mock_resp)
        mock_pulp_client.wait_for_finished_task = Mock(return_value=mock_task)
        mock_pulp_client.check_response = Mock()

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

        with (
            patch("os.path.exists", return_value=True),
            patch("pulp_tool.services.upload_service.validate_file_path"),
            patch("pulp_tool.services.upload_service.create_labels", return_value={"build_id": "test-build"}),
            patch("builtins.open", mock_open(read_data="{}")),
            patch.object(mock_pulp_client, "add_uploaded_artifact_to_results_model") as mock_add,
        ):
            upload_sbom(
                mock_pulp_client,
                context,
                "sbom-prn",
                "2024-01-01",
                results_model,
                "/tmp/sbom.json",
                distribution_urls={"sbom": "https://example.com/sbom/"},
            )

        mock_add.assert_called_once()
        assert mock_add.call_args.kwargs["file_relative_path"] == "publish/foo/sbom.json"

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
            upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model, args.sbom_path)

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
                upload_sbom(mock_pulp_client, args, "test-repo", "2024-01-01", results_model, args.sbom_path)


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

    def test_build_artifact_map_skips_query_when_no_valid_hrefs(self, mock_pulp_client):
        """Content rows without /artifacts/ hrefs: empty map, no get_file_locations (else branch)."""
        from pulp_tool.models.artifacts import PulpContentRow

        rows = [
            PulpContentRow(pulp_href="/c/1/", artifacts={"k": "/pulp/api/v3/content/units/foo/"}),
            PulpContentRow(pulp_href="/c/2/", artifacts={"k": ""}),
        ]
        with (
            patch.object(mock_pulp_client, "get_file_locations") as mock_get_locs,
            patch("pulp_tool.services.upload_service.logging") as log_mock,
        ):
            result = _build_artifact_map(mock_pulp_client, rows)
        assert result == {}
        mock_get_locs.assert_not_called()
        log_mock.warning.assert_called()

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
        context = UploadRpmContext(
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

        # Mock get_distribution_urls_for_upload_context to return URLs
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {
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
            mock_helper.get_distribution_urls_for_upload_context.assert_called_once_with("test-build", context)
            assert result == "https://example.com/results.json"

    def test_collect_results_incremental_only_when_gather_empty(self, mock_pulp_client):
        """When gather returns no content but the model already has artifacts, still upload results JSON."""
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
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
        results_model.add_artifact("pkg.rpm", "https://example.com/pkg.rpm", "deadbeef", {"build_id": "test-build"})

        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {"artifacts": "https://a/artifacts/"}
            mock_helper_class.return_value = mock_helper

            with patch("pulp_tool.services.upload_service._gather_and_validate_content", return_value=None):
                with patch("pulp_tool.services.upload_service._serialize_results_to_json", return_value="{}"):
                    with patch(
                        "pulp_tool.services.upload_service._upload_and_get_results_url",
                        return_value="https://example.com/out.json",
                    ) as mock_upload:
                        result = collect_results(mock_pulp_client, context, "2024-01-01", results_model)

        assert result == "https://example.com/out.json"
        mock_upload.assert_called_once()
        mock_helper.get_distribution_urls_for_upload_context.assert_called()

    def test_collect_results_saves_to_folder_when_artifact_results_is_folder(
        self, mock_pulp_client, httpx_mock, tmp_path
    ):
        """Test that collect_results saves locally when artifact_results is a folder path."""
        output_dir = tmp_path / "output"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=None,
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

        with patch("pulp_tool.services.upload_service._gather_and_validate_content") as mock_gather:
            mock_gather.return_value = Mock(content_results=[], file_results=[], log_results=[], sbom_results=[])
            with patch("pulp_tool.services.upload_service._build_artifact_map", return_value={}):
                with patch("pulp_tool.services.upload_service._populate_results_model"):
                    with patch("pulp_tool.services.upload_service._add_distributions_to_results"):
                        with patch(
                            "pulp_tool.services.upload_service._serialize_results_to_json",
                            return_value='{"artifacts": {}, "distributions": {}}',
                        ):
                            result = collect_results(mock_pulp_client, context, "2024-01-01", results_model)

        assert result is not None
        assert "pulp_results.json" in result
        assert (output_dir / "pulp_results.json").exists()
        assert (output_dir / "pulp_results.json").read_text() == '{"artifacts": {}, "distributions": {}}'

    def test_collect_results_saves_minimal_when_no_content_and_folder_mode(
        self, mock_pulp_client, httpx_mock, tmp_path
    ):
        """Test that collect_results creates minimal pulp_results.json when no content and folder mode."""
        output_dir = tmp_path / "output"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=None,
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

        with patch("pulp_tool.services.upload_service._gather_and_validate_content", return_value=None):
            with patch("pulp_tool.services.upload_service._add_distributions_to_results"):
                result = collect_results(mock_pulp_client, context, "2024-01-01", results_model)

        assert result is not None
        assert (output_dir / "pulp_results.json").exists()
        content = json.loads((output_dir / "pulp_results.json").read_text())
        assert content["artifacts"] == {}
        assert "distributions" in content


class TestSaveResultsToFolder:
    """Test _save_results_to_folder function."""

    def test_save_results_to_folder_success(self, tmp_path):
        """Test saving results JSON to a folder."""
        output_dir = tmp_path / "results"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=None,
        )
        json_content = '{"artifacts": {}, "distributions": {}}'

        result = _save_results_to_folder(str(output_dir), json_content, context)

        assert result is not None
        assert (output_dir / "pulp_results.json").exists()
        assert (output_dir / "pulp_results.json").read_text() == json_content

    def test_save_results_to_folder_creates_parents(self, tmp_path):
        """Test that nested folder path is created."""
        output_dir = tmp_path / "a" / "b" / "c"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=None,
        )
        json_content = '{"test": true}'

        result = _save_results_to_folder(str(output_dir), json_content, context)

        assert result is not None
        assert (output_dir / "pulp_results.json").exists()

    def test_save_results_to_folder_with_sbom_results(self, tmp_path):
        """Test that sbom_results is handled when saving to folder."""
        output_dir = tmp_path / "results"
        sbom_results_file = tmp_path / "sbom_results.txt"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=str(sbom_results_file),
        )
        json_content = json.dumps(
            {
                "artifacts": {
                    "sbom.json": {"url": "https://pulp.example/sbom.json", "labels": {}},
                },
                "distributions": {},
            }
        )

        result = _save_results_to_folder(str(output_dir), json_content, context)

        assert result is not None
        assert (output_dir / "pulp_results.json").exists()
        assert sbom_results_file.exists()
        assert sbom_results_file.read_text() == "https://pulp.example/sbom.json"

    def test_save_results_to_folder_handles_io_error(self, tmp_path):
        """Test that OSError/IOError during save returns None."""
        output_dir = tmp_path / "results"
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path=None,
            artifact_results=str(output_dir),
            sbom_results=None,
        )
        json_content = '{"artifacts": {}, "distributions": {}}'

        with patch("builtins.open", side_effect=IOError("Permission denied")):
            result = _save_results_to_folder(str(output_dir), json_content, context)

        assert result is None


class TestHandleArtifactResults:
    """Test _handle_artifact_results function."""

    def test_handle_artifact_results_success(self, mock_pulp_client, httpx_mock, tmp_path):
        """Test successful artifact results handling."""
        httpx_mock.get(re.compile(r".*/content/\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(r".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]})
        )

        # Use temporary file paths
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"

        # Create proper UploadContext instead of Mock
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results=f"{url_path},{digest_path}",
        )

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/content/"],
            result={"relative_path": "test.txt"},
        )

        with patch("builtins.open", mock_open()) as mock_file:
            _handle_artifact_results(mock_pulp_client, context, task_response)

            assert mock_file.call_count == 2

    def test_handle_artifact_results_no_content(self, mock_pulp_client, tmp_path):
        """Test artifact results handling with no content."""
        # Use temporary file paths
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"

        # Create proper UploadContext instead of Mock
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results=f"{url_path},{digest_path}",
        )

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/other/"],
            result={"relative_path": "test.txt"},
        )

        _handle_artifact_results(mock_pulp_client, context, task_response)

    def test_handle_artifact_results_invalid_format(self, mock_pulp_client, httpx_mock):
        """Test artifact results handling with invalid format."""
        httpx_mock.get(re.compile(r".*/content/\?pulp_href__in=")).mock(
            return_value=httpx.Response(200, json={"results": [{"artifacts": {"file": "/test/artifacts/"}}]})
        )
        httpx_mock.get(re.compile(r".*/artifacts/.*")).mock(
            return_value=httpx.Response(200, json={"results": [{"file": "test.txt", "sha256": "abc123"}]})
        )

        # Create proper UploadContext instead of Mock
        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            artifact_results="invalid_format",
        )

        # Now task_response is a TaskResponse model
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/test/content/"],
            result={"relative_path": "test.txt"},
        )

        _handle_artifact_results(mock_pulp_client, context, task_response)

    def test_handle_artifact_results_no_distribution_url(self, mock_pulp_client, httpx_mock, tmp_path):
        """Test artifact results handling when no distribution URL found."""
        # Use temporary file paths
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"

        # Mock PulpHelper to return empty distribution_urls
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {}  # No artifacts key
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

            with patch("pulp_tool.services.upload_service.logging") as mock_logging:
                _handle_artifact_results(mock_pulp_client, context, task_response)
                # Should log error about no distribution URL
                mock_logging.error.assert_called()

    def test_handle_artifact_results_no_relative_path(self, mock_pulp_client, httpx_mock, tmp_path):
        """Test artifact results handling when task response has no relative_path."""
        # Use temporary file paths
        url_path = tmp_path / "url.txt"
        digest_path = tmp_path / "digest.txt"

        # Mock PulpHelper to return distribution_urls
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
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

            # Task response without relative_path
            task_response = TaskResponse(
                pulp_href="/api/v3/tasks/123/",
                state="completed",
                created_resources=["/test/content/"],
                result={},  # No relative_path
            )

            with patch("pulp_tool.services.upload_service.logging") as mock_logging:
                _handle_artifact_results(mock_pulp_client, context, task_response)
                # Should log error about no relative_path
                mock_logging.error.assert_called()


class TestFindArtifactContent:
    """Test _find_artifact_content function."""

    def test_find_artifact_content_no_artifacts_dict(self, mock_pulp_client):
        """Test _find_artifact_content when artifacts_dict is empty."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        # Create a task response with content href
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
        )

        # Mock find_content to return content with empty artifacts
        mock_response = Mock(spec=httpx.Response)
        mock_response.json.return_value = {"results": [{"artifacts": {}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_response)

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_non_dict_artifacts(self, mock_pulp_client):
        """Test _find_artifact_content when artifacts is not a dict."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        # Create a task response with content href
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
        )

        # Mock find_content to return content with None artifacts
        mock_response = Mock(spec=httpx.Response)
        mock_response.json.return_value = {"results": [{"artifacts": None}]}
        mock_pulp_client.find_content = Mock(return_value=mock_response)

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_no_file_value(self, mock_pulp_client, httpx_mock):
        """Test _find_artifact_content when artifact response has no file value."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        # Create a task response with content href
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
        )

        # Mock find_content to return content with artifacts
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)

        # Mock get_file_locations to return response without file value
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"sha256": "abc123"}]}  # No file key
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_no_sha256_value(self, mock_pulp_client, httpx_mock):
        """Test _find_artifact_content when artifact response has no sha256 value."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        # Create a task response with content href
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
        )

        # Mock find_content to return content with artifacts
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)

        # Mock get_file_locations to return response without sha256 value
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"file": "test.txt@sha256:abc123"}]}  # No sha256 key
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            result = _find_artifact_content(mock_pulp_client, task_response)
            assert result is None
            mock_logging.error.assert_called()

    def test_find_artifact_content_success(self, mock_pulp_client, httpx_mock):
        """Test _find_artifact_content successful path."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        # Create a task response with content href
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
        )

        # Mock find_content to return content with artifacts
        mock_content_response = Mock(spec=httpx.Response)
        mock_content_response.json.return_value = {"results": [{"artifacts": {"test.txt": "/api/v3/artifacts/12345/"}}]}
        mock_pulp_client.find_content = Mock(return_value=mock_content_response)

        # Mock get_file_locations to return valid response
        mock_artifact_response = Mock(spec=httpx.Response)
        mock_artifact_response.json.return_value = {"results": [{"file": "test.txt@sha256:abc123", "sha256": "abc123"}]}
        mock_pulp_client.get_file_locations = Mock(return_value=mock_artifact_response)

        result = _find_artifact_content(mock_pulp_client, task_response)

        assert result is not None
        assert result[0] == "test.txt@sha256:abc123"
        assert result[1] == "abc123"

    def test_find_artifact_content_bare_list_json(self, mock_pulp_client, httpx_mock):
        """find_content JSON may be a list of content objects instead of paginated dict."""
        from pulp_tool.services.upload_service import _find_artifact_content
        from pulp_tool.models.pulp_api import TaskResponse

        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/content/file/files/12345/"],
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

    def test_parse_oci_reference_with_digest(self):
        """Test _parse_oci_reference with digest."""
        from pulp_tool.services.upload_service import _parse_oci_reference

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            image_url, digest = _parse_oci_reference("quay.io/org/repo@sha256:abc123")
            assert image_url == "quay.io/org/repo"
            assert digest == "sha256:abc123"
            mock_logging.debug.assert_called()

    def test_parse_oci_reference_without_digest(self):
        """Test _parse_oci_reference without digest."""
        from pulp_tool.services.upload_service import _parse_oci_reference

        with patch("pulp_tool.services.upload_service.logging") as mock_logging:
            image_url, digest = _parse_oci_reference("quay.io/org/repo")
            assert image_url == "quay.io/org/repo"
            assert digest == ""
            mock_logging.debug.assert_called()


class TestDistributionUrlsForContext:
    """Tests for _distribution_urls_for_context helper."""

    def test_signed_by_requests_include_signed_rpm_distro(self):
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


class TestBuildResultsStructure:
    """Test _populate_results_model function."""

    def test_build_results_structure(self, mock_pulp_client):
        """Test _populate_results_model function (lines 364-367)."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs
        from pulp_tool.models.context import UploadContext

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

        from pulp_tool.models.artifacts import FileInfoModel, PulpContentRow

        content_results = [
            PulpContentRow.model_validate({"pulp_href": "/content/123/", "artifacts": {"test.txt": "/artifacts/123/"}})
        ]
        # Create a proper FileInfoModel instance

        file_info_map: dict[str, FileInfoModel] = {
            "/artifacts/123/": FileInfoModel(
                pulp_href="/artifacts/123/",
                file="test.txt@sha256:abc",
                sha256="abc",
            )
        }

        context = UploadContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
        )

        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {
                "rpms": "https://example.com/rpms/",
                "logs": "https://example.com/logs/",
            }
            mock_helper_class.return_value = mock_helper

            mock_pulp_client.build_results_structure = Mock()

            _populate_results_model(mock_pulp_client, results_model, content_results, file_info_map, context)

            # Verify build_results_structure was called with correct arguments
            mock_pulp_client.build_results_structure.assert_called_once()
            call_args = mock_pulp_client.build_results_structure.call_args
            assert call_args[0][0] == results_model
            assert call_args[0][1] == content_results
            assert call_args[0][2] == file_info_map
            mock_helper_class.assert_called_once_with(mock_pulp_client, parent_package=context.parent_package)
            mock_helper.get_distribution_urls_for_upload_context.assert_called_once_with(context.build_id, context)
            assert call_args.kwargs.get("merge") is True

    def test_populate_results_model_target_arch_repo_uses_flagged_distribution_urls(self, mock_pulp_client):
        """With target_arch_repo, distribution URLs come from get_distribution_urls_for_upload_context."""
        from pulp_tool.models.artifacts import FileInfoModel

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
        content_results: list = []
        file_info_map: dict[str, FileInfoModel] = {}
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            target_arch_repo=True,
        )
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {"logs": "https://example.com/logs/"}
            mock_helper_class.return_value = mock_helper
            mock_pulp_client.build_results_structure = Mock()
            _populate_results_model(mock_pulp_client, results_model, content_results, file_info_map, context)
            mock_helper.get_distribution_urls_for_upload_context.assert_called_once_with("test-build", context)
            mock_pulp_client.build_results_structure.assert_called_once()
            assert mock_pulp_client.build_results_structure.call_args.kwargs["target_arch_repo"] is True

    def test_add_distributions_to_results_target_arch_repo(self, mock_pulp_client):
        """With target_arch_repo, per-arch RPM distribution URLs are added from artifact arch labels."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)
        results_model.add_artifact(
            "pkg.rpm",
            "https://example.com/pkg.rpm",
            "deadbeef",
            {"arch": "x86_64", "build_id": "test-build", "namespace": "test-namespace"},
        )
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            target_arch_repo=True,
        )
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {"logs": "https://example.com/logs/"}
            mock_helper.distribution_url_for_base_path.return_value = (
                "https://pulp.example.com/api/pulp-content/test-namespace/x86_64/"
            )
            mock_helper_class.return_value = mock_helper
            _add_distributions_to_results(mock_pulp_client, context, results_model)
            mock_helper.get_distribution_urls_for_upload_context.assert_called_once_with("test-build", context)
            mock_helper.distribution_url_for_base_path.assert_called_once_with("x86_64")
            assert results_model.distributions["rpm_x86_64"] == (
                "https://pulp.example.com/api/pulp-content/test-namespace/x86_64/"
            )

    def test_add_distributions_to_results_warns_when_no_distribution_urls(self, mock_pulp_client, caplog):
        """When no build-scoped distribution URLs are returned, log a warning."""
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-namespace",
            parent_package="test-package",
            target_arch_repo=False,
        )
        with patch("pulp_tool.services.upload_service.PulpHelper") as mock_helper_class:
            mock_helper = Mock()
            mock_helper.get_distribution_urls_for_upload_context.return_value = {}
            mock_helper_class.return_value = mock_helper
            with caplog.at_level(logging.WARNING):
                _add_distributions_to_results(mock_pulp_client, context, results_model)
            assert "No distribution URLs found" in caplog.text


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

        # Create proper UploadRpmContext instead of Namespace
        args = UploadRpmContext(
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

        args = UploadRpmContext(
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

        args = UploadRpmContext(
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


class TestClassifyArtifactFromKey:
    """Test _classify_artifact_from_key function."""

    def test_classify_rpm(self):
        """Test RPM classification."""
        assert _classify_artifact_from_key("x86_64/pkg.rpm") == "rpms"
        assert _classify_artifact_from_key("pkg.RPM") == "rpms"

    def test_classify_log(self):
        """Test log classification."""
        assert _classify_artifact_from_key("x86_64/state.log") == "logs"
        assert _classify_artifact_from_key("build.LOG") == "logs"

    def test_classify_sbom(self):
        """Test SBOM classification."""
        assert _classify_artifact_from_key("sbom.json") == "sbom"
        assert _classify_artifact_from_key("sbom.spdx") == "sbom"
        assert _classify_artifact_from_key("sbom.spdx.json") == "sbom"
        assert _classify_artifact_from_key("path/to/sbom-file.json") == "sbom"
        # SBOM by extension only (no "sbom" in key) - hits SBOM_EXTENSIONS loop
        assert _classify_artifact_from_key("cyclonedx.json") == "sbom"
        assert _classify_artifact_from_key("manifest.spdx") == "sbom"

    def test_classify_artifacts(self):
        """Test generic artifact classification."""
        assert _classify_artifact_from_key("other.tar.gz") == "artifacts"
        assert _classify_artifact_from_key("data.txt") == "artifacts"


class TestScanResultsJsonForLogAndSbomKeys:
    """Tests for scan_results_json_for_log_and_sbom_keys."""

    def test_detects_log_and_sbom_keys(self, tmp_path):
        """Artifact keys classified as logs and sbom set both flags."""
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"aarch64/build.log": {}, "report.spdx.json": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (True, True)

    def test_logs_only(self, tmp_path):
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"foo.log": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (True, False)

    def test_sbom_only(self, tmp_path):
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"manifest-without-sbom-in-name.json": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, True)

    def test_invalid_json_returns_false_false(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not valid json")
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, False)

    def test_non_dict_artifacts_returns_false_false(self, tmp_path):
        """``artifacts`` must be a dict; a truthy non-dict (e.g. list) hits the guard branch."""
        p = tmp_path / "r.json"
        # [] is falsy: ``data.get("artifacts") or {}`` would become {} and skip the guard.
        p.write_text(json.dumps({"artifacts": ["not-a-dict-entry"]}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, False)


class TestProcessUploadsFromResultsJson:
    """Test process_uploads_from_results_json function."""

    def test_raises_when_logs_present_but_logs_repo_skipped(self, tmp_path, mock_pulp_client):
        """Guard: log uploads require logs_prn."""
        arch_dir = tmp_path / "x86_64"
        arch_dir.mkdir()
        log_path = arch_dir / "build.log"
        log_path.write_text("log line")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/build.log": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            skip_logs_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="/rpm",
            rpms_prn="rprn",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with pytest.raises(ValueError, match="logs repository was not created"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_raises_when_sbom_present_but_sbom_repo_skipped(self, tmp_path, mock_pulp_client):
        sbom_path = tmp_path / "sbom.spdx.json"
        sbom_path.write_text("{}")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"sbom.spdx.json": {}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            skip_sbom_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="/rpm",
            rpms_prn="rprn",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with pytest.raises(ValueError, match="SBOM repository was not created"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_basic(self, tmp_path, mock_pulp_client):
        """Test upload from JSON without signed-by."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {
            "artifacts": {
                "x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}},
            }
        }
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_with_signed_by(self, tmp_path, mock_pulp_client):
        """Test upload with signed-by uses signed repos and labels."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="/test/rpm-signed-href",
            rpms_signed_prn="rpms-signed-prn",
            logs_signed_prn="logs-signed-prn",
            sbom_signed_prn="sbom-signed-prn",
            artifacts_signed_prn="artifacts-signed-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        call_kw = mock_upload_rpms.call_args[1]
        assert call_kw["rpm_repository_href"] == "/test/rpm-signed-href"
        call_context = mock_upload_rpms.call_args[0][1]
        assert call_context.signed_by == "key-123"

    def test_upload_from_results_json_target_arch_repo_uses_ensure_per_arch(self, tmp_path, mock_pulp_client):
        """target_arch_repo sets bulk rpm_href empty and uses ensure_rpm_repository_for_arch per arch."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"x")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            target_arch_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )
        mock_ph_instance = Mock()
        mock_ph_instance.ensure_rpm_repository_for_arch.return_value = "/per-arch/rpm"

        with (
            patch("pulp_tool.services.upload_service.PulpHelper", return_value=mock_ph_instance),
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/r/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/r.json"),
        ):
            result = process_uploads_from_results_json(
                mock_pulp_client, context, repositories, pulp_helper=mock_ph_instance
            )

        assert result == "https://example.com/r.json"
        mock_ph_instance.ensure_rpm_repository_for_arch.assert_called_once_with("test-build", "x86_64")
        assert mock_upload_rpms.call_args.kwargs["rpm_repository_href"] == "/per-arch/rpm"

    def test_upload_from_results_json_files_base_path(self, tmp_path, mock_pulp_client):
        """Test override base path for resolving artifact keys."""
        base_dir = tmp_path / "files"
        base_dir.mkdir()
        rpm_file = base_dir / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            files_base_path=str(base_dir),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_missing_file(self, tmp_path, mock_pulp_client, caplog):
        """Test handling of missing file - skip with warning."""
        results_json_path = tmp_path / "pulp_results.json"
        results_data = {
            "artifacts": {
                "x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}},
            }
        }
        results_json_path.write_text(json.dumps(results_data))
        # No actual file at tmp_path/x86_64/pkg.rpm

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=[]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            caplog.at_level(logging.WARNING),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        assert "Skipping missing file" in caplog.text

    def test_upload_from_results_json_empty_artifacts(self, tmp_path, mock_pulp_client):
        """Test empty artifacts creates minimal results."""
        results_json_path = tmp_path / "pulp_results.json"
        results_data: dict = {"artifacts": {}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with patch(
            "pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_returns_none_when_no_results_json(self, mock_pulp_client):
        """Test returns None when results_json is not set."""
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=None,
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        result = process_uploads_from_results_json(mock_pulp_client, context, repositories)
        assert result is None

    def test_upload_from_results_json_json_read_error(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json raises when JSON cannot be read."""
        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text('{"artifacts": {"x": {}}}')

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with patch("builtins.open", side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_signed_by_no_signed_repos(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json raises when signed_by set but no signed repos."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="",
            rpms_signed_prn="",
        )

        with pytest.raises(ValueError, match="signed_by requires signed repositories"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_invalid_artifact_entry(self, tmp_path, mock_pulp_client, caplog):
        """Test process_uploads_from_results_json skips non-dict artifact entries."""
        rpm_file = tmp_path / "pkg.rpm"
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps({"artifacts": {"pkg.rpm": {"labels": {"arch": "noarch"}}, "bad": "not a dict"}})
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            caplog.at_level(logging.WARNING),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        assert "Skipping invalid artifact entry" in caplog.text

    def test_upload_from_results_json_log_and_sbom_and_artifact(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json with log, sbom, and generic artifact."""
        log_file = tmp_path / "x86_64" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log content")

        sbom_file = tmp_path / "sbom.json"
        sbom_file.write_text('{"sbom": true}')

        artifact_file = tmp_path / "data.txt"
        artifact_file.write_text("data")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "x86_64/state.log": {"labels": {"arch": "x86_64"}},
                        "sbom.json": {"labels": {}},
                        "data.txt": {"labels": {}},
                    }
                }
            )
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        mock_resp = Mock()
        mock_resp.json.return_value = {"task": "/tasks/1/"}
        mock_task = Mock()
        mock_task.created_resources = ["/content/1/"]
        mock_pulp_client.create_file_content = Mock(return_value=mock_resp)
        mock_pulp_client.wait_for_finished_task = Mock(return_value=mock_task)
        mock_pulp_client.check_response = Mock()

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_arch_inference_from_path(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers arch from path when not in labels."""
        rpm_file = tmp_path / "aarch64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"aarch64/pkg.rpm": {"labels": {}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        assert mock_upload_rpms.call_args[0][3] == "aarch64"

    def test_upload_from_results_json_log_arch_inference_from_path(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers log arch from path (lines 290-291)."""
        log_file = tmp_path / "s390x" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"s390x/state.log": {"labels": {}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]) as mock_upload_log,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_log.assert_called_once()
        assert mock_upload_log.call_args[1]["arch"] == "s390x"

    def test_upload_from_results_json_arch_inference_noarch(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers noarch when no arch in path (lines 283, 287-293)."""
        rpm_file = tmp_path / "pkg.rpm"
        rpm_file.write_bytes(b"fake rpm")
        log_file = tmp_path / "state.log"
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {"labels": {}},
                        "state.log": {"labels": {}},
                    }
                }
            )
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]) as mock_upload_rpms,
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        assert mock_upload_rpms.call_args[0][3] == "noarch"

    def test_upload_from_results_json_log_uses_unsigned_repo_with_signed_by(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json uses unsigned logs repo when signed_by set (logs never signed)."""
        log_file = tmp_path / "x86_64" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/state.log": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="/test/rpm-signed-href",
            rpms_signed_prn="rpms-signed-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]) as mock_upload_log,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_log.assert_called_once()
        call_kw = mock_upload_log.call_args[1]
        assert "signed_by" not in call_kw["labels"]
        assert mock_upload_log.call_args[0][1] == "logs-prn"
