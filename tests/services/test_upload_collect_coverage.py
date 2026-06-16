"""Targeted tests for ``upload_collect`` branches required for 100% PR diff coverage."""

import json
from typing import Any
from unittest.mock import Mock, patch

import httpx
import pytest

from pulp_tool.models.artifacts import ContentData, FileInfoModel, PulpContentRow
from pulp_tool.models.context import UploadContext
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.utils.file_operations import FileRepositoryBatch
from pulp_tool.utils.pulp_tasks import FileContentUploadResult
from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.models.results import PulpResultsModel
from pulp_tool.services import upload_collect as uc


def _minimal_context(**kwargs: Any) -> UploadContext:
    base: dict[str, Any] = {
        "build_id": "b1",
        "date_str": "2024-01-01",
        "namespace": "ns",
        "parent_package": "pkg",
    }
    base.update(kwargs)
    return UploadContext(**base)


def _minimal_refs() -> RepositoryRefs:
    return RepositoryRefs(
        rpms_href="/r/",
        rpms_prn="rp",
        logs_href="/l/",
        logs_prn="lp",
        sbom_href="/s/",
        sbom_prn="sp",
        artifacts_href="/a/",
        artifacts_prn="ap",
    )


class TestUploadAndExtract:
    def test_upload_and_get_results_url_success_paths(self, mock_pulp_client: Mock) -> None:
        """Happy path: upload, flush artifacts, extract URL."""
        upload_result = FileContentUploadResult(content_href="/content/file/1/", relative_path="pulp_results.json")
        ctx = _minimal_context(artifact_results=None, sbom_results="/tmp/sbom_out")
        batch = FileRepositoryBatch()
        with (
            patch.object(uc, "upload_file_content", return_value=upload_result),
            patch.object(batch, "flush_artifacts", return_value=[]),
            patch.object(uc, "_extract_results_url", return_value="https://example.com/results.json"),
            patch.object(uc, "_handle_sbom_results") as mock_sbom,
        ):
            out = uc._upload_and_get_results_url(
                mock_pulp_client, ctx, "{}", "2024-01-01", batch, _minimal_refs().artifacts_href
            )
        assert out == "https://example.com/results.json"
        mock_sbom.assert_called_once()

    def test_upload_and_get_results_url_calls_handle_artifact_results(self, mock_pulp_client: Mock) -> None:
        upload_result = FileContentUploadResult(content_href="/c/1/", relative_path="x.json")
        ctx = _minimal_context(artifact_results="/u,/d")
        batch = FileRepositoryBatch()
        with (
            patch.object(uc, "upload_file_content", return_value=upload_result),
            patch.object(batch, "flush_artifacts", return_value=[]),
            patch.object(uc, "_extract_results_url", return_value="https://u/x.json"),
            patch.object(uc, "_handle_artifact_results") as mock_h,
        ):
            uc._upload_and_get_results_url(
                mock_pulp_client, ctx, "{}", "2024-01-01", batch, _minimal_refs().artifacts_href
            )
        mock_h.assert_called_once_with(mock_pulp_client, ctx, "x.json")

    def test_upload_and_get_results_url_failure_logs_traceback(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context()
        batch = FileRepositoryBatch()
        with (
            patch.object(uc, "upload_file_content", side_effect=RuntimeError("boom")),
            patch("pulp_tool.services.upload_collect.logging") as log_mock,
            pytest.raises(RuntimeError, match="boom"),
        ):
            uc._upload_and_get_results_url(
                mock_pulp_client, ctx, "{}", "2024-01-01", batch, _minimal_refs().artifacts_href
            )
        log_mock.error.assert_called()

    def test_extract_results_url_raises_without_artifacts_distribution(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context()
        with patch.object(uc, "PulpHelper") as PH:
            PH.return_value.get_distribution_urls.return_value = {"logs": "https://l/"}
            with pytest.raises(ValueError, match="No distribution URL found for artifacts"):
                uc._extract_results_url(mock_pulp_client, ctx, "x.json")

    def test_extract_results_url_builds_final_url(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context()
        with patch.object(uc, "PulpHelper") as PH:
            PH.return_value.get_distribution_urls.return_value = {"artifacts": "https://a/"}
            url = uc._extract_results_url(mock_pulp_client, ctx, "pulp_results.json")
        assert url == "https://a/pulp_results.json"


class TestGatherAndBuildMap:
    def test_gather_and_validate_content_empty(self) -> None:
        client = Mock()
        client.gather_content_data.return_value = ContentData(content_results=[])
        ctx = _minimal_context()
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            assert uc._gather_and_validate_content(client, ctx, None) is None
        log_mock.error.assert_called()

    def test_gather_and_validate_content_success(self) -> None:
        client = Mock()
        rows = [PulpContentRow(pulp_href="/c/1/", artifacts={})]
        client.gather_content_data.return_value = ContentData(content_results=rows)
        ctx = _minimal_context()
        data = uc._gather_and_validate_content(client, ctx, None)
        assert data is not None
        assert len(data.content_results) == 1

    def test_build_artifact_map_queries_when_hrefs_present(self) -> None:
        client = Mock()
        rows = [
            PulpContentRow(pulp_href="/c/", artifacts={"k": "https://pulp/api/v3/content/artifacts/1/"}),
        ]
        client.get_file_locations.return_value = httpx.Response(
            200,
            json={
                "results": [
                    {
                        "pulp_href": "https://pulp/api/v3/content/artifacts/1/",
                        "file": "f",
                        "sha256": "abc",
                    }
                ]
            },
        )
        m = uc._build_artifact_map(client, rows)
        assert len(m) == 1
        assert isinstance(next(iter(m.values())), FileInfoModel)

    def test_build_artifact_map_no_hrefs_warning(self) -> None:
        client = Mock()
        rows = [PulpContentRow(pulp_href="/c/", artifacts={"k": "/api/v3/content/units/foo/"})]
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            assert uc._build_artifact_map(client, rows) == {}
        log_mock.warning.assert_called()


class TestCollectResultsBranches:
    def test_collect_results_incremental_when_gather_empty(self, mock_pulp_client: Mock) -> None:
        """Lines 279-286: no content_data but model already has artifacts."""
        ctx = _minimal_context()
        refs = _minimal_refs()
        model = PulpResultsModel(build_id="b1", repositories=refs)
        model.add_artifact("a.rpm", "https://x", "dead", {"build_id": "b1"})
        with (
            patch.object(uc, "_gather_and_validate_content", return_value=None),
            patch.object(uc, "_add_distributions_to_results"),
            patch.object(uc, "_serialize_results_to_json", return_value="{}"),
            patch.object(uc, "_upload_and_get_results_url", return_value="https://out") as up,
        ):
            out = uc.collect_results(mock_pulp_client, ctx, "2024-01-01", model)
        assert out == "https://out"
        up.assert_called_once()

    def test_collect_results_returns_none_when_no_content_no_model(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context()
        model = PulpResultsModel(build_id="b1", repositories=_minimal_refs())
        with patch.object(uc, "_gather_and_validate_content", return_value=None):
            assert uc.collect_results(mock_pulp_client, ctx, "2024-01-01", model) is None


class TestFindArtifactContent:
    def test_no_content_in_created_resources(self, mock_pulp_client: Mock) -> None:
        tr = TaskResponse(pulp_href="/t/", state="completed", created_resources=["/api/v3/foo/"])
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            assert uc._find_artifact_content(mock_pulp_client, tr) is None
        log_mock.error.assert_called()

    def test_find_content_returns_empty(self) -> None:
        client = Mock()
        tr = TaskResponse(pulp_href="/t/", state="completed", created_resources=["/api/v3/content/x/"])
        client.find_content.return_value = httpx.Response(200, json={"results": []})
        with patch("pulp_tool.services.upload_collect.content_find_results_from_response", return_value=None):
            with patch("pulp_tool.services.upload_collect.logging") as log_mock:
                assert uc._find_artifact_content(client, tr) is None
        log_mock.error.assert_called()


class TestHandleArtifactResults:
    def test_skips_when_no_artifact_results_config(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context(artifact_results=None)
        with patch.object(uc, "PulpHelper") as PH:
            PH.return_value.get_distribution_urls.return_value = {"artifacts": "https://a/"}
            with patch("pulp_tool.services.upload_collect.logging") as log_mock:
                uc._handle_artifact_results(mock_pulp_client, ctx, "x.json")
        log_mock.debug.assert_called()

    def test_invalid_artifact_results_pair(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context(artifact_results="only-one-path")
        with patch.object(uc, "PulpHelper") as PH:
            PH.return_value.get_distribution_urls.return_value = {"artifacts": "https://a/"}
            with patch("pulp_tool.services.upload_collect.logging") as log_mock:
                uc._handle_artifact_results(mock_pulp_client, ctx, "x.json")
        log_mock.error.assert_called()

    def test_parse_oci_raises(self, mock_pulp_client: Mock) -> None:
        ctx = _minimal_context(artifact_results="/u,/d")
        with (
            patch.object(uc, "PulpHelper") as PH,
            patch.object(uc, "_parse_oci_reference", side_effect=ValueError("bad oci")),
        ):
            PH.return_value.get_distribution_urls.return_value = {"artifacts": "https://a/"}
            with patch("pulp_tool.services.upload_collect.logging") as log_mock:
                uc._handle_artifact_results(mock_pulp_client, ctx, "x.json")
        log_mock.error.assert_called()


class TestHandleSbomResults:
    def test_no_sbom_in_json_info_only(self) -> None:
        ctx = _minimal_context(sbom_results="/tmp/s")
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            uc._handle_sbom_results(Mock(), ctx, json.dumps({"artifacts": {"x.rpm": {"url": "u"}}}))
        log_mock.info.assert_called()

    def test_skip_when_sbom_results_path_missing(self) -> None:
        ctx = _minimal_context(sbom_results=None)
        body = json.dumps({"artifacts": {"sbom.json": {"labels": {}, "url": "https://sbom"}}})
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            uc._handle_sbom_results(Mock(), ctx, body)
        log_mock.debug.assert_called()

    def test_writes_sbom_url_file(self, tmp_path) -> None:
        out = tmp_path / "sbom.url"
        ctx = _minimal_context(sbom_results=str(out))
        body = json.dumps({"artifacts": {"sbom.json": {"labels": {}, "url": "https://sbom/x"}}})
        uc._handle_sbom_results(Mock(), ctx, body)
        assert out.read_text() == "https://sbom/x"

    def test_invalid_json_logs_error(self) -> None:
        ctx = _minimal_context(sbom_results="/tmp/x")
        with patch("pulp_tool.services.upload_collect.logging") as log_mock:
            uc._handle_sbom_results(Mock(), ctx, "{ not json")
        log_mock.error.assert_called()

    def test_ioerror_on_write(self, tmp_path) -> None:
        out = tmp_path / "sbom.url"
        ctx = _minimal_context(sbom_results=str(out))
        body = json.dumps({"artifacts": {"sbom.json": {"labels": {}, "url": "https://u"}}})
        with patch("builtins.open", side_effect=OSError("denied")):
            with patch("pulp_tool.services.upload_collect.logging") as log_mock:
                uc._handle_sbom_results(Mock(), ctx, body)
        log_mock.error.assert_called()
