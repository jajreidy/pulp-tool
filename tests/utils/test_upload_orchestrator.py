"""Tests for UploadOrchestrator class."""

import os
import tempfile
from concurrent.futures import Future
from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest

from pulp_tool.models.context import UploadContext
from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.models.results import PulpResultsModel
from pulp_tool.utils.upload_orchestrator import UploadOrchestrator


class TestUploadOrchestratorFindExistingArchitectures:
    """Tests for UploadOrchestrator._find_existing_architectures() method."""

    def test_find_existing_architectures_with_existing(self):
        """Test _find_existing_architectures finds existing architectures (lines 46-50)."""
        orchestrator = UploadOrchestrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create architecture directories
            os.makedirs(os.path.join(tmpdir, "x86_64"))
            os.makedirs(os.path.join(tmpdir, "aarch64"))

            result = orchestrator._find_existing_architectures(tmpdir)

            assert "x86_64" in result
            assert "aarch64" in result

    def test_find_existing_architectures_skips_non_existent(self):
        """Test _find_existing_architectures skips non-existent paths (lines 52-53)."""
        orchestrator = UploadOrchestrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create only one architecture directory
            os.makedirs(os.path.join(tmpdir, "x86_64"))

            with patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging:
                result = orchestrator._find_existing_architectures(tmpdir)

                assert "x86_64" in result
                # Should log debug for skipped architectures
                debug_calls = [str(call) for call in mock_logging.debug.call_args_list]
                assert any("Skipping" in str(call) for call in debug_calls)

    def test_find_existing_architectures_empty(self):
        """Test _find_existing_architectures with no existing architectures."""
        orchestrator = UploadOrchestrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            result = orchestrator._find_existing_architectures(tmpdir)

            assert result == []


class TestUploadOrchestratorSubmitArchitectureTasks:
    """Tests for UploadOrchestrator._submit_architecture_tasks() method."""

    def test_submit_architecture_tasks(self):
        """Test _submit_architecture_tasks submits tasks (lines 84-87, 98-99)."""
        orchestrator = UploadOrchestrator()

        mock_executor = Mock()
        mock_future1 = Mock()
        mock_future2 = Mock()
        # Return different futures for each call
        mock_executor.submit.side_effect = [mock_future1, mock_future2]

        existing_archs = ["x86_64", "aarch64"]
        rpm_path = "/test/rpms"
        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path=rpm_path,
            sbom_path="/test/sbom.json",
        )
        mock_client = Mock()
        rpm_href = "/test/rpm-href"
        logs_prn = "logs-prn"
        date_str = "2024-01-01"
        results_model = PulpResultsModel(
            build_id="test-build",
            repositories=RepositoryRefs(
                rpms_href="",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            ),
        )

        with patch("pulp_tool.utils.upload_orchestrator.upload_rpms_logs"):
            future_to_arch = orchestrator._submit_architecture_tasks(
                mock_executor, existing_archs, rpm_path, args, mock_client, rpm_href, logs_prn, date_str, results_model
            )

            assert len(future_to_arch) == 2
            assert mock_executor.submit.call_count == 2
            assert mock_future1 in future_to_arch
            assert mock_future2 in future_to_arch


class TestUploadOrchestratorCollectArchitectureResults:
    """Tests for UploadOrchestrator._collect_architecture_results() method."""

    def test_collect_architecture_results_success(self):
        """Test _collect_architecture_results collects results successfully (lines 114-120, 124)."""
        orchestrator = UploadOrchestrator()

        mock_future1: Future[Any] = Future()
        mock_future2: Future[Any] = Future()
        mock_result1 = Mock()
        mock_result1.uploaded_rpms = 5
        mock_result1.created_resources = ["/resource/1", "/resource/2"]
        mock_result2 = Mock()
        mock_result2.uploaded_rpms = 3
        mock_result2.created_resources = ["/resource/3"]

        # Set results for futures
        mock_future1.set_result(mock_result1)
        mock_future2.set_result(mock_result2)

        future_to_arch = {mock_future1: "x86_64", mock_future2: "aarch64"}

        with patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging:
            result = orchestrator._collect_architecture_results(future_to_arch)

            assert "x86_64" in result
            assert "aarch64" in result
            assert result["x86_64"]["uploaded_rpms"] == 5
            assert result["aarch64"]["uploaded_rpms"] == 3
            assert len(result["x86_64"]["created_resources"]) == 2
            assert len(result["aarch64"]["created_resources"]) == 1
            # Verify debug logging
            mock_logging.debug.assert_called()

    def test_collect_architecture_results_exception(self):
        """Test _collect_architecture_results handles exceptions (lines 129-132)."""
        orchestrator = UploadOrchestrator()

        mock_future: Future[Any] = Future()
        mock_future.set_exception(ValueError("Upload failed"))

        future_to_arch = {mock_future: "x86_64"}

        with (
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
            patch("pulp_tool.utils.upload_orchestrator.traceback") as mock_traceback,
        ):
            with pytest.raises(ValueError, match="Upload failed"):
                orchestrator._collect_architecture_results(future_to_arch)

            # Verify error logging
            mock_logging.error.assert_called()
            mock_traceback.format_exc.assert_called()

    def test_collect_architecture_results_logs_processed(self):
        """Test _collect_architecture_results logs processed architectures (lines 134-135)."""
        orchestrator = UploadOrchestrator()

        mock_future: Future[Any] = Future()
        mock_result = Mock()
        mock_result.uploaded_rpms = 5
        mock_result.created_resources = []
        mock_future.set_result(mock_result)

        future_to_arch = {mock_future: "x86_64"}

        with patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging:
            orchestrator._collect_architecture_results(future_to_arch)

            # Verify debug logging with processed architectures
            debug_calls = [str(call) for call in mock_logging.debug.call_args_list]
            assert any("Processed architectures" in str(call) for call in debug_calls)


class TestUploadOrchestratorProcessArchitectureUploads:
    """Tests for UploadOrchestrator.process_architecture_uploads() method."""

    def test_process_architecture_uploads_success(self):
        """Test process_architecture_uploads successfully processes architectures (lines 166, 173, 177, 190, 192)."""
        orchestrator = UploadOrchestrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create architecture directories
            os.makedirs(os.path.join(tmpdir, "x86_64"))
            os.makedirs(os.path.join(tmpdir, "aarch64"))

            args = UploadContext(
                build_id="test-build",
                date_str="2024-01-01 00:00:00",
                namespace="test-ns",
                parent_package="test-pkg",
                rpm_path=tmpdir,
                sbom_path="/test/sbom.json",
            )
            mock_client = Mock()
            repositories = RepositoryRefs(
                rpms_href="/test/",
                rpms_prn="",
                logs_href="",
                logs_prn="logs-prn",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )
            results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

            with (
                patch.object(orchestrator, "_submit_architecture_tasks") as mock_submit,
                patch.object(orchestrator, "_collect_architecture_results") as mock_collect,
            ):
                mock_future1 = Mock()
                mock_future2 = Mock()
                mock_submit.return_value = {mock_future1: "x86_64", mock_future2: "aarch64"}
                mock_collect.return_value = {"x86_64": {}, "aarch64": {}}

                result = orchestrator.process_architecture_uploads(
                    mock_client,
                    args,
                    repositories,
                    date_str="2024-01-01",
                    rpm_href="/test/",
                    results_model=results_model,
                )

                assert result == {"x86_64": {}, "aarch64": {}}
                mock_submit.assert_called_once()
                mock_collect.assert_called_once()

    def test_process_architecture_uploads_no_architectures(self):
        """Test process_architecture_uploads with no architectures (lines 168-170)."""
        orchestrator = UploadOrchestrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            args = UploadContext(
                build_id="test-build",
                date_str="2024-01-01 00:00:00",
                namespace="test-ns",
                parent_package="test-pkg",
                rpm_path=tmpdir,
                sbom_path="/test/sbom.json",
            )
            mock_client = Mock()
            repositories = RepositoryRefs(
                rpms_href="/test/",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )
            results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

            with patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging:
                result = orchestrator.process_architecture_uploads(
                    mock_client,
                    args,
                    repositories,
                    date_str="2024-01-01",
                    rpm_href="/test/",
                    results_model=results_model,
                )

                assert result == {}
                mock_logging.warning.assert_called_once()


class TestUploadOrchestratorProcessUploads:
    """Tests for UploadOrchestrator.process_uploads() method."""

    def test_process_uploads_success(self):
        """Test process_uploads processes all uploads (lines 210-252)."""
        orchestrator = UploadOrchestrator()

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="",
        )

        mock_processed_uploads = {
            "x86_64": {"created_resources": ["/resource/1", "/resource/2"]},
            "aarch64": {"created_resources": ["/resource/3"]},
        }

        with (
            patch.object(orchestrator, "process_architecture_uploads", return_value=mock_processed_uploads),
            patch("pulp_tool.services.upload_service.upload_sbom", return_value=["/sbom/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
        ):
            result = orchestrator.process_uploads(mock_client, args, repositories)

            assert result == "https://example.com/results.json"
            # Verify logging calls
            assert mock_logging.info.call_count >= 2
            assert mock_logging.debug.call_count >= 1

    def test_process_uploads_missing_rpm_href(self):
        """Test process_uploads raises ValueError when rpms_href is missing (lines 213-214)."""
        orchestrator = UploadOrchestrator()

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="",  # Empty href
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        with pytest.raises(ValueError, match="RPM repository href is required"):
            orchestrator.process_uploads(mock_client, args, repositories)

    def test_process_uploads_with_no_created_resources(self):
        """Test process_uploads handles empty created_resources."""
        orchestrator = UploadOrchestrator()

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="",
        )

        mock_processed_uploads: Dict[str, Dict[str, list[str]]] = {
            "x86_64": {"created_resources": []},
        }

        with (
            patch.object(orchestrator, "process_architecture_uploads", return_value=mock_processed_uploads),
            patch("pulp_tool.services.upload_service.upload_sbom", return_value=[]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
        ):
            result = orchestrator.process_uploads(mock_client, args, repositories)

            assert result == "https://example.com/results.json"
            # Verify logging still occurs
            assert mock_logging.info.call_count >= 1
