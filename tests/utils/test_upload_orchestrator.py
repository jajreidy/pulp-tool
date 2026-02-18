"""Tests for UploadOrchestrator class."""

import os
import tempfile
from concurrent.futures import Future
from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest

from pulp_tool.models.context import UploadFilesContext, UploadRpmContext
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
        args = UploadRpmContext(
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

        with (patch("pulp_tool.utils.upload_orchestrator.handle_generic_error") as mock_handle_error,):
            with pytest.raises(ValueError, match="Upload failed"):
                orchestrator._collect_architecture_results(future_to_arch)

            mock_handle_error.assert_called_once()
            assert mock_handle_error.call_args[0][1] == "process architecture x86_64"

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

            args = UploadRpmContext(
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
            args = UploadRpmContext(
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

    def test_process_architecture_uploads_no_rpm_path(self):
        """Test process_architecture_uploads when rpm_path is None (lines 168-170)."""
        orchestrator = UploadOrchestrator()

        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path=None,  # None rpm_path
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
            mock_logging.warning.assert_called_once_with("rpm_path is not set, cannot process architecture uploads")


class TestUploadOrchestratorProcessUploads:
    """Tests for UploadOrchestrator.process_uploads() method."""

    def test_process_uploads_success(self):
        """Test process_uploads processes all uploads (lines 210-252)."""
        orchestrator = UploadOrchestrator()

        args = UploadRpmContext(
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

        args = UploadRpmContext(
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

        args = UploadRpmContext(
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

    def test_process_uploads_without_sbom_path(self):
        """Test process_uploads when sbom_path is None (line 249)."""
        orchestrator = UploadOrchestrator()

        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path=None,  # None sbom_path
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
            "x86_64": {"created_resources": ["/resource/1"]},
        }

        with (
            patch.object(orchestrator, "process_architecture_uploads", return_value=mock_processed_uploads),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
        ):
            result = orchestrator.process_uploads(mock_client, args, repositories)

            assert result == "https://example.com/results.json"
            # Verify that upload_sbom was not called
            mock_logging.debug.assert_any_call("Skipping SBOM upload - no sbom_path provided")
            # Verify logging still occurs
            assert mock_logging.info.call_count >= 1

    def test_process_uploads_root_level_rpms(self):
        """Test process_uploads uploads root-level RPMs when present in rpm_path (lines 241-265)."""
        orchestrator = UploadOrchestrator()

        args = UploadRpmContext(
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

        mock_processed_uploads: Dict[str, Dict[str, list[str]]] = {"x86_64": {"created_resources": []}}
        root_rpm_files = ["/test/rpms/pkg.noarch.rpm"]

        with (
            patch.object(orchestrator, "process_architecture_uploads", return_value=mock_processed_uploads),
            patch("pulp_tool.utils.upload_orchestrator.glob.glob", return_value=root_rpm_files),
            patch("pulp_tool.utils.upload_orchestrator.os.path.isfile", return_value=True),
            patch(
                "pulp_tool.utils.upload_orchestrator.group_rpm_paths_by_arch",
                return_value={"noarch": root_rpm_files},
            ),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms",
                return_value=["/rpm/resource/1"],
            ) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.upload_sbom", return_value=[]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = orchestrator.process_uploads(mock_client, args, repositories)

            assert result == "https://example.com/results.json"
            mock_upload_rpms.assert_called_once()
            call_pos, call_kw = mock_upload_rpms.call_args
            assert call_pos[3] == "noarch"  # arch is 4th positional arg
            assert call_kw["rpm_repository_href"] == "/test/rpm-href"


class TestUploadOrchestratorProcessFileUploads:
    """Tests for UploadOrchestrator.process_file_uploads() method."""

    def test_process_file_uploads_all_file_types(self):
        """Test process_file_uploads with all file types."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package-1.0.0-1.x86_64.rpm"],
            file_files=["/path/to/file.txt"],
            log_files=["/path/to/build.log"],
            sbom_files=["/path/to/sbom.json"],
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
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.upload_orchestrator.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.utils.upload_orchestrator.upload_log", return_value=["/log/resource/1"]),
            patch("pulp_tool.services.upload_service.upload_sbom", return_value=["/sbom/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
            patch("pulp_tool.utils.upload_orchestrator.validate_file_path"),
            patch.object(mock_client, "create_file_content") as mock_create,
            patch.object(mock_client, "check_response"),
            patch.object(mock_client, "wait_for_finished_task") as mock_wait,
        ):
            mock_response = Mock()
            mock_response.json.return_value = {"task": "/api/v3/tasks/123/"}
            mock_create.return_value = mock_response

            mock_task_response = Mock()
            mock_task_response.created_resources = ["/file/resource/1"]
            mock_wait.return_value = mock_task_response

            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Verify all upload functions were called
            assert mock_create.called  # For generic files

    def test_process_file_uploads_rpms_with_arch_detection(self):
        """Test process_file_uploads with RPMs requiring architecture detection."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/x86_64/package-1.0.0-1.rpm", "/path/to/package-1.0.0-1.aarch64.rpm"],
        )
        mock_client = Mock()
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

        with (
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", side_effect=["x86_64", None]),
            patch(
                "pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename",
                side_effect=["aarch64", None],
            ),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms",
                return_value=["/rpm/resource/1"],
            ) as mock_upload_rpms,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should be called twice (once for each architecture)
            assert mock_upload_rpms.call_count == 2

    def test_process_file_uploads_rpms_skip_undetected_arch(self):
        """Test process_file_uploads skips RPMs with undetected architecture."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package.rpm"],  # No architecture in path or filename
        )
        mock_client = Mock()
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

        with (
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename", return_value=None),
            patch("pulp_tool.utils.upload_orchestrator.upload_rpms") as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.artifact_detection.logging") as mock_logging,
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should not be called since RPM was skipped
            mock_upload_rpms.assert_not_called()
            # Warning is logged from artifact_detection when group_rpm_paths_by_arch skips a path
            mock_logging.warning.assert_called()

    def test_process_file_uploads_rpms_with_provided_arch(self):
        """Test process_file_uploads with RPMs using provided architecture."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package.rpm"],
            arch="x86_64",  # Architecture provided
        )
        mock_client = Mock()
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

        with (
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms",
                return_value=["/rpm/resource/1"],
            ) as mock_upload_rpms,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should be called once with x86_64
            mock_upload_rpms.assert_called_once()
            call_args = mock_upload_rpms.call_args
            assert call_args[0][3] == "x86_64"  # arch parameter

    def test_process_file_uploads_logs_with_arch_detection(self):
        """Test process_file_uploads with logs using architecture detection."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            log_files=["/path/to/x86_64/build.log"],
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        with (
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value="x86_64"),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_log",
                return_value=["/log/resource/1"],
            ) as mock_upload_log,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            mock_upload_log.assert_called_once()
            # Verify arch was passed correctly
            call_args = mock_upload_log.call_args
            assert call_args[1]["arch"] == "x86_64"

    def test_process_file_uploads_logs_skip_undetected_arch(self):
        """Test process_file_uploads skips logs with undetected architecture."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            log_files=["/path/to/build.log"],  # No architecture in path
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        with (
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch("pulp_tool.utils.upload_orchestrator.upload_log") as mock_upload_log,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should not be called since log was skipped
            mock_upload_log.assert_not_called()
            # Should log warning
            mock_logging.warning.assert_called()
            # Verify the warning message contains the expected text
            warning_calls = [call for call in mock_logging.warning.call_args_list if "Skipping" in str(call)]
            assert len(warning_calls) > 0

    def test_process_file_uploads_logs_with_provided_arch(self):
        """Test process_file_uploads with logs using provided architecture."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            log_files=["/path/to/build.log"],
            arch="x86_64",  # Architecture provided
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        with (
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_log",
                return_value=["/log/resource/1"],
            ) as mock_upload_log,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should be called once with x86_64
            mock_upload_log.assert_called_once()
            call_args = mock_upload_log.call_args
            assert call_args[1]["arch"] == "x86_64"

    def test_process_file_uploads_multiple_architectures(self):
        """Test process_file_uploads with RPMs from multiple architectures."""
        orchestrator = UploadOrchestrator()

        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=[
                "/path/to/package1-1.0.0-1.x86_64.rpm",
                "/path/to/package2-1.0.0-1.aarch64.rpm",
            ],
        )
        mock_client = Mock()
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

        with (
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch(
                "pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename",
                side_effect=["x86_64", "aarch64"],
            ),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms",
                return_value=["/rpm/resource/1"],
            ) as mock_upload_rpms,
            patch(
                "pulp_tool.services.upload_service.collect_results",
                return_value="https://example.com/results.json",
            ),
        ):
            result = orchestrator.process_file_uploads(mock_client, context, repositories)

            assert result == "https://example.com/results.json"
            # Should be called twice (once for each architecture)
            assert mock_upload_rpms.call_count == 2
            # Verify both architectures were processed
            call_args_list = mock_upload_rpms.call_args_list
            archs = [call[0][3] for call in call_args_list]  # Extract arch from each call
            assert "x86_64" in archs
            assert "aarch64" in archs
