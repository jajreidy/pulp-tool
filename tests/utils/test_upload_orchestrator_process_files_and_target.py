"""Tests for UploadOrchestrator process_file_uploads and target arch repo."""

from unittest.mock import Mock, patch
import pytest
from pulp_tool.models.context import UploadFilesContext, UploadRpmContext
from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.utils.upload_orchestrator import UploadOrchestrator
from tests.support.upload_orchestrator_test_stub import PROCESS_FILE_UPLOAD_DIST_URLS as _PROCESS_FILE_UPLOAD_DIST_URLS


class TestUploadOrchestratorProcessFileUploads:
    """Tests for UploadOrchestrator.process_file_uploads() method."""

    def test_process_file_uploads_all_file_types(self) -> None:
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
            arch="x86_64",
        )
        mock_client = Mock()
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="/logs-href",
            logs_prn="logs-prn",
            sbom_href="/sbom-href",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts-href",
            artifacts_prn="artifacts-prn",
        )
        with (
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.upload_orchestrator.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.utils.upload_orchestrator.upload_logs_parallel", return_value=1) as mock_upload_logs,
            patch("pulp_tool.services.upload_service.upload_sbom"),
            patch("pulp_tool.utils.upload_orchestrator.upload_artifact_phase1") as mock_upload_artifact,
            patch(
                "pulp_tool.utils.file_operations.add_file_content_to_repository",
                return_value=["/version/1/"],
            ) as mock_modify,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_logs.assert_called_once()
            mock_upload_artifact.assert_called_once()
            assert mock_modify.call_count == 2

    def test_process_file_uploads_rpms_with_arch_detection(self) -> None:
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", side_effect=["x86_64", None]),
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename", side_effect=["aarch64", None]),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms", return_value=["/rpm/resource/1"]
            ) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            assert mock_upload_rpms.call_count == 2

    def test_process_file_uploads_rpms_skip_undetected_arch(self) -> None:
        """Test process_file_uploads skips RPMs with undetected architecture."""
        orchestrator = UploadOrchestrator()
        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package.rpm"],
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename", return_value=None),
            patch("pulp_tool.utils.upload_orchestrator.upload_rpms") as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.artifact_detection.logging") as mock_logging,
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_rpms.assert_not_called()
            mock_logging.warning.assert_called()

    def test_process_file_uploads_rpms_with_provided_arch(self) -> None:
        """Test process_file_uploads with RPMs using provided architecture."""
        orchestrator = UploadOrchestrator()
        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package.rpm"],
            arch="x86_64",
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms", return_value=["/rpm/resource/1"]
            ) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_rpms.assert_called_once()
            call_args = mock_upload_rpms.call_args
            assert call_args[0][3] == "x86_64"

    def test_process_file_uploads_logs_with_arch_detection(self) -> None:
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
            logs_href="/logs-href",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        with (
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value="x86_64"),
            patch("pulp_tool.utils.upload_orchestrator.upload_logs_parallel", return_value=1) as mock_upload_logs,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
            patch(
                "pulp_tool.utils.file_operations.add_file_content_to_repository",
                return_value=[],
            ),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_logs.assert_called_once()
            call_kwargs = mock_upload_logs.call_args[1]
            assert call_kwargs["arch"] == "x86_64"

    def test_process_file_uploads_logs_skip_undetected_arch(self) -> None:
        """Test process_file_uploads skips logs with undetected architecture."""
        orchestrator = UploadOrchestrator()
        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            log_files=["/path/to/build.log"],
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch("pulp_tool.utils.upload_orchestrator.upload_logs_parallel") as mock_upload_logs,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.logging") as mock_logging,
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_logs.assert_not_called()
            mock_logging.warning.assert_called()
            warning_calls = [call for call in mock_logging.warning.call_args_list if "Skipping" in str(call)]
            assert len(warning_calls) > 0

    def test_process_file_uploads_logs_with_provided_arch(self) -> None:
        """Test process_file_uploads with logs using provided architecture."""
        orchestrator = UploadOrchestrator()
        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            log_files=["/path/to/build.log"],
            arch="x86_64",
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.upload_orchestrator.upload_logs_parallel", return_value=1) as mock_upload_logs,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            patch("pulp_tool.utils.upload_orchestrator.create_labels"),
            patch(
                "pulp_tool.utils.file_operations.add_file_content_to_repository",
                return_value=[],
            ),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            mock_upload_logs.assert_called_once()
            call_kwargs = mock_upload_logs.call_args[1]
            assert call_kwargs["arch"] == "x86_64"

    def test_process_file_uploads_multiple_architectures(self) -> None:
        """Test process_file_uploads with RPMs from multiple architectures."""
        orchestrator = UploadOrchestrator()
        context = UploadFilesContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_files=["/path/to/package1-1.0.0-1.x86_64.rpm", "/path/to/package2-1.0.0-1.aarch64.rpm"],
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
            patch("pulp_tool.utils.pulp_helper.PulpHelper") as mock_ph_cls,
            patch("pulp_tool.utils.artifact_detection.detect_arch_from_filepath", return_value=None),
            patch(
                "pulp_tool.utils.artifact_detection.detect_arch_from_rpm_filename", side_effect=["x86_64", "aarch64"]
            ),
            patch(
                "pulp_tool.utils.upload_orchestrator.upload_rpms", return_value=["/rpm/resource/1"]
            ) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            mock_ph_cls.return_value.get_distribution_urls_for_upload_context.return_value = (
                _PROCESS_FILE_UPLOAD_DIST_URLS
            )
            result = orchestrator.process_file_uploads(mock_client, context, repositories)
            assert result == "https://example.com/results.json"
            assert mock_upload_rpms.call_count == 2
            call_args_list = mock_upload_rpms.call_args_list
            archs = [call[0][3] for call in call_args_list]
            assert "x86_64" in archs
            assert "aarch64" in archs


class TestUploadOrchestratorTargetArchRepo:
    """process_uploads with --target-arch-repo."""

    def test_process_uploads_target_arch_repo_requires_pulp_helper(self) -> None:
        """Per-arch mode requires pulp_helper for ensure_rpm_repository_for_arch."""
        orchestrator = UploadOrchestrator()
        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package=None,
            rpm_path="/test/rpms",
            sbom_path=None,
            target_arch_repo=True,
        )
        mock_client = Mock()
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
        with pytest.raises(ValueError, match="target_arch_repo requires PulpHelper"):
            orchestrator.process_uploads(mock_client, args, repositories)

    def test_process_uploads_target_arch_repo_allows_empty_rpms_href(self) -> None:
        """Bulk rpms_href may be empty when per-arch repos are created on demand."""
        orchestrator = UploadOrchestrator()
        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package=None,
            rpm_path="/test/rpms",
            sbom_path=None,
            target_arch_repo=True,
        )
        mock_client = Mock()
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
        mock_helper = Mock()
        mock_helper.ensure_rpm_repository_for_arch.return_value = "/per-arch/rpm"
        with (
            patch.object(orchestrator, "process_architecture_uploads", return_value={}),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = orchestrator.process_uploads(mock_client, args, repositories, pulp_helper=mock_helper)
        assert result == "https://example.com/results.json"
