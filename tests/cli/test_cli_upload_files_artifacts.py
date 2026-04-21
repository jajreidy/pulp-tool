"""Tests for upload-files CLI (artifacts and errors)."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import httpx
from click.testing import CliRunner
from pulp_tool.cli import cli


class TestUploadFilesCommandArtifacts:
    """upload-files: artifact-results, SBOM, HTTP errors."""

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_with_artifact_results(self, mock_helper_class, mock_client_class) -> None:
        """Test upload-files with artifact-results output."""
        runner = CliRunner()
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client
        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_file_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            url_path = Path(tmpdir) / "url.txt"
            digest_path = Path(tmpdir) / "digest.txt"
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                    "--artifact-results",
                    f"{url_path},{digest_path}",
                ],
            )
            assert result.exit_code == 0
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]
            assert context.artifact_results == f"{url_path},{digest_path}"

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_with_sbom_results(self, mock_helper_class, mock_client_class) -> None:
        """Test upload-files with sbom-results output."""
        runner = CliRunner()
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client
        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_file_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper
        with tempfile.TemporaryDirectory() as tmpdir:
            sbom_file = Path(tmpdir) / "sbom.json"
            sbom_file.write_text("{}")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            sbom_results_path = Path(tmpdir) / "sbom_results.txt"
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--sbom",
                    str(sbom_file),
                    "--sbom-results",
                    str(sbom_results_path),
                ],
            )
            assert result.exit_code == 0
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]
            assert context.sbom_results == str(sbom_results_path)

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_no_results_json(self, mock_helper_class, mock_client_class) -> None:
        """Test upload-files when results JSON is not created."""
        runner = CliRunner()
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client
        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_file_uploads.return_value = None
        mock_helper_class.return_value = mock_helper
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 1
            assert "results JSON was not created" in result.output

    @patch("pulp_tool.cli.upload_files.PulpClient")
    def test_upload_files_http_error(self, mock_client_class) -> None:
        """Test upload-files with HTTP error."""
        runner = CliRunner()
        mock_client_class.create_from_config_file.side_effect = httpx.HTTPError("Connection failed")
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 1

    @patch("pulp_tool.cli.upload_files.PulpClient")
    def test_upload_files_auth_http_error_exits_one(self, mock_client_class) -> None:
        """Auth-style HTTP errors exit 1 like other HTTP failures."""
        runner = CliRunner()
        mock_client_class.create_from_config_file.side_effect = httpx.HTTPError("403 Forbidden")
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 1

    @patch("pulp_tool.cli.upload_files.PulpClient")
    def test_upload_files_runtime_error_no_access_token_exits_one(self, mock_client_class) -> None:
        """OAuth token failure (RuntimeError) is a failed upload-files run."""
        runner = CliRunner()
        mock_client_class.create_from_config_file.side_effect = RuntimeError("Failed to obtain access token")
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 1

    @patch("pulp_tool.cli.upload_files.PulpClient")
    def test_upload_files_generic_exception(self, mock_client_class) -> None:
        """Test upload-files with generic exception."""
        runner = CliRunner()
        mock_client_class.create_from_config_file.side_effect = ValueError("Unexpected error")
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 1

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_note_about_artifact_results(self, mock_helper_class, mock_client_class) -> None:
        """Test upload-files shows note when artifact-results is not provided."""
        runner = CliRunner()
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client
        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_file_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    str(config_path),
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )
            assert result.exit_code == 0
            assert "NOTE: Results JSON created but not written to Konflux artifact files" in result.output
            assert "Use --artifact-results" in result.output
