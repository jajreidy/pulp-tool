"""Tests for Click CLI commands."""

import base64
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
from click.testing import CliRunner
import httpx

from pulp_tool.cli import cli, main, config_option, debug_option


class TestCLIEntryPoint:
    """Test CLI entry point and main function."""

    def test_main_function_success(self):
        """Test main() entry point calls cli successfully."""
        with patch("pulp_tool.cli.cli") as mock_cli:
            mock_cli.return_value = None
            main()
            mock_cli.assert_called_once()

    def test_main_function_keyboard_interrupt(self):
        """Test main() handles KeyboardInterrupt gracefully."""
        with patch("pulp_tool.cli.cli") as mock_cli, patch("pulp_tool.cli.sys.exit") as mock_exit:
            mock_cli.side_effect = KeyboardInterrupt()
            main()
            mock_exit.assert_called_once_with(130)


class TestCLIHelp:
    """Test CLI help commands."""

    def test_main_help(self):
        """Test main CLI help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Pulp Tool" in result.output
        assert "upload" in result.output
        assert "transfer" in result.output
        assert "create-repository" in result.output
        # Check group-level options
        assert "--config" in result.output
        assert "--build-id" in result.output
        assert "--namespace" in result.output
        assert "--debug" in result.output
        assert "--max-workers" in result.output

    def test_main_help_short_flag(self):
        """Test main CLI help output with -h flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["-h"])
        assert result.exit_code == 0
        assert "Pulp Tool" in result.output
        assert "upload" in result.output
        assert "transfer" in result.output
        assert "create-repository" in result.output
        assert "-h, --help" in result.output

    def test_upload_help(self):
        """Test upload command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["upload", "--help"])
        assert result.exit_code == 0
        assert "Upload RPMs, logs, and SBOM files" in result.output
        # Group-level options are not shown in command help
        assert "--parent-package" in result.output
        assert "--rpm-path" in result.output
        assert "--sbom-results" in result.output
        assert "--artifact-results" in result.output

    def test_upload_files_help(self):
        """Test upload-files command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["upload-files", "--help"])
        assert result.exit_code == 0
        assert "Upload individual files" in result.output
        assert "--parent-package" in result.output
        assert "--rpm" in result.output
        assert "--file" in result.output
        assert "--log" in result.output
        assert "--sbom" in result.output
        assert "--arch" in result.output
        assert "--artifact-results" in result.output
        assert "--sbom-results" in result.output

    def test_transfer_help(self):
        """Test transfer command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["transfer", "--help"])
        assert result.exit_code == 0
        assert "Download artifacts" in result.output
        assert "--artifact-location" in result.output
        assert "--content-types" in result.output
        assert "--archs" in result.output
        # Group-level options are not shown in command help

    def test_create_repository_help(self):
        """Test create-repository command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["create-repository", "--help"])
        assert result.exit_code == 0
        assert "Create a custom defined repository."
        assert "--repository-name" in result.output
        assert "--packages" in result.output
        assert "--compression-type" in result.output
        assert "--checksum-type" in result.output
        assert "--skip-publish" in result.output
        assert "--base-path" in result.output
        assert "--generate-repo-config" in result.output
        assert "-j" in result.output
        assert "--json-data" in result.output


class TestCLIValidation:
    """Test CLI input validation."""

    def test_upload_missing_required_args(self):
        """Test upload command with missing required arguments."""
        runner = CliRunner()
        result = runner.invoke(cli, ["upload"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_transfer_missing_required_args(self):
        """Test transfer command with missing required arguments."""
        runner = CliRunner()
        result = runner.invoke(cli, ["transfer"], catch_exceptions=False, standalone_mode=False)
        assert result.exit_code != 0

    def test_create_repository_missing_required_args(self):
        """Test create-repository command with missing required arguments."""
        runner = CliRunner()
        result = runner.invoke(cli, ["create-repository"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_create_repository_missing_json_fields(self):
        """Test create-repository command with missing json fields."""
        runner = CliRunner()
        result = runner.invoke(cli, ["create-repository", "--json-data", "{}"])
        assert result.exit_code != 0
        assert "Field required" in result.output

    def test_create_repository_bad_json_arg(self):
        """Test create-repository command with impropper json"""
        runner = CliRunner()
        result = runner.invoke(cli, ["create-repository", "--json-data", "{"])
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output


class TestCLIVersion:
    """Test CLI version output."""

    def test_version(self):
        """Test version flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0


class TestUploadCommand:
    """Test upload command functionality."""

    def test_upload_invalid_rpm_path(self):
        """Test upload with non-existent RPM path."""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sbom_file:
            sbom_file.write("{}")
            sbom_path = sbom_file.name

        try:
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    "/nonexistent/path",
                    "--sbom-path",
                    sbom_path,
                ],
            )
            assert result.exit_code != 0
        finally:
            os.unlink(sbom_path)

    def test_upload_invalid_sbom_path(self):
        """Test upload with non-existent SBOM path."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    tmpdir,
                    "--sbom-path",
                    "/nonexistent/sbom.json",
                ],
            )
            assert result.exit_code != 0

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_success(self, mock_helper_class, mock_client_class):
        """Test successful upload flow."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create dummy files
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON URL" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_with_base64_config(self, mock_helper_class, mock_client_class):
        """Test upload command with base64-encoded config."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        # Create base64-encoded config
        config_content = (
            '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
        )
        base64_config = base64.b64encode(config_content.encode()).decode()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create dummy files
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")

            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "--config",
                    base64_config,
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON URL" in result.output
            # Verify that create_from_config_file was called with the base64 string directly
            assert mock_client_class.create_from_config_file.called
            call_args = mock_client_class.create_from_config_file.call_args
            assert call_args is not None
            # Check both kwargs and args
            if call_args.kwargs and "path" in call_args.kwargs:
                config_path = call_args.kwargs["path"]
            elif call_args.args and len(call_args.args) > 0:
                config_path = call_args.args[0]
            else:
                config_path = None
            assert config_path is not None
            # Should be the base64 string directly (not converted to temp file)
            assert config_path == base64_config

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_with_artifact_results(self, mock_helper_class, mock_client_class):
        """Test upload with artifact results output."""
        runner = CliRunner()

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                    "--artifact-results",
                    f"{url_path},{digest_path}",
                ],
            )

            assert result.exit_code == 0

    @patch("pulp_tool.cli.upload.PulpClient")
    def test_upload_http_error(self, mock_client_class):
        """Test upload with HTTP error."""
        runner = CliRunner()

        mock_client_class.create_from_config_file.side_effect = httpx.HTTPError("Connection failed")

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 1

    def test_upload_missing_build_id(self):
        """Test upload command with missing build-id."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")

            result = runner.invoke(
                cli,
                [
                    "--namespace",
                    "test-ns",
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 1
            assert "--build-id is required" in result.output

    def test_upload_missing_namespace(self):
        """Test upload command with missing namespace."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")

            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 1
            assert "--namespace is required" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_no_results_json(self, mock_helper_class, mock_client_class):
        """Test upload when results JSON is not created."""
        runner = CliRunner()

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = None  # No results JSON URL
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 1
            assert "results JSON was not created" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    def test_upload_generic_exception(self, mock_client_class):
        """Test upload with generic exception."""
        runner = CliRunner()

        mock_client_class.create_from_config_file.side_effect = ValueError("Unexpected error")

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 1

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_without_rpm_path(self, mock_helper_class, mock_client_class):
        """Test upload without rpm-path (should use current directory)."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )

            # Change to tmpdir as current directory
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = runner.invoke(
                    cli,
                    [
                        "--build-id",
                        "test-build",
                        "--namespace",
                        "test-ns",
                        "--config",
                        str(config_path),
                        "upload",
                        "--parent-package",
                        "test-pkg",
                        "--sbom-path",
                        str(sbom_path),
                    ],
                )

                assert result.exit_code == 0
                assert "RESULTS JSON URL" in result.output
                # Verify that rpm_path was set to current directory
                call_args = mock_helper_class.call_args
                assert call_args is not None
            finally:
                os.chdir(original_cwd)

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_without_sbom_path(self, mock_helper_class, mock_client_class):
        """Test upload without sbom-path (should skip SBOM upload)."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
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
                    "upload",
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON URL" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_without_parent_package(self, mock_helper_class, mock_client_class):
        """Test upload without parent-package (should not include in labels)."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")
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
                    "upload",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON URL" in result.output
            # Verify that parent_package was None
            call_args = mock_helper_class.call_args
            assert call_args is not None
            assert call_args.kwargs.get("parent_package") is None

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_all_optional_omitted(self, mock_helper_class, mock_client_class):
        """Test upload with all optional parameters omitted."""
        runner = CliRunner()

        # Setup mocks
        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )
        mock_helper.setup_repositories.return_value = mock_repos
        mock_helper.process_uploads.return_value = "https://example.com/results.json"
        mock_helper_class.return_value = mock_helper

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )

            # Change to tmpdir as current directory
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = runner.invoke(
                    cli,
                    [
                        "--build-id",
                        "test-build",
                        "--namespace",
                        "test-ns",
                        "--config",
                        str(config_path),
                        "upload",
                    ],
                )

                assert result.exit_code == 0
                assert "RESULTS JSON URL" in result.output
            finally:
                os.chdir(original_cwd)


class TestUploadFilesCommand:
    """Test upload-files command functionality."""

    def test_upload_files_missing_build_id(self):
        """Test upload-files command with missing build-id."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy")

            result = runner.invoke(
                cli,
                [
                    "--namespace",
                    "test-ns",
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )

            assert result.exit_code == 1
            assert "build-id is required" in result.output

    def test_upload_files_missing_namespace(self):
        """Test upload-files command with missing namespace."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy")

            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "upload-files",
                    "--parent-package",
                    "test-pkg",
                    "--rpm",
                    str(rpm_file),
                ],
            )

            assert result.exit_code == 1
            assert "namespace is required" in result.output

    def test_upload_files_missing_parent_package(self):
        """Test upload-files command with missing parent-package."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy")

            result = runner.invoke(
                cli,
                [
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "upload-files",
                    "--rpm",
                    str(rpm_file),
                ],
            )

            assert result.exit_code != 0
            assert "Missing option" in result.output or "required" in result.output.lower()

    def test_upload_files_no_files_provided(self):
        """Test upload-files command with no files specified."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
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
                ],
            )

            assert result.exit_code == 1
            assert "At least one file must be specified" in result.output

    def test_upload_files_invalid_rpm_path(self):
        """Test upload-files with non-existent RPM file."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
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
                    "/nonexistent/package.rpm",
                ],
            )

            assert result.exit_code != 0

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_success(self, mock_helper_class, mock_client_class):
        """Test successful upload-files flow with all file types."""
        runner = CliRunner()

        # Setup mocks
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
            # Create dummy files
            rpm_file = Path(tmpdir) / "package.rpm"
            rpm_file.write_text("dummy rpm")
            file_file = Path(tmpdir) / "file.txt"
            file_file.write_text("dummy file")
            log_file = Path(tmpdir) / "build.log"
            log_file.write_text("dummy log")
            sbom_file = Path(tmpdir) / "sbom.json"
            sbom_file.write_text("{}")
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
                    "--file",
                    str(file_file),
                    "--log",
                    str(log_file),
                    "--sbom",
                    str(sbom_file),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON URL" in result.output
            assert "https://example.com/results.json" in result.output
            mock_helper.setup_repositories.assert_called_once_with("test-build")
            mock_helper.process_file_uploads.assert_called_once()

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_with_arch(self, mock_helper_class, mock_client_class):
        """Test upload-files with architecture specified."""
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
                    "--arch",
                    "x86_64",
                ],
            )

            assert result.exit_code == 0
            # Verify the context was created with the arch
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]  # Second positional argument is context
            assert context.arch == "x86_64"

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_multiple_files(self, mock_helper_class, mock_client_class):
        """Test upload-files with multiple files of the same type."""
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
            rpm_file1 = Path(tmpdir) / "package1.rpm"
            rpm_file1.write_text("dummy rpm 1")
            rpm_file2 = Path(tmpdir) / "package2.rpm"
            rpm_file2.write_text("dummy rpm 2")
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
                    str(rpm_file1),
                    "--rpm",
                    str(rpm_file2),
                ],
            )

            assert result.exit_code == 0
            # Verify both files were passed to the context
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]
            assert len(context.rpm_files) == 2

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_with_artifact_results(self, mock_helper_class, mock_client_class):
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
            # Verify artifact_results was passed to context
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]
            assert context.artifact_results == f"{url_path},{digest_path}"

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_with_sbom_results(self, mock_helper_class, mock_client_class):
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
            # Verify sbom_results was passed to context
            call_args = mock_helper.process_file_uploads.call_args
            context = call_args[0][1]
            assert context.sbom_results == str(sbom_results_path)

    @patch("pulp_tool.cli.upload_files.PulpClient")
    @patch("pulp_tool.cli.upload_files.PulpHelper")
    def test_upload_files_no_results_json(self, mock_helper_class, mock_client_class):
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
        mock_helper.process_file_uploads.return_value = None  # No results JSON URL
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
    def test_upload_files_http_error(self, mock_client_class):
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
    def test_upload_files_generic_exception(self, mock_client_class):
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
    def test_upload_files_note_about_artifact_results(self, mock_helper_class, mock_client_class):
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


class TestTransferCommand:
    """Test transfer command functionality."""

    def test_transfer_missing_artifact_location_and_build_id(self):
        """Test transfer with neither artifact_location nor build_id provided."""
        runner = CliRunner()
        result = runner.invoke(cli, ["transfer"])
        assert result.exit_code == 1
        assert "Either --artifact-location OR" in result.output

    def test_transfer_build_id_without_namespace(self):
        """Test transfer with build_id but no namespace."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--build-id", "test-build", "transfer"])
        assert result.exit_code == 1
        assert "Both --build-id and --namespace must be provided" in result.output

    def test_transfer_build_id_without_config(self):
        """Test transfer with build_id+namespace but no config."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--build-id", "test-build", "--namespace", "test-ns", "transfer"])
        assert result.exit_code == 1
        assert "--config is required" in result.output

    def test_transfer_conflicting_options(self):
        """Test transfer with both artifact_location and build_id."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--build-id",
                "test-build",
                "--namespace",
                "test-ns",
                "transfer",
                "--artifact-location",
                "http://example.com/artifact.json",
            ],
        )
        assert result.exit_code == 1
        assert "Cannot use --artifact-location with --build-id" in result.output

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_local_file(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with local artifact file."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": {"test.rpm": {"labels": {"build_id": "test"}}}, "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 0
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.DistributionClient")
    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_remote_url(self, mock_report, mock_download, mock_setup, mock_load, mock_dist_client):
        """Test transfer with remote artifact URL."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create temporary cert and key files
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("cert")
            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            # Create temporary config file with cert path
            config_path = Path(tmpdir) / "config.toml"
            config_content = (
                '[cli]\nbase_url = "https://pulp.example.com"\n' f'cert = "{cert_path}"\n' f'key = "{key_path}"'
            )
            config_path.write_text(config_content)

            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            # Mock DistributionClient to avoid SSL errors with test cert files
            mock_client_instance = Mock()
            mock_dist_client.return_value = mock_client_instance

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "transfer",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--cert-path",
                    str(cert_path),
                    "--key-path",
                    str(key_path),
                ],
            )

            assert result.exit_code == 0

    @patch("pulp_tool.cli.transfer.DistributionClient")
    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_key_from_config(self, mock_report, mock_download, mock_setup, mock_load, mock_dist_client):
        """Test transfer with key_path loaded from config when not provided via CLI."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create temporary cert and key files
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("cert")
            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            # Create temporary config file with cert and key paths
            config_path = Path(tmpdir) / "config.toml"
            config_content = (
                '[cli]\nbase_url = "https://pulp.example.com"\n' f'cert = "{cert_path}"\n' f'key = "{key_path}"'
            )
            config_path.write_text(config_content)

            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            # Mock DistributionClient to avoid SSL errors with test cert files
            mock_client_instance = Mock()
            mock_dist_client.return_value = mock_client_instance

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            # Don't provide --key-path, should be loaded from config
            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "transfer",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                ],
            )

            assert result.exit_code == 0

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    def test_transfer_config_load_exception(self, mock_load):
        """Test transfer when config file loading raises an exception."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a config file path that will cause an error (invalid TOML)
            config_path = Path(tmpdir) / "invalid_config.toml"
            config_path.write_text("invalid toml content [unclosed")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "transfer",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                ],
            )

            # Should fail because cert/key are required for remote URLs
            assert result.exit_code == 1
            mock_load.assert_not_called()

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    def test_transfer_remote_url_without_certs(self, mock_load):
        """Test transfer with remote URL but missing certificates."""
        runner = CliRunner()

        result = runner.invoke(cli, ["transfer", "--artifact-location", "https://example.com/artifact.json"])

        assert result.exit_code == 1
        # Check the error was logged
        mock_load.assert_not_called()  # Should fail before loading artifacts

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    def test_transfer_http_error(self, mock_load):
        """Test transfer with HTTP error."""
        runner = CliRunner()

        mock_load.side_effect = httpx.HTTPError("Connection failed")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write("{}")
            artifact_path = artifact_file.name

        try:
            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_content_type_filter(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with --content-types filter."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path, "--content-types", "rpm"])

            assert result.exit_code == 0
            # Verify download_artifacts_concurrently was called with content_types filter
            # Args are: artifacts, distros, distribution_client, max_workers, content_types, archs
            call_args = mock_download.call_args
            # Check positional args (download_artifacts_concurrently is called with positional args)
            assert len(call_args.args) >= 6
            assert call_args.args[4] == ["rpm"]  # content_types
            assert call_args.args[5] is None  # archs
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_arch_filter(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with --archs filter."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path, "--archs", "x86_64"])

            assert result.exit_code == 0
            # Verify download_artifacts_concurrently was called with archs filter
            call_args = mock_download.call_args
            assert len(call_args.args) >= 6
            assert call_args.args[4] is None  # content_types
            assert call_args.args[5] == ["x86_64"]  # archs
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_with_multiple_filters(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with combined --content-types and --archs filters."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(
                cli,
                [
                    "transfer",
                    "--artifact-location",
                    artifact_path,
                    "--content-types",
                    "rpm,log",
                    "--archs",
                    "x86_64,noarch",
                ],
            )

            assert result.exit_code == 0
            # Verify download_artifacts_concurrently was called with both filters
            call_args = mock_download.call_args
            assert len(call_args.args) >= 6
            assert call_args.args[4] == ["rpm", "log"]  # content_types
            assert call_args.args[5] == ["x86_64", "noarch"]  # archs
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_without_filters(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer without filters transfers all artifacts."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 0
            # Verify download_artifacts_concurrently was called with None filters
            call_args = mock_download.call_args
            assert len(call_args.args) >= 6
            assert call_args.args[4] is None  # content_types
            assert call_args.args[5] is None  # archs
        finally:
            os.unlink(artifact_path)

    def test_transfer_invalid_content_type(self):
        """Test transfer with invalid content type raises validation error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            result = runner.invoke(
                cli, ["transfer", "--artifact-location", artifact_path, "--content-types", "invalid"]
            )

            assert result.exit_code == 1
            # Pydantic validation error message contains the error
            output = str(result.output) + str(result.exception) if result.exception else str(result.output)
            assert "Invalid content type" in output or "validation error" in output.lower()
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.DistributionClient")
    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    @patch("pulp_tool.cli.transfer.upload_downloaded_files_to_pulp")
    def test_transfer_with_build_id_namespace(
        self, mock_upload, mock_report, mock_download, mock_setup, mock_load, mock_dist_client
    ):
        """Test transfer with build_id and namespace generates artifact_location."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create cert and key files for remote URL
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("cert")
            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                f'[cli]\nbase_url = "https://pulp.example.com"\ncert = "{cert_path}"\nkey = "{key_path}"'
            )

            # Mock DistributionClient to avoid SSL errors
            mock_dist_client_instance = Mock()
            mock_dist_client_instance.session = Mock()
            mock_dist_client_instance.session.close = Mock()
            mock_dist_client.return_value = mock_dist_client_instance

            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "transfer",
                ],
            )

            assert result.exit_code == 0
            # Verify ConfigManager was used to load base_url
            mock_load.assert_called_once()

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    @patch("pulp_tool.cli.transfer.upload_downloaded_files_to_pulp")
    def test_transfer_with_upload(self, mock_upload, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with pulp_client triggers upload."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata
            from pulp_tool.models.results import PulpResultsModel

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data

            mock_client = Mock()
            mock_client.close = Mock()
            mock_setup.return_value = mock_client

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 1
            mock_result.failed = 0
            mock_download.return_value = mock_result

            from pulp_tool.models.repository import RepositoryRefs
            from pulp_tool.models.statistics import UploadCounts

            mock_upload_info = PulpResultsModel(
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
                artifacts={},
                distributions={},
                uploaded_counts=UploadCounts(),
            )
            # has_errors is a read-only property based on upload_errors length
            # Setting upload_errors to empty list means has_errors will be False
            mock_upload_info.upload_errors = []
            mock_upload.return_value = mock_upload_info

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 0
            mock_upload.assert_called_once()
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    @patch("pulp_tool.cli.transfer.upload_downloaded_files_to_pulp")
    def test_transfer_with_download_failures(self, mock_upload, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with download failures exits with error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 1
            mock_result.failed = 1  # One failure
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    @patch("pulp_tool.cli.transfer.upload_downloaded_files_to_pulp")
    def test_transfer_with_upload_errors(self, mock_upload, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with upload errors exits with error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata
            from pulp_tool.models.results import PulpResultsModel

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data

            mock_client = Mock()
            mock_client.close = Mock()
            mock_setup.return_value = mock_client

            mock_result = Mock()
            mock_result.pulled_artifacts = Mock()
            mock_result.completed = 1
            mock_result.failed = 0
            mock_download.return_value = mock_result

            from pulp_tool.models.repository import RepositoryRefs
            from pulp_tool.models.statistics import UploadCounts

            mock_upload_info = PulpResultsModel(
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
                artifacts={},
                distributions={},
                uploaded_counts=UploadCounts(),
            )
            # has_errors is a read-only property based on upload_errors length
            # Setting upload_errors to a non-empty list means has_errors will be True
            mock_upload_info.upload_errors = ["Error 1", "Error 2"]
            mock_upload.return_value = mock_upload_info

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    def test_transfer_generic_exception(self, mock_load):
        """Test transfer with generic exception."""
        runner = CliRunner()

        mock_load.side_effect = ValueError("Unexpected error")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.transfer.load_and_validate_artifacts")
    @patch("pulp_tool.cli.transfer.setup_repositories_if_needed")
    @patch("pulp_tool.cli.transfer.download_artifacts_concurrently")
    @patch("pulp_tool.cli.transfer.generate_transfer_report")
    def test_transfer_finally_block_cleanup(self, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer finally block cleans up clients."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data

            # Return None so upload doesn't happen (avoids real API calls)
            mock_setup.return_value = None

            from pulp_tool.models.artifacts import PulledArtifacts

            mock_result = Mock()
            mock_result.pulled_artifacts = PulledArtifacts()
            mock_result.completed = 0
            mock_result.failed = 0
            mock_download.return_value = mock_result

            result = runner.invoke(cli, ["transfer", "--artifact-location", artifact_path])

            assert result.exit_code == 0
        finally:
            os.unlink(artifact_path)


class TestCreateRepositoryCommand:

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_success(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "/api/pulp/konflux-test/api/v3/content/rpm/packages/019b1338-f265-7ad6-a278-8bead86e5c1d/",
            ],
        )
        assert result.exit_code == 0

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_no_packages_json(self, mock_helper_class, mock_client_class):
        """Test missing packages."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--json-data",
                """{
                    "name": "test-repo-name",
                    "distribution_options": {
                        "name": "test-distro-name",
                        "base_path": "test-base-path"
                    },
                    "packages":[]
                }""",
            ],
        )
        assert result.exit_code == 1
        assert "List should have at least 1 item" in result.output

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_no_packages_cli(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "",
            ],
        )
        assert "Unable to validate CLI options" in result.output

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_unexpected_error(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(side_effect=Exception())
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "/api/pulp/konflux-test/api/v3/content/file/packages/019b1338-f265-7ad6-a278-8bead86e5c1d/",
            ],
        )
        assert "Unexpected error during create-repository operation" in result.output


class TestGetUrlsCommand:
    """Test get-urls command functionality."""

    def test_config_option_not_required(self):
        """Test config_option with required=False includes default help."""
        decorator = config_option(required=False)
        assert callable(decorator)
        # The decorator should be a click.option function
        # We can't easily test the help text without invoking it, but we can verify it's callable

    def test_config_option_required(self):
        """Test config_option with required=True excludes default help."""
        decorator = config_option(required=True)
        assert callable(decorator)
        # The decorator should be a click.option function

    def test_cli_with_invalid_base64_config(self):
        """Test CLI handles invalid base64 config gracefully."""
        runner = CliRunner()
        invalid_base64 = (
            "A" * 100 + "B" * 100 + "C" * 50 + "=" * 3
        )  # Invalid padding (long enough to be detected as base64)

        # Actually invoke a command that will try to use the config
        # Group-level options (--config, --build-id, --namespace) come before the command
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    invalid_base64,
                    "--build-id",
                    "test",
                    "--namespace",
                    "test",
                    "upload",
                    "--parent-package",
                    "test",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            # Should exit with error code when trying to create client
            assert result.exit_code != 0
            assert "Failed to decode base64 config" in result.output or "Failed to load configuration" in result.output

    def test_debug_option(self):
        """Test debug_option returns a click option decorator."""
        decorator = debug_option()
        assert callable(decorator)
        # The decorator should be a click.option function
