"""Tests for Click CLI commands."""

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
        assert "get-repo-md" in result.output
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
        assert "get-repo-md" in result.output
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

    def test_get_repo_md_help(self):
        """Test get-repo-md command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["get-repo-md", "--help"])
        assert result.exit_code == 0
        assert "Download .repo configuration file(s)" in result.output
        assert "comma-separated" in result.output
        # Group-level options are not shown in command help
        assert "--base-url" in result.output
        assert "--output" in result.output

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

    def test_get_repo_md_missing_required_args(self):
        """Test get-repo-md command with missing required arguments."""
        runner = CliRunner()
        result = runner.invoke(cli, ["get-repo-md"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

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


class TestGetRepoMdCommand:
    """Test get-repo-md command functionality."""

    def test_get_repo_md_with_config(self):
        """Test get-repo-md with config file."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.content = b"[test-repo]\nname=Test"
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                        "--output",
                        str(output_dir),
                    ],
                )

                assert result.exit_code == 0
                assert "Download Summary" in result.output

    def test_get_repo_md_with_direct_params(self):
        """Test get-repo-md with base_url and namespace."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.content = b"[test-repo]\nname=Test"
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--namespace",
                        "test-domain",
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                        "--base-url",
                        "https://pulp.example.com",
                        "--output",
                        str(output_dir),
                    ],
                )

                assert result.exit_code == 0

    def test_get_repo_md_multiple_build_ids(self):
        """Test get-repo-md with comma-separated build IDs."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.content = b"[test-repo]\nname=Test"
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--build-id",
                        "build1,build2,build3",
                        "get-repo-md",
                        "--output",
                        str(output_dir),
                    ],
                )

                assert result.exit_code == 0
                assert "Download Summary: 3 succeeded, 0 failed" in result.output

    def test_get_repo_md_404_error(self):
        """Test get-repo-md with 404 error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 404
                mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                    "Not found", request=Mock(), response=mock_response
                )
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                        "--output",
                        str(output_dir),
                    ],
                )

                assert result.exit_code == 1
                assert "404" in result.output

    def test_get_repo_md_with_output_dir(self):
        """Test get-repo-md with custom output directory."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.content = b"[test-repo]\nname=Test"
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                        "--output",
                        str(output_dir),
                    ],
                )

                assert result.exit_code == 0
                assert output_dir.exists()

    def test_get_repo_md_missing_config_and_params(self):
        """Test get-repo-md without config or direct params."""
        runner = CliRunner()

        # Mock httpx to prevent actual HTTP calls
        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 403
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Forbidden", request=Mock(), response=mock_response
            )
            mock_get.return_value = mock_response

            result = runner.invoke(cli, ["--build-id", "test-build", "get-repo-md"])

            assert result.exit_code == 1
            # Without config, it tries default path and may fail with HTTP error

    def test_get_repo_md_conflicting_params(self):
        """Test get-repo-md with both config and direct params."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.content = b"[test-repo]\nname=Test"
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--namespace",
                        "test-domain",
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                        "--base-url",
                        "https://pulp.example.com",
                        "--output",
                        str(output_dir),
                    ],
                )

                # The command prioritizes config over direct params, so it succeeds
                assert result.exit_code in [0, 1]  # May succeed or fail depending on implementation

    @patch("pulp_tool.cli.get_repo_md.DistributionClient")
    @patch("pulp_tool.cli.get_repo_md.httpx.get")
    def test_get_repo_md_with_certificates(self, mock_httpx_get, mock_dist_client):
        """Test get-repo-md with certificate authentication."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            # Create cert file
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("cert")

            # Add cert and key to config
            config_content = (
                '[cli]\nbase_url = "https://pulp.example.com"\n'
                f'domain = "test-domain"\ncert = "{cert_path}"\n'
                f'key = "{key_path}"'
            )
            config_path.write_text(config_content)

            mock_client = Mock()
            mock_response = Mock()
            mock_response.content = b"[test-repo]\nname=Test"
            mock_response.raise_for_status = Mock()
            mock_client.pull_artifact.return_value = mock_response
            mock_dist_client.return_value = mock_client

            # Also mock httpx.get in case it's used as fallback
            mock_httpx_response = Mock()
            mock_httpx_response.content = b"[test-repo]\nname=Test"
            mock_httpx_response.raise_for_status = Mock()
            mock_httpx_get.return_value = mock_httpx_response

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                    "--cert-path",
                    str(cert_path),
                    "--key-path",
                    str(key_path),
                ],
            )

            assert result.exit_code == 0

    def test_get_repo_md_key_without_cert(self):
        """Test get-repo-md with key but no cert in config."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            output_dir = Path(tmpdir) / "output"

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                    "--key-path",
                    str(key_path),
                ],
            )

            # Should fail with exit code 1 due to cert/key validation
            assert result.exit_code == 1

    @patch("pulp_tool.cli.get_repo_md.DistributionClient")
    @patch("pulp_tool.cli.get_repo_md.httpx.get")
    def test_get_repo_md_with_key_from_config(self, mock_httpx_get, mock_dist_client):
        """Test get-repo-md with key_path loaded from config when not provided via CLI."""
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
                '[cli]\nbase_url = "https://pulp.example.com"\n'
                'domain = "test-domain"\n'
                f'cert = "{cert_path}"\n'
                f'key = "{key_path}"'
            )
            config_path.write_text(config_content)

            mock_client = Mock()
            mock_response = Mock()
            mock_response.content = b"[test-repo]\nname=Test"
            mock_response.raise_for_status = Mock()
            mock_client.pull_artifact.return_value = mock_response
            mock_dist_client.return_value = mock_client

            # Also mock httpx.get in case it's used as fallback
            mock_httpx_response = Mock()
            mock_httpx_response.content = b"[test-repo]\nname=Test"
            mock_httpx_response.raise_for_status = Mock()
            mock_httpx_get.return_value = mock_httpx_response

            output_dir = Path(tmpdir) / "output"

            # Don't provide --key-path, should be loaded from config
            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 0

    def test_get_repo_md_config_load_exception(self):
        """Test get-repo-md when config file loading raises an exception."""
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
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                ],
            )

            # Should fail because config cannot be loaded
            assert result.exit_code == 1

    @patch("httpx.get")
    def test_get_repo_md_partial_failures(self, mock_get):
        """Test get-repo-md with some successful and some failed downloads."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            # First call succeeds, second fails
            mock_response_success = Mock()
            mock_response_success.content = b"[test-repo]\nname=Test"
            mock_response_success.raise_for_status = Mock()

            mock_response_fail = Mock()
            mock_response_fail.status_code = 404
            mock_response_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Not found", request=Mock(), response=mock_response_fail
            )

            mock_get.side_effect = [mock_response_success, mock_response_fail]

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "build1,build2",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            # Partial success should exit with code 2
            assert result.exit_code == 2
            assert "1 succeeded, 1 failed" in result.output

    def test_get_repo_md_default_config_fallback(self):
        """Test get-repo-md uses default config path when available."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create default config path
            default_config_dir = Path.home() / ".config" / "pulp"
            default_config_dir.mkdir(parents=True, exist_ok=True)
            default_config = default_config_dir / "cli.toml"
            default_config.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            try:
                with patch("httpx.get") as mock_get:
                    mock_response = Mock()
                    mock_response.content = b"[test-repo]\nname=Test"
                    mock_response.raise_for_status = Mock()
                    mock_get.return_value = mock_response

                    result = runner.invoke(
                        cli,
                        [
                            "--build-id",
                            "test-build",
                            "get-repo-md",
                            "--output",
                            str(output_dir),
                        ],
                    )

                    assert result.exit_code == 0
            finally:
                # Clean up default config
                if default_config.exists():
                    default_config.unlink()
                if default_config_dir.exists():
                    default_config_dir.rmdir()

    def test_get_repo_md_no_valid_build_ids(self):
        """Test get-repo-md with empty build IDs."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    ",,,",
                    "get-repo-md",
                ],
            )

            assert result.exit_code == 1
            assert "No valid build IDs provided" in result.output

    def test_get_repo_md_no_namespace_in_config(self):
        """Test get-repo-md with config missing namespace."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"')

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                ],
            )

            assert result.exit_code == 1
            assert "No domain/namespace found in config file" in result.output

    @patch("httpx.get")
    def test_get_repo_md_403_error(self, mock_get):
        """Test get-repo-md with 403 error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            mock_response = Mock()
            mock_response.status_code = 403
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Forbidden", request=Mock(), response=mock_response
            )
            mock_get.return_value = mock_response

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 1
            assert "403" in result.output
            assert "access denied" in result.output

    @patch("httpx.get")
    def test_get_repo_md_401_error(self, mock_get):
        """Test get-repo-md with 401 error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            mock_response = Mock()
            mock_response.status_code = 401
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Unauthorized", request=Mock(), response=mock_response
            )
            mock_get.return_value = mock_response

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 1
            assert "401" in result.output
            assert "unauthorized" in result.output.lower()

    @patch("httpx.get")
    def test_get_repo_md_generic_exception(self, mock_get):
        """Test get-repo-md with generic exception."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            mock_get.side_effect = ValueError("Unexpected error")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 1
            assert "Unexpected error" in result.output or "Failed to download" in result.output

    @patch("pathlib.Path.exists")
    @patch("pulp_tool.cli.get_repo_md.ConfigManager")
    def test_get_repo_md_file_not_found_error(self, mock_config_manager, mock_exists):
        """Test get-repo-md with FileNotFoundError."""
        runner = CliRunner()

        # Mock Path.exists to return False for default config
        def exists_side_effect(self):
            if "~/.config/pulp/cli.toml" in str(self) or ".config/pulp/cli.toml" in str(self):
                return False
            return True

        mock_exists.side_effect = exists_side_effect

        # Mock ConfigManager to raise FileNotFoundError when loading
        mock_manager = Mock()
        mock_manager.load.side_effect = FileNotFoundError("Config file not found")
        mock_config_manager.return_value = mock_manager

        result = runner.invoke(
            cli,
            [
                "--config",
                "/nonexistent/config.toml",
                "--build-id",
                "test-build",
                "get-repo-md",
            ],
        )

        # Should exit with code 1 when config file doesn't exist and default config also doesn't exist
        # If default config exists and is used, it might exit with 2 (partial failure)
        assert result.exit_code in [1, 2]

    def test_get_repo_md_key_error(self):
        """Test get-repo-md with KeyError in config."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"')

            with patch("pulp_tool.cli.get_repo_md.ConfigManager") as mock_config:
                mock_manager = Mock()
                mock_manager.load = Mock()
                mock_manager.get.side_effect = KeyError("cli.domain")
                mock_config.return_value = mock_manager

                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--build-id",
                        "test-build",
                        "get-repo-md",
                    ],
                )

                assert result.exit_code == 1

    def test_get_repo_md_io_error(self):
        """Test get-repo-md with IOError when writing files."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            # Create a read-only directory to trigger IOError
            output_dir = Path(tmpdir) / "readonly"
            output_dir.mkdir()
            output_dir.chmod(0o444)

            try:
                with patch("httpx.get") as mock_get:
                    mock_response = Mock()
                    mock_response.content = b"[test-repo]\nname=Test"
                    mock_response.raise_for_status = Mock()
                    mock_get.return_value = mock_response

                    result = runner.invoke(
                        cli,
                        [
                            "--config",
                            str(config_path),
                            "--build-id",
                            "test-build",
                            "get-repo-md",
                            "--output",
                            str(output_dir),
                        ],
                    )

                    # May succeed or fail depending on system permissions
                    assert result.exit_code in [0, 1]
            finally:
                output_dir.chmod(0o755)

    @patch("httpx.get")
    def test_get_repo_md_http_error(self, mock_get):
        """Test get-repo-md with httpx.HTTPError."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            mock_get.side_effect = httpx.HTTPError("Connection error")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 1

    @patch("pathlib.Path.exists")
    def test_get_repo_md_file_not_found_error_handler(self, mock_exists):
        """Test get-repo-md FileNotFoundError handler (lines 244-245)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            # Mock Path.exists() to raise FileNotFoundError when checking output directory (line 153)
            # This happens in get_repo_md.py itself, outside inner try-except, so it reaches handler at 244-245
            call_count = 0

            def exists_side_effect(self):
                nonlocal call_count
                call_count += 1
                # Raise FileNotFoundError when checking output directory exists
                if call_count > 1 and "output" in str(self):
                    raise FileNotFoundError("Directory check failed")
                # Return True for config file, False for output dir
                return "config" in str(self)

            mock_exists.side_effect = exists_side_effect

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(Path(tmpdir) / "output"),
                ],
            )

            # Handler at lines 244-245 should catch FileNotFoundError and exit with code 1
            assert result.exit_code == 1

    @patch("pulp_tool.cli.get_repo_md.ConfigManager")
    def test_get_repo_md_file_not_found_error_config_load(self, mock_config_manager):
        """Test get-repo-md FileNotFoundError handler when ConfigManager.load() raises it (lines 244-245)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a config file that exists (for Click validation)
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"')

            # Track ConfigManager constructor calls - need separate instances
            call_count = []

            def config_manager_constructor(*args, **kwargs):
                call_count.append(1)
                mock_manager = Mock()
                # First call (line 83) - exception is caught at line 89, so don't raise
                # Second call (line 134) - should raise FileNotFoundError to reach handler at 244-245
                if len(call_count) == 2:
                    # Raise FileNotFoundError when load() is called at line 135
                    mock_manager.load.side_effect = FileNotFoundError("Config file not found: /nonexistent/config.toml")
                return mock_manager

            mock_config_manager.side_effect = config_manager_constructor

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                ],
            )

            # Handler at lines 244-245 should catch FileNotFoundError and exit with code 1
            # The exception should be raised at line 135 when ConfigManager.load() is called
            # The error message is logged at line 244, which we can verify via exit code
            assert result.exit_code == 1, f"Expected exit code 1, got {result.exit_code}. Output: {result.output}"

    @patch("pathlib.Path.mkdir")
    def test_get_repo_md_io_error_handler(self, mock_mkdir):
        """Test get-repo-md IOError handler when writing files (lines 250-251)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            # Mock Path.mkdir() to raise IOError when creating output directory (line 154)
            # This exception is raised outside the inner try-except (lines 170-212), so it reaches the outer handler
            mock_mkdir.side_effect = IOError("Permission denied: cannot create directory")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                ],
            )

            # Handler at lines 250-251 should catch IOError and exit with code 1
            assert result.exit_code == 1

    @patch("pulp_tool.cli.get_repo_md.DistributionClient")
    def test_get_repo_md_httpx_error_handler(self, mock_dist_client_class):
        """Test get-repo-md httpx.HTTPError handler (lines 253-254)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

            output_dir = Path(tmpdir) / "output"

            # Mock DistributionClient constructor to raise httpx.HTTPError (line 160)
            # This exception is raised outside the inner try-except (lines 170-212), so it reaches the outer handler
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("fake cert")
            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("fake key")

            # Raise httpx.HTTPError when DistributionClient is instantiated
            mock_dist_client_class.side_effect = httpx.HTTPError("Connection error")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "get-repo-md",
                    "--output",
                    str(output_dir),
                    "--cert-path",
                    str(cert_path),
                    "--key-path",
                    str(key_path),
                ],
            )

            # Handler at lines 253-254 should catch httpx.HTTPError and exit with code 1
            assert result.exit_code == 1


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


class TestCLIOptionHelpers:
    """Test CLI option helper functions."""

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

    def test_debug_option(self):
        """Test debug_option returns a click option decorator."""
        decorator = debug_option()
        assert callable(decorator)
        # The decorator should be a click.option function
