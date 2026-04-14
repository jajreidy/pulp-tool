"""Tests for Click CLI commands."""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
from click.testing import CliRunner

from pulp_tool.cli import cli


class TestPullCommand:
    """Test pull command functionality."""

    def test_pull_missing_artifact_location_and_build_id(self):
        """Test pull with neither artifact_location nor build_id provided."""
        runner = CliRunner()
        result = runner.invoke(cli, ["pull"])
        assert result.exit_code == 1
        assert "Either --artifact-location OR" in result.output

    def test_pull_build_id_without_namespace(self):
        """Test pull with build_id but no namespace."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--build-id", "test-build", "pull"])
        assert result.exit_code == 1
        assert "Both --build-id and --namespace must be provided" in result.output

    def test_pull_build_id_without_config(self):
        """Test pull with build_id+namespace but no --transfer-dest or --config."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--build-id", "test-build", "--namespace", "test-ns", "pull"])
        assert result.exit_code == 1
        assert "transfer-dest" in result.output.lower() or "config" in result.output.lower()

    def test_pull_conflicting_options(self):
        """Test pull with both artifact_location and build_id."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--build-id",
                "test-build",
                "--namespace",
                "test-ns",
                "pull",
                "--artifact-location",
                "http://example.com/artifact.json",
            ],
        )
        assert result.exit_code == 1
        assert "Cannot use --artifact-location with --build-id" in result.output

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

            assert result.exit_code == 0
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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
                    "pull",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--cert-path",
                    str(cert_path),
                    "--key-path",
                    str(key_path),
                ],
            )

            assert result.exit_code == 0

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            # Don't provide --key-path, should be loaded from --transfer-dest config
            result = runner.invoke(
                cli,
                [
                    "pull",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--transfer-dest",
                    str(config_path),
                ],
            )

            assert result.exit_code == 0

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    def test_pull_with_username_password_from_config(
        self, mock_report, mock_download, mock_setup, mock_load, mock_dist_client
    ):
        """Test pull with username/password from config (no cert/key)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\n' 'username = "myuser"\n' 'password = "mypass"\n'
            )

            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

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
                    "pull",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--transfer-dest",
                    str(config_path),
                ],
            )

            assert result.exit_code == 0
            mock_dist_client.assert_called_once_with(username="myuser", password="mypass")

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    def test_pull_with_distribution_config(self, mock_report, mock_download, mock_setup, mock_load, mock_dist_client):
        """Test pull with --distribution-config overrides transfer-dest for auth."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # transfer-dest has cert/key, distribution-config has username/password
            transfer_config = Path(tmpdir) / "transfer.toml"
            transfer_config.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\n' 'cert = "/tmp/cert.pem"\n' 'key = "/tmp/key.pem"\n'
            )
            dist_config = Path(tmpdir) / "dist_auth.toml"
            dist_config.write_text('[cli]\nusername = "distuser"\npassword = "distpass"\n')

            from pulp_tool.models.artifacts import ArtifactData, ArtifactJsonResponse, ArtifactMetadata

            mock_artifact_data = ArtifactData(
                artifact_json=ArtifactJsonResponse(
                    artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})}, distributions={}
                ),
                artifacts={"test.rpm": ArtifactMetadata(labels={"build_id": "test"})},
            )
            mock_load.return_value = mock_artifact_data
            mock_setup.return_value = None

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
                    "pull",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--transfer-dest",
                    str(transfer_config),
                    "--distribution-config",
                    str(dist_config),
                ],
            )

            assert result.exit_code == 0
            # Auth from --distribution-config (username/password), not transfer-dest (cert/key)
            mock_dist_client.assert_called_once_with(username="distuser", password="distpass")

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
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
                    "pull",
                    "--artifact-location",
                    "https://example.com/artifact.json",
                    "--transfer-dest",
                    str(config_path),
                ],
            )

            # Should fail because cert/key are required for remote URLs
            assert result.exit_code == 1
            mock_load.assert_not_called()

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    def test_transfer_remote_url_without_certs(self, mock_load):
        """Test transfer with remote URL but missing certificates."""
        runner = CliRunner()

        result = runner.invoke(cli, ["pull", "--artifact-location", "https://example.com/artifact.json"])

        assert result.exit_code == 1
        # Check the error was logged
        mock_load.assert_not_called()  # Should fail before loading artifacts

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    def test_transfer_http_error(self, mock_load):
        """Test transfer with HTTP error."""
        runner = CliRunner()

        mock_load.side_effect = httpx.HTTPError("Connection failed")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write("{}")
            artifact_path = artifact_file.name

        try:
            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path, "--content-types", "rpm"])

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

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path, "--archs", "x86_64"])

            assert result.exit_code == 0
            # Verify download_artifacts_concurrently was called with archs filter
            call_args = mock_download.call_args
            assert len(call_args.args) >= 6
            assert call_args.args[4] is None  # content_types
            assert call_args.args[5] == ["x86_64"]  # archs
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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
                    "pull",
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

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

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
            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path, "--content-types", "invalid"])

            assert result.exit_code == 1
            # Pydantic validation error message contains the error
            output = str(result.output) + str(result.exception) if result.exception else str(result.output)
            assert "Invalid content type" in output or "validation error" in output.lower()
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    @patch("pulp_tool.cli.pull.upload_downloaded_files_to_pulp")
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
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "pull",
                    "--transfer-dest",
                    str(config_path),
                ],
            )

            assert result.exit_code == 0
            # Verify ConfigManager was used to load base_url
            mock_load.assert_called_once()

    @patch("pulp_tool.cli.pull.DistributionClient")
    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    @patch("pulp_tool.cli.pull.upload_downloaded_files_to_pulp")
    def test_pull_with_build_id_namespace_using_config(
        self, mock_upload, mock_report, mock_download, mock_setup, mock_load, mock_dist_client
    ):
        """Test pull with build_id and namespace using group-level --config instead of --transfer-dest."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = Path(tmpdir) / "cert.pem"
            cert_path.write_text("cert")
            key_path = Path(tmpdir) / "key.pem"
            key_path.write_text("key")

            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                f'[cli]\nbase_url = "https://pulp.example.com"\ncert = "{cert_path}"\nkey = "{key_path}"'
            )

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

            # Use --config instead of --transfer-dest
            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "--build-id",
                    "test-build",
                    "--namespace",
                    "test-ns",
                    "pull",
                ],
            )

            assert result.exit_code == 0
            mock_load.assert_called_once()
            mock_upload.assert_not_called()

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    @patch("pulp_tool.cli.pull.upload_downloaded_files_to_pulp")
    def test_transfer_with_upload(self, mock_upload, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with pulp_client triggers upload."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as dest_cfg:
            dest_cfg.write('[cli]\nbase_url = "https://pulp.example.com"\n')
            transfer_config_path = dest_cfg.name

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

            result = runner.invoke(
                cli,
                ["pull", "--artifact-location", artifact_path, "--transfer-dest", transfer_config_path],
            )

            assert result.exit_code == 0
            mock_upload.assert_called_once()
        finally:
            os.unlink(transfer_config_path)
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    @patch("pulp_tool.cli.pull.upload_downloaded_files_to_pulp")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
    @patch("pulp_tool.cli.pull.upload_downloaded_files_to_pulp")
    def test_transfer_with_upload_errors(self, mock_upload, mock_report, mock_download, mock_setup, mock_load):
        """Test transfer with upload errors exits with error."""
        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as dest_cfg:
            dest_cfg.write('[cli]\nbase_url = "https://pulp.example.com"\n')
            transfer_config_path = dest_cfg.name

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

            result = runner.invoke(
                cli,
                ["pull", "--artifact-location", artifact_path, "--transfer-dest", transfer_config_path],
            )

            assert result.exit_code == 1
        finally:
            os.unlink(transfer_config_path)
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    def test_transfer_generic_exception(self, mock_load):
        """Test transfer with generic exception."""
        runner = CliRunner()

        mock_load.side_effect = ValueError("Unexpected error")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as artifact_file:
            artifact_file.write('{"artifacts": [], "distributions": {}}')
            artifact_path = artifact_file.name

        try:
            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

            assert result.exit_code == 1
        finally:
            os.unlink(artifact_path)

    @patch("pulp_tool.cli.pull.load_and_validate_artifacts")
    @patch("pulp_tool.cli.pull.setup_repositories_if_needed")
    @patch("pulp_tool.cli.pull.download_artifacts_concurrently")
    @patch("pulp_tool.cli.pull.generate_pull_report")
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

            result = runner.invoke(cli, ["pull", "--artifact-location", artifact_path])

            assert result.exit_code == 0
        finally:
            os.unlink(artifact_path)
