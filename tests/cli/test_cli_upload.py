"""Tests for Click CLI commands."""

import base64
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
from click.testing import CliRunner

from pulp_tool.cli import cli


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
            assert "RESULTS JSON:" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_target_arch_repo_flag(self, mock_helper_class, mock_client_class):
        """--target-arch-repo is passed to setup_repositories and UploadRpmContext."""
        runner = CliRunner()

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        from pulp_tool.models.repository import RepositoryRefs

        mock_repos = RepositoryRefs(
            rpms_href="",
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
                    "--parent-package",
                    "test-pkg",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                    "--target-arch-repo",
                ],
            )

            assert result.exit_code == 0
            mock_helper.setup_repositories.assert_called_once_with(
                "test-build",
                signed_by=None,
                skip_artifacts_repo=False,
                target_arch_repo=True,
                skip_logs_repo=True,
                skip_sbom_repo=False,
            )
            context = mock_helper.process_uploads.call_args[0][1]
            assert context.target_arch_repo is True
            assert context.skip_logs_repo is True
            assert context.skip_sbom_repo is False

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
            assert "RESULTS JSON:" in result.output
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
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_with_artifact_results_folder(self, mock_helper_class, mock_client_class):
        """Test upload with --artifact-results as folder path (saves locally, skips Pulp upload)."""
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
            output_dir = Path(tmpdir) / "results"
            expected_path = str(output_dir / "pulp_results.json")
            mock_helper.process_uploads.return_value = expected_path

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
                    str(output_dir),
                ],
            )

            assert result.exit_code == 0
            assert "RESULTS JSON:" in result.output
            assert expected_path in result.output

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

    @patch("pulp_tool.cli.upload.PulpClient")
    def test_upload_auth_http_error_exits_one(self, mock_client_class):
        """Auth-style HTTP errors exit 1 like other HTTP failures."""
        runner = CliRunner()

        mock_client_class.create_from_config_file.side_effect = httpx.HTTPError("401 Unauthorized")

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
    def test_upload_runtime_error_no_access_token_exits_one(self, mock_client_class):
        """OAuth token failure (RuntimeError) is a failed upload."""
        runner = CliRunner()

        mock_client_class.create_from_config_file.side_effect = RuntimeError("Failed to obtain access token")

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

    def test_upload_files_base_path_without_results_json(self):
        """Test --files-base-path without --results-json fails."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "files"
            base_path.mkdir()
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
                    "--files-base-path",
                    str(base_path),
                ],
            )

            assert result.exit_code == 1
            assert "--files-base-path can only be used with --results-json" in result.output

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_with_results_json(self, mock_helper_class, mock_client_class):
        """Test upload with --results-json invokes process_uploads with results_json context."""
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
            results_json_path = Path(tmpdir) / "pulp_results.json"
            results_json_path.write_text('{"artifacts": {}}')
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
                    "--results-json",
                    str(results_json_path),
                    "--signed-by",
                    "key-123",
                ],
            )

            assert result.exit_code == 0
            mock_helper.setup_repositories.assert_called_once_with(
                "test-build",
                signed_by="key-123",
                skip_artifacts_repo=False,
                target_arch_repo=False,
                skip_logs_repo=True,
                skip_sbom_repo=True,
            )
            mock_helper.process_uploads.assert_called_once()
            call_args = mock_helper.process_uploads.call_args[0]
            assert call_args[2].rpms_href == "/test/"
            # Context (args) is second argument
            context = call_args[1]
            assert context.results_json == str(results_json_path)
            assert context.signed_by == "key-123"

    @patch("pulp_tool.cli.upload.PulpClient")
    @patch("pulp_tool.cli.upload.PulpHelper")
    def test_upload_results_json_extracts_build_id_namespace(self, mock_helper_class, mock_client_class):
        """Test upload with --results-json extracts build_id and namespace from artifact labels."""
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
            results_json_path = Path(tmpdir) / "pulp_results.json"
            results_json_path.write_text(
                json.dumps(
                    {
                        "artifacts": {
                            "x86_64/pkg.rpm": {
                                "labels": {
                                    "build_id": "extracted-build",
                                    "namespace": "extracted-ns",
                                    "arch": "x86_64",
                                },
                                "url": "https://example.com/pkg.rpm",
                                "sha256": "abc123",
                            }
                        }
                    }
                )
            )
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "upload",
                    "--results-json",
                    str(results_json_path),
                ],
            )

            assert result.exit_code == 0
            mock_helper.setup_repositories.assert_called_once_with(
                "extracted-build",
                signed_by=None,
                skip_artifacts_repo=False,
                target_arch_repo=False,
                skip_logs_repo=True,
                skip_sbom_repo=True,
            )
            context = mock_helper.process_uploads.call_args[0][1]
            assert context.build_id == "extracted-build"
            assert context.namespace == "extracted-ns"

    def test_upload_results_json_missing_build_id_namespace_in_json(self):
        """Test upload with --results-json but no build_id/namespace in artifact labels fails."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            results_json_path = Path(tmpdir) / "pulp_results.json"
            results_json_path.write_text('{"artifacts": {}}')
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "upload",
                    "--results-json",
                    str(results_json_path),
                ],
            )

            assert result.exit_code == 1
            assert "build_id and namespace" in result.output or "no artifacts" in result.output.lower()

    def test_upload_results_json_invalid_json_fails(self):
        """Test upload with --results-json and invalid JSON raises clear error."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            results_json_path = Path(tmpdir) / "pulp_results.json"
            results_json_path.write_text("{ invalid json }")
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )

            result = runner.invoke(
                cli,
                [
                    "--config",
                    str(config_path),
                    "upload",
                    "--results-json",
                    str(results_json_path),
                ],
            )

            assert result.exit_code == 1
            assert "Failed to read results JSON" in result.output

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
                assert "RESULTS JSON:" in result.output
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
            assert "RESULTS JSON:" in result.output

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
            assert "RESULTS JSON:" in result.output
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
                assert "RESULTS JSON:" in result.output
            finally:
                os.chdir(original_cwd)
