"""Tests for update-build CLI."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
from click.testing import CliRunner

from pulp_tool.cli import cli
from pulp_tool.cli.update_build import update_build

if "update-build" not in cli.commands:
    cli.add_command(update_build)


class TestUpdateBuildCLI:
    def test_fails_without_artifact_results(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config", "/nonexistent.toml", "update-build", "--results-json", "/tmp/x.json"],
        )
        assert result.exit_code != 0
        assert "--artifact-results" in result.output

    def test_fails_invalid_artifact_results_format(self, tmp_path: Path) -> None:
        json_path = tmp_path / "pulp_results.json"
        json_path.write_text('{"version":1,"build_id":"b","namespace":"ns","artifacts":{}}', encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "update-build",
                "--results-json",
                str(json_path),
                "--artifact-results",
                "only-one-path",
            ],
        )
        assert result.exit_code != 0

    def test_invokes_run_update_build(self, tmp_path: Path) -> None:
        json_path = tmp_path / "pulp_results.json"
        json_path.write_text(
            '{"version":1,"build_id":"b1","namespace":"ns1","artifacts":{}}',
            encoding="utf-8",
        )
        url_file = tmp_path / "url"
        digest_file = tmp_path / "dig"
        with patch("pulp_tool.cli.update_build.PulpClient.create_from_config_file") as mock_client:
            mock_client.return_value = MagicMock()
            with patch("pulp_tool.cli.update_build.run_update_build", return_value="quay.io/r@sha256:abc") as mock_run:
                runner = CliRunner()
                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(tmp_path / "cli.toml"),
                        "update-build",
                        "--results-json",
                        str(json_path),
                        "--artifact-results",
                        f"{url_file},{digest_file}",
                        "--oci-storage",
                        "quay.io/ns/repo:tag",
                    ],
                )
        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()

    def test_reads_cluster_and_logs_correlation_id(self, tmp_path: Path) -> None:
        config = tmp_path / "cli.toml"
        config.write_text('[cli]\ncluster = " appsre "\n', encoding="utf-8")
        json_path = tmp_path / "pulp_results.json"
        json_path.write_text('{"version":1,"build_id":"b1","namespace":"ns1","artifacts":{}}', encoding="utf-8")
        url_file = tmp_path / "url"
        digest_file = tmp_path / "dig"
        with patch("pulp_tool.cli.update_build.PulpClient.create_from_config_file") as mock_client:
            mock_client.return_value = MagicMock()
            with patch("pulp_tool.cli.update_build.run_update_build", return_value="quay.io/r@sha256:abc") as mock_run:
                with patch("pulp_tool.cli.update_build.resolve_correlation_id", return_value="corr-e2e"):
                    with patch("pulp_tool.cli.update_build.logging.info") as mock_log_info:
                        runner = CliRunner()
                        result = runner.invoke(
                            cli,
                            [
                                "--config",
                                str(config),
                                "--build-id",
                                "b1",
                                "--namespace",
                                "ns1",
                                "update-build",
                                "--results-json",
                                str(json_path),
                                "--artifact-results",
                                f"{url_file},{digest_file}",
                                "--oci-storage",
                                "quay.io/ns/repo:tag",
                            ],
                        )
        assert result.exit_code == 0, result.output
        mock_log_info.assert_any_call("X-Correlation-ID: %s", "corr-e2e")
        ctx_arg = mock_run.call_args[0][1]
        assert ctx_arg.cluster == "appsre"

    def test_cluster_read_failure_is_non_fatal(self, tmp_path: Path) -> None:
        json_path = tmp_path / "pulp_results.json"
        json_path.write_text('{"version":1,"build_id":"b1","namespace":"ns1","artifacts":{}}', encoding="utf-8")
        url_file = tmp_path / "url"
        digest_file = tmp_path / "dig"
        with patch("pulp_tool.cli.update_build.PulpClient.create_from_config_file") as mock_client:
            mock_client.return_value = MagicMock()
            with patch("pulp_tool.cli.update_build.run_update_build", return_value="quay.io/r@sha256:abc"):
                with patch("pulp_tool.utils.config_manager.ConfigManager") as mock_cm:
                    mock_cm.return_value.load.side_effect = OSError("no config")
                    runner = CliRunner()
                    result = runner.invoke(
                        cli,
                        [
                            "--config",
                            str(tmp_path / "cli.toml"),
                            "update-build",
                            "--results-json",
                            str(json_path),
                            "--artifact-results",
                            f"{url_file},{digest_file}",
                            "--oci-storage",
                            "quay.io/ns/repo:tag",
                        ],
                    )
        assert result.exit_code == 0, result.output

    def test_handles_http_and_generic_errors(self, tmp_path: Path) -> None:
        json_path = tmp_path / "pulp_results.json"
        json_path.write_text('{"version":1,"build_id":"b1","namespace":"ns1","artifacts":{}}', encoding="utf-8")
        url_file = tmp_path / "url"
        digest_file = tmp_path / "dig"
        base = [
            "--config",
            str(tmp_path / "cli.toml"),
            "update-build",
            "--results-json",
            str(json_path),
            "--artifact-results",
            f"{url_file},{digest_file}",
            "--oci-storage",
            "quay.io/ns/repo:tag",
        ]
        with patch("pulp_tool.cli.update_build.PulpClient.create_from_config_file") as mock_client:
            mock_client.return_value = MagicMock()
            with patch("pulp_tool.cli.update_build.run_update_build", side_effect=httpx.HTTPError("boom")):
                with patch("pulp_tool.cli.update_build.handle_http_error") as mock_http_err:
                    runner = CliRunner()
                    result = runner.invoke(cli, base)
        assert result.exit_code == 0
        mock_http_err.assert_called_once()

        with patch("pulp_tool.cli.update_build.PulpClient.create_from_config_file") as mock_client:
            mock_client.return_value = MagicMock()
            with patch("pulp_tool.cli.update_build.run_update_build", side_effect=RuntimeError("fail")):
                with patch("pulp_tool.cli.update_build.handle_generic_error") as mock_gen_err:
                    runner = CliRunner()
                    result = runner.invoke(cli, base)
        assert result.exit_code == 0
        mock_gen_err.assert_called_once()
