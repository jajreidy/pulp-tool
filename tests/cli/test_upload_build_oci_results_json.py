"""Coverage for upload-build OCI --results-json resolution."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pulp_tool.cli import cli


def test_upload_build_results_json_oci_resolves(tmp_path: Path) -> None:
    config = tmp_path / "cli.toml"
    config.write_text(
        '[cli]\nbase_url = "https://pulp.example"\napi_root = "/pulp/api/v3"\ndomain = "d"\n',
        encoding="utf-8",
    )
    local = tmp_path / "pulp_results.json"
    local.write_text(
        '{"version":1,"build_id":"b1","namespace":"ns1","artifacts":{}}',
        encoding="utf-8",
    )
    with patch("pulp_tool.cli.upload_build.PulpClient.create_from_config_file") as mock_client:
        mock_client.return_value = MagicMock()
        with patch("pulp_tool.cli.upload_build.PulpHelper") as mock_helper:
            mock_helper.return_value.setup_repositories.return_value = MagicMock()
            mock_helper.return_value.process_uploads.return_value = "https://example/results"
            with patch(
                "pulp_tool.cli.upload_build.resolve_results_json_path",
                return_value=local,
            ):
                runner = CliRunner()
                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config),
                        "upload-build",
                        "--results-json",
                        "quay.io/ns/r@sha256:abc",
                        "--rpm-path",
                        str(tmp_path),
                    ],
                )
    assert result.exit_code == 0, result.output


def test_upload_build_results_json_oci_error(tmp_path: Path) -> None:
    config = tmp_path / "cli.toml"
    config.write_text(
        '[cli]\nbase_url = "https://pulp.example"\napi_root = "/pulp/api/v3"\ndomain = "d"\n',
        encoding="utf-8",
    )
    from pulp_tool.utils.results_json_io import ResultsJsonIOError

    with patch(
        "pulp_tool.cli.upload_build.resolve_results_json_path",
        side_effect=ResultsJsonIOError("bad ref"),
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config",
                str(config),
                "upload-build",
                "--results-json",
                "quay.io/ns/r@sha256:abc",
            ],
        )
    assert result.exit_code != 0
    assert "bad ref" in result.output


def test_upload_build_results_json_local_missing(tmp_path: Path) -> None:
    config = tmp_path / "cli.toml"
    config.write_text(
        '[cli]\nbase_url = "https://pulp.example"\napi_root = "/pulp/api/v3"\ndomain = "d"\n',
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config",
            str(config),
            "upload-build",
            "--results-json",
            str(tmp_path / "missing.json"),
        ],
    )
    assert result.exit_code != 0
    assert "not found" in result.output.lower()
