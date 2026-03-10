"""Tests for search-by-checksum CLI command."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
from click.testing import CliRunner

from pulp_tool.cli import cli

# Valid 64-char SHA256 checksums for testing
VALID_CHECKSUM_1 = "a" * 64
VALID_CHECKSUM_2 = "b" * 64
VALID_CHECKSUM_3 = "c" * 64


def _make_rpm_response(results: list) -> httpx.Response:
    """Create mock httpx response for RPM packages API."""
    response = Mock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {
        "results": results,
        "count": len(results),
        "next": None,
        "previous": None,
    }
    response.raise_for_status = Mock()
    return response


class TestSearchByChecksumHelp:
    """Test search-by-checksum command help."""

    def test_search_by_checksum_help(self):
        """Test search-by-checksum command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["search-by-checksum", "--help"])
        assert result.exit_code == 0
        assert "Search for RPM packages in Pulp by SHA256 checksum" in result.output
        assert "--checksum" in result.output or "-c" in result.output
        assert "--checksums" in result.output
        assert "--results-json" in result.output
        assert "--output-results" in result.output
        assert "--output" in result.output or "-o" in result.output
        assert "json" in result.output
        assert "table" in result.output
        assert "--checksums-only" in result.output

    def test_main_help_includes_search_by_checksum(self):
        """Test main CLI help includes search-by-checksum command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "search-by-checksum" in result.output


class TestSearchByChecksumValidation:
    """Test search-by-checksum input validation."""

    def test_no_checksums_provided(self):
        """Test error when no checksums are provided."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by-checksum"],
            )
        assert result.exit_code == 1
        assert "At least one checksum must be provided" in result.output

    def test_invalid_checksum_format(self):
        """Test error when checksum has invalid format."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by-checksum", "--checksum", "not64chars"],
            )
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_invalid_checksum_too_short(self):
        """Test error when checksum is too short."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by-checksum", "--checksum", "abc123"],
            )
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_invalid_checksum_non_hex(self):
        """Test error when checksum contains non-hex characters."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    "g" * 64,  # 'g' is not hex
                ],
            )
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_config_required(self):
        """Test error when config is not provided."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["search-by-checksum", "--checksum", VALID_CHECKSUM_1],
        )
        assert result.exit_code == 1
        assert "--config is required" in result.output

    def test_results_json_requires_output_results(self):
        """Test error when --results-json is used without --output-results."""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"artifacts":{},"distributions":{}}')
            results_path = f.name
        try:
            with tempfile_config() as config_path:
                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        config_path,
                        "search-by-checksum",
                        "--results-json",
                        results_path,
                    ],
                )
            assert result.exit_code == 1
            assert "--output-results is required" in result.output
        finally:
            Path(results_path).unlink(missing_ok=True)


class TestSearchByChecksumSuccess:
    """Test successful search-by-checksum scenarios."""

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_single_checksum_json_output(self, mock_client_class):
        """Test single checksum with JSON output."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/12345/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "test-package",
                    "epoch": "0",
                    "version": "1.0.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {"build_id": "test-build"},
                }
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["pkgId"] == VALID_CHECKSUM_1
        assert output[0]["name"] == "test-package"
        assert output[0]["version"] == "1.0.0"
        assert output[0]["arch"] == "x86_64"

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_multiple_checksums_repeated_option(self, mock_client_class):
        """Test multiple checksums via repeated --checksum option."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "epoch": "0",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
                {
                    "pulp_href": "/api/v3/content/rpm/packages/2/",
                    "sha256": VALID_CHECKSUM_2,
                    "name": "pkg2",
                    "epoch": "0",
                    "version": "2.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "-c",
                    VALID_CHECKSUM_1,
                    "-c",
                    VALID_CHECKSUM_2,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 2
        assert output[0]["pkgId"] == VALID_CHECKSUM_1
        assert output[1]["pkgId"] == VALID_CHECKSUM_2

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_comma_separated_checksums(self, mock_client_class):
        """Test comma-separated checksums via --checksums option."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "epoch": "0",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksums",
                    f"{VALID_CHECKSUM_1},{VALID_CHECKSUM_2}",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["pkgId"] == VALID_CHECKSUM_1

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_checksums_only_output(self, mock_client_class):
        """Test --checksums-only outputs one checksum per line."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "epoch": "0",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
                {
                    "pulp_href": "/api/v3/content/rpm/packages/2/",
                    "sha256": VALID_CHECKSUM_2,
                    "name": "pkg2",
                    "epoch": "0",
                    "version": "2.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                    "--checksums-only",
                ],
            )

        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert lines == [VALID_CHECKSUM_1, VALID_CHECKSUM_2]

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_table_output(self, mock_client_class):
        """Test table output format."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/12345/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "test-package",
                    "epoch": "0",
                    "version": "1.0.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                }
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                    "--output",
                    "table",
                ],
            )

        assert result.exit_code == 0
        assert "pkgId" in result.output
        assert "name" in result.output
        assert VALID_CHECKSUM_1 in result.output
        assert "test-package" in result.output

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_empty_results(self, mock_client_class):
        """Test empty results (no matching packages)."""
        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output == []

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_checksums_only_empty_results(self, mock_client_class):
        """Test --checksums-only with no matches outputs nothing."""
        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                    "--checksums-only",
                ],
            )

        assert result.exit_code == 0
        assert result.output.strip() == ""

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_table_output_empty_results(self, mock_client_class):
        """Test table output with no matches returns empty (covers _format_table empty path)."""
        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                    "--output",
                    "table",
                ],
            )

        assert result.exit_code == 0
        assert result.output.strip() == ""

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_skips_invalid_api_response_items(self, mock_client_class):
        """Test that invalid API response items are skipped (covers except block)."""
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "valid-pkg",
                    "epoch": "0",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
                {"invalid": "item", "missing": "required_fields"},  # Invalid
                {"pulp_href": None, "sha256": None},  # Invalid
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["name"] == "valid-pkg"


class TestSearchByChecksumErrors:
    """Test search-by-checksum error handling."""

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_http_error(self, mock_client_class):
        """Test HTTP error handling."""
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.side_effect = httpx.HTTPError("Connection failed")
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 1

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_generic_exception(self, mock_client_class):
        """Test generic exception handling."""
        mock_client_class.create_from_config_file.side_effect = ValueError("Config error")

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--checksum",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 1


class TestSearchByChecksumResultsJsonMode:
    """Test search-by-checksum with --results-json input."""

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_handles_invalid_api_response_items(self, mock_client_class, tmp_path):
        """Test that invalid API response items are skipped when building found set."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg1.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response(
            [
                {
                    "sha256": VALID_CHECKSUM_1,
                    "pulp_href": "/api/1/",
                    "name": "pkg1",
                    "version": "1",
                    "release": "1",
                    "arch": "x86_64",
                    "epoch": "0",
                    "pulp_labels": {},
                },
                {"invalid": "item"},
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_real_world_structure(self, mock_client_class, tmp_path):
        """Test with real-world results.json: path-style keys, distributions, mixed types."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "ns/build-id/sbom-merged.json": {
                            "labels": {"arch": "", "build_id": "build-id"},
                            "url": "https://example.com/sbom/sbom-merged.json",
                            "sha256": "1" * 64,
                        },
                        "ns/build-id/s390x/state.log": {
                            "labels": {"arch": "s390x", "build_id": "build-id"},
                            "url": "https://example.com/logs/s390x/state.log",
                            "sha256": "2" * 64,
                        },
                        "pkg-1.0-1.el10.s390x.rpm": {
                            "labels": {"arch": "s390x", "build_id": "build-id"},
                            "url": "https://example.com/rpms/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg-debuginfo-1.0-1.el10.s390x.rpm": {
                            "labels": {"arch": "s390x", "build_id": "build-id"},
                            "url": "https://example.com/rpms/pkg-debuginfo.rpm",
                            "sha256": VALID_CHECKSUM_2,
                        },
                    },
                    "distributions": {
                        "rpms": "https://example.com/rpms/",
                        "logs": "https://example.com/logs/",
                        "sbom": "https://example.com/sbom/",
                        "artifacts": "https://example.com/artifacts/",
                    },
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "s390x",
                    "epoch": "0",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg-1.0-1.el10.s390x.rpm" not in out["artifacts"]
        assert "pkg-debuginfo-1.0-1.el10.s390x.rpm" in out["artifacts"]
        assert "ns/build-id/sbom-merged.json" in out["artifacts"]
        assert "ns/build-id/s390x/state.log" in out["artifacts"]
        assert out["distributions"] == {
            "rpms": "https://example.com/rpms/",
            "logs": "https://example.com/logs/",
            "sbom": "https://example.com/sbom/",
            "artifacts": "https://example.com/artifacts/",
        }

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_removes_found_rpms(self, mock_client_class, tmp_path):
        """Test that found RPMs are removed from results.json."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg1.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg2.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg2.rpm",
                            "sha256": VALID_CHECKSUM_2,
                        },
                        "log.txt": {
                            "labels": {},
                            "url": "https://example.com/log.txt",
                            "sha256": "f" * 64,
                        },
                    },
                    "distributions": {"rpms": "https://example.com/rpms/"},
                },
                indent=2,
            )
        )

        # Pulp returns pkg1 as found, pkg2 not found
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/v3/content/rpm/packages/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "epoch": "0",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        assert "removed 1 found RPM" in result.output
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]
        assert "pkg2.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]
        assert out["distributions"] == {"rpms": "https://example.com/rpms/"}

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_no_rpm_artifacts(self, mock_client_class, tmp_path):
        """Test results.json with no RPM artifacts writes unchanged."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "log.txt": {"labels": {}, "url": "https://x.com/log", "sha256": "a" * 64},
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        assert "no RPM artifacts to filter" in result.output
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert out == json.loads(results_input.read_text())

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_no_matches_in_pulp(self, mock_client_class, tmp_path):
        """Test results.json when no RPMs are found in Pulp - all remain."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        assert "removed 0 found RPM" in result.output
        out = json.loads(results_output.read_text())
        assert "pkg.rpm" in out["artifacts"]

    def test_results_json_invalid_file(self, tmp_path):
        """Test error when results.json is invalid."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text("not valid json {")

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1
        assert "Failed to read results.json" in result.output

    def test_results_json_invalid_checksum_in_file(self, tmp_path):
        """Test error when results.json contains invalid RPM checksum (64 chars but non-hex)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": "g" * 64,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1
        assert "Invalid checksum in results.json" in result.output

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_skips_non_dict_artifact_info(self, mock_client_class, tmp_path):
        """Test that non-dict artifact info is skipped when extracting checksums."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": "not_a_dict",
                        "valid.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/valid.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg.rpm" in out["artifacts"]
        assert "valid.rpm" in out["artifacts"]

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_http_error(self, mock_client_class, tmp_path):
        """Test HTTP error when searching in results.json mode."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.side_effect = httpx.HTTPError("Connection failed")
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1

    @patch("pulp_tool.cli.search_by_checksum.PulpClient")
    def test_results_json_generic_exception(self, mock_client_class, tmp_path):
        """Test generic exception when searching in results.json mode."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )

        mock_client_class.create_from_config_file.side_effect = ValueError("Config error")

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by-checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1


def tempfile_config():
    """Context manager that creates a temporary config file path."""
    import tempfile

    class TempConfig:
        def __enter__(self):
            self.tmpdir = tempfile.mkdtemp()
            self.path = str(Path(self.tmpdir) / "config.toml")
            Path(self.path).write_text(
                '[cli]\nbase_url = "https://pulp.example.com"\napi_root = "/pulp/api/v3"\ndomain = "test-domain"'
            )
            return self.path

        def __exit__(self, *args):
            import shutil

            shutil.rmtree(self.tmpdir, ignore_errors=True)

    return TempConfig()
