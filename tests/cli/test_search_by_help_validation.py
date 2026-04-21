"""Tests for search-by CLI command."""

import tempfile
from pathlib import Path
import pytest
from click.testing import CliRunner
from pydantic import ValidationError
from pulp_tool.cli import cli
from pulp_tool.models.cli import SearchByRequest
from tests.support.constants import VALID_CHECKSUM_1
from tests.support.temp_config import tempfile_config


class TestSearchByChecksumHelp:
    """Test search-by command help."""

    def test_search_by_help(self) -> None:
        """Test search-by command help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["search-by", "--help"])
        assert result.exit_code == 0
        assert "checksum, filename, and/or signed_by" in result.output
        assert "--checksum" in result.output or "-c" in result.output
        assert "--checksums" in result.output
        assert "--filename" in result.output
        assert "--filenames" in result.output
        assert "--results-json" in result.output
        assert "--output-results" in result.output
        assert "--keep-files" in result.output
        assert "--signed-by" in result.output

    def test_main_help_includes_search_by(self) -> None:
        """Test main CLI help includes search-by command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "search-by" in result.output


class TestSearchByChecksumValidation:
    """Test search-by input validation."""

    def test_no_search_criteria_provided(self) -> None:
        """Test error when no search criteria (checksum, location-href, signed-by) are provided."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by"])
        assert result.exit_code == 1
        assert (
            "At least one of --checksum/--checksums, --filename/--filenames, or --signed-by must be provided"
            in result.output
        )

    def test_invalid_checksum_format(self) -> None:
        """Test error when checksum has invalid format."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by", "--checksums", "not64chars"])
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_invalid_checksum_too_short(self) -> None:
        """Test error when checksum is too short."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by", "--checksums", "abc123"])
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_invalid_checksum_non_hex(self) -> None:
        """Test error when checksum contains non-hex characters."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by", "--checksums", "g" * 64])
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_config_required(self) -> None:
        """Test error when config is not provided."""
        runner = CliRunner()
        result = runner.invoke(cli, ["search-by", "--checksums", VALID_CHECKSUM_1])
        assert result.exit_code == 1
        assert "--config is required" in result.output

    def test_results_json_requires_output_results(self) -> None:
        """Test error when --results-json is used without --output-results."""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"artifacts":{},"distributions":{}}')
            results_path = f.name
        try:
            with tempfile_config() as config_path:
                result = runner.invoke(cli, ["--config", config_path, "search-by", "--results-json", results_path])
            assert result.exit_code == 1
            assert "--output-results is required" in result.output
        finally:
            Path(results_path).unlink(missing_ok=True)

    def test_checksum_flag_requires_results_json(self) -> None:
        """Test error when --checksum is used without --results-json."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by", "--checksum"])
        assert result.exit_code == 1
        assert "--checksum requires --results-json" in result.output

    def test_filename_flag_requires_results_json(self) -> None:
        """Test error when --filename is used without --results-json."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(cli, ["--config", config_path, "search-by", "--filename"])
        assert result.exit_code == 1
        assert "--filename requires --results-json" in result.output

    def test_checksums_and_filenames_mutually_exclusive(self) -> None:
        """Test error when both --checksums and --filenames are provided."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli, ["--config", config_path, "search-by", "--checksums", VALID_CHECKSUM_1, "--filenames", "pkg.rpm"]
            )
        assert result.exit_code == 1
        assert "checksums and filenames cannot be combined" in result.output

    def test_signed_by_max_one_value(self) -> None:
        """Test SearchByRequest rejects multiple signed_by values."""
        with pytest.raises(ValidationError) as exc_info:
            SearchByRequest(checksums=[], filenames=[], signed_by=["key-1", "key-2"])
        assert "signed_by accepts at most one value" in str(exc_info.value)
