"""Tests for search-by CLI command."""

import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import httpx
import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from pulp_tool.cli import cli
from pulp_tool.cli.search_by import (
    _collect_list,
    _filenames_to_nvras_deduplicated,
    _filenames_to_nvrs_deduplicated,
    _handle_validation_error,
    _log_packages_found,
    _search_pulp_by_filenames_incremental,
)
from pulp_tool.models.cli import FoundPackages, SearchByRequest, SearchByResultsJson

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


class TestSearchByChecksumHelpers:
    """Unit tests for search-by helper functions and models."""

    def test_extract_rpm_checksums_from_results(self):
        """Test SearchByResultsJson.extract_rpm_checksums extracts valid checksums, skips invalid."""
        results = {
            "artifacts": {
                "pkg1.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                "pkg2.rpm": {"labels": {}, "url": "y", "sha256": "b" * 64},
                "log.txt": {"labels": {}, "url": "z", "sha256": "c" * 64},
                "bad.rpm": "not a dict",
            },
        }
        checksums = SearchByResultsJson(results).extract_rpm_checksums()
        assert set(checksums) == {"a" * 64, "b" * 64}

    def test_extract_filenames_from_results(self):
        """Test SearchByResultsJson.extract_filenames extracts RPM keys, skips non-RPM and invalid."""
        results = {
            "artifacts": {
                "pkg1.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                "pkg2.rpm": {"labels": {}, "url": "y", "sha256": "b" * 64},
                ".rpm": {"labels": {}, "url": "z", "sha256": "c" * 64},
                "log.txt": {"labels": {}, "url": "z", "sha256": "c" * 64},
                "bad": "not a dict",
            },
        }
        filenames = SearchByResultsJson(results).extract_filenames()
        assert set(filenames) == {"pkg1.rpm", "pkg2.rpm", ".rpm"}

    def test_remove_found_by_signed_by(self):
        """Test SearchByResultsJson.remove_found removes RPMs with matching labels.signed_by."""
        results = {
            "artifacts": {
                "pkg1.rpm": {"labels": {"signed_by": "key-123"}, "url": "x", "sha256": "a" * 64},
                "pkg2.rpm": {"labels": {"signed_by": "key-456"}, "url": "y", "sha256": "b" * 64},
                "pkg3.rpm": {"labels": {"signed_by": "  key-123  "}, "url": "z", "sha256": "c" * 64},
                "pkg4.rpm": {"labels": ["not-a-dict"]},  # labels not a dict - should not match
                "log.txt": {"labels": {}, "url": "w", "sha256": "d" * 64},
            },
        }
        # sha256 must match Pulp response; signed_by filters which matching artifacts to remove
        found = FoundPackages(signed_by={"key-123", "key-456"}, checksums={"a" * 64, "b" * 64, "c" * 64})
        filtered = SearchByResultsJson(results).remove_found(found)
        assert "pkg1.rpm" not in filtered["artifacts"]
        assert "pkg2.rpm" not in filtered["artifacts"]
        assert "pkg3.rpm" not in filtered["artifacts"]
        assert "pkg4.rpm" in filtered["artifacts"]
        assert "log.txt" in filtered["artifacts"]

    def test_remove_found_by_filename_basename_match(self):
        """Test remove_found matches artifact keys by basename when key includes path."""
        results = {
            "artifacts": {
                "namespace/build-123/pkg-1.0-1.x86_64.rpm": {
                    "labels": {},
                    "url": "x",
                    "sha256": "a" * 64,
                },
                "pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "y", "sha256": "b" * 64},
                "log.txt": {"labels": {}, "url": "z", "sha256": "c" * 64},
            },
        }
        # found has location_href from Pulp (basename only); sha256 must match
        found = FoundPackages(filenames={"pkg-1.0-1.x86_64.rpm"}, checksums={"a" * 64, "b" * 64})
        filtered = SearchByResultsJson(results).remove_found(found)
        assert "namespace/build-123/pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        assert "pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        assert "log.txt" in filtered["artifacts"]

    def test_remove_found_location_href_with_path_matches_artifact_key(self):
        """Test remove_found matches artifact keys when location_href from Pulp has path."""
        results = {
            "artifacts": {
                "pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                "path/to/pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "y", "sha256": "b" * 64},
                "log.txt": {"labels": {}, "url": "z", "sha256": "c" * 64},
            },
        }
        # found has location_href from Pulp with path; sha256 must match
        found = FoundPackages(
            filenames={"Packages/l/pkg-1.0-1.x86_64.rpm", "pkg-1.0-1.x86_64.rpm"},
            checksums={"a" * 64, "b" * 64},
        )
        filtered = SearchByResultsJson(results).remove_found(found)
        assert "pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        assert "path/to/pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        assert "log.txt" in filtered["artifacts"]

    def test_remove_found_filename_checksum_pairs_requires_both_match(self):
        """Test remove_found with filename_checksum_pairs requires both basename and sha256 to match."""
        results = {
            "artifacts": {
                "pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                "other-pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "y", "sha256": "a" * 64},
                "path/pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "z", "sha256": "b" * 64},
            },
        }
        # Pulp returned pkg-1.0-1.x86_64.rpm with sha256=aaa
        found = FoundPackages(
            filename_checksum_pairs={("pkg-1.0-1.x86_64.rpm", "a" * 64)},
            checksums={"a" * 64},
        )
        filtered = SearchByResultsJson(results).remove_found(found, only_remove_filenames={"pkg-1.0-1.x86_64.rpm"})
        # Only pkg-1.0-1.x86_64.rpm with sha256=aaa should be removed
        assert "pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        # other-pkg has same sha256 but different basename - keep
        assert "other-pkg-1.0-1.x86_64.rpm" in filtered["artifacts"]
        # path/pkg has same basename but different sha256 - keep (not in Pulp)
        assert "path/pkg-1.0-1.x86_64.rpm" in filtered["artifacts"]

    def test_remove_found_filename_checksum_pairs_same_basename_different_sha256_not_removed(self):
        """Test artifact with same basename as Pulp package but different sha256 is NOT removed."""
        results = {
            "artifacts": {
                "pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "b" * 64},
            },
        }
        # Pulp returned pkg-1.0-1.x86_64.rpm with sha256=aaa (different build)
        found = FoundPackages(
            filename_checksum_pairs={("pkg-1.0-1.x86_64.rpm", "a" * 64)},
            checksums={"a" * 64},
        )
        filtered = SearchByResultsJson(results).remove_found(found, only_remove_filenames={"pkg-1.0-1.x86_64.rpm"})
        # Artifact has same basename but sha256=bbb - should NOT be removed
        assert "pkg-1.0-1.x86_64.rpm" in filtered["artifacts"]

    def test_remove_found_fallback_filenames_when_no_checksum_pairs(self):
        """Test remove_found uses filenames fallback when filename_checksum_pairs empty (no location_href)."""
        results = {
            "artifacts": {
                "path/pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
            },
        }
        # Found has filenames (from packages without location_href) but no filename_checksum_pairs
        found = FoundPackages(filenames={"pkg-1.0-1.x86_64.rpm"}, checksums={"a" * 64})
        filtered = SearchByResultsJson(results).remove_found(found, only_remove_filenames={"pkg-1.0-1.x86_64.rpm"})
        assert "path/pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]

    def test_remove_found_filename_checksum_pairs_exact_match_removed(self):
        """Test artifact with same basename AND same sha256 as Pulp package IS removed."""
        results = {
            "artifacts": {
                "pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                "path/pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "y", "sha256": "a" * 64},
            },
        }
        # Pulp returned pkg-1.0-1.x86_64.rpm with sha256=aaa
        found = FoundPackages(
            filename_checksum_pairs={("pkg-1.0-1.x86_64.rpm", "a" * 64)},
            checksums={"a" * 64},
        )
        filtered = SearchByResultsJson(results).remove_found(found, only_remove_filenames={"pkg-1.0-1.x86_64.rpm"})
        assert "pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]
        assert "path/pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]

    def test_collect_list_with_items(self):
        """Test _collect_list merges items tuple with csv, deduplicates, normalizes."""
        result = _collect_list(("a", "b", "a"), "b,c,d", normalize="lower")
        assert result == ["a", "b", "c", "d"]

    def test_filenames_to_nvras_deduplicated_includes_arch(self):
        """Test _filenames_to_nvras_deduplicated keeps same NVR with different arch as separate entries."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "pkg-1.0-1.aarch64.rpm"]
        result = _filenames_to_nvras_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1", "x86_64"), ("pkg", "1.0", "1", "aarch64")]

    def test_filenames_to_nvras_deduplicated_same_nvra_deduplicates(self):
        """Test _filenames_to_nvras_deduplicated deduplicates identical NVRA."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "path/pkg-1.0-1.x86_64.rpm"]
        result = _filenames_to_nvras_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1", "x86_64")]

    def test_filenames_to_nvras_deduplicated_skips_unparseable(self, caplog):
        """Test _filenames_to_nvras_deduplicated skips unparseable filenames with warning."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "not-an-rpm.txt", "invalid.rpm"]
        result = _filenames_to_nvras_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1", "x86_64")]
        assert "Skipping unparseable RPM filename" in caplog.text
        assert "not-an-rpm.txt" in caplog.text
        assert "invalid.rpm" in caplog.text

    def test_filenames_to_nvrs_deduplicated_merges_arches(self):
        """Test _filenames_to_nvrs_deduplicated collapses same NVR with different arches."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "pkg-1.0-1.aarch64.rpm", "pkg-1.0-1.s390x.rpm"]
        result = _filenames_to_nvrs_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1")]

    def test_filenames_to_nvrs_deduplicated_different_nvrs(self):
        """Test _filenames_to_nvrs_deduplicated keeps different NVRs."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "other-2.0-2.x86_64.rpm"]
        result = _filenames_to_nvrs_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1"), ("other", "2.0", "2")]

    def test_filenames_to_nvrs_deduplicated_skips_unparseable(self):
        """Test _filenames_to_nvrs_deduplicated skips unparseable filenames."""
        filenames = ["pkg-1.0-1.x86_64.rpm", "not-an-rpm.txt", "invalid.rpm"]
        result = _filenames_to_nvrs_deduplicated(filenames)
        assert result == [("pkg", "1.0", "1")]

    def test_log_packages_found_truncates_when_many(self, caplog):
        """Test _log_packages_found truncates DEBUG output when more than max_log packages."""
        from pulp_tool.models.pulp_api import RpmPackageResponse

        packages = [
            RpmPackageResponse(
                pulp_href=f"/api/{i}/",
                sha256=VALID_CHECKSUM_1,
                name="pkg",
                epoch="0",
                version="1.0",
                release="1",
                arch="x86_64",
                pulp_labels={},
            )
            for i in range(15)
        ]
        with caplog.at_level("DEBUG"):
            _log_packages_found(packages, max_log=10)
        assert "RPM exists in Pulp" in caplog.text
        assert "... and 5 more package(s)" in caplog.text

    def test_search_pulp_by_filenames_incremental_empty_artifacts_stops(self):
        """Test _search_pulp_by_filenames_incremental stops when no filenames (empty artifacts)."""
        client = Mock()
        results_data: dict[str, Any] = {"artifacts": {}, "distributions": {}}
        packages, filtered = _search_pulp_by_filenames_incremental(client, results_data, None, initial_filenames=None)
        assert packages == []
        assert filtered == results_data
        client.get_rpm_by_filenames.assert_not_called()

    def test_search_pulp_by_filenames_incremental_skips_when_no_matching_filename(self):
        """Test _search_pulp_by_filenames_incremental continues when first_matching is None (line 228)."""
        client = Mock()
        call_count = [0]

        def parse_side_effect(f: str) -> tuple[str, str, str]:
            call_count[0] += 1
            if call_count[0] <= 2:  # nvrs extraction
                return ("a", "1.0", "1") if "a-" in f else ("b", "2.0", "2")
            if call_count[0] <= 4:  # first_matching for (a,1.0,1) - return wrong so no match
                return ("b", "2.0", "2")
            return ("a", "1.0", "1") if "a-" in f else ("b", "2.0", "2")

        with patch("pulp_tool.cli.search_by.parse_rpm_filename_to_nvr", side_effect=parse_side_effect):
            results_data: dict[str, Any] = {
                "artifacts": {
                    "a-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": "a" * 64},
                    "b-2.0-2.x86_64.rpm": {"labels": {}, "url": "y", "sha256": "b" * 64},
                },
                "distributions": {},
            }
            client.get_rpm_by_filenames.return_value = _make_rpm_response(
                [
                    {
                        "pulp_href": "/api/2/",
                        "sha256": "b" * 64,
                        "name": "b",
                        "version": "2.0",
                        "release": "2",
                        "arch": "x86_64",
                        "location_href": "b-2.0-2.x86_64.rpm",
                        "pulp_labels": {},
                    }
                ]
            )
            packages, filtered = _search_pulp_by_filenames_incremental(client, results_data, None)
            assert "b-2.0-2.x86_64.rpm" not in filtered["artifacts"]
            assert "a-1.0-1.x86_64.rpm" in filtered["artifacts"]

    def test_search_pulp_by_filenames_incremental_with_signed_by(self):
        """Test _search_pulp_by_filenames_incremental uses signed_by when provided."""
        pkg_dict = {
            "pulp_href": "/api/1/",
            "sha256": VALID_CHECKSUM_1,
            "name": "pkg",
            "epoch": "0",
            "version": "1.0",
            "release": "1",
            "arch": "x86_64",
            "location_href": "pkg-1.0-1.x86_64.rpm",
            "pulp_labels": {},
        }
        client = Mock()
        client.get_rpm_by_filenames_and_signed_by.return_value = _make_rpm_response([pkg_dict])
        results_data = {
            "artifacts": {"pkg-1.0-1.x86_64.rpm": {"labels": {}, "url": "x", "sha256": VALID_CHECKSUM_1}},
            "distributions": {},
        }
        packages, filtered = _search_pulp_by_filenames_incremental(
            client, results_data, "key-123", initial_filenames=None
        )
        assert len(packages) == 1
        client.get_rpm_by_filenames_and_signed_by.assert_called()
        assert "pkg-1.0-1.x86_64.rpm" not in filtered["artifacts"]

    def test_handle_validation_error_else_branch(self):
        """Test _handle_validation_error else branch for non-checksum ValidationError."""
        try:
            SearchByRequest(checksums=[], filenames=[], signed_by=[])
        except ValidationError as e:
            with pytest.raises(SystemExit):
                _handle_validation_error(e, results_json_context=False)
            return
        pytest.fail("Expected ValidationError")

    def test_search_by_results_json_to_dict(self):
        """Test SearchByResultsJson.to_dict returns underlying data."""
        data: dict[str, Any] = {"artifacts": {}, "distributions": {}}
        results = SearchByResultsJson(data)
        assert results.to_dict() is data


class TestSearchByChecksumHelp:
    """Test search-by command help."""

    def test_search_by_help(self):
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

    def test_main_help_includes_search_by(self):
        """Test main CLI help includes search-by command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "search-by" in result.output


class TestSearchByChecksumValidation:
    """Test search-by input validation."""

    def test_no_search_criteria_provided(self):
        """Test error when no search criteria (checksum, location-href, signed-by) are provided."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by"],
            )
        assert result.exit_code == 1
        assert (
            "At least one of --checksum/--checksums, --filename/--filenames, or --signed-by must be provided"
            in result.output
        )

    def test_invalid_checksum_format(self):
        """Test error when checksum has invalid format."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--checksums", "not64chars"],
            )
        assert result.exit_code == 1
        assert "Invalid checksum format" in result.output

    def test_invalid_checksum_too_short(self):
        """Test error when checksum is too short."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--checksums", "abc123"],
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
                    "search-by",
                    "--checksums",
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
            ["search-by", "--checksums", VALID_CHECKSUM_1],
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
                        "search-by",
                        "--results-json",
                        results_path,
                    ],
                )
            assert result.exit_code == 1
            assert "--output-results is required" in result.output
        finally:
            Path(results_path).unlink(missing_ok=True)

    def test_checksum_flag_requires_results_json(self):
        """Test error when --checksum is used without --results-json."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksum",
                ],
            )
        assert result.exit_code == 1
        assert "--checksum requires --results-json" in result.output

    def test_filename_flag_requires_results_json(self):
        """Test error when --filename is used without --results-json."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                ],
            )
        assert result.exit_code == 1
        assert "--filename requires --results-json" in result.output

    def test_checksums_and_filenames_mutually_exclusive(self):
        """Test error when both --checksums and --filenames are provided."""
        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                    "--filenames",
                    "pkg.rpm",
                ],
            )
        assert result.exit_code == 1
        assert "checksums and filenames cannot be combined" in result.output

    def test_signed_by_max_one_value(self):
        """Test SearchByRequest rejects multiple signed_by values."""
        with pytest.raises(ValidationError) as exc_info:
            SearchByRequest(
                checksums=[],
                filenames=[],
                signed_by=["key-1", "key-2"],
            )
        assert "signed_by accepts at most one value" in str(exc_info.value)


class TestSearchByChecksumSuccess:
    """Test successful search-by scenarios."""

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
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

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_multiple_checksums_repeated_option(self, mock_client_class):
        """Test multiple checksums via --checksums comma-separated."""
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
                    "search-by",
                    "--checksums",
                    f"{VALID_CHECKSUM_1},{VALID_CHECKSUM_2}",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 2
        assert output[0]["pkgId"] == VALID_CHECKSUM_1
        assert output[1]["pkgId"] == VALID_CHECKSUM_2

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
                    f"{VALID_CHECKSUM_1},{VALID_CHECKSUM_2}",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["pkgId"] == VALID_CHECKSUM_1

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output == []

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["name"] == "valid-pkg"

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_checksums_and_signed_by_single_call(self, mock_client_class):
        """Test --checksums + --signed-by uses single API call (server-side filter)."""
        pkg1 = {
            "pulp_href": "/api/v3/content/rpm/packages/1/",
            "sha256": VALID_CHECKSUM_1,
            "name": "pkg1",
            "epoch": "0",
            "version": "1.0",
            "release": "1",
            "arch": "x86_64",
            "pulp_labels": {"signed_by": "me"},
        }
        mock_client = Mock()
        mock_client.get_rpm_by_checksums_and_signed_by.return_value = _make_rpm_response([pkg1])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                    "--signed-by",
                    "me",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["pulp_labels"]["signed_by"] == "me"
        mock_client.get_rpm_by_checksums_and_signed_by.assert_called_once_with([VALID_CHECKSUM_1], "me")

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_checksums_and_signed_by_no_match_returns_empty(self, mock_client_class):
        """Test --checksums + --signed-by returns empty when no match."""
        mock_client = Mock()
        mock_client.get_rpm_by_checksums_and_signed_by.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                    "--signed-by",
                    "me",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output == []

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_filenames_only_has_packages(self, mock_client_class):
        """Test --filenames only (no signed_by) returns packages (lines 316-318)."""
        mock_client = Mock()
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "pkg-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                }
            ]
        )
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--filenames", "pkg-1.0-1.x86_64.rpm"],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["name"] == "pkg"

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_filenames_only_no_match_returns_empty(self, mock_client_class):
        """Test --filenames only (no signed_by) returns empty when no match (lines 320-322)."""
        mock_client = Mock()
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--filenames", "pkg-1.0-1.x86_64.rpm"],
            )

        assert result.exit_code == 0
        assert json.loads(result.output) == []

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_signed_by_only_has_packages(self, mock_client_class):
        """Test --signed-by only (no checksums/filenames) returns packages (lines 319-324)."""
        mock_client = Mock()
        mock_client.get_rpm_by_signed_by.return_value = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "pkg-1.0-1.x86_64.rpm",
                    "pulp_labels": {"signed_by": "key-123"},
                }
            ]
        )
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--signed-by", "key-123"],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["pulp_labels"]["signed_by"] == "key-123"
        mock_client.get_rpm_by_signed_by.assert_called_once_with(["key-123"])

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_signed_by_only_no_match_returns_empty(self, mock_client_class):
        """Test --signed-by only returns empty when no match (lines 319-324)."""
        mock_client = Mock()
        mock_client.get_rpm_by_signed_by.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                ["--config", config_path, "search-by", "--signed-by", "key-123"],
            )

        assert result.exit_code == 0
        assert json.loads(result.output) == []
        mock_client.get_rpm_by_signed_by.assert_called_once_with(["key-123"])

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_filenames_and_signed_by_single_call(self, mock_client_class):
        """Test --filenames + --signed-by uses single API call (server-side filter)."""
        pkg_href = "/api/v3/content/rpm/packages/1/"
        pkg1 = {
            "pulp_href": pkg_href,
            "sha256": VALID_CHECKSUM_1,
            "name": "pkg1",
            "epoch": "0",
            "version": "1.0",
            "release": "1",
            "arch": "x86_64",
            "location_href": "pkg1-1.0-1.x86_64.rpm",
            "pulp_labels": {"signed_by": "me"},
        }
        mock_client = Mock()
        mock_client.get_rpm_by_filenames_and_signed_by.return_value = _make_rpm_response([pkg1])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filenames",
                    "pkg1-1.0-1.x86_64.rpm",
                    "--signed-by",
                    "me",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 1
        assert output[0]["name"] == "pkg1"
        assert output[0]["pulp_labels"]["signed_by"] == "me"
        mock_client.get_rpm_by_filenames_and_signed_by.assert_called_once_with(["pkg1-1.0-1.x86_64.rpm"], "me")

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_filenames_and_signed_by_no_match_returns_empty(self, mock_client_class):
        """Test --filenames + --signed-by returns empty when no match (server-side filter)."""
        mock_client = Mock()
        mock_client.get_rpm_by_filenames_and_signed_by.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filenames",
                    "pkg1-1.0-1.x86_64.rpm",
                    "--signed-by",
                    "me",
                ],
            )

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert len(output) == 0


class TestSearchByChecksumErrors:
    """Test search-by error handling."""

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 1

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--checksums",
                    VALID_CHECKSUM_1,
                ],
            )

        assert result.exit_code == 1


class TestSearchByChecksumResultsJsonMode:
    """Test search-by with --results-json input."""

    @patch("pulp_tool.cli.search_by.PulpClient")
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
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
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
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
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

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_results_json_checksum_flag_extracts_and_removes_found_rpms(self, mock_client_class, tmp_path):
        """Test --checksum flag: extracts checksums from results.json, removes found RPMs."""
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
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "removed 1 found RPM" in result.output
        assert "pkg1-1.0-1.x86_64" in result.output
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]
        assert "pkg2.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]
        assert out["distributions"] == {"rpms": "https://example.com/rpms/"}

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_flag_extracts_from_results_json(self, mock_client_class, tmp_path):
        """Test --filename flag: extracts artifact keys from results.json, searches Pulp."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1-1.0-1.x86_64.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg1.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg2-2.0-2.x86_64.rpm": {
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

        # Pulp returns pkg1-1.0-1.x86_64.rpm as found (by filename)
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
                    "location_href": "pkg1-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_filenames.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "removed 1 found RPM" in result.output
        # Incremental mode: one call per NVR (pkg1, pkg2)
        assert mock_client.get_rpm_by_filenames.call_count == 2
        all_calls = [c[0][0][0] for c in mock_client.get_rpm_by_filenames.call_args_list]
        assert "pkg1-1.0-1.x86_64.rpm" in all_calls
        assert "pkg2-2.0-2.x86_64.rpm" in all_calls
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert "pkg1-1.0-1.x86_64.rpm" not in out["artifacts"]
        assert "pkg2-2.0-2.x86_64.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_removes_all_arches_for_same_nvr(self, mock_client_class, tmp_path):
        """Test that artifacts sharing the same NVR are all removed in one API call."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg-1.0-1.x86_64.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg-1.0-1.s390x.rpm": {
                            "labels": {"arch": "s390x"},
                            "url": "https://example.com/pkg-s390x.rpm",
                            "sha256": VALID_CHECKSUM_2,
                        },
                    },
                    "distributions": {"rpms": "https://example.com/rpms/"},
                },
                indent=2,
            )
        )

        # Pulp returns BOTH arches for the NVR query (single call)
        mock_response_both = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "pkg-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
                {
                    "pulp_href": "/api/2/",
                    "sha256": VALID_CHECKSUM_2,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "s390x",
                    "location_href": "pkg-1.0-1.s390x.rpm",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_filenames.return_value = mock_response_both
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        # Both arches share the same NVR, so both removed in one pass
        assert "pkg-1.0-1.x86_64.rpm" not in out["artifacts"]
        assert "pkg-1.0-1.s390x.rpm" not in out["artifacts"]
        # Only 1 API call needed (NVR-based tracking)
        assert mock_client.get_rpm_by_filenames.call_count == 1

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_different_nvrs_make_separate_calls(self, mock_client_class, tmp_path):
        """Test that artifacts with different NVRs still make separate API calls."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg-1.0-1.x86_64.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/pkg.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg-1.0-1.s390x.rpm": {
                            "labels": {"arch": "s390x"},
                            "url": "https://example.com/pkg-s390x.rpm",
                            "sha256": VALID_CHECKSUM_2,
                        },
                        "other-2.0-1.x86_64.rpm": {
                            "labels": {"arch": "x86_64"},
                            "url": "https://example.com/other.rpm",
                            "sha256": VALID_CHECKSUM_3,
                        },
                    },
                    "distributions": {"rpms": "https://example.com/rpms/"},
                },
                indent=2,
            )
        )

        mock_response_pkg = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "pkg-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
                {
                    "pulp_href": "/api/2/",
                    "sha256": VALID_CHECKSUM_2,
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": "s390x",
                    "location_href": "pkg-1.0-1.s390x.rpm",
                    "pulp_labels": {},
                },
            ]
        )
        mock_response_other = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/3/",
                    "sha256": VALID_CHECKSUM_3,
                    "name": "other",
                    "version": "2.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "other-2.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_filenames.side_effect = [mock_response_pkg, mock_response_other]
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg-1.0-1.x86_64.rpm" not in out["artifacts"]
        assert "pkg-1.0-1.s390x.rpm" not in out["artifacts"]
        assert "other-2.0-1.x86_64.rpm" not in out["artifacts"]
        # 2 NVRs = 2 API calls (pkg and other), not 3 (one per NVRA)
        assert mock_client.get_rpm_by_filenames.call_count == 2

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_many_packages_truncates_msg(self, mock_client_class, tmp_path):
        """Test results-json with many packages found shows truncated msg (N packages)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        # One NVR with 12 arches; Pulp returns 12 packages for that NVR
        arches = ["x86_64", "aarch64", "s390x", "ppc64le", "src", "noarch"] * 2
        checksums = [("a" * 63 + hex(i)[-1]) for i in range(12)]  # 12 distinct checksums
        artifacts = {
            f"pkg-1.0-1.{arch}.rpm": {"labels": {}, "url": "x", "sha256": checksums[i]}
            for i, arch in enumerate(arches[:12])
        }
        results_input.write_text(
            json.dumps(
                {"artifacts": artifacts, "distributions": {}},
                indent=2,
            )
        )
        # All 12 match same NVR; Pulp returns 12 packages with matching location_href and pkgId
        mock_response = _make_rpm_response(
            [
                {
                    "pulp_href": f"/api/{i}/",
                    "sha256": checksums[i],
                    "name": "pkg",
                    "version": "1.0",
                    "release": "1",
                    "arch": arches[i],
                    "location_href": f"pkg-1.0-1.{arches[i]}.rpm",
                    "pulp_labels": {},
                }
                for i in range(12)
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_filenames.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        assert "(12 packages)" in result.output

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_signed_by_removes_found_rpms(self, mock_client_class, tmp_path):
        """Test --signed-by: user specifies signing key(s), searches Pulp, removes found RPMs."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1.rpm": {
                            "labels": {"arch": "x86_64", "signed_by": "key-id-123"},
                            "url": "https://example.com/pkg1.rpm",
                            "sha256": VALID_CHECKSUM_1,
                        },
                        "pkg2.rpm": {
                            "labels": {"arch": "x86_64", "signed_by": "key-id-456"},
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

        # Pulp returns pkg matching signed_by key-id-123
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
                    "pulp_labels": {"signed_by": "key-id-123"},
                },
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_signed_by.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--signed-by",
                    "key-id-123",
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "removed 1 found RPM" in result.output
        mock_client.get_rpm_by_signed_by.assert_called_once_with(["key-id-123"])
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]
        assert "pkg2.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_results_json_checksums_and_signed_by_single_call(self, mock_client_class, tmp_path):
        """Test results-json with --checksums + --signed-by uses single API call."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1.rpm": {
                            "labels": {"arch": "x86_64", "signed_by": "key-123"},
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

        pkg1 = {
            "pulp_href": "/api/v3/content/rpm/packages/1/",
            "sha256": VALID_CHECKSUM_1,
            "name": "pkg1",
            "epoch": "0",
            "version": "1.0",
            "release": "1",
            "arch": "x86_64",
            "pulp_labels": {"signed_by": "key-123"},
        }
        mock_client = Mock()
        mock_client.get_rpm_by_checksums_and_signed_by.return_value = _make_rpm_response([pkg1])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--checksums",
                    VALID_CHECKSUM_1,
                    "--signed-by",
                    "key-123",
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "removed 1 found RPM" in result.output
        assert "pkg1-1.0-1.x86_64" in result.output
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]
        assert "pkg2.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]
        mock_client.get_rpm_by_checksums_and_signed_by.assert_called_once_with([VALID_CHECKSUM_1], "key-123")

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filenames_explicit_only(self, mock_client_class, tmp_path):
        """Test --filenames with explicit values (no flag, no extraction)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {"artifacts": {}, "distributions": {}},
                indent=2,
            )
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response(
            [
                {
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "explicit",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "explicit-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
            ]
        )
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--filenames",
                    "explicit-1.0-1.x86_64.rpm",
                ],
            )

        assert result.exit_code == 0
        mock_client.get_rpm_by_filenames.assert_called_once_with(["explicit-1.0-1.x86_64.rpm"])

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_handles_invalid_api_response(self, mock_client_class, tmp_path):
        """Test --filename skips invalid API response items (covers exception pass)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1-1.0-1.x86_64.rpm": {
                            "labels": {},
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
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "version": "1.0",
                    "release": "1",
                    "arch": "x86_64",
                    "location_href": "pkg1-1.0-1.x86_64.rpm",
                    "pulp_labels": {},
                },
                {"invalid": "item"},
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_filenames.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--filename",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1-1.0-1.x86_64.rpm" not in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_signed_by_handles_invalid_api_response(self, mock_client_class, tmp_path):
        """Test --signed-by skips invalid API response items (covers exception pass)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg1.rpm": {
                            "labels": {"signed_by": "key-123"},
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
                    "pulp_href": "/api/1/",
                    "sha256": VALID_CHECKSUM_1,
                    "name": "pkg1",
                    "version": "1",
                    "release": "1",
                    "arch": "x86_64",
                    "pulp_labels": {"signed_by": "key-123"},
                },
                {"invalid": "item"},
            ]
        )
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client.get_rpm_by_signed_by.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--signed-by",
                    "key-123",
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" not in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_filename_no_rpm_artifacts(self, mock_client_class, tmp_path):
        """Test results-json with no RPM artifacts writes input unchanged (no Pulp calls)."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "sbom.json": {"labels": {}, "url": "https://x/sbom.json", "sha256": "f" * 64},
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )
        mock_client = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "no RPM artifacts to filter" in result.output
        mock_client_class.create_from_config_file.assert_not_called()
        out = json.loads(results_output.read_text())
        assert out == json.loads(results_input.read_text())

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_signed_by_no_rpm_artifacts(self, mock_client_class, tmp_path):
        """Test --signed-by with no RPM artifacts: still searches Pulp, writes input unchanged."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "sbom.json": {"labels": {}, "url": "https://x/sbom.json", "sha256": "f" * 64},
                    },
                    "distributions": {},
                },
                indent=2,
            )
        )
        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_signed_by.return_value = mock_response
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--signed-by",
                    "key-id-123",
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        mock_client.get_rpm_by_signed_by.assert_called_once_with(["key-id-123"])
        out = json.loads(results_output.read_text())
        assert out == json.loads(results_input.read_text())

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_by_signed_by_empty_key_uses_extraction(self, mock_client_class, tmp_path):
        """Test --signed-by with empty value: empty keys are filtered, extraction used, no RPM artifacts."""
        results_input = tmp_path / "input.json"
        results_output = tmp_path / "output.json"
        results_input.write_text(json.dumps({"artifacts": {}, "distributions": {}}, indent=2))
        mock_client = Mock()
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--signed-by",
                    "",
                ],
            )

        assert result.exit_code == 0
        assert "no RPM artifacts to filter" in result.output

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        assert "no RPM artifacts to filter" in result.output
        assert results_output.exists()
        out = json.loads(results_output.read_text())
        assert out == json.loads(results_input.read_text())

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_output_results_default_removes_logs_and_sboms(self, mock_client_class, tmp_path):
        """Test default (no --keep-files): output-results contains only RPM artifacts."""
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
                        "log.txt": {
                            "labels": {},
                            "url": "https://example.com/log.txt",
                            "sha256": "f" * 64,
                        },
                        "sbom.json": {
                            "labels": {},
                            "url": "https://example.com/sbom.json",
                            "sha256": "e" * 64,
                        },
                    },
                    "distributions": {"rpms": "https://example.com/rpms/"},
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" in out["artifacts"]
        assert "log.txt" not in out["artifacts"]
        assert "sbom.json" not in out["artifacts"]
        assert out["distributions"] == {"rpms": "https://example.com/rpms/"}

    @patch("pulp_tool.cli.search_by.PulpClient")
    def test_output_results_keep_files_preserves_logs_and_sboms(self, mock_client_class, tmp_path):
        """Test --keep-files: output-results preserves logs and sboms."""
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
                        "log.txt": {
                            "labels": {},
                            "url": "https://example.com/log.txt",
                            "sha256": "f" * 64,
                        },
                        "sbom.json": {
                            "labels": {},
                            "url": "https://example.com/sbom.json",
                            "sha256": "e" * 64,
                        },
                    },
                    "distributions": {"rpms": "https://example.com/rpms/"},
                },
                indent=2,
            )
        )

        mock_response = _make_rpm_response([])
        mock_client = Mock()
        mock_client.get_rpm_by_pkgIDs.return_value = mock_response
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
                    "--checksum",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                    "--keep-files",
                ],
            )

        assert result.exit_code == 0
        out = json.loads(results_output.read_text())
        assert "pkg1.rpm" in out["artifacts"]
        assert "log.txt" in out["artifacts"]
        assert "sbom.json" in out["artifacts"]

    @patch("pulp_tool.cli.search_by.PulpClient")
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
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
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
        """Test error when results.json has invalid JSON (JSONDecodeError)."""
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
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1
        assert "Failed to read results.json" in result.output

    def test_results_json_path_is_directory(self, tmp_path):
        """Test error when results.json path is a directory (OSError on read)."""
        results_input = tmp_path / "input"  # directory, not file
        results_input.mkdir()
        results_output = tmp_path / "output.json"

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
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
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1
        assert "Invalid checksum in results.json" in result.output

    @patch("pulp_tool.cli.search_by.PulpClient")
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
        mock_client.get_rpm_by_filenames.return_value = _make_rpm_response([])
        mock_client_class.create_from_config_file.return_value = mock_client

        runner = CliRunner()
        with tempfile_config() as config_path:
            result = runner.invoke(
                cli,
                [
                    "--config",
                    config_path,
                    "search-by",
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

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
                    "--results-json",
                    str(results_input),
                    "--output-results",
                    str(results_output),
                ],
            )

        assert result.exit_code == 1

    @patch("pulp_tool.cli.search_by.PulpClient")
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
                    "search-by",
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
