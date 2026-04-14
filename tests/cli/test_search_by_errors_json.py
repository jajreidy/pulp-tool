"""Tests for search-by CLI command."""

import json
from unittest.mock import Mock, patch

import httpx
from click.testing import CliRunner

from pulp_tool.cli import cli

from tests.support.constants import VALID_CHECKSUM_1, VALID_CHECKSUM_2, VALID_CHECKSUM_3
from tests.support.factories import make_rpm_list_response as _make_rpm_response
from tests.support.temp_config import tempfile_config


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
