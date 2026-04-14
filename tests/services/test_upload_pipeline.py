"""Tests for pulp_upload.py module."""

import json
import logging
from unittest.mock import Mock, patch

import pytest

from pulp_tool.models import RepositoryRefs
from pulp_tool.models.context import UploadRpmContext
from pulp_tool.services.upload_service import (
    _classify_artifact_from_key,
    _handle_sbom_results,
    process_uploads_from_results_json,
    scan_results_json_for_log_and_sbom_keys,
)

# CLI tests live under tests/cli/


class TestHandleSbomResults:
    """Test _handle_sbom_results function."""

    def test_handle_sbom_results_success(self, tmp_path):
        """Test successful SBOM results writing."""

        # Create mock results JSON with SBOM
        # The URL already contains the full reference with digest
        results_json = {
            "artifacts": {
                "test-sbom.spdx.json": {
                    "labels": {"build_id": "test-build", "namespace": "test-ns"},
                    "url": (
                        "https://pulp.example.com/pulp/content/test-build/sbom/"
                        "test-sbom.spdx.json@sha256:abc123def456789"
                    ),
                    "sha256": "abc123def456789",
                },
                "test-package.rpm": {
                    "labels": {"build_id": "test-build", "arch": "x86_64"},
                    "url": "https://pulp.example.com/pulp/content/test-build/rpms/test-package.rpm",
                    "sha256": "rpm123456",
                },
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        # Create proper UploadRpmContext instead of Namespace
        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )

        # Mock client (not actually used in this function)
        mock_client = Mock()

        _handle_sbom_results(mock_client, args, json_content)

        # Verify the file was created with correct content
        assert sbom_file.exists()
        content = sbom_file.read_text()
        expected = "https://pulp.example.com/pulp/content/test-build/sbom/" "test-sbom.spdx.json@sha256:abc123def456789"
        assert content == expected

    def test_handle_sbom_results_no_sbom_found(self, tmp_path, caplog):
        """Test handling when no SBOM is found."""
        # Create mock results JSON without SBOM
        results_json = {
            "artifacts": {
                "test-package.rpm": {
                    "labels": {"build_id": "test-build", "arch": "x86_64"},
                    "url": "https://pulp.example.com/pulp/content/test-build/rpms/test-package.rpm",
                    "sha256": "rpm123456",
                }
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )
        mock_client = Mock()

        # Capture INFO level logs since the message is now at INFO level
        with caplog.at_level(logging.INFO):
            _handle_sbom_results(mock_client, args, json_content)

        # File should not be created
        assert not sbom_file.exists()
        assert "No SBOM file found" in caplog.text

    def test_handle_sbom_results_json_file_without_arch(self, tmp_path):
        """Test SBOM detection with .json extension (no arch label)."""

        # Create mock results JSON with .json file (SBOM) without arch
        # The URL already contains the full reference with digest
        results_json = {
            "artifacts": {
                "cyclonedx.json": {
                    "labels": {"build_id": "test-build", "namespace": "test-ns"},
                    "url": "https://pulp.example.com/pulp/content/test-build/sbom/cyclonedx.json@sha256:def789abc123",
                    "sha256": "def789abc123",
                }
            }
        }

        json_content = json.dumps(results_json)

        sbom_file = tmp_path / "sbom_result.txt"

        args = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/tmp/rpms",
            sbom_path="/tmp/sbom.json",
            sbom_results=str(sbom_file),
        )
        mock_client = Mock()

        _handle_sbom_results(mock_client, args, json_content)

        # Verify the file was created with correct content
        assert sbom_file.exists()
        content = sbom_file.read_text()
        expected = "https://pulp.example.com/pulp/content/test-build/sbom/cyclonedx.json@sha256:def789abc123"
        assert content == expected


class TestClassifyArtifactFromKey:
    """Test _classify_artifact_from_key function."""

    def test_classify_rpm(self):
        """Test RPM classification."""
        assert _classify_artifact_from_key("x86_64/pkg.rpm") == "rpms"
        assert _classify_artifact_from_key("pkg.RPM") == "rpms"

    def test_classify_log(self):
        """Test log classification."""
        assert _classify_artifact_from_key("x86_64/state.log") == "logs"
        assert _classify_artifact_from_key("build.LOG") == "logs"

    def test_classify_sbom(self):
        """Test SBOM classification."""
        assert _classify_artifact_from_key("sbom.json") == "sbom"
        assert _classify_artifact_from_key("sbom.spdx") == "sbom"
        assert _classify_artifact_from_key("sbom.spdx.json") == "sbom"
        assert _classify_artifact_from_key("path/to/sbom-file.json") == "sbom"
        # SBOM by extension only (no "sbom" in key) - hits SBOM_EXTENSIONS loop
        assert _classify_artifact_from_key("cyclonedx.json") == "sbom"
        assert _classify_artifact_from_key("manifest.spdx") == "sbom"

    def test_classify_artifacts(self):
        """Test generic artifact classification."""
        assert _classify_artifact_from_key("other.tar.gz") == "artifacts"
        assert _classify_artifact_from_key("data.txt") == "artifacts"


class TestScanResultsJsonForLogAndSbomKeys:
    """Tests for scan_results_json_for_log_and_sbom_keys."""

    def test_detects_log_and_sbom_keys(self, tmp_path):
        """Artifact keys classified as logs and sbom set both flags."""
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"aarch64/build.log": {}, "report.spdx.json": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (True, True)

    def test_logs_only(self, tmp_path):
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"foo.log": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (True, False)

    def test_sbom_only(self, tmp_path):
        p = tmp_path / "r.json"
        p.write_text(json.dumps({"artifacts": {"manifest-without-sbom-in-name.json": {}}}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, True)

    def test_invalid_json_returns_false_false(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not valid json")
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, False)

    def test_non_dict_artifacts_returns_false_false(self, tmp_path):
        """``artifacts`` must be a dict; a truthy non-dict (e.g. list) hits the guard branch."""
        p = tmp_path / "r.json"
        # [] is falsy: ``data.get("artifacts") or {}`` would become {} and skip the guard.
        p.write_text(json.dumps({"artifacts": ["not-a-dict-entry"]}))
        assert scan_results_json_for_log_and_sbom_keys(str(p)) == (False, False)


class TestProcessUploadsFromResultsJson:
    """Test process_uploads_from_results_json function."""

    def test_raises_when_logs_present_but_logs_repo_skipped(self, tmp_path, mock_pulp_client):
        """Guard: log uploads require logs_prn."""
        arch_dir = tmp_path / "x86_64"
        arch_dir.mkdir()
        log_path = arch_dir / "build.log"
        log_path.write_text("log line")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/build.log": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            skip_logs_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="/rpm",
            rpms_prn="rprn",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with pytest.raises(ValueError, match="logs repository was not created"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_raises_when_sbom_present_but_sbom_repo_skipped(self, tmp_path, mock_pulp_client):
        sbom_path = tmp_path / "sbom.spdx.json"
        sbom_path.write_text("{}")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"sbom.spdx.json": {}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            skip_sbom_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="/rpm",
            rpms_prn="rprn",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with pytest.raises(ValueError, match="SBOM repository was not created"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_basic(self, tmp_path, mock_pulp_client):
        """Test upload from JSON without signed-by."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {
            "artifacts": {
                "x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}},
            }
        }
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_with_signed_by(self, tmp_path, mock_pulp_client):
        """Test upload with signed-by uses signed repos and labels."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="/test/rpm-signed-href",
            rpms_signed_prn="rpms-signed-prn",
            logs_signed_prn="logs-signed-prn",
            sbom_signed_prn="sbom-signed-prn",
            artifacts_signed_prn="artifacts-signed-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        call_kw = mock_upload_rpms.call_args[1]
        assert call_kw["rpm_repository_href"] == "/test/rpm-signed-href"
        call_context = mock_upload_rpms.call_args[0][1]
        assert call_context.signed_by == "key-123"

    def test_upload_from_results_json_target_arch_repo_uses_ensure_per_arch(self, tmp_path, mock_pulp_client):
        """target_arch_repo sets bulk rpm_href empty and uses ensure_rpm_repository_for_arch per arch."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"x")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            target_arch_repo=True,
        )
        repositories = RepositoryRefs(
            rpms_href="",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )
        mock_ph_instance = Mock()
        mock_ph_instance.ensure_rpm_repository_for_arch.return_value = "/per-arch/rpm"

        with (
            patch("pulp_tool.services.upload_service.PulpHelper", return_value=mock_ph_instance),
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/r/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/r.json"),
        ):
            result = process_uploads_from_results_json(
                mock_pulp_client, context, repositories, pulp_helper=mock_ph_instance
            )

        assert result == "https://example.com/r.json"
        mock_ph_instance.ensure_rpm_repository_for_arch.assert_called_once_with("test-build", "x86_64")
        assert mock_upload_rpms.call_args.kwargs["rpm_repository_href"] == "/per-arch/rpm"

    def test_upload_from_results_json_files_base_path(self, tmp_path, mock_pulp_client):
        """Test override base path for resolving artifact keys."""
        base_dir = tmp_path / "files"
        base_dir.mkdir()
        rpm_file = base_dir / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm content")

        results_json_path = tmp_path / "pulp_results.json"
        results_data = {"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            files_base_path=str(base_dir),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/resource/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_missing_file(self, tmp_path, mock_pulp_client, caplog):
        """Test handling of missing file - skip with warning."""
        results_json_path = tmp_path / "pulp_results.json"
        results_data = {
            "artifacts": {
                "x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}},
            }
        }
        results_json_path.write_text(json.dumps(results_data))
        # No actual file at tmp_path/x86_64/pkg.rpm

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=[]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            caplog.at_level(logging.WARNING),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        assert "Skipping missing file" in caplog.text

    def test_upload_from_results_json_empty_artifacts(self, tmp_path, mock_pulp_client):
        """Test empty artifacts creates minimal results."""
        results_json_path = tmp_path / "pulp_results.json"
        results_data: dict = {"artifacts": {}}
        results_json_path.write_text(json.dumps(results_data))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with patch(
            "pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_returns_none_when_no_results_json(self, mock_pulp_client):
        """Test returns None when results_json is not set."""
        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=None,
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        result = process_uploads_from_results_json(mock_pulp_client, context, repositories)
        assert result is None

    def test_upload_from_results_json_json_read_error(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json raises when JSON cannot be read."""
        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text('{"artifacts": {"x": {}}}')

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with patch("builtins.open", side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_signed_by_no_signed_repos(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json raises when signed_by set but no signed repos."""
        rpm_file = tmp_path / "x86_64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/pkg.rpm": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="",
            rpms_signed_prn="",
        )

        with pytest.raises(ValueError, match="signed_by requires signed repositories"):
            process_uploads_from_results_json(mock_pulp_client, context, repositories)

    def test_upload_from_results_json_invalid_artifact_entry(self, tmp_path, mock_pulp_client, caplog):
        """Test process_uploads_from_results_json skips non-dict artifact entries."""
        rpm_file = tmp_path / "pkg.rpm"
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps({"artifacts": {"pkg.rpm": {"labels": {"arch": "noarch"}}, "bad": "not a dict"}})
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
            caplog.at_level(logging.WARNING),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        assert "Skipping invalid artifact entry" in caplog.text

    def test_upload_from_results_json_log_and_sbom_and_artifact(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json with log, sbom, and generic artifact."""
        log_file = tmp_path / "x86_64" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log content")

        sbom_file = tmp_path / "sbom.json"
        sbom_file.write_text('{"sbom": true}')

        artifact_file = tmp_path / "data.txt"
        artifact_file.write_text("data")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "x86_64/state.log": {"labels": {"arch": "x86_64"}},
                        "sbom.json": {"labels": {}},
                        "data.txt": {"labels": {}},
                    }
                }
            )
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        mock_resp = Mock()
        mock_resp.json.return_value = {"task": "/tasks/1/"}
        mock_task = Mock()
        mock_task.created_resources = ["/content/1/"]
        mock_pulp_client.create_file_content = Mock(return_value=mock_resp)
        mock_pulp_client.wait_for_finished_task = Mock(return_value=mock_task)
        mock_pulp_client.check_response = Mock()

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"

    def test_upload_from_results_json_arch_inference_from_path(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers arch from path when not in labels."""
        rpm_file = tmp_path / "aarch64" / "pkg.rpm"
        rpm_file.parent.mkdir()
        rpm_file.write_bytes(b"fake rpm")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"aarch64/pkg.rpm": {"labels": {}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]) as mock_upload_rpms,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        assert mock_upload_rpms.call_args[0][3] == "aarch64"

    def test_upload_from_results_json_log_arch_inference_from_path(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers log arch from path (lines 290-291)."""
        log_file = tmp_path / "s390x" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"s390x/state.log": {"labels": {}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]) as mock_upload_log,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_log.assert_called_once()
        assert mock_upload_log.call_args[1]["arch"] == "s390x"

    def test_upload_from_results_json_arch_inference_noarch(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json infers noarch when no arch in path (lines 283, 287-293)."""
        rpm_file = tmp_path / "pkg.rpm"
        rpm_file.write_bytes(b"fake rpm")
        log_file = tmp_path / "state.log"
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(
            json.dumps(
                {
                    "artifacts": {
                        "pkg.rpm": {"labels": {}},
                        "state.log": {"labels": {}},
                    }
                }
            )
        )

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_rpms", return_value=["/rpm/1"]) as mock_upload_rpms,
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]),
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_rpms.assert_called_once()
        assert mock_upload_rpms.call_args[0][3] == "noarch"

    def test_upload_from_results_json_log_uses_unsigned_repo_with_signed_by(self, tmp_path, mock_pulp_client):
        """Test process_uploads_from_results_json uses unsigned logs repo when signed_by set (logs never signed)."""
        log_file = tmp_path / "x86_64" / "state.log"
        log_file.parent.mkdir()
        log_file.write_text("log")

        results_json_path = tmp_path / "pulp_results.json"
        results_json_path.write_text(json.dumps({"artifacts": {"x86_64/state.log": {"labels": {"arch": "x86_64"}}}}))

        context = UploadRpmContext(
            build_id="test-build",
            date_str="2024-01-01",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/ignored",
            results_json=str(results_json_path),
            signed_by="key-123",
        )
        repositories = RepositoryRefs(
            rpms_href="/test/rpm-href",
            rpms_prn="",
            logs_href="",
            logs_prn="logs-prn",
            sbom_href="",
            sbom_prn="sbom-prn",
            artifacts_href="",
            artifacts_prn="artifacts-prn",
            rpms_signed_href="/test/rpm-signed-href",
            rpms_signed_prn="rpms-signed-prn",
        )

        with (
            patch("pulp_tool.utils.uploads.upload_log", return_value=["/log/1"]) as mock_upload_log,
            patch("pulp_tool.services.upload_service.collect_results", return_value="https://example.com/results.json"),
        ):
            result = process_uploads_from_results_json(mock_pulp_client, context, repositories)

        assert result == "https://example.com/results.json"
        mock_upload_log.assert_called_once()
        call_kw = mock_upload_log.call_args[1]
        assert "signed_by" not in call_kw["labels"]
        assert mock_upload_log.call_args[0][1] == "logs-prn"
