"""Tests for path utility functions."""

import os
import tempfile
from pulp_tool.utils.path_utils import get_artifact_save_path


class TestGetArtifactSavePath:
    """Tests for get_artifact_save_path function."""

    def test_get_artifact_save_path_log_with_base_dir(self) -> None:
        """Test get_artifact_save_path for log files with base_dir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("build.log", "x86_64", "log", base_dir=tmpdir)
            assert path == os.path.join(tmpdir, "logs", "x86_64", "build.log")
            assert os.path.exists(os.path.dirname(path))

    def test_get_artifact_save_path_log_without_base_dir(self, tmp_path) -> None:
        """Test get_artifact_save_path for log files without base_dir."""
        orig_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            path = get_artifact_save_path("build.log", "x86_64", "log")
            assert path == os.path.join("logs", "x86_64", "build.log")
        finally:
            os.chdir(orig_cwd)

    def test_get_artifact_save_path_rpm_with_base_dir(self) -> None:
        """Test get_artifact_save_path for RPM files with base_dir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("package.rpm", "x86_64", "rpm", base_dir=tmpdir)
            assert path == os.path.join(tmpdir, "package.rpm")

    def test_get_artifact_save_path_rpm_without_base_dir(self) -> None:
        """Test get_artifact_save_path for RPM files without base_dir."""
        path = get_artifact_save_path("package.rpm", "x86_64", "rpm")
        assert path == "package.rpm"

    def test_get_artifact_save_path_sbom_with_base_dir(self) -> None:
        """Test get_artifact_save_path for SBOM files with base_dir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("sbom.json", "x86_64", "sbom", base_dir=tmpdir)
            assert path == os.path.join(tmpdir, "sbom.json")

    def test_get_artifact_save_path_with_path_in_filename(self) -> None:
        """Test get_artifact_save_path extracts basename from filename with path."""
        path = get_artifact_save_path("/some/path/package.rpm", "x86_64", "rpm")
        assert path == "package.rpm"
