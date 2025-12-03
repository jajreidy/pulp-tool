"""Tests for path utility functions."""

import os
import tempfile

from pulp_tool.utils.path_utils import (
    ensure_directory_exists,
    get_artifact_save_path,
    get_basename,
    get_dirname,
    is_dir,
    is_file,
    join_path,
    path_exists,
)


class TestGetArtifactSavePath:
    """Tests for get_artifact_save_path function."""

    def test_get_artifact_save_path_log_with_base_dir(self):
        """Test get_artifact_save_path for log files with base_dir (line 38)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("build.log", "x86_64", "log", base_dir=tmpdir)

            assert path == os.path.join(tmpdir, "logs", "x86_64", "build.log")
            assert os.path.exists(os.path.dirname(path))

    def test_get_artifact_save_path_log_without_base_dir(self):
        """Test get_artifact_save_path for log files without base_dir."""
        path = get_artifact_save_path("build.log", "x86_64", "log")

        assert path == os.path.join("logs", "x86_64", "build.log")

    def test_get_artifact_save_path_rpm_with_base_dir(self):
        """Test get_artifact_save_path for RPM files with base_dir (line 46)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("package.rpm", "x86_64", "rpm", base_dir=tmpdir)

            assert path == os.path.join(tmpdir, "package.rpm")

    def test_get_artifact_save_path_rpm_without_base_dir(self):
        """Test get_artifact_save_path for RPM files without base_dir."""
        path = get_artifact_save_path("package.rpm", "x86_64", "rpm")

        assert path == "package.rpm"

    def test_get_artifact_save_path_sbom_with_base_dir(self):
        """Test get_artifact_save_path for SBOM files with base_dir (line 46)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = get_artifact_save_path("sbom.json", "x86_64", "sbom", base_dir=tmpdir)

            assert path == os.path.join(tmpdir, "sbom.json")

    def test_get_artifact_save_path_with_path_in_filename(self):
        """Test get_artifact_save_path extracts basename from filename with path."""
        path = get_artifact_save_path("/some/path/package.rpm", "x86_64", "rpm")

        assert path == "package.rpm"


class TestEnsureDirectoryExists:
    """Tests for ensure_directory_exists function."""

    def test_ensure_directory_exists_with_directory(self):
        """Test ensure_directory_exists creates directory when it doesn't exist (lines 64-66)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "subdir", "file.txt")

            ensure_directory_exists(file_path)

            assert os.path.exists(os.path.dirname(file_path))

    def test_ensure_directory_exists_no_directory(self):
        """Test ensure_directory_exists with file path that has no directory (lines 64-66)."""
        # File in current directory (no directory component)
        file_path = "file.txt"

        # Should not raise an error
        ensure_directory_exists(file_path)

    def test_ensure_directory_exists_existing_directory(self):
        """Test ensure_directory_exists with existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "file.txt")

            # Create directory first
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Should not raise an error
            ensure_directory_exists(file_path)


class TestJoinPath:
    """Tests for join_path function."""

    def test_join_path(self):
        """Test join_path function (line 86)."""
        result = join_path("logs", "x86_64", "build.log")

        assert result == os.path.join("logs", "x86_64", "build.log")

    def test_join_path_single_component(self):
        """Test join_path with single component."""
        result = join_path("file.txt")

        assert result == "file.txt"

    def test_join_path_multiple_components(self):
        """Test join_path with multiple components."""
        result = join_path("/tmp", "logs", "x86_64", "build.log")

        assert result == os.path.join("/tmp", "logs", "x86_64", "build.log")


class TestGetBasename:
    """Tests for get_basename function."""

    def test_get_basename(self):
        """Test get_basename function (line 103)."""
        result = get_basename("/tmp/logs/x86_64/build.log")

        assert result == "build.log"

    def test_get_basename_simple_filename(self):
        """Test get_basename with simple filename."""
        result = get_basename("file.txt")

        assert result == "file.txt"

    def test_get_basename_no_path(self):
        """Test get_basename with no directory path."""
        result = get_basename("build.log")

        assert result == "build.log"


class TestGetDirname:
    """Tests for get_dirname function."""

    def test_get_dirname(self):
        """Test get_dirname function (line 120)."""
        result = get_dirname("/tmp/logs/x86_64/build.log")

        assert result == "/tmp/logs/x86_64"

    def test_get_dirname_simple_filename(self):
        """Test get_dirname with simple filename."""
        result = get_dirname("file.txt")

        assert result == ""

    def test_get_dirname_root_path(self):
        """Test get_dirname with root path."""
        result = get_dirname("/file.txt")

        assert result == "/"


class TestPathExists:
    """Tests for path_exists function."""

    def test_path_exists_true(self):
        """Test path_exists returns True for existing path (line 137)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert path_exists(tmpdir) is True

    def test_path_exists_false(self):
        """Test path_exists returns False for non-existent path."""
        assert path_exists("/nonexistent/path/12345") is False


class TestIsFile:
    """Tests for is_file function."""

    def test_is_file_true(self):
        """Test is_file returns True for file (line 154)."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            assert is_file(temp_path) is True
        finally:
            os.unlink(temp_path)

    def test_is_file_false_for_directory(self):
        """Test is_file returns False for directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert is_file(tmpdir) is False

    def test_is_file_false_for_nonexistent(self):
        """Test is_file returns False for non-existent path."""
        assert is_file("/nonexistent/file.txt") is False


class TestIsDir:
    """Tests for is_dir function."""

    def test_is_dir_true(self):
        """Test is_dir returns True for directory (line 171)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            assert is_dir(tmpdir) is True

    def test_is_dir_false_for_file(self):
        """Test is_dir returns False for file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            assert is_dir(temp_path) is False
        finally:
            os.unlink(temp_path)

    def test_is_dir_false_for_nonexistent(self):
        """Test is_dir returns False for non-existent path."""
        assert is_dir("/nonexistent/directory") is False
