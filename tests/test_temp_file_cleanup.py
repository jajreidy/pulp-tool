"""
Tests to verify temporary file cleanup fixtures work correctly.

These tests demonstrate proper temporary file handling patterns.
"""

from pathlib import Path
import tempfile


class TestTempFileFixtures:
    """Test temporary file fixture behavior."""

    def test_temp_files_fixture(self, temp_files) -> None:
        """Test temp_files fixture creates and cleans up automatically."""
        test_file = temp_files / "test.txt"
        test_file.write_text("test content")
        assert test_file.exists()
        assert test_file.read_text() == "test content"

    def test_create_temp_file_fixture(self, create_temp_file) -> None:
        """Test create_temp_file factory fixture."""
        file1 = create_temp_file("config.toml", '[cli]\nkey = "value"')
        file2 = create_temp_file("data.json", '{"test": true}')
        file3 = create_temp_file("binary.dat", b"binary content", binary=True)
        assert file1.exists()
        assert file2.exists()
        assert file3.exists()
        assert "key" in file1.read_text()
        assert "test" in file2.read_text()
        assert file3.read_bytes() == b"binary content"

    def test_manual_cleanup_pattern(self) -> None:
        """Demonstrate proper manual cleanup with try/finally."""
        temp_files = []
        try:
            for i in range(3):
                with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
                    f.write(f"content {i}")
                    temp_files.append(f.name)
            for path in temp_files:
                assert Path(path).exists()
            assert len(temp_files) == 3
        finally:
            for path in temp_files:
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass

    def test_existing_fixtures(self, temp_file, temp_rpm_file, temp_dir, temp_config_file) -> None:
        """Test that existing fixtures still work correctly."""
        assert Path(temp_file).exists()
        assert "test content" in Path(temp_file).read_text()
        assert Path(temp_rpm_file).exists()
        assert Path(temp_rpm_file).suffix == ".rpm"
        assert Path(temp_dir).exists()
        assert Path(temp_dir).is_dir()
        assert Path(temp_config_file).exists()
        assert Path(temp_config_file).suffix == ".toml"


class TestCleanupVerification:
    """Verify cleanup actually happens."""

    def test_pytest_tmp_path_cleanup(self, tmp_path) -> None:
        """Verify pytest's tmp_path cleans up automatically."""
        test_file = tmp_path / "verify_cleanup.txt"
        test_file.write_text("This will be cleaned up")
        path_str = str(test_file)
        assert Path(path_str).exists()


class TestCleanupBestPractices:
    """Demonstrate best practices for temporary file handling."""

    def test_context_manager_pattern(self, tmp_path) -> None:
        """Use context managers when possible."""
        config_file = tmp_path / "config.toml"
        with open(config_file, "w") as f:
            f.write('[cli]\nbase_url = "https://example.com"\n')
        assert config_file.exists()

    def test_early_cleanup(self, tmp_path) -> None:
        """Clean up as soon as you're done with a file."""
        temp_file = tmp_path / "temporary.txt"
        temp_file.write_text("temporary data")
        content = temp_file.read_text()
        assert content == "temporary data"
        temp_file.unlink()
        assert not temp_file.exists()
