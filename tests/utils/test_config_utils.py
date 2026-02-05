"""Tests for config_utils module."""

import base64

import pytest

from pulp_tool.utils.config_utils import decode_base64_config, is_base64_config, load_config_content


class TestDecodeBase64Config:
    """Tests for decode_base64_config function."""

    def test_decode_valid_base64(self):
        """Test decode_base64_config decodes valid base64."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        result = decode_base64_config(base64_config)

        assert result.decode("utf-8") == config_content

    def test_decode_base64_with_whitespace(self):
        """Test decode_base64_config handles whitespace."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()
        base64_with_ws = "  " + base64_config + "\n  "

        result = decode_base64_config(base64_with_ws)

        assert result.decode("utf-8") == config_content

    def test_decode_invalid_base64_raises_error(self):
        """Test decode_base64_config raises ValueError for invalid base64."""
        invalid_base64 = "A" * 100 + "B" * 100 + "C" * 50 + "=" * 3  # Invalid padding

        with pytest.raises(ValueError, match="Failed to decode base64 config"):
            decode_base64_config(invalid_base64)


class TestIsBase64Config:
    """Tests for is_base64_config function."""

    def test_none_returns_false(self):
        """Test is_base64_config returns False for None."""
        assert is_base64_config(None) is False

    def test_short_string_false(self):
        """Test that short strings return False."""
        assert is_base64_config("short") is False
        assert is_base64_config("config.toml") is False

    def test_path_with_separator_false(self):
        """Test that paths with separators return False."""
        assert is_base64_config("/path/to/file") is False
        assert is_base64_config("path/to/file") is False
        assert is_base64_config("path\\to\\file") is False

    def test_path_starting_with_tilde_false(self):
        """Test that paths starting with ~ return False."""
        assert is_base64_config("~/config.toml") is False

    def test_path_starting_with_dot_false(self):
        """Test that paths starting with . return False."""
        assert is_base64_config("./config.toml") is False
        assert is_base64_config("../config.toml") is False

    def test_long_path_starting_with_slash_false(self):
        """Test that long paths starting with / return False."""
        # Long path starting with / that would otherwise look like base64
        long_path = "/" + "a" * 100 + "b" * 100
        assert is_base64_config(long_path) is False

    def test_long_path_starting_with_tilde_false(self):
        """Test that long paths starting with ~ return False (covers line 65)."""
        # Long path starting with ~ that doesn't contain / or \\
        # Must be long enough (>50 chars) to pass length check
        long_path = "~" + "a" * 100 + "b" * 100
        assert is_base64_config(long_path) is False

    def test_long_path_starting_with_dot_false(self):
        """Test that long paths starting with . return False (covers line 65)."""
        # Long path starting with . that doesn't contain / or \\
        long_path = "." + "a" * 100 + "b" * 100
        assert is_base64_config(long_path) is False

    def test_valid_base64_true(self):
        """Test that valid base64 strings return True."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        assert is_base64_config(base64_config) is True

    def test_long_base64_string_true(self):
        """Test that long base64 strings return True."""
        long_content = "A" * 100 + "B" * 100 + "C" * 100
        base64_long = base64.b64encode(long_content.encode()).decode()

        assert is_base64_config(base64_long) is True

    def test_base64_with_whitespace_true(self):
        """Test that base64 with whitespace returns True."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()
        base64_with_ws = "  " + base64_config + "\n"

        assert is_base64_config(base64_with_ws) is True

    def test_non_base64_long_string_false(self):
        """Test that long non-base64 strings return False."""
        # Long string with invalid base64 characters
        long_invalid = "!" * 200

        assert is_base64_config(long_invalid) is False

    def test_mixed_content_false(self):
        """Test that strings with mixed content return False."""
        mixed = "some/path/with/base64=" + base64.b64encode(b"test").decode()

        assert is_base64_config(mixed) is False


class TestLoadConfigContent:
    """Tests for load_config_content function."""

    def test_none_raises_error(self):
        """Test load_config_content raises ValueError for None."""
        with pytest.raises(ValueError, match="Config cannot be None"):
            load_config_content(None)

    def test_load_from_file(self, tmp_path):
        """Test load_config_content loads from file path."""
        config_file = tmp_path / "config.toml"
        config_content = '[cli]\nbase_url = "https://example.com"'
        config_file.write_text(config_content)

        content, is_base64 = load_config_content(str(config_file))

        assert is_base64 is False
        assert content.decode("utf-8") == config_content

    def test_load_from_file_with_tilde(self, tmp_path, monkeypatch):
        """Test load_config_content handles tilde expansion."""
        config_file = tmp_path / "config.toml"
        config_content = '[cli]\nbase_url = "https://example.com"'
        config_file.write_text(config_content)

        # Mock home directory
        import os.path

        def mock_expanduser(path):
            if path.startswith("~"):
                return str(tmp_path / path[2:])
            return path

        monkeypatch.setattr(os.path, "expanduser", mock_expanduser)

        content, is_base64 = load_config_content("~/config.toml")

        assert is_base64 is False
        assert content.decode("utf-8") == config_content

    def test_load_from_base64(self):
        """Test load_config_content loads from base64."""
        config_content = '[cli]\nbase_url = "https://example.com"\ndomain = "test"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        content, is_base64 = load_config_content(base64_config)

        assert is_base64 is True
        assert content.decode("utf-8") == config_content

    def test_load_from_base64_with_whitespace(self):
        """Test load_config_content handles base64 with whitespace."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()
        base64_with_ws = "  " + base64_config + "\n  "

        content, is_base64 = load_config_content(base64_with_ws)

        assert is_base64 is True
        assert content.decode("utf-8") == config_content

    def test_load_file_not_found(self):
        """Test load_config_content raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            load_config_content("/nonexistent/config.toml")

    def test_load_invalid_base64_raises_error(self):
        """Test load_config_content raises ValueError for invalid base64."""
        invalid_base64 = "A" * 100 + "B" * 100 + "C" * 50 + "=" * 3  # Invalid padding

        with pytest.raises(ValueError, match="Failed to decode base64 config"):
            load_config_content(invalid_base64)
