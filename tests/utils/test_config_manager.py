"""Tests for ConfigManager class."""

import base64
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pulp_tool.utils.config_manager import ConfigManager


class TestConfigManagerInit:
    """Tests for ConfigManager initialization."""

    def test_init_with_path(self):
        """Test ConfigManager initialization with explicit path."""
        config_path = "/tmp/test_config.toml"
        manager = ConfigManager(config_path)
        assert manager.config_path == Path(config_path).expanduser()
        assert manager._config is None

    def test_init_without_path(self):
        """Test ConfigManager initialization with default path."""
        manager = ConfigManager()
        assert manager.config_path is not None
        assert manager._config is None

    def test_init_with_base64(self):
        """Test ConfigManager initialization with base64 config."""
        import base64

        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        manager = ConfigManager(base64_config)
        # For base64, config_path should be None
        assert manager.config_path is None
        assert manager.config_source == base64_config
        assert manager._config is None


class TestConfigManagerLoad:
    """Tests for ConfigManager.load() method."""

    def test_load_cached_config(self):
        """Test load() returns cached config (line 45)."""
        manager = ConfigManager()
        manager._config = {"test": "value"}

        result = manager.load()

        assert result == {"test": "value"}

    def test_load_file_not_found(self):
        """Test load() raises FileNotFoundError when file doesn't exist (line 48)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "nonexistent.toml"
            manager = ConfigManager(str(config_path))

            with pytest.raises(FileNotFoundError) as exc_info:
                manager.load()

            assert str(config_path) in str(exc_info.value)

    def test_load_success(self):
        """Test load() successfully loads TOML file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))
            result = manager.load()

            assert result["cli"]["base_url"] == "https://example.com"
            assert manager._config == result

    def test_load_invalid_toml(self):
        """Test load() raises ValueError for invalid TOML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text("invalid toml content [unclosed")

            manager = ConfigManager(str(config_path))

            with pytest.raises(ValueError) as exc_info:
                manager.load()

            assert "Invalid TOML" in str(exc_info.value)

    def test_load_io_error(self):
        """Test load() raises ValueError for IO errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))

            with patch("builtins.open", side_effect=PermissionError("Permission denied")):
                with pytest.raises(ValueError) as exc_info:
                    manager.load()

                assert "Failed to load configuration" in str(exc_info.value)

    def test_load_from_base64(self):
        """Test load() successfully loads from base64 config."""
        config_content = '[cli]\nbase_url = "https://example.com"\ndomain = "test"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        manager = ConfigManager(base64_config)
        result = manager.load()

        assert result["cli"]["base_url"] == "https://example.com"
        assert result["cli"]["domain"] == "test"
        assert manager._config == result

    def test_load_from_base64_with_whitespace(self):
        """Test load() handles base64 with whitespace."""
        config_content = '[cli]\nbase_url = "https://example.com"'
        base64_config = base64.b64encode(config_content.encode()).decode()
        base64_with_ws = "  " + base64_config + "\n  "

        manager = ConfigManager(base64_with_ws)
        result = manager.load()

        assert result["cli"]["base_url"] == "https://example.com"

    def test_load_invalid_base64(self):
        """Test load() raises ValueError for invalid base64."""
        invalid_base64 = "A" * 100 + "B" * 100 + "C" * 50 + "=" * 3  # Invalid padding

        manager = ConfigManager(invalid_base64)

        with pytest.raises(ValueError, match="Failed to decode base64 config"):
            manager.load()


class TestConfigManagerGet:
    """Tests for ConfigManager.get() method."""

    def test_get_loads_config_if_needed(self):
        """Test get() loads config if not already loaded (line 80)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))
            assert manager._config is None

            result = manager.get("cli.base_url")

            assert result == "https://example.com"
            assert manager._config is not None

    def test_get_nested_key(self):
        """Test get() with nested keys."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com", "domain": "test"}}

        assert manager.get("cli.base_url") == "https://example.com"
        assert manager.get("cli.domain") == "test"

    def test_get_missing_key(self):
        """Test get() returns default for missing key."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}}

        assert manager.get("cli.missing", "default") == "default"
        assert manager.get("missing.key", None) is None

    def test_get_non_dict_value(self):
        """Test get() returns default when value is not a dict (line 91)."""
        manager = ConfigManager()
        manager._config = {"cli": "not a dict"}

        result = manager.get("cli.base_url", "default")

        assert result == "default"

    def test_get_with_default(self):
        """Test get() with explicit default value."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}}

        assert manager.get("cli.missing", "default_value") == "default_value"
        assert manager.get("missing.key", 42) == 42


class TestConfigManagerGetSection:
    """Tests for ConfigManager.get_section() method."""

    def test_get_section_loads_config_if_needed(self):
        """Test get_section() loads config if not already loaded (lines 110-111)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))
            assert manager._config is None

            result = manager.get_section("cli")

            assert result["base_url"] == "https://example.com"
            assert manager._config is not None

    def test_get_section_returns_empty_when_config_none(self):
        """Test get_section() returns empty dict when _config is None after load (lines 113-114)."""
        manager = ConfigManager("/nonexistent/config.toml")

        with patch.object(manager, "load", return_value=None):
            result = manager.get_section("cli")

            assert result == {}

    def test_get_section_existing(self):
        """Test get_section() returns existing section (line 116)."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}, "other": {"key": "value"}}

        result = manager.get_section("cli")

        assert result == {"base_url": "https://example.com"}

    def test_get_section_missing(self):
        """Test get_section() returns empty dict for missing section."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}}

        result = manager.get_section("missing")

        assert result == {}


class TestConfigManagerHasKey:
    """Tests for ConfigManager.has_key() method."""

    def test_has_key_loads_config_if_needed(self):
        """Test has_key() loads config if not already loaded (lines 128-132)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))
            assert manager._config is None

            result = manager.has_key("cli.base_url")

            assert result is True
            assert manager._config is not None

    def test_has_key_handles_file_not_found(self):
        """Test has_key() returns False when config file not found (lines 129-132)."""
        manager = ConfigManager("/nonexistent/config.toml")

        result = manager.has_key("cli.base_url")

        assert result is False

    def test_has_key_handles_value_error(self):
        """Test has_key() returns False when load raises ValueError (lines 129-132)."""
        manager = ConfigManager("/some/path/config.toml")

        with patch.object(manager, "load", side_effect=ValueError("Invalid config")):
            result = manager.has_key("cli.base_url")

            assert result is False

    def test_has_key_existing_nested(self):
        """Test has_key() with existing nested key (lines 134-135, 137-141, 145)."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com", "domain": "test"}}

        assert manager.has_key("cli.base_url") is True
        assert manager.has_key("cli.domain") is True

    def test_has_key_missing_key(self):
        """Test has_key() returns False for missing key (lines 140-141)."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}}

        assert manager.has_key("cli.missing") is False
        assert manager.has_key("missing.key") is False

    def test_has_key_non_dict_value(self):
        """Test has_key() returns False when value is not a dict (line 143)."""
        manager = ConfigManager()
        manager._config = {"cli": "not a dict"}

        result = manager.has_key("cli.base_url")

        assert result is False

    def test_has_key_top_level(self):
        """Test has_key() with top-level key."""
        manager = ConfigManager()
        manager._config = {"cli": {"base_url": "https://example.com"}, "other": "value"}

        assert manager.has_key("cli") is True
        assert manager.has_key("other") is True
        assert manager.has_key("missing") is False


class TestConfigManagerReload:
    """Tests for ConfigManager.reload() method."""

    def test_reload(self):
        """Test reload() clears cache and reloads config (lines 149-150)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.toml"
            config_path.write_text('[cli]\nbase_url = "https://example.com"')

            manager = ConfigManager(str(config_path))
            manager.load()
            original_config = manager._config

            # Modify config file
            config_path.write_text('[cli]\nbase_url = "https://updated.com"')

            manager.reload()

            assert manager._config != original_config
            assert manager.get("cli.base_url") == "https://updated.com"
