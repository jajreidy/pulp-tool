"""
Configuration management utilities.

This module provides centralized configuration loading and validation.
"""

import logging
import tomllib
from pathlib import Path
from typing import Any, Dict, Optional

from .constants import DEFAULT_CONFIG_PATH
from .config_utils import load_config_content


class ConfigManager:
    """
    Manages configuration loading and access.

    This class provides a centralized way to load and access configuration
    from TOML files or base64-encoded config content with proper error handling and validation.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to configuration file or base64-encoded config content.
                        If None, uses default path.
        """
        self.config_source = config_path or DEFAULT_CONFIG_PATH
        # Store original source for reference, but don't expand if it's base64
        # If config_path is None, we use default path which should be expanded
        # If config_path is provided and not base64, expand it
        if config_path is None:
            # Use default path - always expand it
            self.config_path: Optional[Path] = Path(DEFAULT_CONFIG_PATH).expanduser()
        elif self._is_base64():
            # Base64 config - no file path
            self.config_path = None
        else:
            # File path - expand it
            self.config_path = Path(self.config_source).expanduser()
        self._config: Optional[Dict[str, Any]] = None

    def _is_base64(self) -> bool:
        """Check if config_source is base64."""
        from .config_utils import is_base64_config

        return is_base64_config(self.config_source)

    def load(self) -> Dict[str, Any]:
        """
        Load configuration from file or base64 content.

        Returns:
            Dictionary containing configuration data

        Raises:
            FileNotFoundError: If config is a file path and file doesn't exist
            ValueError: If config file is invalid or base64 decoding fails
        """
        if self._config is not None:
            return self._config

        try:
            config_bytes, is_base64 = load_config_content(self.config_source)
            self._config = tomllib.loads(config_bytes.decode("utf-8"))

            source_desc = "base64 content" if is_base64 else str(self.config_path)
            logging.debug("Loaded configuration from %s", source_desc)
            return self._config
        except FileNotFoundError:
            # Re-raise FileNotFoundError as-is
            raise
        except tomllib.TOMLDecodeError as e:
            source_desc = "base64 content" if self._is_base64() else str(self.config_path)
            raise ValueError(f"Invalid TOML in configuration {source_desc}: {e}") from e
        except Exception as e:
            source_desc = "base64 content" if self._is_base64() else str(self.config_path)
            raise ValueError(f"Failed to load configuration from {source_desc}: {e}") from e

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.

        Supports nested keys using dot notation (e.g., "cli.base_url").

        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if key not found

        Returns:
            Configuration value or default

        Example:
            >>> config = ConfigManager("~/.config/pulp/cli.toml")
            >>> config.load()
            >>> config.get("cli.base_url")
            'https://pulp.example.com'
        """
        if self._config is None:
            self.load()

        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value if value is not None else default

    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get an entire configuration section.

        Args:
            section: Section name (e.g., "cli")

        Returns:
            Dictionary containing section data, or empty dict if section not found

        Example:
            >>> config = ConfigManager("~/.config/pulp/cli.toml")
            >>> config.load()
            >>> cli_section = config.get_section("cli")
        """
        if self._config is None:
            self.load()

        if self._config is None:
            return {}

        return self._config.get(section, {})

    def has_key(self, key: str) -> bool:
        """
        Check if a configuration key exists.

        Args:
            key: Configuration key (supports dot notation)

        Returns:
            True if key exists, False otherwise
        """
        if self._config is None:
            try:
                self.load()
            except (FileNotFoundError, ValueError):
                return False

        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return False
            else:
                return False

        return True

    def reload(self) -> None:
        """Force reload configuration from file."""
        self._config = None
        self.load()


__all__ = ["ConfigManager"]
