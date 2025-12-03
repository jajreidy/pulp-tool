"""
Configuration management utilities.

This module provides centralized configuration loading and validation.
"""

import logging
import tomllib
from pathlib import Path
from typing import Any, Dict, Optional

from .constants import DEFAULT_CONFIG_PATH


class ConfigManager:
    """
    Manages configuration loading and access.

    This class provides a centralized way to load and access configuration
    from TOML files with proper error handling and validation.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        self.config_path = Path(config_path).expanduser() if config_path else Path(DEFAULT_CONFIG_PATH).expanduser()
        self._config: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        """
        Load configuration from file.

        Returns:
            Dictionary containing configuration data

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        if self._config is not None:
            return self._config

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, "rb") as f:
                self._config = tomllib.load(f)
            logging.debug("Loaded configuration from %s", self.config_path)
            return self._config
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Invalid TOML in configuration file {self.config_path}: {e}") from e
        except Exception as e:
            raise ValueError(f"Failed to load configuration from {self.config_path}: {e}") from e

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
