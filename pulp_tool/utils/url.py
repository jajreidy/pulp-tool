"""
URL utilities for Pulp operations.

This module provides utilities for constructing and managing URLs
for Pulp content access.
"""

import logging
from pathlib import Path
from typing import Optional

import tomllib


def get_pulp_content_base_url(cert_config_path: Optional[str] = None) -> str:
    """
    Get the Pulp content base URL from cert config or use default.

    Args:
        cert_config_path: Optional path to certificate config file

    Returns:
        Constructed base URL for Pulp content

    Raises:
        ValueError: If cert config path is required but not provided,
                   or if config cannot be read

    Note:
        If cert/key paths in the config are relative and don't exist as-is,
        they will be resolved relative to the config file's directory.

    Example:
        >>> url = get_pulp_content_base_url("/path/to/cert-config.toml")
        >>> print(url)
        'https://pulp.example.com/api/pulp-content'
    """
    if cert_config_path:
        try:
            config_path = Path(cert_config_path).expanduser()
            with open(config_path, "rb") as fp:
                config = tomllib.load(fp)

            base_url = config["cli"]["base_url"]
            # api_root = config["cli"]["api_root"]  # Not currently used, but kept in config for future use

            # Construct the content base URL
            # We always use "/api/pulp-content" regardless of what's in api_root
            # This avoids issues with different api_root formats
            content_path = "/api/pulp-content"

            content_base_url = f"{base_url}{content_path}"
            logging.info("Using cert config base URL: %s", content_base_url)

            # Note: cert/key path resolution is now handled by PulpClient.cert property
            # when the config is loaded via create_from_config_file()

            return content_base_url

        except Exception as e:
            logging.error("Failed to read cert config %s: %s", cert_config_path, e)
            raise ValueError(f"Cannot determine Pulp content base URL: {e}") from e

    # No cert config provided
    raise ValueError("cert_config_path is required to determine Pulp content base URL")


__all__ = ["get_pulp_content_base_url"]
