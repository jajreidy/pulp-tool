"""
Configuration utility functions for handling config input formats.

This module provides utilities for detecting and decoding base64-encoded config content.
"""

import base64
import logging
from pathlib import Path
from typing import Optional, Tuple, Union

logger = logging.getLogger(__name__)


def decode_base64_config(config: str) -> bytes:
    """
    Decode base64-encoded config content.

    Args:
        config: Base64-encoded config string

    Returns:
        Decoded bytes content

    Raises:
        ValueError: If base64 decoding fails
    """
    try:
        # Strip whitespace before decoding
        stripped_config = config.strip()
        decoded_content = base64.b64decode(stripped_config, validate=True)
        logger.debug("Decoded base64 config content (%d bytes)", len(decoded_content))
        return decoded_content
    except Exception as e:
        raise ValueError(f"Failed to decode base64 config: {e}") from e


def is_base64_config(config: Optional[str]) -> bool:
    """
    Check if a string is likely base64-encoded config content.

    This is a heuristic check - base64 strings typically:
    - Are longer than typical file paths
    - Contain only base64 characters (A-Z, a-z, 0-9, +, /, =)
    - Don't contain path separators or start with common path prefixes

    Args:
        config: String to check

    Returns:
        True if string appears to be base64-encoded
    """
    if config is None:
        return False

    # If it's a short string, it's probably a file path
    if len(config) < 50:
        return False

    # If it contains path separators, it's probably a file path
    if "/" in config or "\\" in config:
        return False

    # If it starts with common path prefixes, it's probably a file path
    if config.startswith(("~", ".", "/")):
        return False

    # Check if it contains only base64 characters
    # Base64 alphabet: A-Z, a-z, 0-9, +, /, = (padding)
    base64_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
    if all(c in base64_chars or c.isspace() for c in config):
        # If it's mostly base64 characters and reasonably long, likely base64
        non_space_chars = [c for c in config if not c.isspace()]
        if len(non_space_chars) > 50:
            return True

    return False


def load_file_content_maybe_base64(path: Union[str, Path]) -> Tuple[bytes, bool]:
    """
    Read file content, decoding from base64 if the content looks base64-encoded.

    Use this for files that may be stored as raw content (e.g. PEM) or as
    base64-encoded content (e.g. cert/key files in some environments).

    Args:
        path: Path to the file.

    Returns:
        Tuple of (content_bytes, was_base64). was_base64 is True if the file
        content was detected as base64 and decoded.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If content looks like base64 but decoding fails.
    """
    path = Path(path).expanduser() if isinstance(path, str) else path
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "rb") as f:
        content = f.read()

    try:
        content_str = content.decode("utf-8")
        if is_base64_config(content_str):
            decoded = decode_base64_config(content_str)
            logger.debug("Decoded base64 content from file %s (%d bytes)", path, len(decoded))
            return decoded, True
    except UnicodeDecodeError:
        pass

    return content, False


def load_config_content(config: Optional[str]) -> Tuple[bytes, bool]:
    """
    Load config content from either a file path or base64 string.

    Args:
        config: File path or base64-encoded config content

    Returns:
        Tuple of (config_bytes, is_base64) where is_base64 indicates if input was base64

    Raises:
        FileNotFoundError: If config is a file path and file doesn't exist
        ValueError: If base64 decoding fails
        OSError: If file reading fails
    """
    if config is None:
        raise ValueError("Config cannot be None")

    if is_base64_config(config):
        decoded = decode_base64_config(config)
        return decoded, True

    # Treat as file path
    config_path = Path(config).expanduser()
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "rb") as f:
        content = f.read()

    # If file content looks like base64, decode it before returning (source remains file path)
    try:
        content_str = content.decode("utf-8")
        if is_base64_config(content_str):
            content = decode_base64_config(content_str)
            logger.debug("Decoded base64 config content from file %s", config_path)
    except UnicodeDecodeError:
        # Binary or non-UTF-8 file; treat as raw content
        pass

    return content, False


__all__ = [
    "decode_base64_config",
    "is_base64_config",
    "load_config_content",
    "load_file_content_maybe_base64",
]
