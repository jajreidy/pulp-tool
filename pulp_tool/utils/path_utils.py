"""
File path handling utilities.

This module provides centralized functions for file path operations including
constructing paths, determining save locations, and handling directory creation.
"""

import os
from typing import Optional


def get_artifact_save_path(filename: str, arch: str, artifact_type: str, base_dir: Optional[str] = None) -> str:
    """
    Determine the save path for an artifact based on its type.

    Args:
        filename: Name of the file to save
        arch: Architecture for organizing the file path
        artifact_type: Type of artifact (rpm, log, sbom) - determines save location
        base_dir: Optional base directory (defaults to current directory)

    Returns:
        Full path where the artifact should be saved

    Example:
        >>> get_artifact_save_path("package.rpm", "x86_64", "rpm")
        'package.rpm'
        >>> get_artifact_save_path("build.log", "x86_64", "log")
        'logs/x86_64/build.log'
    """
    # Extract basename from filename (in case it includes a path)
    file_basename = os.path.basename(filename)

    # Determine file path based on artifact type
    if artifact_type == "log":
        # Log files go to logs/<arch>/
        if base_dir:
            file_full_filename = os.path.join(base_dir, "logs", arch, file_basename)
        else:
            file_full_filename = os.path.join("logs", arch, file_basename)
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_full_filename), exist_ok=True)
    else:
        # RPM and SBOM files go to current folder (or base_dir if specified)
        if base_dir:
            file_full_filename = os.path.join(base_dir, file_basename)
        else:
            file_full_filename = file_basename

    return file_full_filename


def ensure_directory_exists(file_path: str) -> None:
    """
    Ensure the directory containing the file path exists.

    Args:
        file_path: Full path to a file

    Example:
        >>> ensure_directory_exists("/tmp/logs/x86_64/build.log")
        # Creates /tmp/logs/x86_64/ if it doesn't exist
    """
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def join_path(*parts: str) -> str:
    """
    Join path components using os.path.join.

    This is a convenience wrapper that provides a consistent interface
    for path joining operations.

    Args:
        *parts: Path components to join

    Returns:
        Joined path string

    Example:
        >>> join_path("logs", "x86_64", "build.log")
        'logs/x86_64/build.log'
    """
    return os.path.join(*parts)


def get_basename(file_path: str) -> str:
    """
    Get the basename (filename) from a file path.

    Args:
        file_path: Full path to a file

    Returns:
        Basename of the file

    Example:
        >>> get_basename("/tmp/logs/x86_64/build.log")
        'build.log'
    """
    return os.path.basename(file_path)


def get_dirname(file_path: str) -> str:
    """
    Get the directory name from a file path.

    Args:
        file_path: Full path to a file

    Returns:
        Directory path

    Example:
        >>> get_dirname("/tmp/logs/x86_64/build.log")
        '/tmp/logs/x86_64'
    """
    return os.path.dirname(file_path)


def path_exists(path: str) -> bool:
    """
    Check if a path exists.

    Args:
        path: Path to check

    Returns:
        True if path exists, False otherwise

    Example:
        >>> path_exists("/tmp")
        True
    """
    return os.path.exists(path)


def is_file(path: str) -> bool:
    """
    Check if a path is a file.

    Args:
        path: Path to check

    Returns:
        True if path is a file, False otherwise

    Example:
        >>> is_file("/tmp/file.txt")
        True
    """
    return os.path.isfile(path)


def is_dir(path: str) -> bool:
    """
    Check if a path is a directory.

    Args:
        path: Path to check

    Returns:
        True if path is a directory, False otherwise

    Example:
        >>> is_dir("/tmp")
        True
    """
    return os.path.isdir(path)


__all__ = [
    "get_artifact_save_path",
    "ensure_directory_exists",
    "join_path",
    "get_basename",
    "get_dirname",
    "path_exists",
    "is_file",
    "is_dir",
]
