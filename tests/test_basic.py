"""
Basic tests for pulp-tool package.

This module contains basic tests to verify the package structure and imports.
"""

import pulp_tool


def test_package_import() -> None:
    """Test that the package can be imported."""
    assert pulp_tool is not None


def test_version() -> None:
    """Test that version is accessible and matches _version module."""
    from pulp_tool._version import __version__ as version_from_module

    assert hasattr(pulp_tool, "__version__")
    assert pulp_tool.__version__ is not None
    assert pulp_tool.__version__ == version_from_module


def test_version_module() -> None:
    """Test that _version module can be imported and has a non-empty version string."""
    from pulp_tool._version import __version__

    assert isinstance(__version__, str)
    assert __version__


def test_main_classes_import() -> None:
    """Test that main classes can be imported."""
    from pulp_tool import PulpClient, PulpHelper

    assert PulpClient is not None
    assert PulpHelper is not None


def test_oauth_auth_import() -> None:
    """Test that OAuth authentication class can be imported."""
    from pulp_tool import OAuth2ClientCredentialsAuth

    assert OAuth2ClientCredentialsAuth is not None


def test_utility_functions_import() -> None:
    """Test that utility functions can be imported."""
    from pulp_tool import setup_logging, create_session_with_retry

    assert setup_logging is not None
    assert create_session_with_retry is not None
