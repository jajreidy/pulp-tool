"""
Pulp Tool - A Python client for Pulp API operations.

This package provides tools for interacting with Pulp API to manage
RPM repositories, file repositories, and content uploads with OAuth2 authentication.
"""

from ._version import __version__

__author__ = "Rok Artifact Storage Team"
__email__ = "rokartifactstorage@redhat.com"

# Import main classes and functions for easy access
from .api import PulpClient, OAuth2ClientCredentialsAuth, DistributionClient
from .exceptions import PulpToolConfigError, PulpToolError, PulpToolHTTPError
from .utils import (
    PulpHelper,
    create_session_with_retry,
    setup_logging,
    WrappingFormatter,
    get_logger,
    RepositoryRefs,
)
from .cli import main as cli_main, cli as cli_group

__all__ = [
    "__version__",
    "PulpToolError",
    "PulpToolConfigError",
    "PulpToolHTTPError",
    "PulpClient",
    "OAuth2ClientCredentialsAuth",
    "DistributionClient",
    "PulpHelper",
    "setup_logging",
    "WrappingFormatter",
    "get_logger",
    "create_session_with_retry",
    "RepositoryRefs",
    "cli_main",
    "cli_group",
]
