"""
Pulp API client modules.

This package provides clients for interacting with Pulp API:
- OAuth2 authentication
- Main Pulp client for repository and content management
- Distribution client for downloading artifacts
- Specialized managers for content, tasks, queries, and repositories
- New resource-based API modules matching Pulp's API structure
"""

from .auth import OAuth2ClientCredentialsAuth
from .base import BaseResourceMixin
from .distribution_client import DistributionClient
from .pulp_client import PulpClient

# Resource-based modules
from .repositories.rpm import RpmRepositoryMixin
from .repositories.file import FileRepositoryMixin
from .distributions.rpm import RpmDistributionMixin
from .distributions.file import FileDistributionMixin
from .content.rpm_packages import RpmPackageContentMixin
from .content.file_files import FileContentMixin
from .artifacts.operations import ArtifactMixin
from .tasks.operations import TaskMixin

# Import Pulp API models for convenience
from ..models.pulp_api import (
    TaskResponse,
    RepositoryResponse,
    DistributionResponse,
    ContentResponse,
    RpmPackageResponse,
    FileResponse,
    OAuthTokenResponse,
    # New models
    RpmRepositoryResponse,
    FileRepositoryResponse,
    RpmDistributionResponse,
    FileDistributionResponse,
    TaskListResponse,
    ArtifactListResponse,
)

__all__ = [
    # Core clients
    "OAuth2ClientCredentialsAuth",
    "DistributionClient",
    "PulpClient",
    # Base mixins
    "BaseResourceMixin",
    # Resource-based mixins
    "RpmRepositoryMixin",
    "FileRepositoryMixin",
    "RpmDistributionMixin",
    "FileDistributionMixin",
    "RpmPackageContentMixin",
    "FileContentMixin",
    "ArtifactMixin",
    "TaskMixin",
    # API Models
    "TaskResponse",
    "TaskListResponse",
    "RepositoryResponse",
    "RpmRepositoryResponse",
    "FileRepositoryResponse",
    "DistributionResponse",
    "RpmDistributionResponse",
    "FileDistributionResponse",
    "ContentResponse",
    "RpmPackageResponse",
    "FileResponse",
    "ArtifactListResponse",
    "OAuthTokenResponse",
]
