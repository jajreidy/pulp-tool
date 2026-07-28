"""
Pulp API client modules.

This package provides clients for interacting with Pulp API:
- OAuth2 authentication
- Main Pulp client for repository and content management
- Distribution client for downloading artifacts
- Specialized managers for content, tasks, queries, and repositories
- New resource-based API modules matching Pulp's API structure
"""

# Import Pulp API models for convenience
from ..models.pulp_api import (
    ArtifactListResponse,
    ContentResponse,
    DistributionResponse,
    FileDistributionResponse,
    FileRepositoryResponse,
    FileResponse,
    OAuthTokenResponse,
    RepositoryResponse,
    RpmDistributionResponse,
    RpmPackageResponse,
    # New models
    RpmRepositoryResponse,
    TaskListResponse,
    TaskResponse,
)
from .artifacts.operations import ArtifactMixin
from .auth import OAuth2ClientCredentialsAuth
from .base import BaseResourceMixin
from .content.file_files import FileContentMixin
from .content.rpm_packages import RpmPackageContentMixin
from .distribution_client import DistributionClient
from .distributions.file import FileDistributionMixin
from .distributions.rpm import RpmDistributionMixin
from .pulp_client import PulpClient
from .repositories.file import FileRepositoryMixin

# Resource-based modules
from .repositories.rpm import RpmRepositoryMixin
from .tasks.operations import TaskMixin

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
