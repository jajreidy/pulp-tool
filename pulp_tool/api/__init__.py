"""
Pulp API client modules.

This package provides clients for interacting with Pulp API:
- OAuth2 authentication
- Main Pulp client for repository and content management
- Distribution client for downloading artifacts
- Specialized managers for content, tasks, queries, and repositories
"""

from .auth import OAuth2ClientCredentialsAuth
from .content_manager import ContentManagerMixin
from .content_query import ContentQueryMixin
from .distribution_client import DistributionClient
from .pulp_client import PulpClient
from .repository_manager import RepositoryManagerMixin
from .task_manager import TaskManagerMixin

# Import Pulp API models for convenience
from ..models.pulp_api import (
    TaskResponse,
    RepositoryResponse,
    DistributionResponse,
    ContentResponse,
    RpmPackageResponse,
    FileResponse,
    OAuthTokenResponse,
)

__all__ = [
    "OAuth2ClientCredentialsAuth",
    "ContentManagerMixin",
    "ContentQueryMixin",
    "DistributionClient",
    "PulpClient",
    "RepositoryManagerMixin",
    "TaskManagerMixin",
    # API Models
    "TaskResponse",
    "RepositoryResponse",
    "DistributionResponse",
    "ContentResponse",
    "RpmPackageResponse",
    "FileResponse",
    "OAuthTokenResponse",
]
