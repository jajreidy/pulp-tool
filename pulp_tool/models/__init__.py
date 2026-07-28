"""
Pydantic models for pulp-tool.

This package contains all Pydantic models used in the application:
- pulp_api: Models for Pulp API responses
- base, repository, context, artifacts, results, validation, statistics: Domain models
"""

# Pulp API Response Models
from .artifacts import ArtifactFile, FileInfoModel, PulledArtifacts

# Domain Models
from .base import KonfluxBaseModel
from .context import PullContext, UploadContext, UploadFilesContext, UploadRpmContext
from .pulp_api import (
    ContentResponse,
    DistributionResponse,
    FileResponse,
    OAuthTokenResponse,
    PaginatedResponse,
    PulpBaseModel,
    RepositoryResponse,
    RpmPackageResponse,
    TaskResponse,
)
from .repository import RepositoryRefs
from .results import ArtifactInfo, PulpResultsModel
from .statistics import UploadCounts
from .validation import RpmCheckResult

__all__ = [
    # Pulp API Models
    "PulpBaseModel",
    "PaginatedResponse",
    "TaskResponse",
    "RepositoryResponse",
    "DistributionResponse",
    "ContentResponse",
    "RpmPackageResponse",
    "FileResponse",
    "OAuthTokenResponse",
    # Domain Models
    "KonfluxBaseModel",
    "RepositoryRefs",
    "RpmCheckResult",
    "ArtifactFile",
    "PulledArtifacts",
    "FileInfoModel",
    "UploadCounts",
    "ArtifactInfo",
    "PulpResultsModel",
    "UploadContext",
    "UploadRpmContext",
    "UploadFilesContext",
    "PullContext",
]
