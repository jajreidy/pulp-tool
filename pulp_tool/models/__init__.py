"""
Pydantic models for pulp-tool.

This package contains all Pydantic models used in the application:
- pulp_api: Models for Pulp API responses
- base, repository, context, artifacts, results, validation, statistics: Domain models
"""

# Pulp API Response Models
from .pulp_api import (
    PulpBaseModel,
    PaginatedResponse,
    TaskResponse,
    RepositoryResponse,
    DistributionResponse,
    ContentResponse,
    RpmPackageResponse,
    FileResponse,
    OAuthTokenResponse,
)

# Domain Models
from .base import KonfluxBaseModel
from .repository import RepositoryRefs
from .validation import RpmCheckResult
from .artifacts import ArtifactFile, PulledArtifacts, FileInfoModel
from .statistics import UploadCounts
from .results import ArtifactInfo, PulpResultsModel
from .context import UploadContext, UploadRpmContext, TransferContext, UploadFilesContext

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
    "TransferContext",
]
