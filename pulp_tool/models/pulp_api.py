"""
Pydantic models for Pulp API responses.

This module provides type-safe models for all Pulp API responses, enabling
better validation, IDE support, and error handling.
"""

from typing import Dict, List, Literal, Optional, Any
from pydantic import BaseModel, Field, ConfigDict, ValidationInfo, field_validator

# ============================================================================
# Base Models
# ============================================================================


class PulpBaseModel(BaseModel):
    """Base model for all Pulp API responses."""

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API


class PaginatedResponse(PulpBaseModel):
    """Base model for paginated Pulp API responses."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Dict[str, Any]]


class PulpRequestModel(BaseModel):

    model_config = ConfigDict(extra="ignore")


# ============================================================================
# Task Models
# ============================================================================


class TaskResult(PulpBaseModel):
    """Result details from a completed task."""

    relative_path: Optional[str] = None
    # Tasks can return various result structures, so we keep this flexible


class TaskResponse(PulpBaseModel):
    """Response from Pulp task endpoints."""

    pulp_href: str
    state: str  # waiting, running, completed, failed, canceled, skipped
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[Dict[str, Any]] = None
    progress_reports: Optional[List[Dict[str, Any]]] = None
    created_resources: List[str] = Field(default_factory=list)
    reserved_resources_record: Optional[List[str]] = None
    result: Optional[Any] = None
    parent_task: Optional[str] = None
    worker: Optional[str] = None
    logging_cid: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        """Check if the task has finished (success or failure)."""
        return self.state not in ["waiting", "running"]

    @property
    def is_successful(self) -> bool:
        """Check if the task completed successfully."""
        return self.state == "completed"

    @property
    def is_failed(self) -> bool:
        """Check if the task failed."""
        return self.state == "failed"


class TaskListResponse(PaginatedResponse):
    """Paginated list of tasks."""

    results: List[TaskResponse]  # type: ignore[assignment]


# ============================================================================
# Repository Models
# ============================================================================


class RepositoryResponse(PulpBaseModel):
    """Response for repository objects."""

    pulp_href: str
    prn: Optional[str] = None  # Pulp Resource Name
    name: str
    description: Optional[str] = None
    pulp_labels: Dict[str, str] = Field(default_factory=dict)
    versions_href: Optional[str] = None
    latest_version_href: Optional[str] = None


class RepositoryListResponse(PaginatedResponse):
    """Paginated list of repositories."""

    results: List[RepositoryResponse]  # type: ignore[assignment]


class RepositoryRequest(PulpRequestModel):
    """Request model for creating/updating repositories."""

    name: str
    pulp_labels: Optional[dict[str, str]] = None
    description: Optional[str] = None
    retain_repo_versions: Optional[str] = None
    remote: Optional[str] = None
    autopublish: Optional[bool] = None
    manifest: Optional[str] = None

    @field_validator("name", mode="after")
    @classmethod
    def is_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value.strip():
            raise ValueError(f"Invalid repository {info.field_name}: {value}")
        return value


class RpmRepositoryResponse(RepositoryResponse):
    """Response for RPM repository objects."""

    metadata_signing_service: Optional[str] = None
    package_signing_service: Optional[str] = None
    package_signing_fingerprint: Optional[str] = None
    retain_package_versions: Optional[int] = None
    checksum_type: Optional[str] = None
    repo_config: Optional[Dict[str, Any]] = None
    compression_type: Optional[str] = None
    layout: Optional[str] = None


class RpmRepositoryListResponse(PaginatedResponse):
    """Paginated list of RPM repositories."""

    results: List[RpmRepositoryResponse]  # type: ignore[assignment]


class FileRepositoryResponse(RepositoryResponse):
    """Response for file repository objects."""

    pass


class FileRepositoryRequest(RepositoryRequest):
    """Request model for creating/updating file repositories."""

    pass


class FileRepositoryListResponse(PaginatedResponse):
    """Paginated list of file repositories."""

    results: List[FileRepositoryResponse]  # type: ignore[assignment]


# ============================================================================
# Distribution Models
# ============================================================================


class DistributionResponse(PulpBaseModel):
    """Response for distribution objects."""

    pulp_href: str
    name: str
    base_path: str
    base_url: Optional[str] = None
    content_guard: Optional[str] = None
    publication: Optional[str] = None
    repository: Optional[str] = None
    pulp_labels: Dict[str, str] = Field(default_factory=dict)


class DistributionListResponse(PaginatedResponse):
    """Paginated list of distributions."""

    results: List[DistributionResponse]  # type: ignore[assignment]


class DistributionRequest(PulpRequestModel):
    base_path: str
    content_guard: Optional[str] = None
    hidden: Optional[bool] = None
    pulp_labels: Optional[dict[str, str]] = None
    name: str
    repository: Optional[str] = None
    publication: Optional[str] = None
    checkpoint: Optional[bool] = None

    @field_validator("base_path", "name", mode="after")
    @classmethod
    def is_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value or not value.strip():
            raise ValueError(f"Invalid distribution {info.field_name}: {value}")
        return value


# ============================================================================
# Content Models
# ============================================================================


class ArtifactRef(PulpBaseModel):
    """Reference to an artifact."""

    pulp_href: str = Field(alias="artifact")
    sha256: Optional[str] = None
    size: Optional[int] = None


class ContentResponse(PulpBaseModel):
    """Response for content objects."""

    pulp_href: str
    artifacts: Dict[str, str] = Field(default_factory=dict)  # filename -> artifact href
    pulp_labels: Dict[str, str] = Field(default_factory=dict)
    pulp_created: Optional[str] = None


class ContentListResponse(PaginatedResponse):
    """Paginated list of content."""

    results: List[ContentResponse]  # type: ignore[assignment]


# ============================================================================
# RPM-Specific Models
# ============================================================================


class RpmPackageResponse(PulpBaseModel):
    """Response for RPM package content."""

    pulp_href: str
    artifact: Optional[str] = None
    name: str
    epoch: str = "0"
    version: str
    release: str
    arch: str
    pkgId: str = Field(alias="sha256")
    location_href: Optional[str] = None
    pulp_labels: Dict[str, str] = Field(default_factory=dict)

    @property
    def nvra(self) -> str:
        """Get NVRA (Name-Version-Release.Arch) string."""
        return f"{self.name}-{self.version}-{self.release}.{self.arch}"

    @property
    def nevra(self) -> str:
        """Get NEVRA (Name-Epoch:Version-Release.Arch) string."""
        if self.epoch and self.epoch != "0":
            return f"{self.name}-{self.epoch}:{self.version}-{self.release}.{self.arch}"
        return self.nvra


class RpmListResponse(PaginatedResponse):
    """Paginated list of RPM packages."""

    results: List[RpmPackageResponse]  # type: ignore[assignment]


class RpmRepositoryRequest(RepositoryRequest):
    metadata_signing_service: Optional[str] = None
    package_signing_service: Optional[str] = None
    package_signing_fingerprint: Optional[str] = None
    retain_package_versions: Optional[int] = None
    checksum_type: Optional[Literal["unknown", "md5", "sha1", "sha224", "sha256", "sha384", "sha512"]] = None
    repo_config: Optional[Any] = None
    compression_type: Optional[Literal["zstd", "gz"]] = None
    layout: Optional[Literal["nested_alphabetically", "flat"]] = None


class RpmDistributionResponse(DistributionResponse):
    """Response for RPM distribution objects."""

    generate_repo_config: Optional[bool] = None


class RpmDistributionListResponse(PaginatedResponse):
    """Paginated list of RPM distributions."""

    results: List[RpmDistributionResponse]  # type: ignore[assignment]


class RpmDistributionRequest(DistributionRequest):
    """Request model for creating/updating RPM distributions."""

    generate_repo_config: Optional[bool] = None


class FileDistributionResponse(DistributionResponse):
    """Response for file distribution objects."""

    pass


class FileDistributionListResponse(PaginatedResponse):
    """Paginated list of file distributions."""

    results: List[FileDistributionResponse]  # type: ignore[assignment]


class FileDistributionRequest(DistributionRequest):
    """Request model for creating/updating file distributions."""

    pass


# ============================================================================
# File/Artifact Models
# ============================================================================


class FileResponse(PulpBaseModel):
    """Response for file content objects."""

    pulp_href: str
    artifact: str
    relative_path: str
    file: Optional[str] = None  # Download URL
    sha256: Optional[str] = None
    pulp_labels: Dict[str, str] = Field(default_factory=dict)


class FileListResponse(PaginatedResponse):
    """Paginated list of files."""

    results: List[FileResponse]  # type: ignore[assignment]


class ArtifactResponse(PulpBaseModel):
    """Response for artifact objects."""

    pulp_href: str
    file: str  # Path or URL
    size: int
    md5: Optional[str] = None
    sha1: Optional[str] = None
    sha224: Optional[str] = None
    sha256: Optional[str] = None
    sha384: Optional[str] = None
    sha512: Optional[str] = None


class ArtifactListResponse(PaginatedResponse):
    """Paginated list of artifacts."""

    results: List[ArtifactResponse]  # type: ignore[assignment]


# ============================================================================
# Upload Models
# ============================================================================


class UploadResponse(PulpBaseModel):
    """Response from upload operations."""

    pulp_href: str
    size: int = 0
    completed: Optional[str] = None


class UploadCommitResponse(PulpBaseModel):
    """Response from committing an upload."""

    task: str  # Task href for the commit operation


# ============================================================================
# Authentication Models
# ============================================================================


class OAuthTokenResponse(PulpBaseModel):
    """Response from OAuth token endpoint."""

    access_token: str
    expires_in: int
    token_type: str = "Bearer"
    refresh_token: Optional[str] = None
    scope: Optional[str] = None


# ============================================================================
# Domain Models
# ============================================================================


class DomainResponse(PulpBaseModel):
    """Response for domain objects."""

    pulp_href: str
    name: str
    description: Optional[str] = None
    storage_class: str = "pulpcore.app.models.storage.FileSystem"
    storage_settings: Dict[str, Any] = Field(default_factory=dict)
    redirect_to_object_storage: bool = True
    hide_guarded_distributions: bool = False


# ============================================================================
# Status/Health Models
# ============================================================================


class VersionInfo(PulpBaseModel):
    """Version information for a Pulp component."""

    component: str
    version: str


class StatusResponse(PulpBaseModel):
    """Response from status endpoint."""

    versions: List[VersionInfo]
    online_workers: List[Dict[str, Any]] = Field(default_factory=list)
    online_content_apps: List[Dict[str, Any]] = Field(default_factory=list)
    database_connection: Dict[str, bool]
    redis_connection: Optional[Dict[str, bool]] = None
    storage: Optional[Dict[str, Any]] = None


__all__ = [
    # Base models
    "PulpBaseModel",
    "PaginatedResponse",
    "PulpRequestModel",
    # Task models
    "TaskResult",
    "TaskResponse",
    "TaskListResponse",
    # Repository models
    "RepositoryRequest",
    "RepositoryResponse",
    "RepositoryListResponse",
    "RpmRepositoryRequest",
    "RpmRepositoryResponse",
    "RpmRepositoryListResponse",
    "FileRepositoryRequest",
    "FileRepositoryResponse",
    "FileRepositoryListResponse",
    # Distribution models
    "DistributionRequest",
    "DistributionResponse",
    "DistributionListResponse",
    "RpmDistributionRequest",
    "RpmDistributionResponse",
    "RpmDistributionListResponse",
    "FileDistributionRequest",
    "FileDistributionResponse",
    "FileDistributionListResponse",
    # Content models
    "ArtifactRef",
    "ContentResponse",
    "ContentListResponse",
    # RPM models
    "RpmPackageResponse",
    "RpmListResponse",
    # File models
    "FileResponse",
    "FileListResponse",
    "ArtifactResponse",
    "ArtifactListResponse",
    # Upload models
    "UploadResponse",
    "UploadCommitResponse",
    # Auth models
    "OAuthTokenResponse",
    # Domain models
    "DomainResponse",
    # Status models
    "VersionInfo",
    "StatusResponse",
]
