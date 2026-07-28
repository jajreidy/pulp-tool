"""
Pydantic models for Pulp API responses.

This module provides type-safe models for all Pulp API responses, enabling
better validation, IDE support, and error handling.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

# ============================================================================
# Base Models
# ============================================================================


class PulpBaseModel(BaseModel):
    """Base model for all Pulp API responses."""

    model_config = ConfigDict(extra="allow")  # Allow extra fields from API


class PaginatedResponse(PulpBaseModel):
    """Base model for paginated Pulp API responses."""

    count: int
    next: str | None = None
    previous: str | None = None
    results: list[dict[str, Any]]


class PulpRequestModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


# ============================================================================
# Task Models
# ============================================================================


class TaskResult(PulpBaseModel):
    """Result details from a completed task."""

    relative_path: str | None = None
    # Tasks can return various result structures, so we keep this flexible


class TaskResponse(PulpBaseModel):
    """Response from Pulp task endpoints."""

    pulp_href: str
    state: str  # waiting, running, completed, failed, canceled, skipped
    started_at: str | None = None
    finished_at: str | None = None
    error: dict[str, Any] | None = None
    progress_reports: list[dict[str, Any]] | None = None
    created_resources: list[str] = Field(default_factory=list)
    reserved_resources_record: list[str] | None = None
    result: Any | None = None
    parent_task: str | None = None
    worker: str | None = None
    logging_cid: str | None = None

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

    results: list[TaskResponse]  # type: ignore[assignment]


# ============================================================================
# Repository Models
# ============================================================================


class RepositoryResponse(PulpBaseModel):
    """Response for repository objects."""

    pulp_href: str
    prn: str | None = None  # Pulp Resource Name
    name: str
    description: str | None = None
    pulp_labels: dict[str, str] = Field(default_factory=dict)
    versions_href: str | None = None
    latest_version_href: str | None = None


class RepositoryListResponse(PaginatedResponse):
    """Paginated list of repositories."""

    results: list[RepositoryResponse]  # type: ignore[assignment]


class RepositoryRequest(PulpRequestModel):
    """Request model for creating/updating repositories."""

    name: str
    pulp_labels: dict[str, str] | None = None
    description: str | None = None
    retain_repo_versions: str | None = None
    remote: str | None = None
    autopublish: bool | None = None
    manifest: str | None = None

    @field_validator("name", mode="after")
    @classmethod
    def is_empty(cls, value: str, info: ValidationInfo) -> str:
        if not value.strip():
            raise ValueError(f"Invalid repository {info.field_name}: {value}")
        return value


class RpmRepositoryResponse(RepositoryResponse):
    """Response for RPM repository objects."""

    metadata_signing_service: str | None = None
    package_signing_service: str | None = None
    package_signing_fingerprint: str | None = None
    retain_package_versions: int | None = None
    checksum_type: str | None = None
    repo_config: dict[str, Any] | None = None
    compression_type: str | None = None
    layout: str | None = None


class RpmRepositoryListResponse(PaginatedResponse):
    """Paginated list of RPM repositories."""

    results: list[RpmRepositoryResponse]  # type: ignore[assignment]


class FileRepositoryResponse(RepositoryResponse):
    """Response for file repository objects."""

    pass


class FileRepositoryRequest(RepositoryRequest):
    """Request model for creating/updating file repositories."""

    pass


class FileRepositoryListResponse(PaginatedResponse):
    """Paginated list of file repositories."""

    results: list[FileRepositoryResponse]  # type: ignore[assignment]


# ============================================================================
# Distribution Models
# ============================================================================


class DistributionResponse(PulpBaseModel):
    """Response for distribution objects."""

    pulp_href: str
    name: str
    base_path: str
    base_url: str | None = None
    content_guard: str | None = None
    publication: str | None = None
    repository: str | None = None
    pulp_labels: dict[str, str] = Field(default_factory=dict)


class DistributionListResponse(PaginatedResponse):
    """Paginated list of distributions."""

    results: list[DistributionResponse]  # type: ignore[assignment]


class DistributionRequest(PulpRequestModel):
    base_path: str
    content_guard: str | None = None
    hidden: bool | None = None
    pulp_labels: dict[str, str] | None = None
    name: str
    repository: str | None = None
    publication: str | None = None
    checkpoint: bool | None = None

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
    sha256: str | None = None
    size: int | None = None


class ContentResponse(PulpBaseModel):
    """Response for content objects."""

    pulp_href: str
    artifacts: dict[str, str] = Field(default_factory=dict)  # filename -> artifact href
    pulp_labels: dict[str, str] = Field(default_factory=dict)
    pulp_created: str | None = None


class ContentListResponse(PaginatedResponse):
    """Paginated list of content."""

    results: list[ContentResponse]  # type: ignore[assignment]


# ============================================================================
# RPM-Specific Models
# ============================================================================


class RpmPackageResponse(PulpBaseModel):
    """Response for RPM package content."""

    pulp_href: str
    artifact: str | None = None
    name: str
    epoch: str = "0"
    version: str
    release: str
    arch: str
    pkgId: str = Field(alias="sha256")
    location_href: str | None = None
    pulp_labels: dict[str, str] = Field(default_factory=dict)


class RpmListResponse(PaginatedResponse):
    """Paginated list of RPM packages."""

    results: list[RpmPackageResponse]  # type: ignore[assignment]


class RpmRepositoryRequest(RepositoryRequest):
    metadata_signing_service: str | None = None
    package_signing_service: str | None = None
    package_signing_fingerprint: str | None = None
    retain_package_versions: int | None = None
    checksum_type: Literal["unknown", "md5", "sha1", "sha224", "sha256", "sha384", "sha512"] | None = None
    repo_config: Any | None = None
    compression_type: Literal["zstd", "gz"] | None = None
    layout: Literal["nested_alphabetically", "flat"] | None = None


class RpmDistributionResponse(DistributionResponse):
    """Response for RPM distribution objects."""

    generate_repo_config: bool | None = None


class RpmDistributionListResponse(PaginatedResponse):
    """Paginated list of RPM distributions."""

    results: list[RpmDistributionResponse]  # type: ignore[assignment]


class RpmDistributionRequest(DistributionRequest):
    """Request model for creating/updating RPM distributions."""

    generate_repo_config: bool | None = None


class FileDistributionResponse(DistributionResponse):
    """Response for file distribution objects."""

    pass


class FileDistributionListResponse(PaginatedResponse):
    """Paginated list of file distributions."""

    results: list[FileDistributionResponse]  # type: ignore[assignment]


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
    file: str | None = None  # Download URL
    sha256: str | None = None
    pulp_labels: dict[str, str] = Field(default_factory=dict)


class FileListResponse(PaginatedResponse):
    """Paginated list of files."""

    results: list[FileResponse]  # type: ignore[assignment]


class ArtifactResponse(PulpBaseModel):
    """Response for artifact objects."""

    pulp_href: str
    file: str  # Path or URL
    size: int
    md5: str | None = None
    sha1: str | None = None
    sha224: str | None = None
    sha256: str | None = None
    sha384: str | None = None
    sha512: str | None = None


class ArtifactListResponse(PaginatedResponse):
    """Paginated list of artifacts."""

    results: list[ArtifactResponse]  # type: ignore[assignment]


# ============================================================================
# Upload Models
# ============================================================================


class UploadResponse(PulpBaseModel):
    """Response from upload operations."""

    pulp_href: str
    size: int = 0
    completed: str | None = None


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
    token_type: str = "Bearer"  # noqa: S105
    refresh_token: str | None = None
    scope: str | None = None


# ============================================================================
# Domain Models
# ============================================================================


class DomainResponse(PulpBaseModel):
    """Response for domain objects."""

    pulp_href: str
    name: str
    description: str | None = None
    storage_class: str = "pulpcore.app.models.storage.FileSystem"
    storage_settings: dict[str, Any] = Field(default_factory=dict)
    redirect_to_object_storage: bool = True
    hide_guarded_distributions: bool = False


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
]
