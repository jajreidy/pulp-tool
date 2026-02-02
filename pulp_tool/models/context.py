"""Context and configuration models for Konflux Pulp operations."""

from typing import Optional, Dict, Callable, List

from pydantic import Field, ConfigDict, field_validator

from .base import KonfluxBaseModel


class UploadContext(KonfluxBaseModel):
    """
    Base context information for upload operations.

    This is the base class containing common attributes shared by
    UploadRpmContext and UploadFilesContext.

    Attributes:
        build_id: Unique build identifier
        date_str: Build date string
        namespace: Namespace for the upload operation
        parent_package: Parent package name
        config: Optional path to config file
        debug: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=DEBUG with HTTP logs)
        artifact_results: Optional artifact results configuration
        sbom_results: Optional path to write SBOM results
    """

    build_id: str
    date_str: str
    namespace: str
    parent_package: str
    config: Optional[str] = None
    debug: int = 0
    artifact_results: Optional[str] = None
    sbom_results: Optional[str] = None


class UploadRpmContext(UploadContext):
    """
    Context information for upload operations (RPM directory-based).

    This context is used for the upload command which processes RPMs
    from directory structures organized by architecture.

    Attributes:
        rpm_path: Path to directory containing RPM files
        sbom_path: Path to SBOM file
    """

    rpm_path: str
    sbom_path: str


class TransferContext(KonfluxBaseModel):
    """
    Context information for transfer operations.

    Attributes:
        artifact_location: Path or URL to artifact metadata (can be generated from namespace+build_id)
        namespace: Optional namespace for auto-generating artifact URL (requires build_id and config)
        key_path: Optional path to SSL private key (required for remote URLs, can come from config)
        config: Optional path to Pulp config file
        build_id: Optional build identifier (can be used for override or with namespace for URL generation)
        debug: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=DEBUG with HTTP logs)
        max_workers: Maximum number of concurrent workers
        content_types: Optional list of content types to filter (rpm, log, sbom)
        archs: Optional list of architectures to filter
    """

    artifact_location: Optional[str] = None
    namespace: Optional[str] = None
    key_path: Optional[str] = None
    config: Optional[str] = None
    build_id: Optional[str] = None
    debug: int = 0
    max_workers: int = Field(default=10, ge=1, le=100)
    content_types: Optional[List[str]] = None
    archs: Optional[List[str]] = None

    @field_validator("content_types")
    @classmethod
    def validate_content_types(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate that content_types only contains valid values."""
        if v is None:
            return v

        valid_types = {"rpm", "log", "sbom"}
        invalid_types = [ct for ct in v if ct not in valid_types]

        if invalid_types:
            raise ValueError(
                f"Invalid content type(s): {', '.join(invalid_types)}. "
                f"Valid types are: {', '.join(sorted(valid_types))}"
            )

        return v


class ArchUploadConfig(KonfluxBaseModel):
    """
    Configuration for uploading architecture-specific content.

    Attributes:
        rpm_path: Path to RPM files
        arch: Architecture name (e.g., 'x86_64', 'noarch')
        rpm_repository_href: Repository href for RPMs
        file_repository_prn: PRN for file repository (logs)
        build_id: Build identifier
        date_str: Build date string
        labels: Dictionary of labels to apply
    """

    rpm_path: str
    arch: str
    rpm_repository_href: str
    file_repository_prn: str
    build_id: str
    date_str: str
    labels: Dict[str, str]


class UploadCallbacks(KonfluxBaseModel):
    """
    Callback functions for upload operations.

    Attributes:
        upload_sbom_func: Function to upload SBOM
        collect_results_func: Function to collect and save results
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    upload_sbom_func: Callable
    collect_results_func: Callable


class UploadFilesContext(UploadContext):
    """
    Context information for upload-files operations.

    This context is used for the upload-files command which processes
    individual files specified via command-line options.

    Attributes:
        rpm_files: List of RPM file paths to upload
        file_files: List of generic file paths to upload
        log_files: List of log file paths to upload
        sbom_files: List of SBOM file paths to upload
        arch: Optional architecture for RPMs (if not provided, will try to detect)
    """

    rpm_files: List[str] = Field(default_factory=list)
    file_files: List[str] = Field(default_factory=list)
    log_files: List[str] = Field(default_factory=list)
    sbom_files: List[str] = Field(default_factory=list)
    arch: Optional[str] = None


__all__ = [
    "UploadContext",
    "UploadRpmContext",
    "TransferContext",
    "ArchUploadConfig",
    "UploadCallbacks",
    "UploadFilesContext",
]
