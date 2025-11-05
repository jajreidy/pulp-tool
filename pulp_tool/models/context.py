"""Context and configuration models for Konflux Pulp operations."""

from typing import Optional, Dict, Callable, List

from pydantic import Field, ConfigDict, field_validator

from .base import KonfluxBaseModel


class UploadContext(KonfluxBaseModel):
    """
    Context information for upload operations.

    Attributes:
        build_id: Unique build identifier
        date_str: Build date string
        namespace: Namespace for the upload operation
        parent_package: Parent package name
        rpm_path: Path to RPM files
        sbom_path: Path to SBOM file
        config: Optional path to config file
        cert_config: Optional certificate configuration path
        debug: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=DEBUG with HTTP logs)
        artifact_results: Optional artifact results configuration
        sbom_results: Optional path to write SBOM results
    """

    build_id: str
    date_str: str
    namespace: str
    parent_package: str
    rpm_path: str
    sbom_path: str
    config: Optional[str] = None
    cert_config: Optional[str] = None
    debug: int = 0
    artifact_results: Optional[str] = None
    sbom_results: Optional[str] = None


class TransferContext(KonfluxBaseModel):
    """
    Context information for transfer operations.

    Attributes:
        artifact_location: Path or URL to artifact metadata (can be generated from namespace+build_id)
        namespace: Optional namespace for auto-generating artifact URL (requires build_id and config)
        cert_path: Optional path to SSL certificate (required for remote URLs)
        key_path: Optional path to SSL private key (required for remote URLs)
        config: Optional path to Pulp config file
        build_id: Optional build identifier (can be used for override or with namespace for URL generation)
        debug: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=DEBUG with HTTP logs)
        max_workers: Maximum number of concurrent workers
        content_types: Optional list of content types to filter (rpm, log, sbom)
        archs: Optional list of architectures to filter
    """

    artifact_location: Optional[str] = None
    namespace: Optional[str] = None
    cert_path: Optional[str] = None
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


__all__ = [
    "UploadContext",
    "TransferContext",
    "ArchUploadConfig",
    "UploadCallbacks",
]
