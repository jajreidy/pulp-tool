"""
Transfer operations for downloading artifacts from Pulp.

This package provides functionality for downloading RPM packages, logs, and SBOM files
from Pulp repositories and organizing them by type and architecture. It supports
concurrent downloads, filtering by content type and architecture, and optional
re-upload to destination repositories.

Modules:
    - download: Download operations and artifact loading
    - upload: Upload operations for re-uploading artifacts
    - reporting: Transfer reporting and logging utilities
"""

from .download import (
    _categorize_artifacts,
    download_artifacts_concurrently,
    load_artifact_metadata,
    load_and_validate_artifacts,
    setup_repositories_if_needed,
)
from .upload import upload_downloaded_files_to_pulp
from .reporting import generate_transfer_report

__all__ = [
    "_categorize_artifacts",
    "download_artifacts_concurrently",
    "load_artifact_metadata",
    "load_and_validate_artifacts",
    "setup_repositories_if_needed",
    "upload_downloaded_files_to_pulp",
    "generate_transfer_report",
]
