"""Artifact-related models for Konflux Pulp."""

from typing import Optional, Dict, Any, List

from pydantic import Field

from .base import KonfluxBaseModel


class DownloadTask(KonfluxBaseModel):
    """
    Information needed to download a single artifact.

    Attributes:
        artifact_name: Name of the artifact file
        file_url: URL to download the artifact from
        arch: Architecture of the artifact (e.g., x86_64, noarch)
        artifact_type: Type of artifact (rpm, sbom, or log)
    """

    artifact_name: str
    file_url: str
    arch: str
    artifact_type: str

    def to_tuple(self) -> tuple:
        """Convert to tuple format (artifact_name, file_url, arch, artifact_type)."""
        return (self.artifact_name, self.file_url, self.arch, self.artifact_type)


class ArtifactFile(KonfluxBaseModel):
    """
    Represents a single downloaded artifact file.

    Attributes:
        file: Path to the downloaded file
        labels: Metadata labels associated with the artifact
    """

    file: str
    labels: Dict[str, str] = Field(default_factory=dict)

    @property
    def file_name(self) -> str:
        """Extract just the filename from the path."""
        import os  # pylint: disable=import-outside-toplevel

        return os.path.basename(self.file)

    @property
    def file_dir(self) -> str:
        """Extract the directory from the path."""
        import os  # pylint: disable=import-outside-toplevel

        return os.path.dirname(self.file)

    @property
    def build_id(self) -> Optional[str]:
        """Get build_id from labels if available."""
        return self.labels.get("build_id")  # pylint: disable=no-member

    @property
    def arch(self) -> Optional[str]:
        """Get architecture from labels if available."""
        return self.labels.get("arch")  # pylint: disable=no-member

    @property
    def namespace(self) -> Optional[str]:
        """Get namespace from labels if available."""
        return self.labels.get("namespace")  # pylint: disable=no-member

    @property
    def parent_package(self) -> Optional[str]:
        """Get parent_package from labels if available."""
        return self.labels.get("parent_package")  # pylint: disable=no-member


class PulledArtifacts(KonfluxBaseModel):
    """
    Collection of downloaded artifacts organized by type.

    Attributes:
        sboms: Dictionary of SBOM artifacts (name -> ArtifactFile)
        logs: Dictionary of log artifacts (name -> ArtifactFile)
        rpms: Dictionary of RPM artifacts (name -> ArtifactFile)
    """

    sboms: Dict[str, ArtifactFile] = Field(default_factory=dict)
    logs: Dict[str, ArtifactFile] = Field(default_factory=dict)
    rpms: Dict[str, ArtifactFile] = Field(default_factory=dict)

    @property
    def total_count(self) -> int:
        """Total number of artifacts across all types."""
        return len(self.sboms) + len(self.logs) + len(self.rpms)

    @property
    def sbom_count(self) -> int:
        """Number of SBOM artifacts."""
        return len(self.sboms)

    @property
    def log_count(self) -> int:
        """Number of log artifacts."""
        return len(self.logs)

    @property
    def rpm_count(self) -> int:
        """Number of RPM artifacts."""
        return len(self.rpms)

    def add_sbom(self, name: str, file: str, labels: Dict[str, str]) -> None:
        """Add a SBOM artifact."""
        self.sboms[name] = ArtifactFile(file=file, labels=labels)  # pylint: disable=unsupported-assignment-operation

    def add_log(self, name: str, file: str, labels: Dict[str, str]) -> None:
        """Add a log artifact."""
        self.logs[name] = ArtifactFile(file=file, labels=labels)  # pylint: disable=unsupported-assignment-operation

    def add_rpm(self, name: str, file: str, labels: Dict[str, str]) -> None:
        """Add an RPM artifact."""
        self.rpms[name] = ArtifactFile(file=file, labels=labels)  # pylint: disable=unsupported-assignment-operation

    def get_all_build_ids(self) -> set:
        """Get all unique build IDs from all artifacts."""
        build_ids = set()
        for artifacts in [self.sboms, self.logs, self.rpms]:
            for artifact in artifacts.values():  # pylint: disable=no-member
                if artifact.build_id:
                    build_ids.add(artifact.build_id)
        return build_ids

    def get_all_architectures(self) -> set:
        """Get all unique architectures from all artifacts."""
        architectures = set()
        for artifacts in [self.sboms, self.logs, self.rpms]:
            for artifact in artifacts.values():  # pylint: disable=no-member
                if artifact.arch:
                    architectures.add(artifact.arch)
        return architectures

    def get_all_namespaces(self) -> set:
        """Get all unique namespaces from all artifacts."""
        namespaces = set()
        for artifacts in [self.sboms, self.logs, self.rpms]:
            for artifact in artifacts.values():  # pylint: disable=no-member
                if artifact.namespace:
                    namespaces.add(artifact.namespace)
        return namespaces


class ArtifactMetadata(KonfluxBaseModel):
    """
    Metadata for a single artifact.

    Attributes:
        labels: Labels associated with the artifact (build_id, arch, namespace, etc.)
        url: URL where the artifact can be downloaded
        sha256: SHA256 checksum of the artifact
    """

    labels: Dict[str, str] = Field(default_factory=dict)
    url: Optional[str] = None
    sha256: Optional[str] = None

    @property
    def build_id(self) -> Optional[str]:
        """Get build ID from labels."""
        return self.labels.get("build_id")  # pylint: disable=no-member

    @property
    def arch(self) -> Optional[str]:
        """Get architecture from labels."""
        return self.labels.get("arch")  # pylint: disable=no-member

    @property
    def namespace(self) -> Optional[str]:
        """Get namespace from labels."""
        return self.labels.get("namespace")  # pylint: disable=no-member

    @property
    def parent_package(self) -> Optional[str]:
        """Get parent package from labels."""
        return self.labels.get("parent_package")  # pylint: disable=no-member


class ArtifactJsonResponse(KonfluxBaseModel):
    """
    Full artifact JSON response structure.

    Attributes:
        artifacts: Dictionary of artifact names to their metadata
        distributions: Dictionary of distribution type to URL mappings
    """

    artifacts: Dict[str, ArtifactMetadata] = Field(default_factory=dict)
    distributions: Dict[str, str] = Field(default_factory=dict)

    @property
    def artifact_count(self) -> int:
        """Total number of artifacts."""
        return len(self.artifacts)

    @property
    def has_distributions(self) -> bool:
        """Check if distributions are present."""
        return len(self.distributions) > 0

    @property
    def rpms_distribution_url(self) -> Optional[str]:
        """Get RPMs distribution URL."""
        return self.distributions.get("rpms")  # pylint: disable=no-member

    @property
    def logs_distribution_url(self) -> Optional[str]:
        """Get logs distribution URL."""
        return self.distributions.get("logs")  # pylint: disable=no-member

    @property
    def sbom_distribution_url(self) -> Optional[str]:
        """Get SBOM distribution URL."""
        return self.distributions.get("sbom")  # pylint: disable=no-member

    def get_artifact(self, name: str) -> Optional[ArtifactMetadata]:
        """Get artifact metadata by name."""
        return self.artifacts.get(name)  # pylint: disable=no-member


class ArtifactData(KonfluxBaseModel):
    """
    Loaded and validated artifact metadata.

    Attributes:
        artifact_json: Full JSON metadata from artifact location
        artifacts: Dictionary of individual artifacts with their metadata
    """

    artifact_json: ArtifactJsonResponse = Field(default_factory=ArtifactJsonResponse)
    artifacts: Dict[str, ArtifactMetadata] = Field(default_factory=dict)

    @property
    def artifact_count(self) -> int:
        """Total number of artifacts."""
        return len(self.artifacts)

    @property
    def has_distributions(self) -> bool:
        """Check if distributions are present in metadata."""
        return self.artifact_json.has_distributions  # pylint: disable=no-member

    def get_distributions(self) -> Dict[str, str]:
        """Get distribution URLs."""
        return self.artifact_json.distributions  # pylint: disable=no-member


class ContentData(KonfluxBaseModel):
    """
    Content data and artifacts gathered from Pulp.

    Attributes:
        content_results: List of content data from Pulp
        artifacts: List of artifact information dictionaries
    """

    content_results: List[Dict[str, Any]] = Field(default_factory=list)
    artifacts: List[Dict[str, str]] = Field(default_factory=list)

    @property
    def content_count(self) -> int:
        """Total number of content results."""
        return len(self.content_results)

    @property
    def artifact_count(self) -> int:
        """Total number of artifacts."""
        return len(self.artifacts)

    @property
    def is_empty(self) -> bool:
        """Check if content data is empty."""
        return len(self.content_results) == 0


class FileInfoModel(KonfluxBaseModel):
    """
    File location information from Pulp artifacts API.

    This model represents the file information returned by the Pulp API
    when querying artifact details.

    Attributes:
        pulp_href: Pulp API href for the artifact
        file: Download URL for the artifact file
        sha256: SHA256 checksum of the file
        size: File size in bytes
    """

    pulp_href: str
    file: str  # URL
    sha256: Optional[str] = None
    size: Optional[int] = None


__all__ = [
    "DownloadTask",
    "ArtifactFile",
    "PulledArtifacts",
    "ArtifactMetadata",
    "ArtifactJsonResponse",
    "ArtifactData",
    "ContentData",
    "FileInfoModel",
]
