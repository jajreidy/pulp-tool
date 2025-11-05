"""Repository-related models for Konflux Pulp."""

from typing import Optional

from .base import KonfluxBaseModel


class RepositoryRefs(KonfluxBaseModel):
    """
    Repository references for all content types.

    Attributes:
        rpms_href: Pulp href for RPM repository
        rpms_prn: Pulp Resource Name for RPM repository
        logs_href: Pulp href for logs repository
        logs_prn: Pulp Resource Name for logs repository
        sbom_href: Pulp href for SBOM repository
        sbom_prn: Pulp Resource Name for SBOM repository
        artifacts_href: Pulp href for artifacts repository
        artifacts_prn: Pulp Resource Name for artifacts repository
    """

    rpms_href: str
    rpms_prn: str
    logs_href: str
    logs_prn: str
    sbom_href: str
    sbom_prn: str
    artifacts_href: str
    artifacts_prn: str


class RepositoryInfo(KonfluxBaseModel):
    """
    Information about a created or retrieved repository.

    Attributes:
        href: Pulp href for the repository
        prn: Pulp Resource Name (optional, for file repositories)
    """

    href: str
    prn: Optional[str] = None


__all__ = ["RepositoryRefs", "RepositoryInfo"]
