"""Structured results from pull / transfer workflow steps."""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict, Field

from .artifacts import PulledArtifacts
from .base import KonfluxBaseModel
from .pulp_results import PulpResultsDocument, SideTagRpmTransfer
from .repository import RepositoryRefs


class PullDestinationSetup(KonfluxBaseModel):
    """Destination Pulp client and repositories created for a pull transfer."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, extra="forbid")

    client: Any
    repositories: RepositoryRefs


class SideTagUploadResult(KonfluxBaseModel):
    """RPMs promoted to a side-tag repository and its distribution base URL."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    transfers: list[SideTagRpmTransfer] = Field(default_factory=list)
    distribution_base_url: str = ""


class PullRunResult(KonfluxBaseModel):
    """Outcome of a full pull run (download, optional transfer upload, side-tag)."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, extra="forbid")

    pulled_artifacts: PulledArtifacts
    completed: int = Field(ge=0)
    failed: int = Field(ge=0)
    upload_info: PulpResultsDocument | None = None
    error_messages: list[str] = Field(default_factory=list)
    pulp_client: Any | None = None
    distribution_client: Any | None = None

    @property
    def success(self) -> bool:
        return not self.error_messages


__all__ = ["PullDestinationSetup", "PullRunResult", "SideTagUploadResult"]
