"""Tests for pull workflow result models."""

from unittest.mock import Mock

from pulp_tool.models.artifacts import PulledArtifacts
from pulp_tool.models.pull_workflow import PullDestinationSetup, PullRunResult, SideTagUploadResult
from pulp_tool.models.repository import RepositoryRefs


def _sample_repos() -> RepositoryRefs:
    return RepositoryRefs(
        rpms_href="h1",
        rpms_prn="p1",
        logs_href="h2",
        logs_prn="p2",
        sbom_href="h3",
        sbom_prn="p3",
        artifacts_href="h4",
        artifacts_prn="p4",
    )


class TestPullWorkflowModels:
    def test_pull_run_result_success(self) -> None:
        result = PullRunResult(
            pulled_artifacts=PulledArtifacts(),
            completed=1,
            failed=0,
        )
        assert result.success

    def test_pull_destination_setup(self) -> None:
        setup = PullDestinationSetup(client=Mock(), repositories=_sample_repos())
        assert setup.repositories.rpms_href == "h1"

    def test_side_tag_upload_result_defaults(self) -> None:
        assert SideTagUploadResult().transfers == []
