"""Tests for PulpHelper upload methods."""

from unittest.mock import patch

from pulp_tool.utils import PulpHelper
from pulp_tool.models.context import UploadContext
from pulp_tool.models.results import PulpResultsModel
from pulp_tool.models.repository import RepositoryRefs


class TestPulpHelperUploadMethods:
    """Test PulpHelper upload methods."""

    def test_process_architecture_uploads(self, mock_pulp_client):
        """Test process_architecture_uploads method (line 124)."""
        helper = PulpHelper(mock_pulp_client)

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        with patch.object(helper._upload_orchestrator, "process_architecture_uploads") as mock_process:
            mock_process.return_value = {"x86_64": {}}

            result = helper.process_architecture_uploads(
                mock_pulp_client,
                args,
                repositories,
                date_str="2024-01-01",
                rpm_href="/test/",
                results_model=results_model,
            )

            assert result == {"x86_64": {}}
            mock_process.assert_called_once()

    def test_process_uploads(self, mock_pulp_client):
        """Test process_uploads method (line 142)."""
        helper = PulpHelper(mock_pulp_client)

        args = UploadContext(
            build_id="test-build",
            date_str="2024-01-01 00:00:00",
            namespace="test-ns",
            parent_package="test-pkg",
            rpm_path="/test/rpms",
            sbom_path="/test/sbom.json",
        )

        repositories = RepositoryRefs(
            rpms_href="/test/",
            rpms_prn="",
            logs_href="",
            logs_prn="",
            sbom_href="",
            sbom_prn="",
            artifacts_href="",
            artifacts_prn="",
        )

        with patch.object(helper._upload_orchestrator, "process_uploads") as mock_process:
            mock_process.return_value = "https://example.com/results.json"

            result = helper.process_uploads(mock_pulp_client, args, repositories)

            assert result == "https://example.com/results.json"
            mock_process.assert_called_once_with(mock_pulp_client, args, repositories)
