"""
Tests for PulpHelper class.

This module contains comprehensive tests for the PulpHelper class methods including
repository setup, distribution URL retrieval, and helper methods.
"""

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch
from httpx import HTTPError
from pulp_tool.models.pulp_api import RpmDistributionRequest, RpmRepositoryRequest
from pulp_tool.utils import PulpHelper
from pulp_tool.utils.repository_manager import RepositoryApiOps


class TestPulpHelperDistributionOperations:
    """Test PulpHelper distribution checking and creation."""

    def test_check_existing_distribution(self, mock_pulp_client) -> None:
        """Test PulpHelper _check_existing_distribution."""
        helper = PulpHelper(mock_pulp_client)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"name": "test-build/rpms", "base_path": "test-build/rpms"}]}
        methods = cast(RepositoryApiOps, SimpleNamespace(get_distro=Mock(return_value=mock_response)))
        result = helper._repository_manager._check_existing_distribution(methods, "test-build/rpms", "rpms")
        assert result is True

    def test_check_existing_distribution_not_found(self, mock_pulp_client) -> None:
        """Test PulpHelper _check_existing_distribution when not found."""
        helper = PulpHelper(mock_pulp_client)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        methods = cast(RepositoryApiOps, SimpleNamespace(get_distro=Mock(return_value=mock_response)))
        result = helper._repository_manager._check_existing_distribution(methods, "test-build/rpms", "rpms")
        assert result is False

    def test_check_existing_distribution_error(self, mock_pulp_client) -> None:
        """Test PulpHelper _check_existing_distribution with error."""
        helper = PulpHelper(mock_pulp_client)
        methods = cast(RepositoryApiOps, SimpleNamespace(get_distro=Mock(side_effect=HTTPError("API error"))))
        result = helper._repository_manager._check_existing_distribution(methods, "test-build/rpms", "rpms")
        assert result is False

    def test_check_existing_distribution_attribute_error(self, mock_pulp_client) -> None:
        """Test PulpHelper _check_existing_distribution with AttributeError."""
        helper = PulpHelper(mock_pulp_client)
        methods = cast(RepositoryApiOps, SimpleNamespace())
        result = helper._repository_manager._check_existing_distribution(methods, "test-build/rpms", "rpms")
        assert result is False

    def test_check_existing_distribution_value_error(self, mock_pulp_client) -> None:
        """Test PulpHelper _check_existing_distribution with ValueError."""
        helper = PulpHelper(mock_pulp_client)
        methods = cast(RepositoryApiOps, SimpleNamespace(get_distro=Mock(side_effect=ValueError("JSON error"))))
        result = helper._repository_manager._check_existing_distribution(methods, "test-build/rpms", "rpms")
        assert result is False

    def test_new_distribution_task(self, mock_pulp_client) -> None:
        """Test pulphelper _test_new_distribution_task"""
        helper = PulpHelper(mock_pulp_client)
        mock_distro_response = Mock()
        mock_distro_response.json.return_value = {"task": "/pulp/api/v3/tasks/12345/"}
        methods = cast(
            RepositoryApiOps,
            SimpleNamespace(
                distro=Mock(return_value=mock_distro_response),
                get_distro=Mock(return_value=Mock(json=lambda: {"results": []})),
            ),
        )
        mock_pulp_client.check_response = Mock()
        new_distro = RpmDistributionRequest(name="test-distro", base_path="test-distro", repository="test-repo")
        task_id = helper._repository_manager._new_distribution_task(methods, new_distro, "rpm")
        assert task_id == "/pulp/api/v3/tasks/12345/"

    def test_create_distribution_task(self, mock_pulp_client) -> None:
        """Test PulpHelper _create_distribution_task."""
        helper = PulpHelper(mock_pulp_client)
        methods = cast(RepositoryApiOps, SimpleNamespace())
        new_distro = RpmDistributionRequest(name="test-distro", base_path="test-distro", repository="test-repo")
        with (
            patch.object(helper._repository_manager, "_check_existing_distribution", return_value=False),
            patch.object(
                helper._repository_manager, "_new_distribution_task", return_value="/pulp/api/v3/tasks/12345/"
            ),
        ):
            task_id = helper._repository_manager._create_distribution_task(
                methods, new_distro, "rpms", build_id="test-build"
            )
        assert task_id == "/pulp/api/v3/tasks/12345/"

    def test_create_distribution_task_already_exists(self, mock_pulp_client) -> None:
        """Test PulpHelper _create_distribution_task when already exists."""
        helper = PulpHelper(mock_pulp_client)
        methods = cast(RepositoryApiOps, SimpleNamespace())
        new_distro = RpmDistributionRequest(name="test-distro", base_path="test-distro", repository="test-repo")
        with patch.object(helper._repository_manager, "_check_existing_distribution", return_value=True):
            task_id = helper._repository_manager._create_distribution_task(methods, new_distro, "rpms")
        assert task_id == ""

    def test_get_single_distribution_url(self, mock_pulp_client) -> None:
        """Test PulpHelper _get_single_distribution_url."""
        helper = PulpHelper(mock_pulp_client, "/path/to/cert-config.toml")
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"results": [{"base_path": "test-build/rpms"}]}
        mock_pulp_client.repository_operation = Mock(return_value=mock_response)
        url = helper._distribution_manager._get_single_distribution_url(
            "test-build", "rpms", "https://pulp.example.com/pulp-content/"
        )
        assert url == "https://pulp.example.com/pulp-content/test-domain/test-build/rpms/"

    def test_get_single_distribution_url_not_found(self, mock_pulp_client) -> None:
        """Test PulpHelper _get_single_distribution_url when not found.

        Even when distribution is not found in API, we compute and return the expected URL.
        """
        helper = PulpHelper(mock_pulp_client, "/path/to/cert-config.toml")
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"results": []}
        url = helper._distribution_manager._get_single_distribution_url(
            "test-build", "rpms", "https://pulp.example.com/pulp-content/"
        )
        assert url == "https://pulp.example.com/pulp-content/test-domain/test-build/rpms/"

    def test_get_single_distribution_url_error(self, mock_pulp_client) -> None:
        """Test PulpHelper _get_single_distribution_url with error.

        Even with API errors, we compute and return the expected URL.
        """
        helper = PulpHelper(mock_pulp_client, "/path/to/cert-config.toml")
        mock_response = Mock()
        mock_response.is_success = False
        mock_response.status_code = 404
        mock_response.text = "Not found"
        url = helper._distribution_manager._get_single_distribution_url(
            "test-build", "rpms", "https://pulp.example.com/pulp-content/"
        )
        assert url == "https://pulp.example.com/pulp-content/test-domain/test-build/rpms/"

    def test_get_single_distribution_url_exception(self, mock_pulp_client) -> None:
        """Test PulpHelper _get_single_distribution_url with exception.

        Even when exceptions occur, we compute and return the expected URL.
        """
        helper = PulpHelper(mock_pulp_client, "/path/to/cert-config.toml")
        url = helper._distribution_manager._get_single_distribution_url(
            "test-build", "rpms", "https://pulp.example.com/pulp-content/"
        )
        assert url == "https://pulp.example.com/pulp-content/test-domain/test-build/rpms/"

    def test_get_distribution_urls_impl(self, mock_pulp_client) -> None:
        """Test PulpHelper _get_distribution_urls_impl."""
        helper = PulpHelper(mock_pulp_client)
        with patch.object(helper._distribution_manager, "_get_single_distribution_url") as mock_get_url:
            mock_get_url.side_effect = (
                lambda build_id, repo_type, base_url: f"{base_url}{helper.namespace}/{build_id}/{repo_type}/"
            )
            result = helper._distribution_manager._get_distribution_urls_impl("test-build")
        assert len(result) == 4
        assert "rpms" in result
        assert "logs" in result
        assert "sbom" in result
        assert "artifacts" in result
        assert result["rpms"] == "https://pulp.example.com/api/pulp-content/test-domain/test-build/rpms/"


class TestPulpHelperRepositoryImplementation:
    """Test PulpHelper repository implementation methods."""

    def test_create_or_get_repository_impl_new(self, mock_pulp_client) -> None:
        """Test PulpHelper _create_or_get_repository_impl with new repository."""
        helper = PulpHelper(mock_pulp_client)
        with (
            patch.object(helper._repository_manager, "get_repository_methods") as mock_get_methods,
            patch.object(helper._repository_manager, "_get_existing_repository", return_value=None),
            patch.object(helper._repository_manager, "_create_new_repository", return_value=("test-prn", "test-href")),
            patch.object(helper._repository_manager, "_create_distribution_task", return_value="task-123"),
            patch.object(helper._repository_manager, "_wait_for_distribution_task"),
        ):
            mock_get_methods.return_value = {}
            new_repo_def = RpmRepositoryRequest(name="test-repo")
            new_distro_def = RpmDistributionRequest(name="test-repo", base_path="test-repo")
            prn, href = helper._repository_manager._create_or_get_repository_impl(
                new_repo_def, new_distro_def, "rpms", "test-build"
            )
        assert new_distro_def.repository == "test-prn"
        assert prn == "test-prn"
        assert href == "test-href"

    def test_create_or_get_repository_impl_existing(self, mock_pulp_client) -> None:
        """Test PulpHelper _create_or_get_repository_impl with existing repository."""
        helper = PulpHelper(mock_pulp_client)
        with (
            patch.object(helper._repository_manager, "get_repository_methods") as mock_get_methods,
            patch.object(
                helper._repository_manager, "_get_existing_repository", return_value=("test-prn", "test-href")
            ),
            patch.object(helper._repository_manager, "_create_distribution_task", return_value="task-123"),
            patch.object(helper._repository_manager, "_wait_for_distribution_task"),
        ):
            mock_get_methods.return_value = {}
            new_repo_def = RpmRepositoryRequest(name="test-repo")
            new_distro_def = RpmDistributionRequest(name="test-repo", base_path="test-repo")
            prn, href = helper._repository_manager._create_or_get_repository_impl(
                new_repo_def, new_distro_def, "file", "test-build"
            )
        assert new_distro_def.repository == "test-prn"
        assert prn == "test-prn"
        assert href == "test-href"

    def test_create_or_get_repository_impl_no_task(self, mock_pulp_client) -> None:
        """Test PulpHelper _create_or_get_repository_impl with no distribution task."""
        helper = PulpHelper(mock_pulp_client)
        with (
            patch.object(helper._repository_manager, "get_repository_methods") as mock_get_methods,
            patch.object(
                helper._repository_manager, "_get_existing_repository", return_value=("test-prn", "test-href")
            ),
            patch.object(helper._repository_manager, "_create_distribution_task", return_value=""),
        ):
            mock_get_methods.return_value = {}
            mock_get_methods.return_value = {}
            new_repo_def = RpmRepositoryRequest(name="test-repo")
            new_distro_def = RpmDistributionRequest(name="test-repo", base_path="test-repo")
            prn, href = helper._repository_manager._create_or_get_repository_impl(
                new_repo_def, new_distro_def, "rpms", "test-build"
            )
        assert prn == "test-prn"
        assert href == "test-href"
