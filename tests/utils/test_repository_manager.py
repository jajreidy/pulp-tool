"""Tests for RepositoryManager class."""

from unittest.mock import Mock, patch

import httpx
import pytest

from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.utils.repository_manager import RepositoryManager


class TestRepositoryManagerSetupRepositories:
    """Tests for RepositoryManager.setup_repositories() method."""

    def test_setup_repositories_invalid_after_sanitization(self):
        """Test setup_repositories raises ValueError when sanitized build_id is invalid (line 72)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock sanitize_build_id_for_repository to return something that will fail validation
        # Patch where it's imported in repository_manager module
        with (
            patch("pulp_tool.utils.repository_manager.sanitize_build_id_for_repository", return_value="invalid build"),
            patch("pulp_tool.utils.repository_manager.validate_build_id", return_value=False),
        ):
            with pytest.raises(ValueError) as exc_info:
                manager.setup_repositories("test-build")

            assert "Invalid build ID" in str(exc_info.value)
            assert "sanitized:" in str(exc_info.value)

    def test_setup_repositories_success(self):
        """Test setup_repositories successfully creates repositories."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        mock_repositories = {
            "rpms_prn": "test-rpms-prn",
            "rpms_href": "test-rpms-href",
            "logs_prn": "test-logs-prn",
            "sbom_prn": "test-sbom-prn",
            "artifacts_prn": "test-artifacts-prn",
        }

        with (
            patch.object(manager, "_setup_repositories_impl", return_value=mock_repositories),
            patch("pulp_tool.utils.repository_manager.validate_repository_setup", return_value=(True, [])),
        ):
            result = manager.setup_repositories("test-build")

            assert isinstance(result, RepositoryRefs)
            assert result.rpms_prn == "test-rpms-prn"
            assert result.rpms_href == "test-rpms-href"


class TestRepositoryManagerCreateOrGetRepository:
    """Tests for RepositoryManager.create_or_get_repository() method."""

    def test_create_or_get_repository_invalid_after_sanitization(self):
        """Test create_or_get_repository raises ValueError when sanitized build_id is invalid (line 127)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock sanitize_build_id_for_repository to return something that will fail validation
        # Patch where it's imported in repository_manager module
        with (
            patch("pulp_tool.utils.repository_manager.sanitize_build_id_for_repository", return_value="invalid build"),
            patch("pulp_tool.utils.repository_manager.validate_build_id", return_value=False),
        ):
            with pytest.raises(ValueError) as exc_info:
                manager.create_or_get_repository("test-build", "rpms")

            assert "Invalid build ID" in str(exc_info.value)
            assert "sanitized:" in str(exc_info.value)


class TestRepositoryManagerCreateNewRepository:
    """Tests for RepositoryManager._create_new_repository() method."""

    def test_create_new_repository_wrapped_results(self):
        """Test _create_new_repository with wrapped results (lines 204, 206-210)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"prn": "test-prn", "pulp_href": "test-href"}]}

        methods = {
            "create": Mock(return_value=mock_response),
        }

        prn, href = manager._create_new_repository(methods, "test-build/rpms", "rpms")

        assert prn == "test-prn"
        assert href == "test-href"

    def test_create_new_repository_wrapped_results_empty(self):
        """Test _create_new_repository with empty results list (lines 207-208)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.json.return_value = {"results": []}

        methods = {
            "create": Mock(return_value=mock_response),
        }

        with pytest.raises(ValueError) as exc_info:
            manager._create_new_repository(methods, "test-build/rpms", "rpms")

        assert "No rpms repository found after creation" in str(exc_info.value)

    def test_create_new_repository_unexpected_format(self):
        """Test _create_new_repository with unexpected response format (line 212)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.json.return_value = {"unexpected": "format"}

        methods = {
            "create": Mock(return_value=mock_response),
        }

        with pytest.raises(ValueError) as exc_info:
            manager._create_new_repository(methods, "test-build/rpms", "rpms")

        assert "Unexpected response format" in str(exc_info.value)


class TestRepositoryManagerWaitForDistributionTask:
    """Tests for RepositoryManager._wait_for_distribution_task() method."""

    def test_wait_for_distribution_task_exception_handling(self):
        """Test _wait_for_distribution_task exception handling (lines 256-257)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.session = Mock()
        mock_client.timeout = 30
        mock_client.request_params = {}

        manager = RepositoryManager(mock_client)

        mock_task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            created_resources=["/api/v3/distributions/rpm/123/"],
        )

        methods = {
            "wait_for_finished_task": Mock(return_value=mock_task_response),
        }

        # Mock session.get to raise HTTPError
        mock_client.session.get.side_effect = httpx.HTTPError("Connection error")

        with patch("pulp_tool.utils.repository_manager.logging") as mock_logging:
            base_path = manager._wait_for_distribution_task(methods, "task-123", "rpms", "test-build")

            # Should log warning and continue
            mock_logging.warning.assert_called()
            assert base_path is None


class TestRepositoryManagerSetupRepositoriesAsync:
    """Tests for RepositoryManager._setup_repositories_impl_async() method."""

    def test_setup_repositories_impl_async_success(self):
        """Test _setup_repositories_impl_async creates repositories (lines 276-301)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch.object(manager, "_create_or_get_repository_impl") as mock_create:
            mock_create.side_effect = [
                ("rpms-prn", "rpms-href"),
                ("logs-prn", None),
                ("sbom-prn", None),
                ("artifacts-prn", None),
            ]

            import asyncio

            result = asyncio.run(manager._setup_repositories_impl_async("test-build"))

            assert result["rpms_prn"] == "rpms-prn"
            assert result["rpms_href"] == "rpms-href"
            assert result["logs_prn"] == "logs-prn"
            assert result["sbom_prn"] == "sbom-prn"
            assert result["artifacts_prn"] == "artifacts-prn"
            # File repos don't have href
            assert "logs_href" not in result

    def test_setup_repositories_impl_async_http_error_403(self):
        """Test _setup_repositories_impl_async with 403 HTTP error (lines 303, 305-307)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch.object(manager, "_create_or_get_repository_impl", side_effect=httpx.HTTPError("403 Forbidden")):
            import asyncio

            with pytest.raises(httpx.HTTPError):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_async_http_error_401(self):
        """Test _setup_repositories_impl_async with 401 HTTP error (lines 311-312)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch.object(manager, "_create_or_get_repository_impl", side_effect=httpx.HTTPError("401 Unauthorized")):
            import asyncio

            with pytest.raises(httpx.HTTPError):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_async_generic_exception(self):
        """Test _setup_repositories_impl_async with generic exception (lines 319-322)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch.object(manager, "_create_or_get_repository_impl", side_effect=ValueError("Generic error")):
            import asyncio

            with pytest.raises(ValueError):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_calls_async(self):
        """Test _setup_repositories_impl calls async version (line 453)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        mock_repositories = {
            "rpms_prn": "test-rpms-prn",
            "rpms_href": "test-rpms-href",
            "logs_prn": "test-logs-prn",
            "sbom_prn": "test-sbom-prn",
            "artifacts_prn": "test-artifacts-prn",
        }

        with patch.object(manager, "_setup_repositories_impl_async", return_value=mock_repositories) as mock_async:
            result = manager._setup_repositories_impl("test-build")

            mock_async.assert_called_once_with("test-build")
            assert result == mock_repositories


class TestRepositoryManagerCheckExistingDistribution:
    """Tests for RepositoryManager._check_existing_distribution() method."""

    def test_check_existing_distribution_attribute_error(self):
        """Test _check_existing_distribution handles AttributeError (lines 380-381)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Create a mock response that doesn't have status_code attribute
        # This will raise AttributeError when accessed at line 368
        mock_response = Mock()
        del mock_response.status_code  # Remove status_code attribute
        mock_methods = {
            "get_distro": Mock(return_value=mock_response),
        }

        with patch("pulp_tool.utils.repository_manager.logging") as mock_logging:
            result = manager._check_existing_distribution(mock_methods, "test-build/rpms", "rpms")

            assert result is False
            # Check that the AttributeError was logged with the expected message
            debug_calls = [call[0][0] if call[0] else "" for call in mock_logging.debug.call_args_list]
            assert any("Distribution check method not available" in str(call) for call in debug_calls)
