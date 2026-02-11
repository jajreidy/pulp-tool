"""Tests for RepositoryManager class."""

from unittest.mock import Mock, patch

import httpx
import pytest

from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.models.pulp_api import RpmRepositoryRequest, TaskResponse
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

    def test_create_or_get_repository_empty_build_name(self):
        """Test create_or_get_repository raises ValueError when build_name is empty (line 160)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value=""):
            with pytest.raises(ValueError, match="Empty build_name"):
                manager.create_or_get_repository("test-build", "rpms")

    def test_create_or_get_repository_invalid_full_name_empty(self):
        """Test create_or_get_repository raises ValueError when full_name is empty (line 163)."""
        from pulp_tool.utils.repository_manager import RepositoryManager
        from pulp_tool.api import PulpClient

        mock_client = Mock(spec=PulpClient)
        mock_client.namespace = "test-namespace"
        manager = RepositoryManager(mock_client)

        # Mock strip_namespace_from_build_id to return empty string
        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value=""):
            with pytest.raises(ValueError, match="Empty build_name"):
                manager.create_or_get_repository("test-build", "rpms")

    def test_create_or_get_repository_invalid_full_name_whitespace(self):
        """Test create_or_get_repository raises ValueError when build_name is whitespace (line 160)."""
        from pulp_tool.utils.repository_manager import RepositoryManager
        from pulp_tool.api import PulpClient

        mock_client = Mock(spec=PulpClient)
        mock_client.namespace = "test-namespace"
        manager = RepositoryManager(mock_client)

        # Mock strip_namespace_from_build_id to return whitespace
        # The code now checks build_name.strip(), so whitespace should trigger ValueError
        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="   "):
            with pytest.raises(ValueError, match="Empty build_name"):
                manager.create_or_get_repository("test-build", "rpms")

    def test_create_or_get_repository_invalid_full_name(self):
        """Test create_or_get_repository raises ValueError when full_name is invalid."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        # Mock client methods needed for the method
        mock_client.repository_operation = Mock(return_value=Mock(status_code=404))
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        # Test _validate_full_name directly with an empty string to ensure coverage
        # This tests the defensive check that was previously at line 163
        with pytest.raises(ValueError, match="Invalid full_name"):
            manager._validate_full_name("", "test", "rpms")

        # Also test with whitespace-only string
        with pytest.raises(ValueError, match="Invalid full_name"):
            manager._validate_full_name("   ", "test", "rpms")

        # Now test the full method path by patching _validate_full_name to raise
        def mock_validate_full_name(full_name, build_name, repo_type):
            """Mock that raises ValueError to test the error path."""
            raise ValueError(f"Invalid full_name constructed: build_name={build_name}, repo_type={repo_type}")

        with (
            patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="test"),
            patch.object(manager, "_validate_full_name", side_effect=mock_validate_full_name),
        ):
            with pytest.raises(ValueError, match="Invalid full_name"):
                manager.create_or_get_repository("test-build", "rpms")

    def test_create_or_get_repository_empty_base_path(self):
        """Test create_or_get_repository raises ValueError when base_path is empty (line 170)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock DistributionRequest to have empty base_path
        with (
            patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="test-build"),
            patch("pulp_tool.utils.repository_manager.DistributionRequest") as mock_dist_req,
        ):
            mock_dist = Mock()
            mock_dist.base_path = ""
            mock_dist_req.return_value = mock_dist

            with pytest.raises(ValueError, match="base_path is empty"):
                manager.create_or_get_repository("test-build", "rpms")

    def test_get_existing_repository_404(self):
        """Test _get_existing_repository handles 404 gracefully (line 230)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.status_code = 404

        methods = {"get": Mock(return_value=mock_response)}

        with patch("pulp_tool.utils.repository_manager.logging") as mock_logging:
            result = manager._get_existing_repository(methods, "test-repo", "rpms")
            assert result is None
            mock_logging.debug.assert_called()

    def test_setup_repositories_impl_async_empty_build_name(self):
        """Test _setup_repositories_impl_async raises ValueError when build_name is empty (line 344)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value=""):
            import asyncio

            async def run_test():
                with pytest.raises(ValueError, match="Empty build_name"):
                    await manager._setup_repositories_impl_async("test-build")

            asyncio.run(run_test())

    def test_setup_repositories_impl_async_invalid_full_name_empty(self):
        """Test _setup_repositories_impl_async raises ValueError when full_name is empty (line 347)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock strip_namespace_from_build_id to return empty string
        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value=""):
            import asyncio

            with pytest.raises(ValueError, match="Empty build_name"):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_async_invalid_full_name_whitespace(self):
        """Test _setup_repositories_impl_async raises ValueError when build_name is whitespace (line 344)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock strip_namespace_from_build_id to return whitespace
        # The code now checks build_name.strip(), so whitespace should trigger ValueError
        with patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="   "):
            import asyncio

            with pytest.raises(ValueError, match="Empty build_name"):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_async_invalid_full_name(self):
        """Test _setup_repositories_impl_async raises ValueError when full_name is invalid."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        # Mock client methods to prevent crashes
        mock_client.repository_operation = Mock(return_value=Mock(status_code=404))
        mock_client.check_response = Mock()
        mock_client.wait_for_finished_task = Mock()

        manager = RepositoryManager(mock_client)

        # Test _validate_full_name directly with an empty string to ensure coverage
        # This tests the defensive check that was previously at line 347
        with pytest.raises(ValueError, match="Invalid full_name"):
            manager._validate_full_name("", "test", "rpms")

        # Also test with whitespace-only string
        with pytest.raises(ValueError, match="Invalid full_name"):
            manager._validate_full_name("   ", "test", "rpms")

        # Now test the full async method path by patching _validate_full_name to raise
        def mock_validate_full_name(full_name, build_name, repo_type):
            """Mock that raises ValueError to test the error path."""
            raise ValueError(f"Invalid full_name constructed: build_name={build_name}, repo_type={repo_type}")

        with (
            patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="test"),
            patch.object(manager, "_validate_full_name", side_effect=mock_validate_full_name),
        ):
            import asyncio

            # This should trigger ValueError when full_name validation fails
            with pytest.raises(ValueError, match="Invalid full_name"):
                asyncio.run(manager._setup_repositories_impl_async("test-build"))

    def test_setup_repositories_impl_async_empty_base_path(self):
        """Test _setup_repositories_impl_async raises ValueError when base_path is empty (line 352)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        # Mock DistributionRequest to have empty base_path
        with (
            patch("pulp_tool.utils.repository_manager.strip_namespace_from_build_id", return_value="test-build"),
            patch("pulp_tool.utils.repository_manager.DistributionRequest") as mock_dist_req,
        ):
            mock_dist = Mock()
            mock_dist.base_path = ""
            mock_dist_req.return_value = mock_dist

            import asyncio

            async def run_test():
                with pytest.raises(ValueError, match="base_path is empty"):
                    await manager._setup_repositories_impl_async("test-build")

            asyncio.run(run_test())

    def test_create_or_get_repository_impl_empty_base_path_after_repo_set(self):
        """Test _create_or_get_repository_impl raises ValueError when base_path is empty."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        from pulp_tool.models.pulp_api import RepositoryRequest, DistributionRequest

        new_repo = RepositoryRequest(name="test-repo", autopublish=True)
        new_distro = DistributionRequest(name="test-repo", base_path="test-repo")
        new_distro.repository = "test-prn"
        # Set base_path to empty after setting repository
        new_distro.base_path = ""

        # Mock _parse_repository_response to return proper structure
        manager._parse_repository_response = Mock(return_value={"prn": "test-prn", "pulp_href": "/repo/123/"})
        manager._create_distribution_task = Mock(return_value=None)
        manager._wait_for_distribution_task = Mock(return_value="test-repo")

        with pytest.raises(ValueError, match="base_path is empty before creating"):
            manager._create_or_get_repository_impl(new_repo, new_distro, "rpms")

    def test_check_existing_distribution_404(self):
        """Test _check_existing_distribution handles 404 gracefully (line 465)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.status_code = 404

        methods = {"get_distro": Mock(return_value=mock_response)}

        with patch("pulp_tool.utils.repository_manager.logging") as mock_logging:
            result = manager._check_existing_distribution(methods, "test-distro", "rpms")
            assert result is False
            mock_logging.debug.assert_called()

    def test_create_distribution_task_empty_base_path(self):
        """Test _create_distribution_task raises ValueError when base_path is empty (line 535)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"

        manager = RepositoryManager(mock_client)

        from pulp_tool.models.pulp_api import DistributionRequest

        # Create a DistributionRequest with empty base_path
        # Note: DistributionRequest validates base_path, so we need to bypass validation
        # by creating it with a valid base_path first, then modifying it
        new_distro = DistributionRequest(name="test-distro", base_path="test-distro")
        # Use object.__setattr__ to bypass Pydantic validation
        object.__setattr__(new_distro, "base_path", "")

        methods = {"create_distro": Mock()}

        with (
            patch("pulp_tool.utils.repository_manager.logging") as mock_logging,
            pytest.raises(ValueError, match="Invalid base_path"),
        ):
            manager._create_distribution_task(methods, new_distro, "rpms", True, "test-build")
            mock_logging.error.assert_called()


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

        new_repo = RpmRepositoryRequest(name="test-build/rpms")

        prn, href = manager._create_new_repository(methods, new_repo, "rpms")

        assert prn == "test-prn"
        assert href == "test-href"

    def test_create_new_repository_wrapped_results_file_api(self):
        """Test _create_new_repository with wrapped results use file api_type."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.check_response = Mock()

        manager = RepositoryManager(mock_client)

        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"prn": "test-prn", "pulp_href": "test-href"}]}

        methods = {
            "create": Mock(return_value=mock_response),
        }

        new_repo = RpmRepositoryRequest(name="test-build/rpms")

        prn, href = manager._create_new_repository(methods, new_repo, "file")

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

        new_repo = RpmRepositoryRequest(name="test-build/rpms")

        with pytest.raises(ValueError) as exc_info:
            manager._create_new_repository(methods, new_repo, "rpms")

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

        new_repo = RpmRepositoryRequest(name="test-build/rpms")

        with pytest.raises(ValueError) as exc_info:
            manager._create_new_repository(methods, new_repo, "rpms")

        assert "Unexpected response format" in str(exc_info.value)


class TestRepositoryManagerWaitForDistributionTask:
    """Tests for RepositoryManager._wait_for_distribution_task() method."""

    def test_wait_for_distribution_task_exception_handling(self):
        """Test _wait_for_distribution_task exception handling (lines 256-257)."""
        mock_client = Mock()
        mock_client.namespace = "test-namespace"
        mock_client.config = {"base_url": "https://pulp.example.com"}
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
