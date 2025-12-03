"""Tests for DistributionManager class."""

from unittest.mock import Mock, patch

import pytest

from pulp_tool.utils.distribution_manager import DistributionManager


class TestDistributionManager:
    """Tests for DistributionManager class."""

    def test_get_distribution_urls_invalid_after_sanitization(self):
        """Test get_distribution_urls raises ValueError when sanitized build_id is invalid (line 64)."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}

        manager = DistributionManager(mock_client, "test-namespace")

        # Mock sanitize_build_id_for_repository to return something that will fail validation
        # validate_build_id returns False for strings with spaces or slashes
        with (
            patch(
                "pulp_tool.utils.distribution_manager.sanitize_build_id_for_repository", return_value="invalid build"
            ),
            patch("pulp_tool.utils.distribution_manager.validate_build_id") as mock_validate,
        ):
            # Make validate_build_id return False for the sanitized value
            def validate_side_effect(build_id):
                return False if build_id == "invalid build" else True

            mock_validate.side_effect = validate_side_effect

            with pytest.raises(ValueError) as exc_info:
                manager.get_distribution_urls("test-build")

            assert "Invalid build ID" in str(exc_info.value)
            assert "sanitized:" in str(exc_info.value)
            assert "invalid build" in str(exc_info.value)

    def test_get_single_distribution_url_cache_hit(self):
        """Test _get_single_distribution_url uses cached base_path (lines 85, 87-88, 91)."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}

        # Create manager with pre-populated cache
        cache = {("test-build", "rpms"): "cached-build/rpms"}
        manager = DistributionManager(mock_client, "test-namespace", distribution_cache=cache)

        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url = manager._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )

            # Should use cached base_path
            assert url == "https://pulp.example.com/api/pulp-content/test-namespace/cached-build/rpms/"
            # Should log cache usage
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using cached distribution" in call_args[0]
            assert "cached-build/rpms" in call_args[2]  # base_path
            assert url in call_args[3]  # distribution_url

    def test_get_single_distribution_url_cache_miss(self):
        """Test _get_single_distribution_url computes URL when cache miss."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}

        manager = DistributionManager(mock_client, "test-namespace")

        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url = manager._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )

            # Should compute URL from build_id
            assert url == "https://pulp.example.com/api/pulp-content/test-namespace/test-build/rpms/"
            # Should log computed URL
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using computed distribution URL" in call_args[0]
            # Should cache the base_path
            assert ("test-build", "rpms") in manager._distribution_cache
            assert manager._distribution_cache[("test-build", "rpms")] == "test-build/rpms"

    def test_get_single_distribution_url_cache_shared(self):
        """Test that distribution_cache is shared across instances."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}

        # Create shared cache
        shared_cache: dict[tuple[str, str], str] = {}

        manager1 = DistributionManager(mock_client, "test-namespace", distribution_cache=shared_cache)
        manager2 = DistributionManager(mock_client, "test-namespace", distribution_cache=shared_cache)

        # First call populates cache
        url1 = manager1._get_single_distribution_url("test-build", "rpms", "https://pulp.example.com/api/pulp-content/")

        # Second call should use cache
        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url2 = manager2._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )

            # Should use cached value
            assert url1 == url2
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using cached distribution" in call_args[0]
