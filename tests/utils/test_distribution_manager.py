"""Tests for DistributionManager class."""

from unittest.mock import Mock, patch
import pytest
from pulp_tool.utils.distribution_manager import DistributionManager


class TestDistributionManager:
    """Tests for DistributionManager class."""

    def test_get_distribution_urls_invalid_after_sanitization(self) -> None:
        """Test get_distribution_urls raises ValueError when sanitized build_id is invalid (line 64)."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")
        with (
            patch(
                "pulp_tool.utils.distribution_manager.sanitize_build_id_for_repository", return_value="invalid build"
            ),
            patch("pulp_tool.utils.distribution_manager.validate_build_id") as mock_validate,
        ):

            def validate_side_effect(build_id) -> None:
                return False if build_id == "invalid build" else True

            mock_validate.side_effect = validate_side_effect
            with pytest.raises(ValueError) as exc_info:
                manager.get_distribution_urls("test-build")
            assert "Invalid build ID" in str(exc_info.value)
            assert "sanitized:" in str(exc_info.value)
            assert "invalid build" in str(exc_info.value)

    def test_get_single_distribution_url_cache_hit(self) -> None:
        """Test _get_single_distribution_url uses cached base_path (lines 85, 87-88, 91)."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        cache = {("test-build", "rpms"): "cached-build/rpms"}
        manager = DistributionManager(mock_client, "test-namespace", distribution_cache=cache)
        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url = manager._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )
            assert url == "https://pulp.example.com/api/pulp-content/test-namespace/cached-build/rpms/"
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using cached distribution" in call_args[0]
            assert "cached-build/rpms" in call_args[2]
            assert url in call_args[3]

    def test_get_single_distribution_url_cache_miss(self) -> None:
        """Test _get_single_distribution_url computes URL when cache miss."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")
        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url = manager._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )
            assert url == "https://pulp.example.com/api/pulp-content/test-namespace/test-build/rpms/"
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using computed distribution URL" in call_args[0]
            assert ("test-build", "rpms") in manager._distribution_cache
            assert manager._distribution_cache["test-build", "rpms"] == "test-build/rpms"

    def test_get_single_distribution_url_cache_shared(self) -> None:
        """Test that distribution_cache is shared across instances."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        shared_cache: dict[tuple[str, str], str] = {}
        manager1 = DistributionManager(mock_client, "test-namespace", distribution_cache=shared_cache)
        manager2 = DistributionManager(mock_client, "test-namespace", distribution_cache=shared_cache)
        url1 = manager1._get_single_distribution_url("test-build", "rpms", "https://pulp.example.com/api/pulp-content/")
        with patch("pulp_tool.utils.distribution_manager.logging") as mock_logging:
            url2 = manager2._get_single_distribution_url(
                "test-build", "rpms", "https://pulp.example.com/api/pulp-content/"
            )
            assert url1 == url2
            mock_logging.info.assert_called_once()
            call_args = mock_logging.info.call_args[0]
            assert "Using cached distribution" in call_args[0]

    def test_distribution_url_for_base_path(self) -> None:
        """Per-arch and arbitrary base_path URLs include namespace and trailing slash."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "my-ns")
        assert (
            manager.distribution_url_for_base_path("x86_64")
            == "https://pulp.example.com/api/pulp-content/my-ns/x86_64/"
        )

    def test_distribution_url_for_base_path_empty_raises(self) -> None:
        """Empty base_path raises ValueError."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "ns")
        with pytest.raises(ValueError, match="Invalid base_path"):
            manager.distribution_url_for_base_path("  ")

    def test_get_distribution_urls_skip_logs_and_sbom(self) -> None:
        """skip_logs_repo / skip_sbom_repo omit those keys from the map."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")
        urls = manager.get_distribution_urls("b1", skip_logs_repo=True, skip_sbom_repo=True)
        assert "logs" not in urls
        assert "sbom" not in urls
        assert "rpms" in urls
        assert "artifacts" in urls

    def test_get_distribution_urls_skip_artifacts_repo(self) -> None:
        """skip_artifacts_repo omits artifacts (local results JSON folder mode)."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")
        urls = manager.get_distribution_urls("b1", skip_artifacts_repo=True)
        assert "artifacts" not in urls
        assert "rpms" in urls
        assert "logs" in urls
        assert "sbom" in urls


class TestDistributionManagerTargetArchRepo:
    """Distribution URLs when using per-architecture RPM repositories."""

    def test_get_distribution_urls_skips_rpms_when_target_arch_repo(self) -> None:
        """Aggregate rpms URL is omitted when RPM repos are per-architecture."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")
        urls = manager.get_distribution_urls("my-build", target_arch_repo=True)
        assert "rpms" not in urls
        assert "logs" in urls
        assert "sbom" in urls
        assert "artifacts" in urls

    def test_get_distribution_urls_adds_rpms_signed_when_include_flag(self) -> None:
        """include_signed_rpm_distro adds rpms_signed when lookup succeeds."""
        mock_client = Mock()
        mock_client.config = {"base_url": "https://pulp.example.com"}
        manager = DistributionManager(mock_client, "test-namespace")

        def fake_get(build_id: str, repo_type: str, base: str) -> str:
            if repo_type == "rpms-signed":
                return "https://pulp.example.com/api/pulp-content/ns/b/rpms-signed/"
            return f"{base.rstrip('/')}/{build_id}/{repo_type}/"

        with patch.object(manager, "_get_single_distribution_url", side_effect=fake_get):
            urls = manager.get_distribution_urls("my-build", include_signed_rpm_distro=True)
        assert urls["rpms_signed"] == "https://pulp.example.com/api/pulp-content/ns/b/rpms-signed/"
        assert "rpms" in urls
