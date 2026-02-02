"""
Tests for PulpClient async methods.

This module tests async methods that need coverage.
"""

import pytest
from unittest.mock import Mock, patch
import httpx

from pulp_tool.api import PulpClient, OAuth2ClientCredentialsAuth


class TestPulpClientAsync:
    """Test PulpClient async methods."""

    def test_prepare_async_kwargs_with_auth(self, mock_config):
        """Test _prepare_async_kwargs with auth configured (lines 762-764)."""
        # PulpClient doesn't take auth in __init__, it's set via _auth property
        # But _prepare_async_kwargs checks self.auth, so we need to set it
        client = PulpClient(mock_config)
        auth = OAuth2ClientCredentialsAuth("client-id", "client-secret", "token-url")
        client._auth = auth  # type: ignore[assignment]

        kwargs = client._prepare_async_kwargs()
        # _prepare_async_kwargs uses setdefault, so if auth exists, it will be added
        assert "auth" in kwargs
        assert kwargs["auth"] == auth

    def test_prepare_async_kwargs_without_auth(self, mock_config):
        """Test _prepare_async_kwargs without auth."""
        # Create config without auth credentials
        # The auth property checks for client_id/client_secret, so we need to ensure they're not in config
        config_no_auth = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config.get("domain", ""),
        }
        client = PulpClient(config_no_auth)
        # Ensure _auth is None - the auth property will try to access client_id which doesn't exist
        # But _prepare_async_kwargs checks self.auth, which accesses config["client_id"]
        # So we need to mock the auth property to return None
        with patch.object(type(client), "auth", property(lambda self: None)):
            kwargs = client._prepare_async_kwargs()
            # When no auth is configured, setdefault won't add it
            assert "auth" not in kwargs

    def test_prepare_async_kwargs_with_existing_auth(self, mock_config):
        """Test _prepare_async_kwargs when auth already in kwargs."""
        client = PulpClient(mock_config)
        auth = OAuth2ClientCredentialsAuth("client-id", "client-secret", "token-url")
        client._auth = auth  # type: ignore[assignment]

        other_auth = OAuth2ClientCredentialsAuth("other-id", "other-secret", "token-url")
        kwargs = client._prepare_async_kwargs(auth=other_auth)
        # When auth is provided in kwargs, setdefault won't override it
        assert kwargs["auth"] == other_auth

    @pytest.mark.asyncio
    async def test_async_get(self, mock_config):
        """Test async_get method."""
        import respx

        client = PulpClient(mock_config)

        with respx.mock:
            # Mock OAuth token endpoint
            respx.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
                return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
            )
            # Mock the actual API call
            respx.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"status": "ok"}))

            response = await client.async_get("https://test.com/api")

            assert response.status_code == 200
            assert response.json()["status"] == "ok"

            # Clean up async session
            if hasattr(client, "_async_session") and client._async_session:
                await client._async_session.aclose()

    @pytest.mark.asyncio
    async def test_async_post(self, mock_config):
        """Test async_post method."""
        import respx

        client = PulpClient(mock_config)

        with respx.mock:
            # Mock OAuth token endpoint
            respx.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
                return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
            )
            # Mock the actual API call
            respx.post("https://test.com/api").mock(return_value=httpx.Response(201, json={"status": "created"}))

            response = await client.async_post("https://test.com/api", json={"data": "test"})

            assert response.status_code == 201
            assert response.json()["status"] == "created"

            # Clean up async session
            if hasattr(client, "_async_session") and client._async_session:
                await client._async_session.aclose()

    @pytest.mark.asyncio
    async def test_async_get_rpm_by_pkg_ids(self, mock_config):
        """Test async_get_rpm_by_pkg_ids method."""
        import respx

        client = PulpClient(mock_config)

        with respx.mock:
            # Mock OAuth token endpoint
            respx.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
                return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
            )
            # Mock the actual API call
            respx.get(
                "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/"
                "?pkgId__in=abcd1234%2Cefgh5678"
            ).mock(return_value=httpx.Response(200, json={"results": [{"pkgId": "abcd1234"}]}))

            pkg_ids = ["abcd1234", "efgh5678"]
            response = await client.async_get_rpm_by_pkgIDs(pkg_ids)  # type: ignore[attr-defined]

            assert response.status_code == 200
            assert len(response.json()["results"]) == 1

            # Clean up async session
            if hasattr(client, "_async_session") and client._async_session:
                await client._async_session.aclose()


class TestPulpClientErrorPaths:
    """Test PulpClient error paths."""

    def test_repository_operation_update_distro_no_href(self, mock_pulp_client):
        """Test repository_operation update_distro without distribution_href (line 939)."""
        with pytest.raises(ValueError, match="Distribution href is required"):
            mock_pulp_client.repository_operation(
                "update_distro", repo_type="file", name="test", publication="/pub/123/", distribution_href=None
            )

    def test_get_content_type_from_href_rpm(self):
        """Test _get_content_type_from_href for RPM."""
        from pulp_tool.api.pulp_client import PulpClient

        result = PulpClient._get_content_type_from_href("/api/v3/content/rpm/packages/123/")
        assert result == "rpm.package"

    def test_get_content_type_from_href_file(self):
        """Test _get_content_type_from_href for file."""
        from pulp_tool.api.pulp_client import PulpClient

        result = PulpClient._get_content_type_from_href("/api/v3/content/file/files/123/")
        assert result == "file.file"

    def test_get_content_type_from_href_unknown(self):
        """Test _get_content_type_from_href for unknown."""
        from pulp_tool.api.pulp_client import PulpClient

        result = PulpClient._get_content_type_from_href("/api/v3/content/unknown/123/")
        assert result == "unknown"

    def test_build_rpm_distribution_url(self):
        """Test _build_rpm_distribution_url."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls = {"rpms": "https://example.com/rpms/"}
        result = PulpClient._build_rpm_distribution_url("test/package.rpm", distribution_urls)
        assert result == "https://example.com/rpms/Packages/l/package.rpm"

    def test_build_rpm_distribution_url_no_rpms(self):
        """Test _build_rpm_distribution_url without rpms URL."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls: dict[str, str] = {}
        result = PulpClient._build_rpm_distribution_url("test/package.rpm", distribution_urls)
        assert result == "test/package.rpm"

    def test_build_file_distribution_url_with_arch(self):
        """Test _build_file_distribution_url with arch prefix."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls = {"logs": "https://example.com/logs/"}
        result = PulpClient._build_file_distribution_url("x86_64/test.log", {}, distribution_urls)
        assert result == "https://example.com/logs/x86_64/test.log"

    def test_build_file_distribution_url_sbom(self):
        """Test _build_file_distribution_url for SBOM."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls = {"sbom": "https://example.com/sbom/"}
        result = PulpClient._build_file_distribution_url("test.sbom.json", {}, distribution_urls)
        assert result == "https://example.com/sbom/test.sbom.json"

    def test_build_file_distribution_url_log_with_arch_label(self):
        """Test _build_file_distribution_url for log with arch label."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls = {"logs": "https://example.com/logs/"}
        labels = {"arch": "x86_64"}
        result = PulpClient._build_file_distribution_url("test.log", labels, distribution_urls)
        assert result == "https://example.com/logs/x86_64/test.log"

    def test_build_file_distribution_url_log_without_arch(self):
        """Test _build_file_distribution_url for log without arch."""
        from pulp_tool.api.pulp_client import PulpClient

        distribution_urls = {"logs": "https://example.com/logs/"}
        result = PulpClient._build_file_distribution_url("test.log", {}, distribution_urls)
        assert result == "https://example.com/logs/test.log"

    def test_build_file_distribution_url_no_urls(self):
        """Test _build_file_distribution_url without distribution URLs."""
        from pulp_tool.api.pulp_client import PulpClient

        result = PulpClient._build_file_distribution_url("test.log", {}, {})
        assert result == "test.log"

    def test_gather_content_data_with_extra_artifacts(self, mock_pulp_client, httpx_mock):
        """Test gather_content_data with extra_artifacts."""
        # Mock find_content to return empty results first, then results on href query
        call_count = 0

        def mock_find_content(search_type, value):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (build_id) returns empty
                return Mock(json=lambda: {"results": []})
            else:
                # Second call (href) returns results
                return Mock(json=lambda: {"results": [{"pulp_href": "/content/123/"}]})

        mock_pulp_client.find_content = Mock(side_effect=mock_find_content)

        extra_artifacts = [{"pulp_href": "/content/123/"}]
        result = mock_pulp_client.gather_content_data("test-build", extra_artifacts=extra_artifacts)

        assert result is not None
        assert mock_pulp_client.find_content.call_count == 2

    def test_gather_content_data_href_query_exception(self, mock_pulp_client, httpx_mock):
        """Test gather_content_data when href query raises exception."""
        # Mock find_content to raise exception on href query
        call_count = 0

        def mock_find_content(search_type, value):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (build_id) returns empty
                return Mock(json=lambda: {"results": []})
            else:
                # Second call (href) raises exception
                raise Exception("Href query failed")

        mock_pulp_client.find_content = Mock(side_effect=mock_find_content)

        extra_artifacts = [{"pulp_href": "/content/123/"}]
        with patch("pulp_tool.api.pulp_client.logging") as mock_logging:
            result = mock_pulp_client.gather_content_data("test-build", extra_artifacts=extra_artifacts)

            # Should handle exception gracefully and return empty ContentData
            assert result is not None
            mock_logging.error.assert_called()

    def test_build_results_structure_no_build_id(self, mock_pulp_client):
        """Test build_results_structure with no build_id in labels."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs

        repositories = RepositoryRefs(
            rpms_href="/rpms/",
            rpms_prn="rpms-prn",
            logs_href="/logs/",
            logs_prn="logs-prn",
            sbom_href="/sbom/",
            sbom_prn="sbom-prn",
            artifacts_href="/artifacts/",
            artifacts_prn="artifacts-prn",
        )
        results_model = PulpResultsModel(build_id="test-build", repositories=repositories)

        content_results = [
            {
                "pulp_href": "/content/123/",
                "artifacts": {"test.txt": "/artifacts/123/"},
                "relative_path": "test.txt",
            }
        ]
        file_info_map = {"/artifacts/123/": Mock(file="test.txt@sha256:abc", sha256="abc")}

        distribution_urls = {"logs": "https://example.com/logs/"}

        # Content with no build_id in labels
        with patch("pulp_tool.api.pulp_client.logging") as mock_logging:
            mock_pulp_client.build_results_structure(results_model, content_results, file_info_map, distribution_urls)
            # Should log warning about no build_id
            mock_logging.warning.assert_called()
