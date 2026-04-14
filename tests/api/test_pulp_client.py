"""
Tests for PulpClient class and its mixin components.

This module contains comprehensive tests for:
- PulpClient: Main client class (pulp_client.py)
- Content upload operations migrated to PulpClient
- Content querying and retrieval methods migrated to PulpClient
- RepositoryManagerMixin: Repository operations (repository_manager.py)
- TaskManagerMixin: Pulp task management (task_manager.py)

All mixin functionality is tested through the integrated PulpClient class,
which is the correct approach for testing mixin-based architecture.
"""

import json
import re
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
import httpx
from httpx import HTTPError

from pulp_tool.api import PulpClient, OAuth2ClientCredentialsAuth
from pulp_tool.models.artifacts import ExtraArtifactRef, PulpContentRow
from pulp_tool.models.pulp_api import RpmDistributionRequest, RpmRepositoryRequest


class TestPulpClient:
    """Test PulpClient class."""

    def test_init(self, mock_config):
        """Test PulpClient initialization."""
        client = PulpClient(mock_config)

        assert client.config == mock_config
        assert client.domain is None
        # namespace is set from config["domain"] when domain parameter is not provided
        assert client.namespace == "test-domain"
        assert client.timeout == 120  # DEFAULT_TIMEOUT
        assert client._auth is None
        assert client.session is not None

    def test_init_with_domain(self, mock_config):
        """Test PulpClient initialization with explicit domain."""
        client = PulpClient(mock_config, domain="explicit-domain")

        assert client.domain == "explicit-domain"
        # namespace is set from the domain parameter
        assert client.namespace == "explicit-domain"

    def test_create_session(self, mock_config):
        """Test _create_session method."""
        client = PulpClient(mock_config)
        session = client._create_session()

        # Should return an httpx.Client instance (not requests.Session)
        assert isinstance(session, httpx.Client)
        # Verify client is configured (limits not accessible after init, only timeout)
        assert session.timeout is not None
        assert not session.is_closed

    def test_close(self, mock_pulp_client):
        """Test close method."""
        mock_pulp_client.session.close = Mock()
        mock_pulp_client.close()

        mock_pulp_client.session.close.assert_called_once()

    def test_context_manager(self, mock_config):
        """Test context manager functionality."""
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            with PulpClient(mock_config) as client:
                assert client.session == mock_session

            mock_session.close.assert_called_once()

    def test_create_from_config_file(self, temp_config_file):
        """Test create_from_config_file class method."""
        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            config_content = '[cli]\nbase_url = "https://test.com"'
            mock_load_content.return_value = (config_content.encode(), False)

            client = PulpClient.create_from_config_file(path=temp_config_file)

            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"

    def test_create_from_config_file_default_path(self):
        """Test create_from_config_file with default path."""
        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            config_content = '[cli]\nbase_url = "https://test.com"'
            mock_load_content.return_value = (config_content.encode(), False)

            client = PulpClient.create_from_config_file()

            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"

    def test_create_from_config_file_with_base64(self):
        """Test create_from_config_file with base64-encoded config."""
        import base64

        config_content = '[cli]\nbase_url = "https://test.com"\ndomain = "test-domain"'
        base64_config = base64.b64encode(config_content.encode()).decode()

        with patch("pulp_tool.api.pulp_client.tomllib.loads") as mock_loads:
            mock_loads.return_value = {"cli": {"base_url": "https://test.com", "domain": "test-domain"}}

            client = PulpClient.create_from_config_file(path=base64_config)

            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"
            assert client.config_path is None  # Should be None for base64 config
            mock_loads.assert_called_once()

    def test_create_from_config_file_invalid_toml_raises_value_error(self, temp_config_file):
        """Test create_from_config_file raises ValueError with clear message for invalid TOML."""
        import tomllib

        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            mock_load_content.return_value = (b"invalid toml [cli\nbase_url", False)

            with pytest.raises(ValueError, match=r"Invalid TOML in configuration .*: .*") as exc_info:
                PulpClient.create_from_config_file(path=temp_config_file)

            assert "Invalid TOML" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, tomllib.TOMLDecodeError)

    def test_headers_property(self, mock_pulp_client):
        """Test headers property."""
        assert mock_pulp_client.headers is None

    def test_auth_property(self, mock_pulp_client):
        """Test auth property."""
        auth = mock_pulp_client.auth

        assert isinstance(auth, OAuth2ClientCredentialsAuth)
        assert auth._token_url == "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"

    def test_auth_property_cached(self, mock_pulp_client):
        """Test auth property caching."""
        auth1 = mock_pulp_client.auth
        auth2 = mock_pulp_client.auth

        assert auth1 is auth2

    def test_auth_property_missing_credentials_raises(self, mock_config):
        """Test auth property raises clear error when no credentials provided."""
        config_no_creds = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config["domain"],
        }
        client = PulpClient(config_no_creds)
        with pytest.raises(ValueError, match="Authentication credentials missing"):
            _ = client.auth

    def test_auth_property_empty_client_id_raises(self, mock_config):
        """Test auth property raises when client_id is empty and no username/password."""
        config = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config["domain"],
            "client_id": "",
            "client_secret": "secret",
        }
        client = PulpClient(config)
        with pytest.raises(ValueError, match="Authentication credentials missing"):
            _ = client.auth

    def test_auth_property_username_password_basic_auth(self, mock_config):
        """Test auth property uses Basic Auth when username/password provided."""
        config = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config["domain"],
            "username": "myuser",
            "password": "mypass",
        }
        client = PulpClient(config)
        auth = client.auth
        assert isinstance(auth, httpx.BasicAuth)

    def test_cert_property(self, mock_config):
        """Test cert property returns PEM paths from config (files created in mock_config fixture)."""
        client = PulpClient(mock_config)
        assert client.cert == (mock_config["cert"], mock_config["key"])

    def test_cert_property_with_relative_paths(self, mock_config, tmp_path):
        """Test cert property with relative paths resolved from config_path."""
        # Create a config file and cert/key files relative to it
        config_file = tmp_path / "config.toml"
        config_dir = config_file.parent

        # Create cert and key files in the config directory
        cert_file = config_dir / "cert.pem"
        key_file = config_dir / "key.pem"
        cert_file.write_text("cert content")
        key_file.write_text("key content")

        # Update config with relative paths
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"

        # Create client with config_path, mocking session creation to avoid SSL errors
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_with_relative, config_path=config_file)

            # Cert property should resolve relative paths
            cert_tuple = client.cert

            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_with_absolute_paths(self, mock_config, tmp_path):
        """Test cert property with absolute paths (should not resolve relative to config)."""
        # Create cert and key files in a different location
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        cert_file = other_dir / "cert.pem"
        key_file = other_dir / "key.pem"
        cert_file.write_text("cert content")
        key_file.write_text("key content")

        config_file = tmp_path / "config.toml"

        # Update config with absolute paths
        config_with_absolute = mock_config.copy()
        config_with_absolute["cert"] = str(cert_file)
        config_with_absolute["key"] = str(key_file)

        # Create client with config_path, mocking session creation to avoid SSL errors
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_with_absolute, config_path=config_file)

            # Cert property should return absolute paths as-is
            cert_tuple = client.cert

            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_with_base64_cert_and_key(self, mock_config, tmp_path):
        """Test cert property decodes base64-encoded cert/key file content."""
        import base64

        config_file = tmp_path / "config.toml"
        config_dir = config_file.parent
        cert_file = config_dir / "cert.pem"
        key_file = config_dir / "key.pem"

        cert_pem = b"-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJAK\n-----END CERTIFICATE-----"
        key_pem = b"-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBg\n-----END PRIVATE KEY-----"
        cert_file.write_text(base64.b64encode(cert_pem).decode())
        key_file.write_text(base64.b64encode(key_pem).decode())

        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"

        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_with_relative, config_path=config_file)
            cert_tuple = client.cert

            assert client._cert_paths is not None
            assert cert_tuple == client._cert_paths
            assert Path(cert_tuple[0]).exists() and Path(cert_tuple[1]).exists()
            assert Path(cert_tuple[0]).read_bytes() == cert_pem
            assert Path(cert_tuple[1]).read_bytes() == key_pem

            client.close()
            assert client._cert_temp_dir is None
            assert client._cert_paths is None

    def test_close_handles_oserror_on_cert_temp_cleanup(self, mock_config, tmp_path):
        """Test close() catches OSError when cleaning up base64 cert temp dir."""
        import base64

        config_file = tmp_path / "config.toml"
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_pem = b"-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJAK\n-----END CERTIFICATE-----"
        key_pem = b"-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBg\n-----END PRIVATE KEY-----"
        cert_file.write_text(base64.b64encode(cert_pem).decode())
        key_file.write_text(base64.b64encode(key_pem).decode())
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"

        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_create_session.return_value = Mock()
            client = PulpClient(config_with_relative, config_path=config_file)
            _ = client.cert
            assert client._cert_temp_dir is not None
            client._cert_temp_dir.cleanup = Mock(side_effect=OSError("cleanup failed"))
            client.close()
            assert client._cert_temp_dir is None
            assert client._cert_paths is None

    def test_cert_property_with_existing_relative_paths(self, mock_config, tmp_path):
        """Test cert property when relative paths exist in current directory."""
        # Create cert and key files in current directory (not relative to config)
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            cert_file = tmp_path / "cert.pem"
            key_file = tmp_path / "key.pem"
            cert_file.write_text("cert content")
            key_file.write_text("key content")

            config_file = tmp_path / "config.toml"

            # Update config with relative paths
            config_with_relative = mock_config.copy()
            config_with_relative["cert"] = "cert.pem"
            config_with_relative["key"] = "key.pem"

            # Create client with config_path, mocking session creation to avoid SSL errors
            with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
                mock_session = Mock()
                mock_create_session.return_value = mock_session

                client = PulpClient(config_with_relative, config_path=config_file)

                # Cert property should return paths as-is since they exist in current directory
                cert_tuple = client.cert

                assert cert_tuple == ("cert.pem", "key.pem")
        finally:
            os.chdir(original_cwd)

    def test_cert_property_without_config_path(self, mock_config, tmp_path):
        """Test cert property when config_path is None (relative paths use cwd)."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            cert_file = tmp_path / "cert.pem"
            key_file = tmp_path / "key.pem"
            cert_file.write_text("cert content")
            key_file.write_text("key content")
            config_with_relative = mock_config.copy()
            config_with_relative["cert"] = "cert.pem"
            config_with_relative["key"] = "key.pem"

            with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
                mock_create_session.return_value = Mock()
                client = PulpClient(config_with_relative, config_path=None)
                cert_tuple = client.cert
                assert cert_tuple == ("cert.pem", "key.pem")
        finally:
            os.chdir(original_cwd)

    def test_cert_property_with_config_path_no_parent(self, mock_config, tmp_path):
        """Test cert property when config_path has no parent (use absolute PEM paths)."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("c")
        key_file.write_text("k")
        config_with_absolute = mock_config.copy()
        config_with_absolute["cert"] = str(cert_file)
        config_with_absolute["key"] = str(key_file)

        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_create_session.return_value = Mock()
            from pathlib import Path

            mock_config_path = Mock(spec=Path)
            mock_config_path.parent = None

            client = PulpClient(config_with_absolute, config_path=mock_config_path)
            cert_tuple = client.cert
            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_relative_path_not_found(self, mock_config, tmp_path):
        """Misconfigured mTLS (missing PEM files) fails fast on client init."""
        config_file = tmp_path / "config.toml"
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "nonexistent_cert.pem"
        config_with_relative["key"] = "nonexistent_key.pem"

        with pytest.raises(ValueError, match="Certificate or key file not found"):
            PulpClient(config_with_relative, config_path=config_file)

    def test_pulp_client_raises_when_only_cert_or_only_key_set(self, mock_config):
        """cert without key (or vice versa) is invalid for mTLS."""
        cfg_only_cert = {k: v for k, v in mock_config.items() if k != "key"}
        with pytest.raises(ValueError, match="both `cert` and `key`"):
            PulpClient(cfg_only_cert)
        cfg_only_key = {k: v for k, v in mock_config.items() if k != "cert"}
        with pytest.raises(ValueError, match="both `cert` and `key`"):
            PulpClient(cfg_only_key)

    def test_cert_property_mixed_absolute_and_relative(self, mock_config, tmp_path):
        """Test cert property with one absolute path and one relative path."""
        # Create cert file in a different location (absolute)
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        cert_file = other_dir / "cert.pem"
        cert_file.write_text("cert content")

        # Create key file relative to config
        config_file = tmp_path / "config.toml"
        config_dir = config_file.parent
        key_file = config_dir / "key.pem"
        key_file.write_text("key content")

        # Update config with mixed paths
        config_mixed = mock_config.copy()
        config_mixed["cert"] = str(cert_file)  # Absolute
        config_mixed["key"] = "key.pem"  # Relative

        # Create client with config_path, mocking session creation to avoid SSL errors
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_mixed, config_path=config_file)

            # Cert property should return absolute cert as-is, resolve relative key
            cert_tuple = client.cert

            assert cert_tuple == (str(cert_file), str(key_file))

    def test_request_params_with_cert(self, mock_pulp_client):
        """Test request_params property with certificate.

        Note: With httpx, cert is passed to Client constructor, not per-request.
        So request_params should NOT contain cert when using certificate auth.
        """
        params = mock_pulp_client.request_params

        # Cert is handled at Client level, not per-request
        assert "cert" not in params
        # When using cert auth, we don't add auth to request_params either
        assert "auth" not in params

    def test_request_params_without_cert(self, mock_config):
        """Test request_params property without certificate."""
        config_no_cert = {k: v for k, v in mock_config.items() if k not in ("cert", "key")}
        client = PulpClient(config_no_cert)
        params = client.request_params

        assert "auth" in params
        assert "cert" not in params

    def test_url_building(self, mock_pulp_client):
        """Test _url method."""
        url = mock_pulp_client._url("api/v3/test/")

        expected = "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/test/"
        assert url == expected

    def test_url_building_with_domain(self, mock_config):
        """Test _url method with domain."""
        client = PulpClient(mock_config, domain="custom-domain")
        url = client._url("api/v3/test/")

        expected = "https://pulp.example.com/pulp/api/v3/custom-domain/api/v3/test/"
        assert url == expected

    def test_url_building_with_explicit_domain(self, mock_config):
        """Test _url method with explicitly provided domain parameter."""
        # Create config without domain field
        config_without_domain = {k: v for k, v in mock_config.items() if k != "domain"}
        # Pass domain explicitly
        client = PulpClient(config_without_domain, domain="custom-domain")
        url = client._url("api/v3/test/")

        expected = "https://pulp.example.com/pulp/api/v3/custom-domain/api/v3/test/"
        assert url == expected

    def test_get_domain(self, mock_pulp_client):
        """Test get_domain method."""
        domain = mock_pulp_client.get_domain()
        assert domain == "test-domain"

    def test_get_domain_with_tenant_suffix(self, mock_config):
        """Test get_domain method with tenant suffix (no longer removes -tenant)."""
        client = PulpClient(mock_config, domain="test-domain-tenant")
        domain = client.get_domain()
        assert domain == "test-domain-tenant"

    def test_get_single_resource(self, mock_pulp_client, httpx_mock):
        """Test _get_single_resource method."""
        # Mock the API response - URL includes domain and query params
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=test-repo&offset=0&limit=1"
        ).mock(return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/repositories/12345/"}))

        result = mock_pulp_client._get_single_resource("api/v3/repositories/", "test-repo")

        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/repositories/12345/"

    def test_get_single_resource_404_not_checked(self, mock_pulp_client, httpx_mock):
        """404 is returned for 'not found' name lookups; callers treat it as missing resource."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=no-such&offset=0&limit=1"
        ).mock(return_value=httpx.Response(404))

        result = mock_pulp_client._get_single_resource("api/v3/repositories/", "no-such")

        assert result.status_code == 404

    def test_get_single_resource_non404_error_raises(self, mock_pulp_client, httpx_mock):
        """Other error statuses fail fast with HTTPError (same as other API calls)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=bad&offset=0&limit=1"
        ).mock(return_value=httpx.Response(502, text="Bad Gateway"))

        with pytest.raises(HTTPError, match="Failed to get single resource by name"):
            mock_pulp_client._get_single_resource("api/v3/repositories/", "bad")

    def test_check_response_success(self, mock_pulp_client, mock_response):
        """Test _check_response method with successful response."""
        mock_pulp_client._check_response(mock_response, "test operation")
        # Should not raise any exception

    def test_check_response_error(self, mock_pulp_client, mock_error_response):
        """Test _check_response method with error response."""
        with pytest.raises(HTTPError, match="Failed to test operation"):
            mock_pulp_client._check_response(mock_error_response, "test operation")

    def test_check_response_public(self, mock_pulp_client, mock_response):
        """Test check_response public method."""
        mock_pulp_client.check_response(mock_response, "test operation")
        # Should not raise any exception

    def test_chunked_get_no_chunking(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method without chunking."""
        # Mock the API response
        httpx_mock.get("https://test.com/api").mock(
            return_value=httpx.Response(200, json={"results": [{"id": 1}, {"id": 2}]})
        )

        result = mock_pulp_client._chunked_get("https://test.com/api", {"param": "value"})

        assert result.status_code == 200
        assert len(result.json()["results"]) == 2

    def test_chunked_get_with_chunking(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method with chunking."""
        # Create a large parameter list
        large_param = ",".join([f"item{i}" for i in range(100)])
        params = {"large_param": large_param}

        # Mock multiple responses for chunking - each chunk returns 20 items
        httpx_mock.get("https://test.com/api").mock(
            return_value=httpx.Response(200, json={"results": [{"id": i} for i in range(20)]})
        )

        with patch.object(mock_pulp_client, "_check_response"):
            result = mock_pulp_client._chunked_get(
                "https://test.com/api", params, chunk_param="large_param", chunk_size=20
            )

        assert result.status_code == 200
        # Should aggregate results from multiple chunks (5 chunks * 20 items = 100 items)
        assert len(result.json()["results"]) == 100

    def test_chunked_get_async_fallback_when_no_aggregated_response(self, mock_pulp_client, httpx_mock):
        """Defensive fallback when chunk gather returns nothing still performs a checked GET."""
        large_param = ",".join([f"item{i}" for i in range(100)])
        params = {"large_param": large_param}
        httpx_mock.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"results": [], "count": 0}))

        async def _gather_returns_empty(*_a: object, **_kw: object) -> list:
            return []

        with patch("pulp_tool.api.pulp_client.asyncio.gather", side_effect=_gather_returns_empty):
            result = mock_pulp_client._chunked_get(
                "https://test.com/api", params, chunk_param="large_param", chunk_size=20
            )
        assert result.status_code == 200
        assert result.json()["results"] == []

    def test_upload_content_rpm(self, mock_pulp_client, temp_rpm_file, httpx_mock):
        """Test upload_content method for RPM."""
        # Mock the RPM upload endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )

        labels = {"build_id": "test-build", "arch": "x86_64"}

        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_rpm_file, labels, file_type="RPM", arch="x86_64")

        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_file(self, mock_pulp_client, temp_file, httpx_mock):
        """Test upload_content method for file."""
        # Mock the file content creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )

        labels = {"build_id": "test-build"}

        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_file, labels, file_type="File")

        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_missing_arch(self, mock_pulp_client, temp_file):
        """Test upload_content method with missing arch for RPM."""
        labels = {"build_id": "test-build"}

        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            with pytest.raises(ValueError, match="arch parameter is required for RPM uploads"):
                mock_pulp_client.upload_content(temp_file, labels, file_type="RPM")

    def test_create_file_content_from_file(self, mock_pulp_client, temp_file, httpx_mock):
        """Test create_file_content method with file path."""
        # Mock the file content creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )

        labels = {"build_id": "test-build"}

        result = mock_pulp_client.create_file_content("test-repo", temp_file, build_id="test-build", pulp_label=labels)

        assert result.status_code == 202
        assert result.json()["task"] == "/pulp/api/v3/tasks/12345/"

    def test_create_file_content_from_string(self, mock_pulp_client, httpx_mock):
        """Test create_file_content method with string content."""
        # Mock the file content creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )

        labels = {"build_id": "test-build"}
        content = '{"test": "data"}'

        result = mock_pulp_client.create_file_content(
            "test-repo", content, build_id="test-build", pulp_label=labels, filename="test.json"
        )

        assert result.status_code == 202
        assert result.json()["task"] == "/pulp/api/v3/tasks/12345/"

    def test_create_file_content_missing_filename(self, mock_pulp_client):
        """Test create_file_content method with missing filename for string content."""
        labels = {"build_id": "test-build"}
        content = '{"test": "data"}'

        with pytest.raises(ValueError, match="filename is required when providing in-memory content"):
            mock_pulp_client.create_file_content("test-repo", content, build_id="test-build", pulp_label=labels)

    def test_add_content(self, mock_pulp_client, httpx_mock):
        """Test add_content method."""
        # Mock the add content endpoint - repository href gets modify/ appended
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/repositories/rpm/rpm/12345/modify/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/67890/"})
        )

        # Mock the task endpoint (add_content now fetches the task)
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/67890/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/pulp/api/v3/tasks/67890/", "state": "completed", "created_resources": []}
            )
        )

        artifacts = ["/pulp/api/v3/content/12345/", "/pulp/api/v3/content/67890/"]

        result = mock_pulp_client.add_content("/pulp/api/v3/repositories/rpm/rpm/12345/", artifacts)

        # Now returns a TaskResponse model
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.pulp_href == "/pulp/api/v3/tasks/67890/"
        assert result.state == "completed"

    def test_modify_repository_content_remove_only(self, mock_pulp_client, httpx_mock):
        """Test modify_repository_content with remove_content_units only."""
        posted: dict = {}

        def capture_modify(request: httpx.Request) -> httpx.Response:
            posted["body"] = json.loads(request.content.decode())
            return httpx.Response(202, json={"task": "/pulp/api/v3/tasks/99999/"})

        httpx_mock.post("https://pulp.example.com/pulp/api/v3/repositories/rpm/rpm/12345/modify/").mock(
            side_effect=capture_modify
        )
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/99999/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/pulp/api/v3/tasks/99999/", "state": "completed", "created_resources": []}
            )
        )
        removes = ["/pulp/api/v3/content/rpm/packages/old/"]
        result = mock_pulp_client.modify_repository_content(
            "/pulp/api/v3/repositories/rpm/rpm/12345/", remove_content_units=removes
        )
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert posted["body"] == {"remove_content_units": removes}
        assert "add_content_units" not in posted["body"]

    def test_modify_repository_content_requires_add_or_remove(self, mock_pulp_client):
        """modify_repository_content raises if both add and remove are empty."""
        with pytest.raises(ValueError, match="modify_repository_content requires"):
            mock_pulp_client.modify_repository_content("/pulp/api/v3/repositories/rpm/rpm/1/")

    def test_modify_repository_content_add_and_remove(self, mock_pulp_client, httpx_mock):
        """Test modify_repository_content with both add_content_units and remove_content_units."""
        posted: dict = {}

        def capture_modify(request: httpx.Request) -> httpx.Response:
            posted["body"] = json.loads(request.content.decode())
            return httpx.Response(202, json={"task": "/pulp/api/v3/tasks/88888/"})

        httpx_mock.post("https://pulp.example.com/pulp/api/v3/repositories/rpm/rpm/99999/modify/").mock(
            side_effect=capture_modify
        )
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/88888/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/pulp/api/v3/tasks/88888/", "state": "completed", "created_resources": []}
            )
        )
        mock_pulp_client.modify_repository_content(
            "/pulp/api/v3/repositories/rpm/rpm/99999/",
            add_content_units=["/add/1/"],
            remove_content_units=["/rm/1/"],
        )
        assert posted["body"] == {"add_content_units": ["/add/1/"], "remove_content_units": ["/rm/1/"]}

    def test_get_task(self, mock_pulp_client, httpx_mock):
        """Test _get_task method."""
        # Mock the task endpoint
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(
                200,
                json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed", "result": {"status": "success"}},
            )
        )

        result = mock_pulp_client.get_task("/pulp/api/v3/tasks/12345/")

        # Now returns a TaskResponse model
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "completed"

    def test_wait_for_finished_task_success(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task method with successful completion."""
        # Mock the task endpoint
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed"})
        )

        with patch("time.sleep"):
            result = mock_pulp_client.wait_for_finished_task("/pulp/api/v3/tasks/12345/")

        # Now returns a TaskResponse model
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "completed"

    def test_wait_for_finished_task_timeout(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task method with timeout."""
        # Mock the task endpoint to return running state
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "running"})
        )

        # The method now raises TimeoutError instead of returning the last response
        with patch("time.sleep"), patch("time.time", side_effect=[0, 0.5, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]):
            with patch("pulp_tool.api.task_manager.logging"):
                result = mock_pulp_client.wait_for_finished_task("/pulp/api/v3/tasks/12345/", timeout=1)

        # Now returns a TaskResponse model even on timeout (last state)
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "running"

    def test_find_content_by_build_id(self, mock_pulp_client, httpx_mock):
        """Test find_content method by build_id."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/pulp/api/v3/content/rpm/packages/12345/"}]}
            )
        )

        result = mock_pulp_client.find_content("build_id", "test-build-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_find_content_by_href(self, mock_pulp_client, httpx_mock):
        """Test find_content method by href."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_href__in=/pulp/api/v3/content/12345/"
        ).mock(return_value=httpx.Response(200, json={"results": [{"pulp_href": "/pulp/api/v3/content/12345/"}]}))

        result = mock_pulp_client.find_content("href", "/pulp/api/v3/content/12345/")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_find_content_invalid_type(self, mock_pulp_client):
        """Test find_content method with invalid search type."""
        with pytest.raises(ValueError, match="Unknown search type"):
            mock_pulp_client.find_content("invalid", "test-value")

    def test_find_content_raises_on_http_error(self, mock_pulp_client, httpx_mock):
        """Non-success responses from content search are checked before JSON parsing."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~bad"
        ).mock(return_value=httpx.Response(502, text="Bad Gateway"))

        with pytest.raises(httpx.HTTPError, match="Failed to find content"):
            mock_pulp_client.find_content("build_id", "bad")

    def test_gather_content_data_empty_body_after_success_status(self, mock_pulp_client, httpx_mock):
        """Malformed empty200 from Pulp produces a clear error (regression for JSONDecodeError)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~empty-body"
        ).mock(return_value=httpx.Response(200, content=b""))

        with pytest.raises(ValueError, match="Empty response body"):
            mock_pulp_client.gather_content_data("empty-body")

    def test_get_file_locations(self, mock_pulp_client, httpx_mock):
        """Test get_file_locations method."""
        # Mock the artifacts endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/"
            "?pulp_href__in=/pulp/api/v3/artifacts/12345/"
        ).mock(return_value=httpx.Response(200, json={"results": [{"pulp_href": "/pulp/api/v3/artifacts/12345/"}]}))

        artifacts = [{"file": "/pulp/api/v3/artifacts/12345/"}]

        result = mock_pulp_client.get_file_locations(artifacts)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_pkgIDs(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_pkgIDs method."""
        # Mock the RPM search endpoint - URL encoding uses %2C for comma
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/"
            "?pkgId__in=abcd1234%2Cefgh5678"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/pulp/api/v3/content/rpm/packages/12345/"}]}
            )
        )

        pkg_ids = ["abcd1234", "efgh5678"]

        result = mock_pulp_client.get_rpm_by_pkgIDs(pkg_ids)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_filenames(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames parses filename to NVR and searches by name+version+release."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "location_href": "pkg-1.0-1.x86_64.rpm",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames(["pkg-1.0-1.x86_64.rpm"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["location_href"] == "pkg-1.0-1.x86_64.rpm"

    def test_get_rpm_by_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_signed_by method."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pulp_label_select.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "pulp_labels": {"signed_by": "key-id-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_signed_by(["key-id-123"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-id-123"

    def test_get_rpm_by_checksums_and_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_checksums_and_signed_by with empty checksums returns empty (line 1436)."""
        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by([], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_checksums_and_signed_by_multi_chunk(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_checksums_and_signed_by with 4+ checksums uses multi-chunk path (lines 1454-1471)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64, "b" * 64, "c" * 64, "d" * 64]  # 4 checksums = 2 chunks (chunk_size=3)
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pkgId.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": f"/pkg/{i}/", "pkgId": c, "pulp_labels": {"signed_by": "key-123"}}
                        for i, c in enumerate(checksums)
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by(checksums, "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 4

    def test_get_rpm_by_checksums_and_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_checksums_and_signed_by combines filters in single query."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksum = "a" * 64
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pkgId.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "pkgId": checksum,
                            "pulp_labels": {"signed_by": "key-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by([checksum], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-123"

    def test_get_rpm_by_filenames_and_signed_by_combined_multi_nvr(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames_and_signed_by combined path with 2 NVRs uses multi-chunk (lines 1535-1544)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 2 NVRs -> 2 chunks in _fetch_rpm_by_nvr_and_signed_by_combined
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pkg/1/",
                            "name": "pkg1",
                            "version": "1.0",
                            "release": "1",
                            "pulp_labels": {"signed_by": "key-123"},
                        },
                        {
                            "pulp_href": "/pkg/2/",
                            "name": "pkg2",
                            "version": "1.0",
                            "release": "1",
                            "pulp_labels": {"signed_by": "key-123"},
                        },
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(
            ["pkg1-1.0-1.x86_64.rpm", "pkg2-1.0-1.x86_64.rpm"], "key-123"
        )

        assert result.status_code == 200
        assert len(result.json()["results"]) == 2

    def test_get_rpm_by_filenames_and_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames_and_signed_by parses to NVR and combines with signed_by."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "location_href": "pkg-1.0-1.x86_64.rpm",
                            "pulp_labels": {"signed_by": "key-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["pkg-1.0-1.x86_64.rpm"], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-123"

    def test_get_rpm_by_filenames_and_signed_by_fallback_on_400(self, mock_pulp_client, httpx_mock):
        """Test fallback when combined query returns 400 (line 1487)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        pkg1 = {"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.rpm", "pulp_labels": {"signed_by": "key-123"}}
        err_400 = httpx.Response(400, content=b"Bad Request")
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                err_400,
                httpx.Response(200, json={"results": [pkg1]}),
                httpx.Response(200, json={"results": [pkg1]}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["pkg1-1.0-1.x86_64.rpm"], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_filenames_and_signed_by_fallback_on_500(self, mock_pulp_client, httpx_mock):
        """Test fallback to two calls + intersect when combined query returns 500."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        pkg1 = {"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.rpm", "pulp_labels": {"signed_by": "key-123"}}
        pkg2 = {"pulp_href": "/pkg/2/", "location_href": "pkg2-1.0-1.rpm", "pulp_labels": {"signed_by": "other"}}
        pkg3 = {"pulp_href": "/pkg/3/", "location_href": "pkg3.rpm", "pulp_labels": {"signed_by": "key-123"}}
        err_500 = httpx.Response(500, content=b"Server Error")
        # Order: 2 combined chunk requests (both 500), 2 by_nvr chunks, 1 by_signed
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                err_500,
                err_500,
                httpx.Response(200, json={"results": [pkg1]}),
                httpx.Response(200, json={"results": [pkg2]}),
                httpx.Response(200, json={"results": [pkg1, pkg3]}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(
            ["pkg1-1.0-1.x86_64.rpm", "pkg2-1.0-1.x86_64.rpm"], "key-123"
        )

        assert result.status_code == 200
        # Intersection: only pkg1 (in both by_hrefs and by_signed)
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_href"] == "/pkg/1/"

    def test_get_rpm_by_filenames_and_signed_by_fallback_signed_by_first_when_many_nvrs(
        self, mock_pulp_client, httpx_mock
    ):
        """Test fallback uses signed_by-first (1 call) when NVRs >= 5 instead of N+1."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # Combined returns 500 (5 parallel chunk requests); with 5 NVRs we use signed_by-first - 1 call
        pkgs = [
            {
                "pulp_href": f"/pkg/{i}/",
                "name": f"pkg{i}",
                "version": "1.0",
                "release": "1",
                "pulp_labels": {"signed_by": "key-123"},
            }
            for i in range(5)
        ]
        err_500 = httpx.Response(500, content=b"Server Error")
        ok_signed = httpx.Response(200, json={"results": pkgs, "next": None})
        # 5 combined chunk requests (all 500) + 1 signed_by request (200)
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(side_effect=[err_500] * 5 + [ok_signed])

        filenames = [f"pkg{i}-1.0-1.x86_64.rpm" for i in range(5)]
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(filenames, "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 5
        # Combined makes 5 chunk requests (all 500); fallback uses signed_by-first = 1 call (not 5 NVR + 1)
        package_calls = [c for c in httpx_mock.calls if "packages" in str(getattr(c.request, "url", ""))]
        assert len(package_calls) == 6  # 5 combined + 1 signed_by

    def test_get_rpm_by_filenames_and_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_filenames_and_signed_by with empty filenames returns empty (line 1487)."""
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by([], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_and_signed_by_all_unparseable(self, mock_pulp_client):
        """Test get_rpm_by_filenames_and_signed_by with all unparseable filenames returns empty (line 1487)."""
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["bad.rpm", "nover.rpm"], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_empty(self, mock_pulp_client):
        """Test get_rpm_by_filenames with empty list returns empty."""
        result = mock_pulp_client.get_rpm_by_filenames([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_skips_unparseable(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames skips unparseable filenames and searches parseable ones."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=good-pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pkg/1/",
                            "location_href": "good-pkg-1.0-1.x86_64.rpm",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames(["bad.rpm", "good-pkg-1.0-1.x86_64.rpm", "malformed"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["location_href"] == "good-pkg-1.0-1.x86_64.rpm"

    def test_get_rpm_by_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_signed_by with empty list returns empty results."""
        result = mock_pulp_client.get_rpm_by_signed_by([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_unsigned_checksums_empty(self, mock_pulp_client):
        """Test get_rpm_by_unsigned_checksums with empty list returns empty results."""
        result = mock_pulp_client.get_rpm_by_unsigned_checksums([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_unsigned_checksums_single_chunk(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_unsigned_checksums with 1-20 items uses single request path."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64 for _ in range(5)]
        results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}} for i, c in enumerate(checksums)
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*unsigned_checksum")).mock(
            return_value=httpx.Response(200, json={"results": results})
        )

        result = mock_pulp_client.get_rpm_by_unsigned_checksums(checksums)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 5

    def test_get_rpm_by_filenames_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames with 3 NVRs chunks and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 3 NVRs = 3 chunks (chunk_size=1 for packages.redhat.com complexity limit)
        chunk_results = [
            [{"pulp_href": "/pkg/0/", "location_href": "pkg0-1.0-1.x86_64.rpm"}],
            [{"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.x86_64.rpm"}],
            [{"pulp_href": "/pkg/2/", "location_href": "pkg2-1.0-1.x86_64.rpm"}],
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[httpx.Response(200, json={"results": r}) for r in chunk_results]
        )

        filenames = [f"pkg{i}-1.0-1.x86_64.rpm" for i in range(3)]
        result = mock_pulp_client.get_rpm_by_filenames(filenames)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 3

    def test_get_rpm_by_filenames_deduplicates_by_pulp_href(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames deduplicates results by pulp_href when chunks return same package."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # Two chunks both return same package (duplicate pulp_href) - should dedupe to 1
        pkg = {"pulp_href": "/pkg/0/", "location_href": "pkg0-1.0-1.x86_64.rpm"}
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                httpx.Response(200, json={"results": [pkg]}),
                httpx.Response(200, json={"results": [pkg]}),
            ]
        )

        filenames = ["pkg0-1.0-1.x86_64.rpm", "pkg0-1.0-1.src.rpm"]  # same NVR, different arch
        result = mock_pulp_client.get_rpm_by_filenames(filenames)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_href"] == "/pkg/0/"

    def test_get_rpm_by_signed_by_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_signed_by with 9 items triggers chunking and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 9 items = 3 chunks (4 + 4 + 1) with chunk_size=4
        chunk_results = [
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(0, 4)],
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(4, 8)],
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(8, 9)],
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pulp_label_select.*signed_by")).mock(
            side_effect=[httpx.Response(200, json={"results": r}) for r in chunk_results]
        )

        keys = [f"key-{i}" for i in range(9)]
        result = mock_pulp_client.get_rpm_by_signed_by(keys)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 9

    def test_get_rpm_by_unsigned_checksums_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_unsigned_checksums with 25+ items triggers chunking and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64 for _ in range(25)]
        chunk1_results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}} for i, c in enumerate(checksums[:20])
        ]
        chunk2_results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}}
            for i, c in enumerate(checksums[20:], start=20)
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*unsigned_checksum")).mock(
            side_effect=[
                httpx.Response(200, json={"results": chunk1_results}),
                httpx.Response(200, json={"results": chunk2_results}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_unsigned_checksums(checksums)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 25

    def test_gather_content_data(self, mock_pulp_client, mock_content_data, httpx_mock):
        """Test gather_content_data method."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(return_value=httpx.Response(200, json=mock_content_data, headers={"content-type": "application/json"}))

        content_data = mock_pulp_client.gather_content_data("test-build-123")

        assert len(content_data.content_results) == 1
        assert len(content_data.artifacts) == 1
        assert content_data.content_results[0].pulp_href == "/pulp/api/v3/content/rpm/packages/12345/"

    def test_gather_content_data_no_results(self, mock_pulp_client, httpx_mock):
        """Test gather_content_data method with no results."""
        # Mock the content search endpoint with empty results
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(return_value=httpx.Response(200, json={"results": []}, headers={"content-type": "application/json"}))

        content_data = mock_pulp_client.gather_content_data("test-build-123")

        assert content_data.content_results == []
        assert content_data.artifacts == []

    def test_gather_content_data_with_extra_artifacts(self, mock_pulp_client, mock_content_data, httpx_mock):
        """Test gather_content_data method with extra artifacts."""
        # Mock the content search endpoint - gather_content_data always queries by build_id first
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(return_value=httpx.Response(200, json=mock_content_data, headers={"content-type": "application/json"}))

        extra_artifacts = [
            ExtraArtifactRef.model_validate({"file": "/pulp/api/v3/artifacts/67890/"}),
            ExtraArtifactRef.model_validate({"extra": "/pulp/api/v3/artifacts/99999/"}),
        ]

        content_data = mock_pulp_client.gather_content_data("test-build-123", extra_artifacts)

        # Should get content from API query by build_id
        assert len(content_data.content_results) == 1  # From API response
        assert len(content_data.artifacts) == 1  # Extracted from content_results

    def test_gather_content_data_href_fallback_bare_list_json(self, mock_pulp_client, mock_content_data, httpx_mock):
        """When build_id finds nothing, href query may return a bare JSON array instead of {\"results\": ...}."""
        httpx_mock.get(
            re.compile(
                r"https://pulp\.example\.com/pulp/api/v3/test-domain/api/v3/content/"
                r"\?pulp_label_select=build_id~test-bare-list"
            )
        ).mock(return_value=httpx.Response(200, json={"results": []}))

        row = mock_content_data["results"][0]
        httpx_mock.get(re.compile(r".*api/v3/content/\?pulp_href__in=.*")).mock(
            return_value=httpx.Response(200, json=[row])
        )

        href = row["pulp_href"]
        extra = [ExtraArtifactRef.model_validate({"pulp_href": href})]
        content_data = mock_pulp_client.gather_content_data("test-bare-list", extra)

        assert len(content_data.content_results) == 1
        assert content_data.content_results[0].pulp_href == href
        assert len(content_data.artifacts) >= 1

    def test_build_results_structure(self, mock_pulp_client, mock_content_data, mock_file_locations, httpx_mock):
        """Test build_results_structure method."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs, FileInfoModel

        # Mock the file locations endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/"
            "?pulp_href__in=/pulp/api/v3/artifacts/67890/"
        ).mock(return_value=httpx.Response(200, json=mock_file_locations))

        content_results = [PulpContentRow.model_validate(r) for r in mock_content_data["results"]]

        # Create PulpResultsModel
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
        results_model = PulpResultsModel(build_id="test-build-123", repositories=repositories)

        # Create FileInfoModel
        file_info = FileInfoModel(**mock_file_locations["results"][0])
        file_info_map = {"/pulp/api/v3/artifacts/67890/": file_info}

        result = mock_pulp_client.build_results_structure(results_model, content_results, file_info_map)

        assert result.artifact_count == 1
        # Verify the result uses relative_path as the key
        assert "test-build-123/x86_64/test-package.rpm" in result.artifacts

    def test_build_results_structure_merge_preserves_incremental_and_adds_new(
        self, mock_pulp_client, mock_content_data
    ):
        """merge=True keeps existing artifact entries; still adds keys from gather."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs, FileInfoModel

        base = mock_content_data["results"][0]
        labels = dict(base["pulp_labels"])
        content_results = [
            PulpContentRow.model_validate(
                {
                    **base,
                    "artifacts": {
                        "test-build-123/x86_64/test-package.rpm": "/pulp/api/v3/artifacts/67890/",
                        "test-build-123/x86_64/extra.rpm": "/pulp/api/v3/artifacts/11111/",
                    },
                }
            )
        ]

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
        results_model = PulpResultsModel(build_id="test-build-123", repositories=repositories)
        inc_url = "https://incremental.example/test-package.rpm"
        results_model.add_artifact(
            "test-build-123/x86_64/test-package.rpm",
            inc_url,
            "incremental-sha",
            labels,
        )

        file_info_map = {
            "/pulp/api/v3/artifacts/67890/": FileInfoModel(
                pulp_href="/pulp/api/v3/artifacts/67890/",
                file="test-package.rpm@sha256:gather67890",
                sha256="gather67890",
            ),
            "/pulp/api/v3/artifacts/11111/": FileInfoModel(
                pulp_href="/pulp/api/v3/artifacts/11111/",
                file="extra.rpm@sha256:gather11111",
                sha256="gather11111",
            ),
        }
        distribution_urls = {"rpms": "https://pulp.example.com/pulp/content/test-build/rpms/"}

        with patch("pulp_tool.api.pulp_client.logging") as mock_logging:
            result = mock_pulp_client.build_results_structure(
                results_model,
                content_results,
                file_info_map,
                distribution_urls,
                merge=True,
            )

        assert result.artifact_count == 2
        assert result.artifacts["test-build-123/x86_64/test-package.rpm"].url == inc_url
        assert result.artifacts["test-build-123/x86_64/test-package.rpm"].sha256 == "incremental-sha"
        assert "test-build-123/x86_64/extra.rpm" in result.artifacts
        warn_msgs = [str(c) for c in mock_logging.warning.call_args_list]
        assert any("differs from incremental" in m for m in warn_msgs)

    def test_build_results_structure_invalid_artifact_href(self, mock_pulp_client, mock_content_data, httpx_mock):
        """Test build_results_structure with invalid artifact hrefs (line 1249)."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs, FileInfoModel

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
        results_model = PulpResultsModel(build_id="test-build-123", repositories=repositories)

        # Content with invalid artifact hrefs (no "/artifacts/" in href)
        content_results = [
            PulpContentRow.model_validate(
                {
                    "pulp_href": "/content/123/",
                    "pulp_labels": {"build_id": "test-build-123"},
                    "artifacts": {
                        "valid.rpm": "/pulp/api/v3/artifacts/67890/",  # Valid
                        "invalid1.txt": "/content/invalid/",  # Invalid - no "/artifacts/"
                        "invalid2.txt": "",  # Invalid - empty
                        "invalid3.txt": None,  # Invalid - None
                    },
                    "relative_path": "test-package.rpm",
                }
            )
        ]

        file_info = FileInfoModel(
            pulp_href="/pulp/api/v3/artifacts/67890/",
            file="test-package.rpm@sha256:abc",
            sha256="abc",
        )
        file_info_map = {"/pulp/api/v3/artifacts/67890/": file_info}

        result = mock_pulp_client.build_results_structure(results_model, content_results, file_info_map)

        # Should only process the valid artifact
        assert result.artifact_count == 1
        # Invalid hrefs should be skipped (line 1249)

    def test_build_results_structure_missing_file_info_many(self, mock_pulp_client, mock_content_data, httpx_mock):
        """Test build_results_structure with many missing file info entries (line 1286)."""
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
        results_model = PulpResultsModel(build_id="test-build-123", repositories=repositories)

        # Content with multiple artifacts, but file_info_map only has one entry
        content_results = [
            PulpContentRow.model_validate(
                {
                    "pulp_href": "/content/123/",
                    "pulp_labels": {"build_id": "test-build-123"},
                    "artifacts": {f"file{i}.txt": f"/pulp/api/v3/artifacts/{i}/" for i in range(10)},  # 10 artifacts
                    "relative_path": "test.txt",
                }
            )
        ]

        # Only provide file_info for first artifact
        from pulp_tool.models import FileInfoModel

        file_info_map = {
            "/pulp/api/v3/artifacts/0/": FileInfoModel(
                pulp_href="/pulp/api/v3/artifacts/0/",
                file="file0.txt@sha256:abc",
                sha256="abc",
            )
        }

        with patch("pulp_tool.api.pulp_client.logging") as mock_logging:
            result = mock_pulp_client.build_results_structure(results_model, content_results, file_info_map)

            # Should only process the one with file_info
            assert result.artifact_count == 1
            # Should log warning for missing file info > 3 (line 1286)
            mock_logging.warning.assert_called()
            # Check that the summary warning was logged
            warning_calls = [call for call in mock_logging.warning.call_args_list if "Missing file info" in str(call)]
            assert len(warning_calls) > 0

    def test_repository_operation_create_repo(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for creating repository."""
        # Mock the repository creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/repositories/rpm/rpm/12345/"})
        )
        new_repo = RpmRepositoryRequest(name="test-repo", autopublish=True)

        result = mock_pulp_client.repository_operation("create_repo", "rpm", repository_data=new_repo)

        captured_request = httpx_mock.calls[0].request.content
        captured_request_body = json.loads(captured_request)
        expected_request_body = {"name": "test-repo", "autopublish": True}
        assert captured_request_body == expected_request_body
        assert result.status_code == 201
        assert result.json()["pulp_href"] == "/pulp/api/v3/repositories/rpm/rpm/12345/"

    def test_repository_operation_create_repo_missing_data(self, mock_pulp_client):
        with pytest.raises(ValueError, match="Repository data is required for 'create_repo' operations"):
            mock_pulp_client.repository_operation("create_repo", "rpm", repository_data=None)

    def test_repository_operation_get_repo(self, mock_pulp_client, mock_response):
        """Test repository_operation method for getting repository."""
        mock_pulp_client._get_single_resource = Mock()
        mock_pulp_client._get_single_resource.return_value = mock_response

        result = mock_pulp_client.repository_operation("get_repo", "rpm", name="test-repo")

        assert result == mock_response
        mock_pulp_client._get_single_resource.assert_called_once()

    def test_repository_operation_get_repo_missing_name(self, mock_pulp_client):
        with pytest.raises(ValueError, match="Name is required for 'get_repo' operations"):
            mock_pulp_client.repository_operation("get_repo", "rpm", name=None)

    def test_repository_operation_create_distro(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for creating distribution."""
        # Mock the distribution creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )
        new_distro = RpmDistributionRequest(name="test-distro", base_path="test-distro", repository="test-repo")

        result = mock_pulp_client.repository_operation("create_distro", "rpm", distribution_data=new_distro)

        captured_request = httpx_mock.calls[0].request.content
        captured_request_body = json.loads(captured_request)
        expected_request_body = {"name": "test-distro", "base_path": "test-distro", "repository": "test-repo"}
        assert captured_request_body == expected_request_body
        assert result.status_code == 201
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"

    def test_repository_operation_create_distro_missing_data(self, mock_pulp_client):
        with pytest.raises(ValueError, match="Distribution data is required for 'create_distro' operations"):
            mock_pulp_client.repository_operation("create_distro", "rpm", distribution_data=None)

    def test_repository_operation_get_distro(self, mock_pulp_client, mock_response):
        """Test repository_operation method for getting distribution."""
        mock_pulp_client._get_single_resource = Mock()
        mock_pulp_client._get_single_resource.return_value = mock_response

        result = mock_pulp_client.repository_operation("get_distro", "rpm", name="test-distro")

        assert result == mock_response
        mock_pulp_client._get_single_resource.assert_called_once()

    def test_repository_operation_get_distro_missing_name(self, mock_pulp_client):
        with pytest.raises(ValueError, match="Name is required for 'get_distro' operations"):
            mock_pulp_client.repository_operation("get_distro", "rpm", name=None)

    def test_repository_operation_update_distro(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for updating distribution."""
        # Mock the distribution update endpoint - URL uses the distribution_href directly
        httpx_mock.patch("https://pulp.example.com/pulp/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation(
            "update_distro",
            "rpm",
            name="test-distro",
            distribution_href="/pulp/api/v3/distributions/12345/",
            publication="/pulp/api/v3/publications/67890/",
        )

        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"

    def test_tomllib_import(self):
        """Test tomllib import (built-in in Python 3.12+)."""
        # tomllib is built-in in Python 3.11+, so no fallback needed for 3.12+
        # The actual import happens at module level, so this is mainly for coverage
        import pulp_tool.api.pulp_client

        # Verify the module imported successfully
        assert hasattr(pulp_tool.api.pulp_client, "tomllib")

    def test_chunked_get_small_list(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method with small parameter list (no chunking)."""
        # Mock the API response for small list
        httpx_mock.get("https://test.com/api").mock(
            return_value=httpx.Response(200, json={"results": [{"id": 1}, {"id": 2}]})
        )

        # Small parameter list that doesn't need chunking
        params = {"small_param": "item1,item2"}

        result = mock_pulp_client._chunked_get("https://test.com/api", params, chunk_param="small_param", chunk_size=50)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 2

    def test_chunked_get_empty_chunk_fallback(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method with empty chunk fallback."""
        # Mock the fallback request
        httpx_mock.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"results": []}))

        # This will trigger the fallback when no chunks are created
        params = {"empty_param": ""}

        result = mock_pulp_client._chunked_get("https://test.com/api", params, chunk_param="empty_param", chunk_size=50)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 0

    def test_request_params_without_headers(self, mock_config):
        """Test request_params property without headers."""
        # OAuth path: mTLS must be fully absent (not only cert removed).
        config_without_cert = {k: v for k, v in mock_config.items() if k not in ("cert", "key")}
        client = PulpClient(config_without_cert)

        params = client.request_params

        # Headers should not be in params when headers property returns None
        assert "headers" not in params
        # Should have auth instead of cert
        assert "auth" in params
        assert "cert" not in params

    def test_check_response_json_decode_error(self, mock_pulp_client, httpx_mock):
        """Test _check_response method with JSON decode error."""
        # Mock a server error response that will trigger _check_response - need to mock the chunked URL
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?test_param=value1").mock(
            return_value=httpx.Response(500, text="Invalid JSON response", headers={"content-type": "application/json"})
        )

        with patch("pulp_tool.api.pulp_client.logging") as mock_logging:
            with pytest.raises(HTTPError, match="Failed to chunked request"):
                mock_pulp_client._chunked_get(
                    "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/",
                    {"test_param": "value1,value2"},
                    chunk_param="test_param",
                    chunk_size=1,  # Force chunking
                )

            # Verify error logging was called
            mock_logging.error.assert_called()

    def test_create_file_content_with_arch(self, mock_pulp_client, httpx_mock):
        """Test create_file_content method with arch parameter."""
        # Mock the file content creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )

        labels = {"build_id": "test-build"}
        content = '{"test": "data"}'

        result = mock_pulp_client.create_file_content(
            "test-repo", content, build_id="test-build", pulp_label=labels, filename="test.json", arch="x86_64"
        )

        assert result.status_code == 202
        assert result.json()["task"] == "/pulp/api/v3/tasks/12345/"

    def test_repository_operation_update_distro_with_publication(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for updating distribution with publication."""
        # Mock the distribution update endpoint
        httpx_mock.patch("https://pulp.example.com/pulp/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation(
            "update_distro",
            "rpm",
            name="test-distro",
            distribution_href="/pulp/api/v3/distributions/12345/",
            publication="/pulp/api/v3/publications/67890/",
        )

        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"


class TestPulpClientAdditional:
    """Additional tests for PulpClient class to achieve 100% coverage."""

    def test_tomllib_import_error(self):
        """Test tomllib import error fallback."""
        # This tests the import fallback logic in lines 33-35
        # We can't easily test the actual ImportError, but we can verify the module works
        import pulp_tool.api.pulp_client

        assert hasattr(pulp_tool.api.pulp_client, "tomllib")

    def test_chunked_get_empty_param_fallback(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method with empty parameter fallback."""
        # Mock the fallback request for empty parameter
        httpx_mock.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"results": []}))

        # This will trigger the fallback when param value is empty
        params = {"empty_param": ""}

        result = mock_pulp_client._chunked_get("https://test.com/api", params, chunk_param="empty_param", chunk_size=50)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 0

    def test_chunked_get_no_chunks_processed(self, mock_pulp_client, httpx_mock):
        """Test _chunked_get method when chunking encounters an error."""
        # Test error handling in chunked get by mocking a failing response

        # Mock the first chunk request to fail
        httpx_mock.get("https://test.com/api?test_param=value1").mock(side_effect=httpx.HTTPError("Network error"))

        params = {"test_param": "value1,value2"}

        with pytest.raises(httpx.HTTPError, match="Network error"):
            mock_pulp_client._chunked_get("https://test.com/api", params, chunk_param="test_param", chunk_size=1)

    def test_request_params_with_headers_property(self, mock_config):
        """Test request_params property when headers property returns non-None."""
        client = PulpClient(mock_config)

        # Mock headers property to return actual headers
        with patch("pulp_tool.api.PulpClient.headers", new_callable=lambda: lambda self: {"Custom-Header": "test"}):
            params = client.request_params

        # Should include headers when headers property returns non-None
        assert "headers" in params

    def test_repository_operation_update_distro_without_publication(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for updating distribution without publication."""
        # Mock the distribution update endpoint
        httpx_mock.patch("https://pulp.example.com/pulp/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation(
            "update_distro", "rpm", name="test-distro", distribution_href="/pulp/api/v3/distributions/12345/"
        )

        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"

    def test_repository_operation_invalid_operation(self, mock_pulp_client):
        """Test repository_operation method with invalid operation."""
        with pytest.raises(ValueError, match="Unknown operation"):
            mock_pulp_client.repository_operation("invalid", "rpm", name="test")

    def test_add_uploaded_artifact_to_results_model_rpm_key_is_basename(self, mock_pulp_client, tmp_path):
        """RPM incremental path uses basename as artifact key (is_rpm branch)."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs

        rpm_path = tmp_path / "my-pkg-1.0-1.x86_64.rpm"
        rpm_path.write_bytes(b"rpm-bytes")

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
        labels = {"build_id": "test-build", "arch": "x86_64"}
        urls = {"rpms": "https://pulp.example.com/content/test/rpms/"}

        with (
            patch(
                "pulp_tool.api.pulp_client.calculate_sha256_checksum",
                return_value="a" * 64,
            ),
            patch.object(
                mock_pulp_client,
                "_build_artifact_distribution_url",
                return_value="https://dist.example/foo.rpm",
            ),
        ):
            mock_pulp_client.add_uploaded_artifact_to_results_model(
                results_model,
                local_path=str(rpm_path),
                labels=labels,
                is_rpm=True,
                distribution_urls=urls,
            )

        assert "my-pkg-1.0-1.x86_64.rpm" in results_model.artifacts
        meta = results_model.artifacts["my-pkg-1.0-1.x86_64.rpm"]
        assert meta.url == "https://dist.example/foo.rpm"
        assert meta.sha256 == "a" * 64
