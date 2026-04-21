"""PulpClient tests (split module)."""

from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
import importlib.util
import httpx
import pytest
from httpx import HTTPError
from pulp_tool.api import PulpClient, OAuth2ClientCredentialsAuth
from pulp_tool.exceptions import PulpToolConfigError, PulpToolHTTPError


class TestPulpClient:
    """Test PulpClient class."""

    def test_init(self, mock_config) -> None:
        """Test PulpClient initialization."""
        client = PulpClient(mock_config)
        assert client.config == mock_config
        assert client.domain is None
        assert client.namespace == "test-domain"
        assert client.timeout == 120
        assert client._auth is None
        assert client.session is not None

    def test_init_with_domain(self, mock_config) -> None:
        """Test PulpClient initialization with explicit domain."""
        client = PulpClient(mock_config, domain="explicit-domain")
        assert client.domain == "explicit-domain"
        assert client.namespace == "explicit-domain"

    def test_create_session(self, mock_config) -> None:
        """Test _create_session method."""
        client = PulpClient(mock_config)
        session = client._create_session()
        assert isinstance(session, httpx.Client)
        assert session.timeout is not None
        assert not session.is_closed

    def test_close(self, mock_pulp_client) -> None:
        """Test close method."""
        mock_pulp_client.session.close = Mock()
        mock_pulp_client.close()
        mock_pulp_client.session.close.assert_called_once()

    def test_context_manager(self, mock_config) -> None:
        """Test context manager functionality."""
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session
            with PulpClient(mock_config) as client:
                assert client.session == mock_session
            mock_session.close.assert_called_once()

    def test_create_from_config_file(self, temp_config_file) -> None:
        """Test create_from_config_file class method."""
        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            config_content = '[cli]\nbase_url = "https://test.com"'
            mock_load_content.return_value = (config_content.encode(), False)
            client = PulpClient.create_from_config_file(path=temp_config_file)
            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"

    def test_create_from_config_file_default_path(self) -> None:
        """Test create_from_config_file with default path."""
        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            config_content = '[cli]\nbase_url = "https://test.com"'
            mock_load_content.return_value = (config_content.encode(), False)
            client = PulpClient.create_from_config_file()
            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"

    def test_create_from_config_file_with_base64(self) -> None:
        """Test create_from_config_file with base64-encoded config."""
        import base64

        config_content = '[cli]\nbase_url = "https://test.com"\ndomain = "test-domain"'
        base64_config = base64.b64encode(config_content.encode()).decode()
        with patch("pulp_tool.api.pulp_client.client.tomllib.loads") as mock_loads:
            mock_loads.return_value = {"cli": {"base_url": "https://test.com", "domain": "test-domain"}}
            client = PulpClient.create_from_config_file(path=base64_config)
            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"
            assert client.config_path is None
            mock_loads.assert_called_once()

    def test_create_from_config_file_invalid_toml_raises_value_error(self, temp_config_file) -> None:
        """Test create_from_config_file raises PulpToolConfigError for invalid TOML."""
        import tomllib

        with patch("pulp_tool.utils.config_utils.load_config_content") as mock_load_content:
            mock_load_content.return_value = (b"invalid toml [cli\nbase_url", False)
            with pytest.raises(PulpToolConfigError, match="Invalid TOML in configuration .*: .*") as exc_info:
                PulpClient.create_from_config_file(path=temp_config_file)
            assert "Invalid TOML" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, tomllib.TOMLDecodeError)

    def test_headers_property(self, mock_pulp_client) -> None:
        """Test headers property."""
        assert mock_pulp_client.headers is None

    def test_auth_property(self, mock_pulp_client) -> None:
        """Test auth property."""
        auth = mock_pulp_client.auth
        assert isinstance(auth, OAuth2ClientCredentialsAuth)
        assert auth._token_url == "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"

    def test_auth_property_cached(self, mock_pulp_client) -> None:
        """Test auth property caching."""
        auth1 = mock_pulp_client.auth
        auth2 = mock_pulp_client.auth
        assert auth1 is auth2

    def test_auth_property_missing_credentials_raises(self, mock_config) -> None:
        """Test auth property raises clear error when no credentials provided."""
        config_no_creds = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config["domain"],
        }
        client = PulpClient(config_no_creds)
        with pytest.raises(PulpToolConfigError, match="Authentication credentials missing"):
            _ = client.auth

    def test_auth_property_empty_client_id_raises(self, mock_config) -> None:
        """Test auth property raises when client_id is empty and no username/password."""
        config = {
            "base_url": mock_config["base_url"],
            "api_root": mock_config["api_root"],
            "domain": mock_config["domain"],
            "client_id": "",
            "client_secret": "secret",
        }
        client = PulpClient(config)
        with pytest.raises(PulpToolConfigError, match="Authentication credentials missing"):
            _ = client.auth

    def test_auth_property_username_password_basic_auth(self, mock_config) -> None:
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

    def test_cert_property(self, mock_config) -> None:
        """Test cert property returns PEM paths from config (files created in mock_config fixture)."""
        client = PulpClient(mock_config)
        assert client.cert == (mock_config["cert"], mock_config["key"])

    def test_cert_property_with_relative_paths(self, mock_config, tmp_path) -> None:
        """Test cert property with relative paths resolved from config_path."""
        config_file = tmp_path / "config.toml"
        config_dir = config_file.parent
        cert_file = config_dir / "cert.pem"
        key_file = config_dir / "key.pem"
        cert_file.write_text("cert content")
        key_file.write_text("key content")
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session
            client = PulpClient(config_with_relative, config_path=config_file)
            cert_tuple = client.cert
            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_with_absolute_paths(self, mock_config, tmp_path) -> None:
        """Test cert property with absolute paths (should not resolve relative to config)."""
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        cert_file = other_dir / "cert.pem"
        key_file = other_dir / "key.pem"
        cert_file.write_text("cert content")
        key_file.write_text("key content")
        config_file = tmp_path / "config.toml"
        config_with_absolute = mock_config.copy()
        config_with_absolute["cert"] = str(cert_file)
        config_with_absolute["key"] = str(key_file)
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session
            client = PulpClient(config_with_absolute, config_path=config_file)
            cert_tuple = client.cert
            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_with_base64_cert_and_key(self, mock_config, tmp_path) -> None:
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
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
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

    def test_close_handles_oserror_on_cert_temp_cleanup(self, mock_config, tmp_path) -> None:
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
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_create_session.return_value = Mock()
            client = PulpClient(config_with_relative, config_path=config_file)
            _ = client.cert
            assert client._cert_temp_dir is not None
            client._cert_temp_dir.cleanup = Mock(side_effect=OSError("cleanup failed"))
            client.close()
            assert client._cert_temp_dir is None
            assert client._cert_paths is None

    def test_cert_property_with_existing_relative_paths(self, mock_config, tmp_path) -> None:
        """Test cert property when relative paths exist in current directory."""
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(str(tmp_path))
            cert_file = tmp_path / "cert.pem"
            key_file = tmp_path / "key.pem"
            cert_file.write_text("cert content")
            key_file.write_text("key content")
            config_file = tmp_path / "config.toml"
            config_with_relative = mock_config.copy()
            config_with_relative["cert"] = "cert.pem"
            config_with_relative["key"] = "key.pem"
            with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
                mock_session = Mock()
                mock_create_session.return_value = mock_session
                client = PulpClient(config_with_relative, config_path=config_file)
                cert_tuple = client.cert
                assert cert_tuple == ("cert.pem", "key.pem")
        finally:
            os.chdir(original_cwd)

    def test_cert_property_without_config_path(self, mock_config, tmp_path) -> None:
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
            with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
                mock_create_session.return_value = Mock()
                client = PulpClient(config_with_relative, config_path=None)
                cert_tuple = client.cert
                assert cert_tuple == ("cert.pem", "key.pem")
        finally:
            os.chdir(original_cwd)

    def test_cert_property_with_config_path_no_parent(self, mock_config, tmp_path) -> None:
        """Test cert property when config_path has no parent (use absolute PEM paths)."""
        cert_file = tmp_path / "cert.pem"
        key_file = tmp_path / "key.pem"
        cert_file.write_text("c")
        key_file.write_text("k")
        config_with_absolute = mock_config.copy()
        config_with_absolute["cert"] = str(cert_file)
        config_with_absolute["key"] = str(key_file)
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_create_session.return_value = Mock()
            from pathlib import Path

            mock_config_path = Mock(spec=Path)
            mock_config_path.parent = None
            client = PulpClient(config_with_absolute, config_path=mock_config_path)
            cert_tuple = client.cert
            assert cert_tuple == (str(cert_file), str(key_file))

    def test_cert_property_relative_path_not_found(self, mock_config, tmp_path) -> None:
        """Misconfigured mTLS (missing PEM files) fails fast on client init."""
        config_file = tmp_path / "config.toml"
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "nonexistent_cert.pem"
        config_with_relative["key"] = "nonexistent_key.pem"
        with pytest.raises(ValueError, match="Certificate or key file not found"):
            PulpClient(config_with_relative, config_path=config_file)

    def test_pulp_client_raises_when_only_cert_or_only_key_set(self, mock_config) -> None:
        """cert without key (or vice versa) is invalid for mTLS."""
        cfg_only_cert = {k: v for k, v in mock_config.items() if k != "key"}
        with pytest.raises(ValueError, match="both `cert` and `key`"):
            PulpClient(cfg_only_cert)
        cfg_only_key = {k: v for k, v in mock_config.items() if k != "cert"}
        with pytest.raises(ValueError, match="both `cert` and `key`"):
            PulpClient(cfg_only_key)

    def test_cert_property_mixed_absolute_and_relative(self, mock_config, tmp_path) -> None:
        """Test cert property with one absolute path and one relative path."""
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        cert_file = other_dir / "cert.pem"
        cert_file.write_text("cert content")
        config_file = tmp_path / "config.toml"
        config_dir = config_file.parent
        key_file = config_dir / "key.pem"
        key_file.write_text("key content")
        config_mixed = mock_config.copy()
        config_mixed["cert"] = str(cert_file)
        config_mixed["key"] = "key.pem"
        with patch("pulp_tool.api.pulp_client.client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session
            client = PulpClient(config_mixed, config_path=config_file)
            cert_tuple = client.cert
            assert cert_tuple == (str(cert_file), str(key_file))

    def test_request_params_with_cert(self, mock_pulp_client) -> None:
        """Test request_params property with certificate.

        Note: With httpx, cert is passed to Client constructor, not per-request.
        So request_params should NOT contain cert when using certificate auth.
        """
        params = mock_pulp_client.request_params
        assert "cert" not in params
        assert "auth" not in params

    def test_request_params_without_cert(self, mock_config) -> None:
        """Test request_params property without certificate."""
        config_no_cert = {k: v for k, v in mock_config.items() if k not in ("cert", "key")}
        client = PulpClient(config_no_cert)
        params = client.request_params
        assert "auth" in params
        assert "cert" not in params

    def test_url_building(self, mock_pulp_client) -> None:
        """Test _url method."""
        url = mock_pulp_client._url("api/v3/test/")
        expected = "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/test/"
        assert url == expected

    def test_url_building_with_domain(self, mock_config) -> None:
        """Test _url method with domain."""
        client = PulpClient(mock_config, domain="custom-domain")
        url = client._url("api/v3/test/")
        expected = "https://pulp.example.com/pulp/api/v3/custom-domain/api/v3/test/"
        assert url == expected

    def test_url_building_with_explicit_domain(self, mock_config) -> None:
        """Test _url method with explicitly provided domain parameter."""
        config_without_domain = {k: v for k, v in mock_config.items() if k != "domain"}
        client = PulpClient(config_without_domain, domain="custom-domain")
        url = client._url("api/v3/test/")
        expected = "https://pulp.example.com/pulp/api/v3/custom-domain/api/v3/test/"
        assert url == expected

    def test_get_domain(self, mock_pulp_client) -> None:
        """Test get_domain method."""
        domain = mock_pulp_client.get_domain()
        assert domain == "test-domain"

    def test_get_domain_with_tenant_suffix(self, mock_config) -> None:
        """Test get_domain method with tenant suffix (no longer removes -tenant)."""
        client = PulpClient(mock_config, domain="test-domain-tenant")
        domain = client.get_domain()
        assert domain == "test-domain-tenant"

    def test_get_single_resource(self, mock_pulp_client, httpx_mock) -> None:
        """Test _get_single_resource method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=test-repo&offset=0&limit=1"
        ).mock(return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/repositories/12345/"}))
        result = mock_pulp_client._get_single_resource("api/v3/repositories/", "test-repo")
        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/repositories/12345/"

    def test_get_single_resource_404_not_checked(self, mock_pulp_client, httpx_mock) -> None:
        """404 is returned for 'not found' name lookups; callers treat it as missing resource."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=no-such&offset=0&limit=1"
        ).mock(return_value=httpx.Response(404))
        result = mock_pulp_client._get_single_resource("api/v3/repositories/", "no-such")
        assert result.status_code == 404

    def test_get_single_resource_non404_error_raises(self, mock_pulp_client, httpx_mock) -> None:
        """Other error statuses fail fast with HTTPError (same as other API calls)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=bad&offset=0&limit=1"
        ).mock(return_value=httpx.Response(502, text="Bad Gateway"))
        with pytest.raises(HTTPError, match="Failed to get single resource by name"):
            mock_pulp_client._get_single_resource("api/v3/repositories/", "bad")

    def test_get_single_resource_cache_key_includes_name(self, mock_pulp_client, httpx_mock) -> None:
        """Regression: cache must not return one repo's response for a different ``name``."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=first-repo&offset=0&limit=1"
        ).mock(return_value=httpx.Response(200, json={"results": [{"prn": "prn-first"}]}))
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/?name=second-repo&offset=0&limit=1"
        ).mock(return_value=httpx.Response(200, json={"results": [{"prn": "prn-second"}]}))
        r_first = mock_pulp_client._get_single_resource("api/v3/repositories/", "first-repo")
        r_second = mock_pulp_client._get_single_resource("api/v3/repositories/", "second-repo")
        assert r_first.json()["results"][0]["prn"] == "prn-first"
        assert r_second.json()["results"][0]["prn"] == "prn-second"

    def test_check_response_success(self, mock_pulp_client, mock_response) -> None:
        """Test _check_response method with successful response."""
        mock_pulp_client._check_response(mock_response, "test operation")

    def test_check_response_error(self, mock_pulp_client, mock_error_response) -> None:
        """Test _check_response method with error response."""
        with pytest.raises(HTTPError, match="Failed to test operation"):
            mock_pulp_client._check_response(mock_error_response, "test operation")

    def test_check_response_public(self, mock_pulp_client, mock_response) -> None:
        """Test check_response public method."""
        mock_pulp_client.check_response(mock_response, "test operation")

    def test_get_async_session_http2_probe_find_spec_raises(self, monkeypatch, mock_config, httpx_mock) -> None:
        """importlib.util.find_spec can raise (lines 223-224); client still builds without HTTP/2."""
        monkeypatch.setattr(importlib.util, "find_spec", lambda _name: (_ for _ in ()).throw(AttributeError("no spec")))
        client = PulpClient(mock_config)
        session = client._get_async_session()
        assert session is not None

    @pytest.mark.asyncio
    async def test_async_close_aclose_open_session(self, mock_config, httpx_mock) -> None:
        """async_close awaits aclose when async session exists and is open (lines 282-284)."""
        client = PulpClient(mock_config)
        mock_session = Mock()
        mock_session.is_closed = False
        mock_session.aclose = AsyncMock()
        client._async_session = mock_session
        await client.async_close()
        mock_session.aclose.assert_awaited_once()

    def test_get_domain_empty_without_config_domain(self, mock_config) -> None:
        """_get_domain returns empty string when neither ctor domain nor config domain is set (line 580)."""
        cfg = {k: v for k, v in mock_config.items() if k != "domain"}
        client = PulpClient(cfg)
        assert client._get_domain() == ""

    def test_check_response_redirect_logs_debug_branch(self, mock_pulp_client) -> None:
        """Non-success with status <400 (e.g. redirect) hits the else debug branch (lines 677-679)."""
        req = httpx.Request("GET", "https://pulp.example.com/r")
        resp = httpx.Response(302, text="Redirect", request=req)
        assert resp.is_success is False
        with pytest.raises(PulpToolHTTPError, match="Failed to test op"):
            mock_pulp_client._check_response(resp, "test op")

    def test_log_server_error_full_paths(self, mock_pulp_client, caplog) -> None:
        """Exercise request header redaction, body variants, and response body logging (lines 608-632, 647)."""
        caplog.set_level("ERROR", logger="root")

        base_url = "https://pulp.example.com/x"
        req = Mock(spec=httpx.Request)
        req.method = "GET"
        req.url = httpx.URL(base_url)
        req.headers = httpx.Headers(
            {
                "authorization": "secret",
                "cookie": "sid=1",
                "content-type": "application/json",
            }
        )
        big = b"x" * 2000
        req.content = big

        bad_json = "not json " + ("y" * 600)
        resp = httpx.Response(502, text=bad_json, request=req)
        mock_pulp_client._log_server_error(resp, "unit op")

        class _BytesDecodeRaises(bytes):
            def decode(self, encoding="utf-8", errors="replace"):
                raise RuntimeError("decode failed")

        req2 = Mock(spec=httpx.Request)
        req2.method = "GET"
        req2.url = httpx.URL(base_url)
        req2.headers = httpx.Headers({"content-type": "text/plain"})
        req2.content = _BytesDecodeRaises(b"\xffbad")
        resp2 = httpx.Response(502, text="e", request=req2)
        mock_pulp_client._log_server_error(resp2, "unit op")

        class _ReqStream(Mock):
            @property
            def content(self):
                raise httpx.RequestNotRead()

        req3 = _ReqStream(spec=httpx.Request)
        req3.method = "GET"
        req3.url = httpx.URL(base_url)
        req3.headers = httpx.Headers({"content-type": "multipart/form-data; boundary=x"})
        resp3 = httpx.Response(502, text="z", request=req3)
        mock_pulp_client._log_server_error(resp3, "unit op")

        req4 = _ReqStream(spec=httpx.Request)
        req4.method = "GET"
        req4.url = httpx.URL(base_url)
        req4.headers = httpx.Headers({"content-type": "application/octet-stream"})
        resp4 = httpx.Response(502, text="z", request=req4)
        mock_pulp_client._log_server_error(resp4, "unit op")

    def test_log_response_details_non_json_short_body(self, mock_pulp_client, caplog) -> None:
        """Non-JSON error body under 500 chars logs without truncation (lines 648-649)."""
        caplog.set_level("ERROR", logger="root")
        req = httpx.Request("GET", "https://pulp.example.com/x")
        resp = httpx.Response(502, text="plain error text", request=req)
        mock_pulp_client._log_response_details(resp)
        assert "Response Body: plain error text" in caplog.text
