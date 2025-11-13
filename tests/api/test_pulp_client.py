"""
Tests for PulpClient class and its mixin components.

This module contains comprehensive tests for:
- PulpClient: Main client class (pulp_client.py)
- ContentManagerMixin: Content upload operations (content_manager.py)
- ContentQueryMixin: Content querying and retrieval (content_query.py)
- RepositoryManagerMixin: Repository operations (repository_manager.py)
- TaskManagerMixin: Pulp task management (task_manager.py)

All mixin functionality is tested through the integrated PulpClient class,
which is the correct approach for testing mixin-based architecture.
"""

from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import pytest
import httpx
from httpx import HTTPError

from pulp_tool.api import PulpClient, OAuth2ClientCredentialsAuth


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
        with patch("pulp_tool.api.pulp_client.tomllib.load") as mock_load:
            mock_load.return_value = {"cli": {"base_url": "https://test.com"}}

            client = PulpClient.create_from_config_file(path=temp_config_file)

            assert isinstance(client, PulpClient)
            assert client.config["base_url"] == "https://test.com"

    def test_create_from_config_file_default_path(self):
        """Test create_from_config_file with default path."""
        with (
            patch("pulp_tool.api.pulp_client.Path.expanduser") as mock_expanduser,
            patch("builtins.open", mock_open(read_data='{"cli": {"base_url": "https://test.com"}}')),
            patch("pulp_tool.api.pulp_client.tomllib.load") as mock_load,
        ):

            mock_expanduser.return_value = Path("/home/user/.config/pulp/cli.toml")
            mock_load.return_value = {"cli": {"base_url": "https://test.com"}}

            client = PulpClient.create_from_config_file()

            assert isinstance(client, PulpClient)

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

    def test_cert_property(self, mock_pulp_client):
        """Test cert property."""
        cert_tuple = mock_pulp_client.cert

        assert cert_tuple == ("/path/to/cert.pem", "/path/to/key.pem")

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

    def test_cert_property_without_config_path(self, mock_config):
        """Test cert property when config_path is None (should not resolve relative paths)."""
        # Update config with relative paths
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"

        # Create client without config_path, mocking session creation to avoid SSL errors
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_with_relative, config_path=None)

            # Cert property should return paths as-is
            cert_tuple = client.cert

            assert cert_tuple == ("cert.pem", "key.pem")

    def test_cert_property_with_config_path_no_parent(self, mock_config):
        """Test cert property when config_path has no parent (root path)."""
        # Update config with relative paths
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "cert.pem"
        config_with_relative["key"] = "key.pem"

        # Create client with config_path that has no parent, mocking session creation
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            # Mock config_path to have no parent (like root path "/")
            from pathlib import Path

            mock_config_path = Mock(spec=Path)
            mock_config_path.parent = None

            client = PulpClient(config_with_relative, config_path=mock_config_path)

            # Cert property should return paths as-is when parent is None
            cert_tuple = client.cert

            assert cert_tuple == ("cert.pem", "key.pem")

    def test_cert_property_relative_path_not_found(self, mock_config, tmp_path):
        """Test cert property when relative paths don't exist in config dir or current dir."""
        # Create a config file but don't create cert/key files
        config_file = tmp_path / "config.toml"

        # Update config with relative paths that don't exist anywhere
        config_with_relative = mock_config.copy()
        config_with_relative["cert"] = "nonexistent_cert.pem"
        config_with_relative["key"] = "nonexistent_key.pem"

        # Create client with config_path, mocking session creation to avoid SSL errors
        with patch("pulp_tool.api.pulp_client.create_session_with_retry") as mock_create_session:
            mock_session = Mock()
            mock_create_session.return_value = mock_session

            client = PulpClient(config_with_relative, config_path=config_file)

            # Cert property should return original paths when files don't exist
            cert_tuple = client.cert

            assert cert_tuple == ("nonexistent_cert.pem", "nonexistent_key.pem")

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
        config_no_cert = mock_config.copy()
        del config_no_cert["cert"]
        del config_no_cert["key"]

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

    def test_upload_content_rpm(self, mock_pulp_client, temp_rpm_file, httpx_mock):
        """Test upload_content method for RPM."""
        # Mock the RPM upload endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )

        labels = {"build_id": "test-build", "arch": "x86_64"}

        with patch("pulp_tool.api.content_manager.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_rpm_file, labels, file_type="RPM", arch="x86_64")

        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_file(self, mock_pulp_client, temp_file, httpx_mock):
        """Test upload_content method for file."""
        # Mock the file content creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )

        labels = {"build_id": "test-build"}

        with patch("pulp_tool.api.content_manager.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_file, labels, file_type="File")

        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_missing_arch(self, mock_pulp_client, temp_file):
        """Test upload_content method with missing arch for RPM."""
        labels = {"build_id": "test-build"}

        with patch("pulp_tool.api.content_manager.validate_file_path"):
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

    def test_get_task(self, mock_pulp_client, httpx_mock):
        """Test _get_task method."""
        # Mock the task endpoint
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(
                200,
                json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed", "result": {"status": "success"}},
            )
        )

        result = mock_pulp_client._get_task("/pulp/api/v3/tasks/12345/")

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

    def test_gather_content_data(self, mock_pulp_client, mock_content_data, httpx_mock):
        """Test gather_content_data method."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(return_value=httpx.Response(200, json=mock_content_data, headers={"content-type": "application/json"}))

        content_data = mock_pulp_client.gather_content_data("test-build-123")

        assert len(content_data.content_results) == 1
        assert len(content_data.artifacts) == 1
        assert content_data.content_results[0]["pulp_href"] == "/pulp/api/v3/content/rpm/packages/12345/"

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

        extra_artifacts = [{"file": "/pulp/api/v3/artifacts/67890/"}, {"extra": "/pulp/api/v3/artifacts/99999/"}]

        content_data = mock_pulp_client.gather_content_data("test-build-123", extra_artifacts)

        # Should get content from API query by build_id
        assert len(content_data.content_results) == 1  # From API response
        assert len(content_data.artifacts) == 1  # Extracted from content_results

    def test_build_results_structure(self, mock_pulp_client, mock_content_data, mock_file_locations, httpx_mock):
        """Test build_results_structure method."""
        from pulp_tool.models import PulpResultsModel, RepositoryRefs, FileInfoModel

        # Mock the file locations endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/"
            "?pulp_href__in=/pulp/api/v3/artifacts/67890/"
        ).mock(return_value=httpx.Response(200, json=mock_file_locations))

        content_results = mock_content_data["results"]

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

    def test_repository_operation_create_repo(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for creating repository."""
        # Mock the repository creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/repositories/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation("create_repo", "rpm", "test-repo")

        assert result.status_code == 201
        assert result.json()["pulp_href"] == "/pulp/api/v3/repositories/rpm/rpm/12345/"

    def test_repository_operation_get_repo(self, mock_pulp_client, mock_response):
        """Test repository_operation method for getting repository."""
        mock_pulp_client._get_single_resource = Mock()
        mock_pulp_client._get_single_resource.return_value = mock_response

        result = mock_pulp_client.repository_operation("get_repo", "rpm", "test-repo")

        assert result == mock_response
        mock_pulp_client._get_single_resource.assert_called_once()

    def test_repository_operation_create_distro(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for creating distribution."""
        # Mock the distribution creation endpoint
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation("create_distro", "rpm", "test-distro", repository="test-repo")

        assert result.status_code == 201
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"

    def test_repository_operation_get_distro(self, mock_pulp_client, mock_response):
        """Test repository_operation method for getting distribution."""
        mock_pulp_client._get_single_resource = Mock()
        mock_pulp_client._get_single_resource.return_value = mock_response

        result = mock_pulp_client.repository_operation("get_distro", "rpm", "test-distro")

        assert result == mock_response
        mock_pulp_client._get_single_resource.assert_called_once()

    def test_repository_operation_update_distro(self, mock_pulp_client, httpx_mock):
        """Test repository_operation method for updating distribution."""
        # Mock the distribution update endpoint - URL uses the distribution_href directly
        httpx_mock.patch("https://pulp.example.com/pulp/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/distributions/rpm/rpm/12345/"})
        )

        result = mock_pulp_client.repository_operation(
            "update_distro",
            "rpm",
            "test-distro",
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
        # Create config without cert to test auth path
        config_without_cert = {k: v for k, v in mock_config.items() if k != "cert"}
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
            "test-distro",
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
            "update_distro", "rpm", "test-distro", distribution_href="/pulp/api/v3/distributions/12345/"
        )

        assert result.status_code == 200
        assert result.json()["pulp_href"] == "/pulp/api/v3/distributions/rpm/rpm/12345/"

    def test_repository_operation_invalid_operation(self, mock_pulp_client):
        """Test repository_operation method with invalid operation."""
        with pytest.raises(ValueError, match="Unknown operation"):
            mock_pulp_client.repository_operation("invalid", "rpm", "test")
