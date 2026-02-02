"""
Tests for BaseResourceMixin error handling and edge cases.

This module tests error paths in BaseResourceMixin that need coverage.
"""

import pytest
from unittest.mock import Mock, patch
import httpx

from pulp_tool.api.base import BaseResourceMixin
from pulp_tool.models.pulp_api import RepositoryResponse


class TestBaseResourceMixin:
    """Test BaseResourceMixin error handling."""

    def test_parse_response_validation_error(self, mock_config):
        """Test _parse_response with ValidationError."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a response with invalid data
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = '{"invalid": "data"}'
        response.json = Mock(return_value={"invalid": "data"})

        with patch("pulp_tool.api.base.logging") as mock_logging:
            with pytest.raises(ValueError, match="Invalid response format"):
                mixin._parse_response(response, RepositoryResponse, "test operation")

            # Verify error logging was called
            assert mock_logging.error.called

    def test_parse_response_value_error(self, mock_config):
        """Test _parse_response with ValueError (invalid JSON)."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a response with invalid JSON
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = "not json"
        response.json = Mock(side_effect=ValueError("Invalid JSON"))

        with patch("pulp_tool.api.base.logging") as mock_logging:
            with pytest.raises(ValueError, match="Invalid JSON response"):
                mixin._parse_response(response, RepositoryResponse, "test operation")

            # Verify error logging was called
            assert mock_logging.error.called

    def test_parse_response_no_check_success(self, mock_config):
        """Test _parse_response with check_success=False."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a valid response
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.json = Mock(return_value={"pulp_href": "/test/", "name": "test"})

        result = mixin._parse_response(response, RepositoryResponse, "test operation", check_success=False)

        assert isinstance(result, RepositoryResponse)
        assert result.name == "test"
        # _check_response should not be called when check_success=False
        mixin._check_response.assert_not_called()

    def test_parse_list_response_validation_error(self, mock_config):
        """Test _parse_list_response with ValidationError."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a response with invalid data
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = '{"results": [{"invalid": "data"}]}'
        response.json = Mock(return_value={"results": [{"invalid": "data"}]})

        with patch("pulp_tool.api.base.logging") as mock_logging:
            with pytest.raises(ValueError, match="Invalid response format"):
                mixin._parse_list_response(response, RepositoryResponse, "test operation")

            # Verify error logging was called
            assert mock_logging.error.called

    def test_parse_list_response_key_error(self, mock_config):
        """Test _parse_list_response with KeyError (missing results)."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a response that will cause KeyError when accessing nested keys
        # The actual implementation uses json_data.get("results", []) which won't raise KeyError
        # But if results contains dicts without required fields, ValidationError will be raised
        # However, ValidationError is caught as (ValidationError, KeyError), so we test ValidationError path
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = '{"results": [{"invalid": "data"}]}'
        response.json = Mock(return_value={"results": [{"invalid": "data"}]})

        with patch("pulp_tool.api.base.logging") as mock_logging:
            # This will raise ValidationError because RepositoryResponse requires 'name' field
            with pytest.raises(ValueError, match="Invalid response format"):
                mixin._parse_list_response(response, RepositoryResponse, "test operation")

            # Verify error logging was called
            assert mock_logging.error.called

    def test_parse_list_response_value_error(self, mock_config):
        """Test _parse_list_response with ValueError (invalid JSON)."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a response with invalid JSON
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.text = "not json"
        response.json = Mock(side_effect=ValueError("Invalid JSON"))

        with patch("pulp_tool.api.base.logging") as mock_logging:
            with pytest.raises(ValueError, match="Invalid JSON response"):
                mixin._parse_list_response(response, RepositoryResponse, "test operation")

            # Verify error logging was called
            assert mock_logging.error.called

    def test_parse_list_response_no_check_success(self, mock_config):
        """Test _parse_list_response with check_success=False."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin._check_response = Mock()

        # Create a valid response
        response = Mock(spec=httpx.Response)
        response.status_code = 200
        response.json = Mock(return_value={"results": [{"pulp_href": "/test/", "name": "test"}]})

        result = mixin._parse_list_response(response, RepositoryResponse, "test operation", check_success=False)

        assert len(result) == 1
        assert isinstance(result[0], RepositoryResponse)
        # _check_response should not be called when check_success=False
        mixin._check_response.assert_not_called()

    def test_get_resource_with_name(self, mock_config, httpx_mock):
        """Test _get_resource with name parameter."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._url = Mock(return_value="https://pulp.example.com/api/v3/repositories/?")
        mixin._check_response = Mock()

        httpx_mock.get("https://pulp.example.com/api/v3/repositories/?name=test-repo&offset=0&limit=1").mock(
            return_value=httpx.Response(200, json={"results": [{"pulp_href": "/test/", "name": "test-repo"}]})
        )

        result = mixin._get_resource("api/v3/repositories/", RepositoryResponse, name="test-repo")

        assert isinstance(result, RepositoryResponse)
        assert result.name == "test-repo"

    def test_get_resource_no_results(self, mock_config, httpx_mock):
        """Test _get_resource when no results found."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._url = Mock(return_value="https://pulp.example.com/api/v3/repositories/?")
        mixin._check_response = Mock()

        httpx_mock.get("https://pulp.example.com/api/v3/repositories/?name=test-repo&offset=0&limit=1").mock(
            return_value=httpx.Response(200, json={"results": []})
        )

        with pytest.raises(ValueError, match="Resource not found"):
            mixin._get_resource("api/v3/repositories/", RepositoryResponse, name="test-repo")

    def test_get_resource_multiple_results(self, mock_config, httpx_mock):
        """Test _get_resource when multiple results found."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._url = Mock(return_value="https://pulp.example.com/api/v3/repositories/?")
        mixin._check_response = Mock()

        httpx_mock.get("https://pulp.example.com/api/v3/repositories/?name=test-repo&offset=0&limit=1").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": "/test/1/", "name": "test-repo"},
                        {"pulp_href": "/test/2/", "name": "test-repo"},
                    ]
                },
            )
        )

        with patch("pulp_tool.api.base.logging") as mock_logging:
            result = mixin._get_resource("api/v3/repositories/", RepositoryResponse, name="test-repo")

            assert isinstance(result, RepositoryResponse)
            # Should log warning about multiple results
            mock_logging.warning.assert_called()

    def test_list_resources_with_query_params(self, mock_config, httpx_mock):
        """Test _list_resources with query parameters."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._url = Mock(return_value="https://pulp.example.com/api/v3/repositories/?")
        mixin._check_response = Mock()

        httpx_mock.get("https://pulp.example.com/api/v3/repositories/?offset=0&limit=10").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [{"pulp_href": "/test/", "name": "test-repo"}],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mixin._list_resources(
            "api/v3/repositories/", RepositoryResponse, offset=0, limit=10
        )

        assert len(results) == 1
        assert next_url is None
        assert prev_url is None
        assert count == 1

    def test_create_resource(self, mock_config, httpx_mock):
        """Test _create_resource."""
        from pulp_tool.models.pulp_api import RepositoryRequest

        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._url = Mock(return_value="https://pulp.example.com/api/v3/repositories/")
        mixin._parse_response = Mock(return_value=RepositoryResponse(pulp_href="/test/", name="test-repo"))

        httpx_mock.post("https://pulp.example.com/api/v3/repositories/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/test/", "name": "test-repo"})
        )

        request = RepositoryRequest(name="test-repo")
        result = mixin._create_resource("api/v3/repositories/", request, RepositoryResponse, "create repository")

        assert isinstance(result, RepositoryResponse)
        assert result.name == "test-repo"

    def test_update_resource(self, mock_config, httpx_mock):
        """Test _update_resource."""
        from pulp_tool.models.pulp_api import RepositoryRequest

        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._parse_response = Mock(return_value=RepositoryResponse(pulp_href="/test/", name="test-repo-updated"))

        httpx_mock.patch("https://pulp.example.com/api/v3/repositories/test/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/test/", "name": "test-repo-updated"})
        )

        request = RepositoryRequest(name="test-repo-updated")
        result = mixin._update_resource("/api/v3/repositories/test/", request, RepositoryResponse, "update repository")

        assert isinstance(result, RepositoryResponse)
        assert result.name == "test-repo-updated"

    def test_delete_resource(self, mock_config, httpx_mock):
        """Test _delete_resource."""
        mixin = BaseResourceMixin()
        mixin.config = mock_config
        mixin.session = httpx.Client()
        mixin.timeout = 120
        mixin.request_params = {}
        mixin._check_response = Mock()

        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/test/").mock(return_value=httpx.Response(204))

        mixin._delete_resource("/api/v3/repositories/test/", "delete repository")

        mixin._check_response.assert_called_once()
