"""Tests for pulp_upload.py module."""

import json
import re
from unittest.mock import Mock, patch

import httpx
import pytest

from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.services.upload_service import (
    _extract_results_url,
    _serialize_results_to_json,
    _upload_and_get_results_url,
)

# CLI tests live under tests/cli/


class TestSerializeResultsToJson:
    """Test _serialize_results_to_json function."""

    def test_serialize_results_to_json_success(self):
        """Test successful JSON serialization."""
        results = {"content": "test", "number": 123}

        json_content = _serialize_results_to_json(results)

        assert isinstance(json_content, str)
        parsed = json.loads(json_content)
        assert parsed == results

    def test_serialize_results_to_json_error(self):
        """Test JSON serialization with error."""

        # Create an object that can't be serialized
        class Unserializable:
            pass

        results = {"content": "test", "unserializable": Unserializable()}

        with pytest.raises((TypeError, ValueError)):
            _serialize_results_to_json(results)


class TestUploadAndGetResultsUrl:
    """Test _upload_and_get_results_url function."""

    def test_upload_and_get_results_url_error(self, mock_pulp_client, httpx_mock):
        """Test results upload with error."""
        httpx_mock.post(re.compile(r".*/content/file/files/")).mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        args = Mock()
        args.build_id = "test-build"
        args.namespace = "test-namespace"
        args.parent_package = "test-package"

        with patch("pulp_tool.utils.create_labels", return_value={"build_id": "test-build"}):
            with pytest.raises(Exception):
                _upload_and_get_results_url(mock_pulp_client, args, "test-repo", "test json content", "2024-01-01")


class TestExtractResultsUrl:
    """Test _extract_results_url function."""

    def test_extract_results_url_success(self, mock_pulp_client):
        """Test successful results URL extraction."""
        args = Mock()
        args.build_id = "test-build"

        # Now task_response is a TaskResponse model, not a Mock
        # relative_path should just be the filename, not the full path
        task_response = TaskResponse(
            pulp_href="/api/v3/tasks/123/",
            state="completed",
            result={"relative_path": "pulp_results.json"},
        )

        # Mock PulpHelper and its get_distribution_urls method
        with patch("pulp_tool.services.upload_collect.PulpHelper") as MockPulpHelper:
            mock_helper = Mock()
            mock_helper.get_distribution_urls.return_value = {
                "artifacts": "https://pulp-content.example.com/test-domain/test-build/artifacts/"
            }
            MockPulpHelper.return_value = mock_helper

            result = _extract_results_url(mock_pulp_client, args, task_response)

            assert result == "https://pulp-content.example.com/test-domain/test-build/artifacts/pulp_results.json"
            mock_helper.get_distribution_urls.assert_called_once_with("test-build")
