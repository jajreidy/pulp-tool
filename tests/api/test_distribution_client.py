"""
Tests for DistributionClient class.

This module contains comprehensive tests for the DistributionClient class
and related distribution functionality.
"""

import pytest
from unittest.mock import patch, mock_open
import httpx
from httpx import HTTPError

from pulp_tool.api import DistributionClient


class TestDistributionClient:
    """Test DistributionClient class functionality."""

    def test_init(self):
        """Test DistributionClient initialization."""
        client = DistributionClient("cert.pem", "key.pem")
        assert client.cert == "cert.pem"
        assert client.key == "key.pem"
        assert client.session is not None

    def test_create_session(self):
        """Test _create_session method."""
        client = DistributionClient("cert.pem", "key.pem")
        session = client._create_session()
        assert session is not None

    def test_pull_artifact(self, httpx_mock):
        """Test pull_artifact method."""
        client = DistributionClient("cert.pem", "key.pem")

        # Mock the artifact endpoint
        httpx_mock.get("https://example.com/artifacts.json").mock(
            return_value=httpx.Response(
                200,
                json={"artifacts": {"test.rpm": {"labels": {"build_id": "test"}}}},
            )
        )

        response = client.pull_artifact("https://example.com/artifacts.json")

        assert response.status_code == 200
        assert response.json()["artifacts"]["test.rpm"]["labels"]["build_id"] == "test"

    def test_pull_data(self, httpx_mock):
        """Test pull_data method."""
        httpx_mock.get("https://example.com/file.rpm").mock(
            return_value=httpx.Response(200, content=b"file content", headers={"content-length": "12"})
        )

        with patch("os.makedirs"), patch(
            "builtins.open", mock_open(read_data=b"file content")
        ) as mock_open_func, patch("pulp_tool.api.distribution_client.logging") as mock_logging:

            client = DistributionClient("cert.pem", "key.pem")
            result = client.pull_data("file.rpm", "https://example.com/file.rpm", "x86_64")

            assert result == "rpms/x86_64/file.rpm"
            mock_logging.info.assert_called()
            mock_open_func.assert_called_once_with("rpms/x86_64/file.rpm", "wb")

    def test_pull_data_async_success(self):
        """Test successful async data pull."""
        client = DistributionClient("/tmp/cert.pem", "/tmp/key.pem")
        download_info = ("test.rpm", "https://example.com/test.rpm", "x86_64", "rpm")

        with patch.object(client, "pull_data", return_value="/tmp/test.rpm"):
            result = client.pull_data_async(download_info)

            assert result == ("test.rpm", "/tmp/test.rpm")

    def test_pull_data_async_exception(self):
        """Test async data pull with exception."""
        client = DistributionClient("/tmp/cert.pem", "/tmp/key.pem")
        download_info = ("test.rpm", "https://example.com/test.rpm", "x86_64", "rpm")

        with patch.object(client, "pull_data", side_effect=HTTPError("Network error")):
            with pytest.raises(HTTPError):
                client.pull_data_async(download_info)
