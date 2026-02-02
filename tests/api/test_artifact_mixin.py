"""
Tests for ArtifactMixin.

This module tests ArtifactMixin methods that need coverage.
"""

from unittest.mock import Mock
import httpx

from pulp_tool.models.pulp_api import ArtifactResponse


class TestArtifactMixin:
    """Test ArtifactMixin methods."""

    def test_get_artifact(self, mock_pulp_client, httpx_mock):
        """Test get_artifact method."""
        httpx_mock.get("https://pulp.example.com/api/v3/artifacts/12345/").mock(
            return_value=httpx.Response(
                200,
                json={"pulp_href": "/api/v3/artifacts/12345/", "file": "test.txt", "sha256": "abc123", "size": 1024},
            )
        )

        result = mock_pulp_client.get_artifact("/api/v3/artifacts/12345/")

        assert isinstance(result, ArtifactResponse)
        assert result.pulp_href == "/api/v3/artifacts/12345/"
        assert result.size == 1024

    def test_list_artifacts(self, mock_pulp_client, httpx_mock):
        """Test list_artifacts method."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [{"pulp_href": "/api/v3/artifacts/12345/", "file": "test.txt", "size": 1024}],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mock_pulp_client.list_artifacts()

        assert len(results) == 1
        assert isinstance(results[0], ArtifactResponse)
        assert count == 1

    def test_get_file_locations_with_chunked_get(self, mock_pulp_client, httpx_mock):
        """Test ArtifactMixin.get_file_locations with _chunked_get available."""
        from pulp_tool.api.artifacts.operations import ArtifactMixin

        # Create a class that uses ArtifactMixin directly
        class TestClient(ArtifactMixin):
            def __init__(self, config, session):
                self.config = config
                self.session = session
                self.timeout = 120
                self.request_params = {}

            def _url(self, endpoint):
                return f"{self.config['base_url']}/{endpoint}"

            def _check_response(self, response, operation):
                pass

        # Create test client instance
        test_client = TestClient(mock_pulp_client.config, mock_pulp_client.session)
        test_client._chunked_get = Mock()
        test_client._url = Mock(return_value="https://pulp.example.com/api/v3/artifacts/")
        mock_response = Mock()
        mock_response.json = Mock(
            return_value={
                "results": [
                    {"pulp_href": "/api/v3/artifacts/12345/", "file": "test.txt", "sha256": "abc123", "size": 1024},
                    {"pulp_href": "/api/v3/artifacts/67890/", "file": "test2.txt", "sha256": "def456", "size": 2048},
                ]
            }
        )
        test_client._chunked_get.return_value = mock_response

        # ArtifactMixin.get_file_locations takes List[str] of artifact hrefs
        artifact_hrefs = ["/api/v3/artifacts/12345/", "/api/v3/artifacts/67890/"]
        result = test_client.get_file_locations(artifact_hrefs)

        assert len(result) == 2
        assert isinstance(result[0], ArtifactResponse)
        assert result[0].pulp_href == "/api/v3/artifacts/12345/"
        # Verify _chunked_get was called with correct parameters
        test_client._chunked_get.assert_called_once()
        call_args = test_client._chunked_get.call_args
        assert "pulp_href__in" in call_args[1]["params"]

    def test_get_file_locations_fallback(self, mock_pulp_client, httpx_mock):
        """Test get_file_locations fallback when _chunked_get not available."""
        from pulp_tool.api.artifacts.operations import ArtifactMixin

        # Create a class that uses ArtifactMixin directly without _chunked_get
        class TestClient(ArtifactMixin):
            def __init__(self, config, session):
                self.config = config
                self.session = session
                self.timeout = 120
                self.request_params = {}

            def _url(self, endpoint):
                # Match PulpClient._url format
                domain = self.config.get("domain", "test-domain")
                api_root = self.config.get("api_root", "/pulp/api/v3")
                import os

                relative = os.path.normpath(f"{api_root}/{domain}/{endpoint}")
                if endpoint.endswith("/"):
                    relative += "/"
                return f"{self.config['base_url']}{relative}"

            def _check_response(self, response, operation):
                pass

        test_client = TestClient(mock_pulp_client.config, mock_pulp_client.session)
        # Ensure _chunked_get is not available
        assert not hasattr(test_client, "_chunked_get")

        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/?"
            "pulp_href__in=/api/v3/artifacts/12345/,/api/v3/artifacts/67890/"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": "/api/v3/artifacts/12345/", "file": "test.txt", "size": 1024},
                        {"pulp_href": "/api/v3/artifacts/67890/", "file": "test2.txt", "size": 2048},
                    ],
                    "next": None,
                    "previous": None,
                    "count": 2,
                },
            )
        )

        artifact_hrefs = ["/api/v3/artifacts/12345/", "/api/v3/artifacts/67890/"]
        result = test_client.get_file_locations(artifact_hrefs)

        assert len(result) == 2
        assert isinstance(result[0], ArtifactResponse)
