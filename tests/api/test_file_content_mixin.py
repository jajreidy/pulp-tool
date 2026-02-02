"""
Tests for FileContentMixin.

This module tests FileContentMixin methods that need coverage.
"""

import httpx

from pulp_tool.models.pulp_api import FileResponse


class TestFileContentMixin:
    """Test FileContentMixin methods."""

    def test_get_file_content(self, mock_pulp_client, httpx_mock):
        """Test get_file_content method."""
        # get_file_content uses config["base_url"] + href directly
        httpx_mock.get("https://pulp.example.com/api/v3/content/file/files/12345/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": "/api/v3/content/file/files/12345/",
                    "relative_path": "test.txt",
                    "artifact": "/api/v3/artifacts/12345/",
                },
            )
        )

        result = mock_pulp_client.get_file_content("/api/v3/content/file/files/12345/")

        assert isinstance(result, FileResponse)
        assert result.pulp_href == "/api/v3/content/file/files/12345/"

    def test_list_file_content(self, mock_pulp_client, httpx_mock):
        """Test list_file_content method."""
        # list_file_content uses _list_resources which uses _url
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/?").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/content/file/files/12345/",
                            "relative_path": "test.txt",
                            "artifact": "/api/v3/artifacts/12345/",
                        }
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mock_pulp_client.list_file_content()

        assert len(results) == 1
        assert isinstance(results[0], FileResponse)
        assert count == 1

    def test_find_content_by_build_id(self, mock_pulp_client, httpx_mock):
        """Test find_content_by_build_id method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/"
            "?pulp_label_select=build_id~test-build-123"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/content/file/files/12345/",
                            "relative_path": "test.txt",
                            "artifact": "/api/v3/artifacts/12345/",
                        },
                        {
                            "pulp_href": "/api/v3/content/file/files/67890/",
                            "relative_path": "test2.txt",
                            "artifact": "/api/v3/artifacts/67890/",
                        },
                    ],
                    "next": None,
                    "previous": None,
                    "count": 2,
                },
            )
        )

        results = mock_pulp_client.find_content_by_build_id("test-build-123")

        assert len(results) == 2
        assert isinstance(results[0], FileResponse)
        assert results[0].relative_path == "test.txt"

    def test_find_content_by_hrefs(self, mock_pulp_client, httpx_mock):
        """Test find_content_by_hrefs method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/"
            "?pulp_href__in=/api/v3/content/file/files/12345/,/api/v3/content/file/files/67890/"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/content/file/files/12345/",
                            "relative_path": "test.txt",
                            "artifact": "/api/v3/artifacts/12345/",
                        },
                        {
                            "pulp_href": "/api/v3/content/file/files/67890/",
                            "relative_path": "test2.txt",
                            "artifact": "/api/v3/artifacts/67890/",
                        },
                    ],
                    "next": None,
                    "previous": None,
                    "count": 2,
                },
            )
        )

        hrefs = ["/api/v3/content/file/files/12345/", "/api/v3/content/file/files/67890/"]
        results = mock_pulp_client.find_content_by_hrefs(hrefs)

        assert len(results) == 2
        assert isinstance(results[0], FileResponse)

    def test_add_content_fallback(self, mock_pulp_client, httpx_mock):
        """Test add_content fallback when get_task not available."""
        # Create a mock client without get_task
        from pulp_tool.api.content.file_files import FileContentMixin

        class TestClient(FileContentMixin):
            def __init__(self, config, session):
                self.config = config
                self.session = session
                self.timeout = 120
                self.request_params = {}

            def _url(self, endpoint):
                return f"{self.config['base_url']}/{endpoint}"

            def _check_response(self, response, operation):
                pass

        test_client = TestClient(mock_pulp_client.config, mock_pulp_client.session)

        httpx_mock.post("https://pulp.example.com/api/v3/repositories/test/modify/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )

        httpx_mock.get("https://pulp.example.com/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/api/v3/tasks/12345/", "state": "completed", "result": {}}
            )
        )

        artifacts = ["/api/v3/artifacts/12345/"]
        result = test_client.add_content("/api/v3/repositories/test/", artifacts)

        assert result is not None
