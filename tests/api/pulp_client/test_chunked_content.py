"""PulpClient tests (split module)."""

import asyncio
import json
from unittest.mock import patch
import httpx
import pytest


class TestPulpClient:

    def test_chunked_get_no_chunking(self, mock_pulp_client, httpx_mock) -> None:
        """Test _chunked_get method without chunking."""
        httpx_mock.get("https://test.com/api").mock(
            return_value=httpx.Response(200, json={"results": [{"id": 1}, {"id": 2}]})
        )
        result = mock_pulp_client._chunked_get("https://test.com/api", {"param": "value"})
        assert result.status_code == 200
        assert len(result.json()["results"]) == 2

    def test_chunked_get_async_no_chunking(self, mock_pulp_client, httpx_mock) -> None:
        """``_chunked_get_async`` delegates to the shared chunked GET implementation."""
        httpx_mock.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"results": [{"id": 1}]}))

        async def _run() -> httpx.Response:
            return await mock_pulp_client._chunked_get_async("https://test.com/api", {"param": "value"})

        result = asyncio.run(_run())
        assert result.status_code == 200
        assert result.json()["results"][0]["id"] == 1

    def test_chunked_get_module_raises_when_event_loop_running(self, mock_pulp_client) -> None:
        """``chunked_get`` must not run sync wrapper inside async context (line 119)."""
        from pulp_tool.api.pulp_client.chunked_get import chunked_get

        async def _inner() -> None:
            with pytest.raises(RuntimeError, match="_chunked_get called from async context"):
                chunked_get(mock_pulp_client, "https://test.com/api")

        asyncio.run(_inner())

    def test_chunked_get_with_chunking(self, mock_pulp_client, httpx_mock) -> None:
        """Test _chunked_get method with chunking."""
        large_param = ",".join([f"item{i}" for i in range(100)])
        params = {"large_param": large_param}
        httpx_mock.get("https://test.com/api").mock(
            return_value=httpx.Response(200, json={"results": [{"id": i} for i in range(20)]})
        )
        with patch.object(mock_pulp_client, "_check_response"):
            result = mock_pulp_client._chunked_get(
                "https://test.com/api", params, chunk_param="large_param", chunk_size=20
            )
        assert result.status_code == 200
        assert len(result.json()["results"]) == 100

    def test_chunked_get_async_fallback_when_no_aggregated_response(self, mock_pulp_client, httpx_mock) -> None:
        """Defensive fallback when chunk gather returns nothing still performs a checked GET."""
        large_param = ",".join([f"item{i}" for i in range(100)])
        params = {"large_param": large_param}
        httpx_mock.get("https://test.com/api").mock(return_value=httpx.Response(200, json={"results": [], "count": 0}))

        async def _gather_returns_empty(*_a: object, **_kw: object) -> list:
            return []

        with patch("pulp_tool.api.pulp_client.chunked_get.asyncio.gather", side_effect=_gather_returns_empty):
            result = mock_pulp_client._chunked_get(
                "https://test.com/api", params, chunk_param="large_param", chunk_size=20
            )
        assert result.status_code == 200
        assert result.json()["results"] == []

    def test_upload_content_rpm(self, mock_pulp_client, temp_rpm_file, httpx_mock) -> None:
        """Test upload_content method for RPM."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/upload/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )
        labels = {"build_id": "test-build", "arch": "x86_64"}
        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_rpm_file, labels, file_type="RPM", arch="x86_64")
        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_file(self, mock_pulp_client, temp_file, httpx_mock) -> None:
        """Test upload_content method for file."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"pulp_href": "/pulp/api/v3/content/12345/"})
        )
        labels = {"build_id": "test-build"}
        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            result = mock_pulp_client.upload_content(temp_file, labels, file_type="File")
        assert result == "/pulp/api/v3/content/12345/"

    def test_upload_content_missing_arch(self, mock_pulp_client, temp_file) -> None:
        """Test upload_content method with missing arch for RPM."""
        labels = {"build_id": "test-build"}
        with patch("pulp_tool.utils.validation.file.validate_file_path"):
            with pytest.raises(ValueError, match="arch parameter is required for RPM uploads"):
                mock_pulp_client.upload_content(temp_file, labels, file_type="RPM")

    def test_create_file_content_from_file(self, mock_pulp_client, temp_file, httpx_mock) -> None:
        """Test create_file_content method with file path."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        )
        labels = {"build_id": "test-build"}
        result = mock_pulp_client.create_file_content("test-repo", temp_file, build_id="test-build", pulp_label=labels)
        assert result.status_code == 202
        assert result.json()["task"] == "/pulp/api/v3/tasks/12345/"

    def test_create_file_content_from_string(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_file_content method with string content."""
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

    def test_create_file_content_missing_filename(self, mock_pulp_client) -> None:
        """Test create_file_content method with missing filename for string content."""
        labels = {"build_id": "test-build"}
        content = '{"test": "data"}'
        with pytest.raises(ValueError, match="filename is required when providing in-memory content"):
            mock_pulp_client.create_file_content("test-repo", content, build_id="test-build", pulp_label=labels)

    def test_add_content(self, mock_pulp_client, httpx_mock) -> None:
        """Test add_content method."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/repositories/rpm/rpm/12345/modify/").mock(
            return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/67890/"})
        )
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/67890/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/pulp/api/v3/tasks/67890/", "state": "completed", "created_resources": []}
            )
        )
        artifacts = ["/pulp/api/v3/content/12345/", "/pulp/api/v3/content/67890/"]
        result = mock_pulp_client.add_content("/pulp/api/v3/repositories/rpm/rpm/12345/", artifacts)
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.pulp_href == "/pulp/api/v3/tasks/67890/"
        assert result.state == "completed"

    def test_modify_repository_content_remove_only(self, mock_pulp_client, httpx_mock) -> None:
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

    def test_modify_repository_content_requires_add_or_remove(self, mock_pulp_client) -> None:
        """modify_repository_content raises if both add and remove are empty."""
        with pytest.raises(ValueError, match="modify_repository_content requires"):
            mock_pulp_client.modify_repository_content("/pulp/api/v3/repositories/rpm/rpm/1/")

    def test_modify_repository_content_add_and_remove(self, mock_pulp_client, httpx_mock) -> None:
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
            "/pulp/api/v3/repositories/rpm/rpm/99999/", add_content_units=["/add/1/"], remove_content_units=["/rm/1/"]
        )
        assert posted["body"] == {"add_content_units": ["/add/1/"], "remove_content_units": ["/rm/1/"]}
