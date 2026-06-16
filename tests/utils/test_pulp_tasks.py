"""Tests for pulp_task helpers."""

import os
from unittest.mock import patch

import httpx
import pytest
from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.utils.pulp_tasks import (
    FileContentCreateResponse,
    FileContentUploadResult,
    _relative_path_fallback,
    resolve_file_content_create,
    submit_file_content_create,
    upload_file_content,
)


def test_relative_path_fallback_returns_none_for_missing_path() -> None:
    assert _relative_path_fallback(filename=None, arch=None, content_or_path="/does/not/exist") is None


def test_resolve_file_content_create_skips_failed_task(mock_pulp_client) -> None:
    created = FileContentCreateResponse(task_href="/pulp/api/v3/tasks/failed/")
    with patch.object(
        mock_pulp_client,
        "wait_for_finished_task",
        return_value=TaskResponse(
            pulp_href="/pulp/api/v3/tasks/failed/",
            state="failed",
            error={"description": "worker died"},
        ),
    ):
        assert resolve_file_content_create(mock_pulp_client, created, "/tmp/a.log") is None


def test_resolve_file_content_create_empty_response(mock_pulp_client) -> None:
    assert resolve_file_content_create(mock_pulp_client, FileContentCreateResponse(), "/tmp/a.log") is None


def test_resolve_file_content_create_skips_incomplete_task(mock_pulp_client) -> None:
    created = FileContentCreateResponse(task_href="/pulp/api/v3/tasks/waiting/")
    with patch.object(
        mock_pulp_client,
        "wait_for_finished_task",
        return_value=TaskResponse(pulp_href="/pulp/api/v3/tasks/waiting/", state="waiting"),
    ):
        assert resolve_file_content_create(mock_pulp_client, created, "/tmp/a.log") is None


def test_upload_file_content_task_response(mock_pulp_client, httpx_mock) -> None:
    """POST file content without repository; poll task until complete."""
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/abc/"})
    )
    httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/abc/").mock(
        return_value=httpx.Response(
            200,
            json={
                "pulp_href": "/pulp/api/v3/tasks/abc/",
                "state": "completed",
                "created_resources": ["/pulp/api/v3/content/file/files/1/"],
                "result": {"relative_path": "x.json"},
            },
        )
    )
    result = upload_file_content(
        mock_pulp_client,
        "/tmp/x.json",
        build_id="b1",
        pulp_label={"build_id": "b1"},
        filename="x.json",
        operation="test upload",
    )
    assert isinstance(result, FileContentUploadResult)
    assert result.content_href == "/pulp/api/v3/content/file/files/1/"
    assert result.relative_path == "x.json"


def test_upload_file_content_immediate_href(mock_pulp_client, httpx_mock) -> None:
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"pulp_href": "/pulp/api/v3/content/file/files/9/"})
    )
    result = upload_file_content(
        mock_pulp_client,
        "/tmp/x.json",
        build_id="b1",
        pulp_label={"build_id": "b1"},
        filename="x.json",
    )
    assert result.content_href == "/pulp/api/v3/content/file/files/9/"


def test_upload_file_content_unexpected_response(mock_pulp_client, httpx_mock, temp_file) -> None:
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"status": "weird"})
    )
    with pytest.raises(ValueError, match="Unexpected create file content response"):
        upload_file_content(
            mock_pulp_client,
            temp_file,
            build_id="b1",
            pulp_label={"build_id": "b1"},
        )


def test_upload_file_content_task_without_content_href(mock_pulp_client, httpx_mock, temp_file) -> None:
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/abc/"})
    )
    httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/abc/").mock(
        return_value=httpx.Response(
            200,
            json={
                "pulp_href": "/pulp/api/v3/tasks/abc/",
                "state": "completed",
                "created_resources": [],
                "result": {},
            },
        )
    )
    created = submit_file_content_create(
        mock_pulp_client,
        temp_file,
        build_id="b1",
        pulp_label={"build_id": "b1"},
    )
    assert resolve_file_content_create(mock_pulp_client, created, temp_file) is None
    with pytest.raises(ValueError, match="did not produce a content href"):
        upload_file_content(
            mock_pulp_client,
            temp_file,
            build_id="b1",
            pulp_label={"build_id": "b1"},
        )


def test_upload_file_content_task_href_in_result(mock_pulp_client, httpx_mock, temp_file) -> None:
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/abc/"})
    )
    httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/abc/").mock(
        return_value=httpx.Response(
            200,
            json={
                "pulp_href": "/pulp/api/v3/tasks/abc/",
                "state": "completed",
                "created_resources": [],
                "result": {"pulp_href": "/pulp/api/v3/content/file/files/result/"},
            },
        )
    )
    result = upload_file_content(
        mock_pulp_client,
        temp_file,
        build_id="b1",
        pulp_label={"build_id": "b1"},
    )
    assert result.content_href == "/pulp/api/v3/content/file/files/result/"
    assert result.relative_path == os.path.basename(temp_file)
