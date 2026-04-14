"""Tests for pulp_task helpers."""

import httpx
import pytest

from pulp_tool.utils.pulp_tasks import create_file_content_and_wait
from pulp_tool.models.pulp_api import TaskResponse


def test_create_file_content_and_wait(mock_pulp_client, httpx_mock):
    """POST file content, check response, poll task until complete."""
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(202, json={"task": "/pulp/api/v3/tasks/abc/"})
    )
    httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/abc/").mock(
        return_value=httpx.Response(
            200,
            json={
                "pulp_href": "/pulp/api/v3/tasks/abc/",
                "state": "completed",
                "created_resources": ["/r/1/"],
            },
        )
    )

    task = create_file_content_and_wait(
        mock_pulp_client,
        "artifacts-prn",
        "/tmp/x.json",
        build_id="b1",
        pulp_label={"build_id": "b1"},
        filename="x.json",
        operation="test upload",
    )

    assert isinstance(task, TaskResponse)
    assert task.state == "completed"


def test_create_file_content_and_wait_http_error(mock_pulp_client, httpx_mock):
    httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/file/files/").mock(
        return_value=httpx.Response(500, text="no")
    )

    with pytest.raises(httpx.HTTPError):
        create_file_content_and_wait(
            mock_pulp_client,
            "prn",
            "/tmp/x.json",
            build_id="b1",
            pulp_label={"build_id": "b1"},
            filename="x.json",
            operation="test",
        )
