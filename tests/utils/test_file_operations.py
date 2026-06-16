"""Tests for file_operations helpers."""

from unittest.mock import Mock, patch

import pytest

from pulp_tool.models.pulp_api import TaskResponse
from pulp_tool.utils.file_operations import (
    FileRepositoryBatch,
    FileUploadSpec,
    add_file_content_to_repository,
    upload_files_parallel,
)


def test_add_file_content_to_repository_noop(mock_pulp_client) -> None:
    assert add_file_content_to_repository(mock_pulp_client, "/repo/", []) == []


def test_add_file_content_to_repository_requires_href(mock_pulp_client) -> None:
    with pytest.raises(ValueError, match="repository_href is required"):
        add_file_content_to_repository(mock_pulp_client, "", ["/content/1/"])


def test_add_file_content_to_repository_batches(mock_pulp_client) -> None:
    mock_task = TaskResponse(pulp_href="/tasks/1/", state="pending", created_resources=[])
    final_task = TaskResponse(pulp_href="/tasks/1/", state="completed", created_resources=["/version/1/"])
    mock_pulp_client.add_content = Mock(return_value=mock_task)
    mock_pulp_client.wait_for_finished_task = Mock(return_value=final_task)
    created = add_file_content_to_repository(mock_pulp_client, "/repo/", ["/content/1/", "/content/2/"])
    assert created == ["/version/1/"]
    mock_pulp_client.add_content.assert_called_once_with("/repo/", ["/content/1/", "/content/2/"])


def test_add_file_content_to_repository_incomplete_modify(mock_pulp_client) -> None:
    mock_task = TaskResponse(pulp_href="/tasks/1/", state="pending", created_resources=[])
    final_task = TaskResponse(pulp_href="/tasks/1/", state="waiting", created_resources=[])
    mock_pulp_client.add_content = Mock(return_value=mock_task)
    mock_pulp_client.wait_for_finished_task = Mock(return_value=final_task)
    assert add_file_content_to_repository(mock_pulp_client, "/repo/", ["/content/1/"]) == []


def test_add_file_content_to_repository_no_created_resources(mock_pulp_client) -> None:
    mock_task = TaskResponse(pulp_href="/tasks/1/", state="pending", created_resources=[])
    final_task = TaskResponse(pulp_href="/tasks/1/", state="completed", created_resources=[])
    mock_pulp_client.add_content = Mock(return_value=mock_task)
    mock_pulp_client.wait_for_finished_task = Mock(return_value=final_task)
    assert add_file_content_to_repository(mock_pulp_client, "/repo/", ["/content/1/"]) == []


def test_file_repository_batch_flush_once(mock_pulp_client) -> None:
    batch = FileRepositoryBatch()
    batch.add_log("/log/1/")
    with patch(
        "pulp_tool.utils.file_operations.add_file_content_to_repository",
        return_value=["/v/1/"],
    ) as mock_add:
        first = batch.flush_logs(mock_pulp_client, "/logs/")
        second = batch.flush_logs(mock_pulp_client, "/logs/")
    assert first == ["/v/1/"]
    assert second == []
    mock_add.assert_called_once()


def test_file_repository_batch_flush_sbom_and_artifacts_once(mock_pulp_client) -> None:
    batch = FileRepositoryBatch()
    batch.add_sbom("/sbom/1/")
    batch.add_artifact("/artifact/1/")
    with patch(
        "pulp_tool.utils.file_operations.add_file_content_to_repository",
        return_value=["/v/1/"],
    ) as mock_add:
        assert batch.flush_sbom(mock_pulp_client, "/sbom/") == ["/v/1/"]
        assert batch.flush_sbom(mock_pulp_client, "/sbom/") == []
        assert batch.flush_artifacts(mock_pulp_client, "/artifacts/") == ["/v/1/"]
        assert batch.flush_artifacts(mock_pulp_client, "/artifacts/") == []
    assert mock_add.call_count == 2


def test_upload_files_parallel_empty_specs(mock_pulp_client) -> None:
    assert upload_files_parallel(mock_pulp_client, []) == []


def test_upload_files_parallel_collects_successes(mock_pulp_client) -> None:
    from pulp_tool.utils.pulp_tasks import FileContentCreateResponse, FileContentUploadResult

    specs = [
        FileUploadSpec(
            content_or_path="/a.log",
            labels={"build_id": "b"},
            local_key="/a.log",
            build_id="b",
        )
    ]
    with (
        patch(
            "pulp_tool.utils.file_operations.submit_file_content_create",
            return_value=FileContentCreateResponse(pulp_href="/content/1/"),
        ),
        patch(
            "pulp_tool.utils.file_operations.resolve_file_content_create",
            return_value=FileContentUploadResult(content_href="/content/1/"),
        ),
    ):
        pairs = upload_files_parallel(mock_pulp_client, specs)
    assert pairs == [("/a.log", FileContentUploadResult(content_href="/content/1/"))]


def test_upload_files_parallel_logs_task_wait(mock_pulp_client) -> None:
    from pulp_tool.utils.pulp_tasks import FileContentCreateResponse, FileContentUploadResult

    specs = [
        FileUploadSpec(
            content_or_path="/a.log",
            labels={"build_id": "b"},
            local_key="/a.log",
            build_id="b",
        )
    ]
    with (
        patch(
            "pulp_tool.utils.file_operations.submit_file_content_create",
            return_value=FileContentCreateResponse(task_href="/tasks/1/"),
        ),
        patch(
            "pulp_tool.utils.file_operations.resolve_file_content_create",
            return_value=FileContentUploadResult(content_href="/content/1/"),
        ),
        patch("pulp_tool.utils.file_operations.logging") as mock_logging,
    ):
        pairs = upload_files_parallel(mock_pulp_client, specs)
    assert len(pairs) == 1
    mock_logging.warning.assert_called()


def test_upload_files_parallel_submit_failure(mock_pulp_client) -> None:
    specs = [
        FileUploadSpec(
            content_or_path="/a.log",
            labels={"build_id": "b"},
            local_key="/a.log",
            build_id="b",
        )
    ]
    with (
        patch(
            "pulp_tool.utils.file_operations.submit_file_content_create",
            side_effect=RuntimeError("submit failed"),
        ),
        patch("pulp_tool.utils.file_operations.logging") as mock_logging,
    ):
        assert upload_files_parallel(mock_pulp_client, specs) == []
    mock_logging.error.assert_called()


def test_upload_files_parallel_resolve_failure(mock_pulp_client) -> None:
    from pulp_tool.utils.pulp_tasks import FileContentCreateResponse

    specs = [
        FileUploadSpec(
            content_or_path="/a.log",
            labels={"build_id": "b"},
            local_key="/a.log",
            build_id="b",
        )
    ]
    with (
        patch(
            "pulp_tool.utils.file_operations.submit_file_content_create",
            return_value=FileContentCreateResponse(pulp_href="/content/1/"),
        ),
        patch(
            "pulp_tool.utils.file_operations.resolve_file_content_create",
            side_effect=RuntimeError("resolve failed"),
        ),
        patch("pulp_tool.utils.file_operations.logging") as mock_logging,
    ):
        assert upload_files_parallel(mock_pulp_client, specs) == []
    mock_logging.error.assert_called()


def test_upload_files_parallel_skips_none_resolve(mock_pulp_client) -> None:
    from pulp_tool.utils.pulp_tasks import FileContentCreateResponse

    specs = [
        FileUploadSpec(
            content_or_path="/a.log",
            labels={"build_id": "b"},
            local_key="/a.log",
            build_id="b",
        )
    ]
    with (
        patch(
            "pulp_tool.utils.file_operations.submit_file_content_create",
            return_value=FileContentCreateResponse(task_href="/tasks/1/"),
        ),
        patch(
            "pulp_tool.utils.file_operations.resolve_file_content_create",
            return_value=None,
        ),
    ):
        assert upload_files_parallel(mock_pulp_client, specs) == []
