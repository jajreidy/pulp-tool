"""
Helpers for Pulp operations that create file content and complete an async task.

Centralizes ``create_file_content`` → ``check_response`` → extract ``task`` →
``wait_for_finished_task`` so behavior and error handling stay consistent.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..models.pulp_api import TaskResponse
from .response_utils import check_task_success

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient  # pragma: no cover


def wait_for_successful_task(client: PulpClient, task_href: str, operation: str) -> TaskResponse:
    """
    Poll until the Pulp task finishes, then require ``state == completed``.

    Raises:
        ValueError: If the task ends in failed, canceled, or skipped state.
    """
    task_response = client.wait_for_finished_task(task_href)
    check_task_success(task_response, operation)
    return task_response


def create_file_content_and_wait(
    client: PulpClient,
    repository: str,
    content_or_path: str | Path,
    *,
    build_id: str,
    pulp_label: dict[str, Any],
    filename: str | None = None,
    arch: str | None = None,
    operation: str = "create file content",
) -> TaskResponse:
    """
    Upload file (path or in-memory string), validate HTTP status, wait for the Pulp task.

    Args:
        client: Pulp API client.
        repository: Target repository PRN.
        content_or_path: File path or string body (see ``create_file_content``).
        build_id: Build id for labels/path.
        pulp_label: Pulp labels dict.
        filename: Required when ``content_or_path`` is in-memory content.
        arch: Optional architecture segment for relative path.
        operation: Label for ``check_response`` error messages.

    Returns:
        Final ``TaskResponse`` after the task completes successfully.
    """
    response = client.create_file_content(
        repository,
        content_or_path,
        build_id=build_id,
        pulp_label=pulp_label,
        filename=filename,
        arch=arch,
    )
    client.check_response(response, operation)
    task_href = response.json()["task"]
    return wait_for_successful_task(client, task_href, operation)


__all__ = ["create_file_content_and_wait", "wait_for_successful_task"]
