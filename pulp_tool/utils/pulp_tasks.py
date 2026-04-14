"""
Helpers for Pulp operations that create file content and complete an async task.

Centralizes ``create_file_content`` → ``check_response`` → extract ``task`` →
``wait_for_finished_task`` so behavior and error handling stay consistent.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ..models.pulp_api import TaskResponse

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient


def create_file_content_and_wait(
    client: "PulpClient",
    repository: str,
    content_or_path: Union[str, Path],
    *,
    build_id: str,
    pulp_label: Dict[str, Any],
    filename: Optional[str] = None,
    arch: Optional[str] = None,
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
        Final ``TaskResponse`` after the task reaches a terminal state.
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
    return client.wait_for_finished_task(task_href)


__all__ = ["create_file_content_and_wait"]
