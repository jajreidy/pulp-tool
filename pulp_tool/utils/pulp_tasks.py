"""
Helpers for Pulp file content creation and async task completion.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ..models.pulp_api import TaskResponse
from .constants import FILE_CONTENT_CREATE_TASK_TIMEOUT

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient  # pragma: no cover


@dataclass(frozen=True)
class FileContentUploadResult:
    """Result of creating file content without associating a repository."""

    content_href: str
    relative_path: Optional[str] = None


@dataclass(frozen=True)
class FileContentCreateResponse:
    """Immediate response from a file content POST (href and/or async task)."""

    pulp_href: Optional[str] = None
    relative_path: Optional[str] = None
    task_href: Optional[str] = None


def _relative_path_fallback(
    *,
    filename: Optional[str],
    arch: Optional[str],
    content_or_path: Union[str, Path],
) -> Optional[str]:
    if filename:
        return f"{arch}/{filename}" if arch else filename
    if isinstance(content_or_path, (str, Path)) and os.path.exists(str(content_or_path)):
        name = Path(content_or_path).name
        return f"{arch}/{name}" if arch else name
    return None


def _content_href_from_task(task: TaskResponse) -> str:
    for href in task.created_resources:
        if "content" in href:
            return href
    if task.result and isinstance(task.result, dict):
        raw_href = task.result.get("pulp_href")
        if isinstance(raw_href, str) and raw_href:
            return raw_href
    raise ValueError(f"Task {task.pulp_href} did not produce file content href (state: {task.state})")


def _relative_path_from_task(task: TaskResponse) -> Optional[str]:
    if task.result and isinstance(task.result, dict):
        rel = task.result.get("relative_path")
        if isinstance(rel, str) and rel:
            return rel
    return None


def _result_from_task(
    task: TaskResponse,
    *,
    filename: Optional[str],
    arch: Optional[str],
    content_or_path: Union[str, Path],
) -> Optional[FileContentUploadResult]:
    if not task.is_complete:
        logging.warning(
            "File content task %s did not finish (state: %s); skipping file",
            task.pulp_href,
            task.state,
        )
        return None
    if not task.is_successful:
        error_msg = task.error.get("description", "Unknown error") if task.error else "Unknown error"
        logging.warning("File content task %s failed: %s; skipping file", task.pulp_href, error_msg)
        return None
    try:
        href = _content_href_from_task(task)
    except ValueError as exc:
        logging.warning("%s; skipping file", exc)
        return None
    rel = _relative_path_from_task(task) or _relative_path_fallback(
        filename=filename, arch=arch, content_or_path=content_or_path
    )
    return FileContentUploadResult(content_href=href, relative_path=rel)


def submit_file_content_create(
    client: "PulpClient",
    content_or_path: Union[str, Path],
    *,
    build_id: str,
    pulp_label: Dict[str, Any],
    filename: Optional[str] = None,
    arch: Optional[str] = None,
    operation: str = "create file content",
) -> FileContentCreateResponse:
    """POST file content without waiting for an async task to finish."""
    response = client.create_file_content(
        None,
        content_or_path,
        build_id=build_id,
        pulp_label=pulp_label,
        filename=filename,
        arch=arch,
    )
    client.check_response(response, operation)
    data = response.json()

    if "pulp_href" in data:
        return FileContentCreateResponse(
            pulp_href=data["pulp_href"],
            relative_path=data.get("relative_path"),
        )
    if "task" in data:
        return FileContentCreateResponse(task_href=data["task"])
    raise ValueError(f"Unexpected create file content response keys: {sorted(data.keys())}")


def resolve_file_content_create(
    client: "PulpClient",
    created: FileContentCreateResponse,
    content_or_path: Union[str, Path],
    *,
    filename: Optional[str] = None,
    arch: Optional[str] = None,
    timeout: int = FILE_CONTENT_CREATE_TASK_TIMEOUT,
) -> Optional[FileContentUploadResult]:
    """Resolve a file content POST into a content href, waiting at most ``timeout`` seconds."""
    if created.pulp_href:
        rel = created.relative_path
        if not rel:
            rel = _relative_path_fallback(filename=filename, arch=arch, content_or_path=content_or_path)
        return FileContentUploadResult(content_href=created.pulp_href, relative_path=rel)

    if created.task_href:
        task = client.wait_for_finished_task(created.task_href, timeout=timeout)
        return _result_from_task(
            task,
            filename=filename,
            arch=arch,
            content_or_path=content_or_path,
        )

    return None


def upload_file_content(
    client: "PulpClient",
    content_or_path: Union[str, Path],
    *,
    build_id: str,
    pulp_label: Dict[str, Any],
    filename: Optional[str] = None,
    arch: Optional[str] = None,
    operation: str = "create file content",
    timeout: int = FILE_CONTENT_CREATE_TASK_TIMEOUT,
) -> FileContentUploadResult:
    """
    Create file content without associating a repository and return the content href.

    Waits only when Pulp returns an async task for the create request, using a shorter
    timeout than distribution or repository modify tasks.
    """
    created = submit_file_content_create(
        client,
        content_or_path,
        build_id=build_id,
        pulp_label=pulp_label,
        filename=filename,
        arch=arch,
        operation=operation,
    )
    result = resolve_file_content_create(
        client,
        created,
        content_or_path,
        filename=filename,
        arch=arch,
        timeout=timeout,
    )
    if result is None:
        raise ValueError(f"File content create did not produce a content href ({operation})")
    return result


__all__ = [
    "FileContentCreateResponse",
    "FileContentUploadResult",
    "submit_file_content_create",
    "resolve_file_content_create",
    "upload_file_content",
]
