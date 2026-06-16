"""
Parallel file content uploads and batched repository modify operations.

File uploads follow the same two-phase pattern as RPMs:
1. Create file content without associating a repository (parallel).
2. Add all content hrefs to the target file repository in a single modify call.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

from .constants import (
    DEFAULT_MAX_WORKERS,
    FILE_CONTENT_CREATE_TASK_TIMEOUT,
    FILE_REPOSITORY_MODIFY_TASK_TIMEOUT,
)
from .pulp_tasks import (
    FileContentCreateResponse,
    FileContentUploadResult,
    resolve_file_content_create,
    submit_file_content_create,
)

if TYPE_CHECKING:
    from ..api.pulp_client import PulpClient


@dataclass(frozen=True)
class FileUploadSpec:
    """Specification for a single file content upload (phase 1)."""

    content_or_path: Union[str, Path]
    labels: Dict[str, str]
    local_key: str
    build_id: str
    arch: Optional[str] = None
    filename: Optional[str] = None
    operation: Optional[str] = None


@dataclass
class FileRepositoryBatch:
    """Pending file content hrefs grouped by target repository for batched modify."""

    logs: List[str] = field(default_factory=list)
    sbom: List[str] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    results_json_relative_path: Optional[str] = None
    _logs_flushed: bool = field(default=False, repr=False)
    _sbom_flushed: bool = field(default=False, repr=False)
    _artifacts_flushed: bool = field(default=False, repr=False)

    def add_log(self, content_href: str) -> None:
        self.logs.append(content_href)

    def add_sbom(self, content_href: str) -> None:
        self.sbom.append(content_href)

    def add_artifact(self, content_href: str) -> None:
        self.artifacts.append(content_href)

    def flush_logs(self, client: "PulpClient", logs_href: str) -> List[str]:
        if self._logs_flushed:
            return []
        self._logs_flushed = True
        return add_file_content_to_repository(client, logs_href, self.logs)

    def flush_sbom(self, client: "PulpClient", sbom_href: str) -> List[str]:
        if self._sbom_flushed:
            return []
        self._sbom_flushed = True
        return add_file_content_to_repository(client, sbom_href, self.sbom)

    def flush_artifacts(self, client: "PulpClient", artifacts_href: str) -> List[str]:
        if self._artifacts_flushed:
            return []
        self._artifacts_flushed = True
        return add_file_content_to_repository(client, artifacts_href, self.artifacts)


def upload_files_parallel(
    client: "PulpClient",
    specs: List[FileUploadSpec],
) -> List[Tuple[str, FileContentUploadResult]]:
    """
    Upload multiple file contents in parallel without associating a repository.

    Submits all create requests first, then resolves async tasks in parallel so a slow
    Pulp queue does not serialize waits behind the default 30-minute task timeout.

    Returns:
        List of (local_key, upload result) for successful uploads.
    """
    if not specs:
        return []

    pending: List[Tuple[str, FileContentCreateResponse, FileUploadSpec]] = []
    with ThreadPoolExecutor(thread_name_prefix="upload_files_submit", max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_to_spec = {
            executor.submit(
                submit_file_content_create,
                client,
                spec.content_or_path,
                build_id=spec.build_id,
                pulp_label=spec.labels,
                filename=spec.filename,
                arch=spec.arch,
                operation=spec.operation or f"upload file {spec.local_key}",
            ): spec
            for spec in specs
        }
        for submit_future in as_completed(future_to_spec):
            spec = future_to_spec[submit_future]
            try:
                created = submit_future.result()
                pending.append((spec.local_key, created, spec))
            except Exception as e:  # pylint: disable=broad-except
                logging.error("Failed to submit file upload %s: %s", spec.local_key, e)

    if not pending:
        return []

    task_count = sum(1 for _, created, _ in pending if created.task_href)
    if task_count:
        logging.warning(
            "Waiting for %d file content task(s) (timeout: %ds each)",
            task_count,
            FILE_CONTENT_CREATE_TASK_TIMEOUT,
        )

    results: List[Tuple[str, FileContentUploadResult]] = []
    with ThreadPoolExecutor(thread_name_prefix="upload_files_resolve", max_workers=DEFAULT_MAX_WORKERS) as executor:
        resolve_future_to_key = {
            executor.submit(
                resolve_file_content_create,
                client,
                created,
                spec.content_or_path,
                filename=spec.filename,
                arch=spec.arch,
                timeout=FILE_CONTENT_CREATE_TASK_TIMEOUT,
            ): local_key
            for local_key, created, spec in pending
        }
        for resolve_future in as_completed(resolve_future_to_key):
            local_key = resolve_future_to_key[resolve_future]
            try:
                upload_result = resolve_future.result()
                if upload_result is not None:
                    results.append((local_key, upload_result))
            except Exception as e:  # pylint: disable=broad-except
                logging.error("Failed to upload file %s: %s", local_key, e)

    return results


def add_file_content_to_repository(
    client: "PulpClient",
    repository_href: str,
    content_hrefs: List[str],
) -> List[str]:
    """
    Add file content units to a repository in a single modify call.

    Returns:
        created_resources from the modify task, or empty list when there is nothing to add.
    """
    if not content_hrefs:
        return []
    if not repository_href or not str(repository_href).strip():
        raise ValueError("repository_href is required to add file content")

    logging.debug("Adding %d file content unit(s) to repository %s", len(content_hrefs), repository_href)
    modify_task = client.add_content(repository_href, content_hrefs)
    final_task = client.wait_for_finished_task(
        modify_task.pulp_href,
        timeout=FILE_REPOSITORY_MODIFY_TASK_TIMEOUT,
    )
    if not final_task.is_complete:
        logging.warning(
            "File repository modify task %s did not finish (state: %s); continuing without created_resources",
            final_task.pulp_href,
            final_task.state,
        )
        return []
    if final_task.created_resources:
        logging.debug("Captured %d created resources from file repository modify", len(final_task.created_resources))
        return list(final_task.created_resources)
    return []


__all__ = [
    "FileUploadSpec",
    "FileRepositoryBatch",
    "upload_files_parallel",
    "add_file_content_to_repository",
]
