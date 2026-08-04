"""Tests for e2e run-scoped naming helpers."""

from __future__ import annotations

import sys
from pathlib import Path

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
sys.path.insert(0, str(E2E_DIR))

from names import (  # noqa: E402
    BASE_PATH_CREATE_REPOSITORY,
    BASE_PATH_CREATE_REPOSITORY_JSON,
    BUILD_ID_UPLOAD_MINIMAL,
    BUILD_ID_UPLOAD_TARGET_ARCH,
    file_repos_for_run,
    resolve_run_id,
    rpm_repos_for_run,
    scoped_base_path,
    scoped_build_id,
    scoped_repo_name,
)


def test_resolve_run_id_sanitizes() -> None:
    assert resolve_run_id("abc/123") == "abc-123"


def test_scoped_build_id_without_run_id() -> None:
    assert scoped_build_id(BUILD_ID_UPLOAD_MINIMAL, None) == BUILD_ID_UPLOAD_MINIMAL


def test_scoped_build_id_with_run_id() -> None:
    assert scoped_build_id(BUILD_ID_UPLOAD_MINIMAL, "run1") == f"{BUILD_ID_UPLOAD_MINIMAL}-run1"


def test_scoped_base_path_without_run_id() -> None:
    assert scoped_base_path(BASE_PATH_CREATE_REPOSITORY, None) == BASE_PATH_CREATE_REPOSITORY


def test_scoped_base_path_with_run_id() -> None:
    assert scoped_base_path(BASE_PATH_CREATE_REPOSITORY_JSON, "run1") == f"{BASE_PATH_CREATE_REPOSITORY_JSON}-run1"


def test_scoped_repo_name_qualifies_build_scoped_repo() -> None:
    assert scoped_repo_name(f"{BUILD_ID_UPLOAD_MINIMAL}/rpms", "run1") == f"{BUILD_ID_UPLOAD_MINIMAL}-run1/rpms"


def test_scoped_repo_name_leaves_global_arch_repos_unsuffixed() -> None:
    assert scoped_repo_name("aarch64", "run1") == "aarch64"


def test_rpm_repos_for_run_keeps_global_arch_repos_when_isolated() -> None:
    legacy = rpm_repos_for_run(None)
    isolated = rpm_repos_for_run("run1")
    assert "aarch64" in legacy
    assert "aarch64" in isolated
    assert f"{BUILD_ID_UPLOAD_MINIMAL}-run1/rpms" in isolated


def test_file_repos_for_run_scopes_target_arch_artifacts_when_isolated() -> None:
    isolated = file_repos_for_run("run1")
    assert f"{BUILD_ID_UPLOAD_TARGET_ARCH}-run1/artifacts" in isolated
    assert f"{BUILD_ID_UPLOAD_MINIMAL}-run1/artifacts" in isolated
