"""Tests for scripts/sync-container-build-args.sh version sync."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync-container-build-args.sh"


@pytest.mark.unit
def test_sync_container_build_args_writes_version_files(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({".": "2.3.4"}), encoding="utf-8")
    build_args = tmp_path / "build-args"
    version_file = tmp_path / "VERSION"
    version_module = tmp_path / "_version.py"

    env = {
        **os.environ,
        "RELEASE_PLEASE_MANIFEST_FILE": str(manifest),
        "CONTAINER_BUILD_ARGS_FILE": str(build_args),
        "PULP_TOOL_VERSION_FILE": str(version_file),
        "PULP_TOOL_VERSION_MODULE_FILE": str(version_module),
        "CONTAINER_RELEASE": "1",
    }
    result = subprocess.run(
        [str(SYNC_SCRIPT)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    assert build_args.read_text(encoding="utf-8") == "VERSION=2.3.4\nRELEASE=1\n"
    assert '__version__ = "2.3.4"' in version_file.read_text(encoding="utf-8")

    module_text = version_module.read_text(encoding="utf-8")
    assert "__version__ = version = '2.3.4'" in module_text
    assert "__version_tuple__ = version_tuple = (2, 3, 4)" in module_text
    assert "__commit_id__ = commit_id = None" in module_text
