#!/usr/bin/env bash
# Sync release version from .release-please-manifest.json into versioned artifacts:
#   - .tekton/pulp-tool-container.build-args (Konflux VERSION/RELEASE)
#   - VERSION (Dockerfile / setuptools-scm fallback in image build)
#   - pulp_tool/_version.py (imported __version__ on main between release PR and tag)
#
# Uses .release-please-manifest.json (updated in Release Please PRs). That version
# becomes the git tag when maintainers run `make release-publish`; Konflux builds on
# main usually run before the tag exists, so the manifest is the reliable source.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MANIFEST_FILE="${RELEASE_PLEASE_MANIFEST_FILE:-.release-please-manifest.json}"
OUT_FILE="${CONTAINER_BUILD_ARGS_FILE:-.tekton/pulp-tool-container.build-args}"
VERSION_FILE="${PULP_TOOL_VERSION_FILE:-VERSION}"
VERSION_MODULE_FILE="${PULP_TOOL_VERSION_MODULE_FILE:-pulp_tool/_version.py}"
RELEASE="${CONTAINER_RELEASE:-1}"

if [[ ! -f "$MANIFEST_FILE" ]]; then
  echo "Manifest not found: ${MANIFEST_FILE}" >&2
  exit 1
fi

python3 - "$MANIFEST_FILE" "$OUT_FILE" "$VERSION_FILE" "$VERSION_MODULE_FILE" "$RELEASE" <<'PY'
import json
import re
import sys
from pathlib import Path

manifest_path, out_file, version_file, version_module_file, release = sys.argv[1:6]

with open(manifest_path, encoding="utf-8") as fh:
    data = json.load(fh)
version = data.get(".")
if not version or not isinstance(version, str):
    raise SystemExit(f"Invalid manifest version in {manifest_path!r}: {version!r}")
version = version.strip()

Path(out_file).write_text(f"VERSION={version}\nRELEASE={release}\n", encoding="utf-8")
Path(version_file).write_text(
    "# Version information for pulp-tool package\n"
    f'__version__ = "{version}"\n',
    encoding="utf-8",
)

match = re.match(r"^(\d+)\.(\d+)\.(\d+)(.*)$", version)
if not match:
    raise SystemExit(f"Cannot build version tuple from {version!r} (expected X.Y.Z prefix)")
major, minor, patch, suffix = match.groups()
tuple_parts: list[int | str] = [int(major), int(minor), int(patch)]
if suffix.startswith("-") and len(suffix) > 1:
    tuple_parts.append(suffix[1:])

tuple_repr = ", ".join(repr(part) for part in tuple_parts)

version_module = f'''# Pinned by scripts/sync-container-build-args.sh from {manifest_path}
# Local editable installs may regenerate via setuptools-scm (see pyproject.toml).
from __future__ import annotations

__all__ = [
    "__version__",
    "__version_tuple__",
    "version",
    "version_tuple",
    "__commit_id__",
    "commit_id",
]

version: str
__version__: str
__version_tuple__: tuple[int | str, ...]
version_tuple: tuple[int | str, ...]
commit_id: str | None
__commit_id__: str | None

__version__ = version = {version!r}
__version_tuple__ = version_tuple = ({tuple_repr})

__commit_id__ = commit_id = None
'''
Path(version_module_file).write_text(version_module, encoding="utf-8")

print(f"Wrote {out_file} (VERSION={version}, RELEASE={release})")
print(f'Wrote {version_file} (__version__ = "{version}")')
print(f"Wrote {version_module_file} (__version__ = {version!r})")
PY
