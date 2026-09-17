#!/usr/bin/env bash
# Write Konflux build-args-file for pulp-tool-container from the release version.
#
# Uses .release-please-manifest.json (updated in Release Please PRs). That version
# becomes the git tag when maintainers run `make release-publish`; Konflux builds on
# main usually run before the tag exists, so the manifest is the reliable source.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MANIFEST_FILE="${RELEASE_PLEASE_MANIFEST_FILE:-.release-please-manifest.json}"
OUT_FILE="${CONTAINER_BUILD_ARGS_FILE:-.tekton/pulp-tool-container.build-args}"
RELEASE="${CONTAINER_RELEASE:-1}"

if [[ ! -f "$MANIFEST_FILE" ]]; then
  echo "Manifest not found: ${MANIFEST_FILE}" >&2
  exit 1
fi

VERSION="$(python3 - "$MANIFEST_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
version = data.get(".")
if not version or not isinstance(version, str):
    raise SystemExit(f"Invalid manifest version in {sys.argv[1]!r}: {version!r}")
print(version.strip())
PY
)"

cat >"$OUT_FILE" <<EOF
VERSION=${VERSION}
RELEASE=${RELEASE}
EOF

echo "Wrote ${OUT_FILE} (VERSION=${VERSION}, RELEASE=${RELEASE})"
