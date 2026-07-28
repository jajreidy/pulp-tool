#!/usr/bin/env bash
# Run Checkton (ShellCheck on embedded Tekton scripts), matching the tekton-lint CI job.
set -euo pipefail

CHECKTON_VERSION="${CHECKTON_VERSION:-v0.4.0}"
CHECKTON_IMAGE="ghcr.io/chmeliik/checkton:${CHECKTON_VERSION}"

if command -v podman >/dev/null 2>&1; then
    ENGINE=podman
elif command -v docker >/dev/null 2>&1; then
    ENGINE=docker
else
    echo "checkton: docker or podman is required (same as CI tekton-lint job)" >&2
    exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

exec "${ENGINE}" run --rm \
    -v "${REPO_ROOT}:/github/workspace" \
    -w /github/workspace \
    -e CHECKTON_FAIL_ON_FINDINGS=true \
    "${CHECKTON_IMAGE}"
