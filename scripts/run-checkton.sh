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

# Differential mode compares against a base ref. PR CI passes base/head SHAs; locally
# default to origin/main (checkton otherwise falls back to `main`, which may not exist
# on feature branches).
if [[ -z "${CHECKTON_DIFF_BASE+x}" ]]; then
    CHECKTON_DIFF_BASE=origin/main
    if ! git -C "${REPO_ROOT}" fetch origin main; then
        if ! git -C "${REPO_ROOT}" rev-parse --verify "${CHECKTON_DIFF_BASE}" >/dev/null 2>&1; then
            cat >&2 <<EOF
checkton: could not fetch origin/main and ${CHECKTON_DIFF_BASE} is not available.
Run: git fetch origin main
Or set CHECKTON_DIFF_BASE to an existing ref (e.g. export CHECKTON_DIFF_BASE=origin/main).
EOF
            exit 1
        fi
        echo "checkton: warning: git fetch origin main failed; using existing ${CHECKTON_DIFF_BASE}" >&2
    fi
fi
CHECKTON_DIFF_HEAD="${CHECKTON_DIFF_HEAD:-HEAD}"

if [[ "${ENGINE}" == podman ]]; then
    WORKSPACE_MOUNT="${REPO_ROOT}:/github/workspace:Z"
else
    WORKSPACE_MOUNT="${REPO_ROOT}:/github/workspace"
fi

exec "${ENGINE}" run --rm \
    -v "${WORKSPACE_MOUNT}" \
    -w /github/workspace \
    -e GITHUB_WORKSPACE=/github/workspace \
    -e CHECKTON_DIFFERENTIAL=true \
    -e CHECKTON_DIFF_BASE="${CHECKTON_DIFF_BASE}" \
    -e CHECKTON_DIFF_HEAD="${CHECKTON_DIFF_HEAD}" \
    -e CHECKTON_FAIL_ON_FINDINGS=true \
    "${CHECKTON_IMAGE}"
