#!/usr/bin/env bash
# Run hadolint, downloading a release binary into the pre-commit cache when missing.
set -euo pipefail

HADOLINT_VERSION="${HADOLINT_VERSION:-v2.14.0}"
CACHE_DIR="${PRE_COMMIT_HOME:-${XDG_CACHE_HOME:-$HOME/.cache}/pre-commit}/hadolint/${HADOLINT_VERSION}"

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) HADOLINT_ASSET=hadolint-Linux-x86_64 ;;
    Linux-aarch64 | Linux-arm64) HADOLINT_ASSET=hadolint-Linux-arm64 ;;
    Darwin-x86_64) HADOLINT_ASSET=hadolint-Darwin-x86_64 ;;
    Darwin-arm64) HADOLINT_ASSET=hadolint-Darwin-arm64 ;;
    *)
        echo "hadolint: unsupported platform $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

HADOLINT_BIN="${CACHE_DIR}/${HADOLINT_ASSET}"

if [[ ! -x "${HADOLINT_BIN}" ]]; then
    mkdir -p "${CACHE_DIR}"
    curl -sSfL \
        "https://github.com/hadolint/hadolint/releases/download/${HADOLINT_VERSION}/${HADOLINT_ASSET}" \
        -o "${HADOLINT_BIN}"
    chmod +x "${HADOLINT_BIN}"
fi

exec "${HADOLINT_BIN}" --ignore DL3041 --ignore DL3013 "$@"
