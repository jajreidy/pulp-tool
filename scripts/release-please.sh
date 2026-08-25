#!/usr/bin/env bash
# Run Release Please locally (maintainer workflow). See docs/releasing.md.
#
# Usage:
#   ./scripts/release-please.sh pr [-- extra release-please flags]
#   ./scripts/release-please.sh publish
#
# pr:      uses release-please release-pr (GitHub API — needs gh login or a token)
# publish: git tag + push from .release-please-manifest.json (git credentials only)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RELEASE_PLEASE_VERSION="${RELEASE_PLEASE_VERSION:-17.2.0}"
TARGET_BRANCH="${RELEASE_PLEASE_TARGET_BRANCH:-main}"
RELEASE_GIT_REMOTE="${RELEASE_GIT_REMOTE:-origin}"
CONFIG_FILE="${RELEASE_PLEASE_CONFIG_FILE:-release-please-config.json}"
MANIFEST_FILE="${RELEASE_PLEASE_MANIFEST_FILE:-.release-please-manifest.json}"

usage() {
  cat <<'EOF'
Usage: release-please.sh <command> [-- extra release-please flags]

Commands:
  pr        Create or update the release pull request (run on main after feature merges)
  publish   Create and push v* tag from .release-please-manifest.json (triggers release.yml)

Authentication:
  pr        release-please talks to the GitHub API. Provide GITHUB_TOKEN/GH_TOKEN, or run
            `gh auth login` — the script uses `gh auth token` when no token env var is set.
  publish   plain git tag push only (SSH or HTTPS git credentials; no GitHub API token)

Environment:
  GITHUB_TOKEN or GH_TOKEN   Optional if `gh auth login` is configured (pr command only)
  GITHUB_REPOSITORY          owner/repo (optional; inferred from RELEASE_GIT_REMOTE)
  RELEASE_GIT_REMOTE         Git remote for fetch/pull/tag push and repo inference (default: origin)
  RELEASE_PLEASE_VERSION     npm package pin (default: 17.2.0)
  RELEASE_PLEASE_TARGET_BRANCH Target branch (default: main)

Examples:
  gh auth login
  ./scripts/release-please.sh pr
  ./scripts/release-please.sh pr -- --dry-run --debug
  # Fork workflow (canonical repo on upstream remote):
  RELEASE_GIT_REMOTE=upstream ./scripts/release-please.sh pr
  RELEASE_GIT_REMOTE=upstream ./scripts/release-please.sh publish
EOF
}

parse_github_repo_from_remote_url() {
  local remote_url="$1"
  if [[ "$remote_url" =~ github\.com[:/]([^/]+)/([^/.]+)(\.git)?$ ]]; then
    printf '%s/%s' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

resolve_repo_url() {
  if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
    printf '%s' "$GITHUB_REPOSITORY"
    return 0
  fi
  local remote_url=""
  remote_url="$(git remote get-url "$RELEASE_GIT_REMOTE" 2>/dev/null || true)"
  if parse_github_repo_from_remote_url "$remote_url"; then
    return 0
  fi
  echo "Set GITHUB_REPOSITORY=owner/repo or configure RELEASE_GIT_REMOTE (${RELEASE_GIT_REMOTE}) with a github.com URL." >&2
  return 1
}

resolve_github_token() {
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    printf '%s' "$GITHUB_TOKEN"
    return 0
  fi
  if [[ -n "${GH_TOKEN:-}" ]]; then
    printf '%s' "$GH_TOKEN"
    return 0
  fi
  if command -v gh >/dev/null 2>&1; then
    gh auth token 2>/dev/null || true
    return 0
  fi
  return 1
}

read_manifest_version() {
  if [[ ! -f "$MANIFEST_FILE" ]]; then
    echo "Manifest not found: ${MANIFEST_FILE}" >&2
    return 1
  fi
  python3 - "$MANIFEST_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
version = data.get(".")
if not version or not isinstance(version, str):
    raise SystemExit(f"Invalid manifest version in {sys.argv[1]!r}: {version!r}")
print(version.strip())
PY
}

publish_git_tag() {
  local version tag

  git fetch "$RELEASE_GIT_REMOTE" "$TARGET_BRANCH"
  git checkout "$TARGET_BRANCH"
  git pull --ff-only "$RELEASE_GIT_REMOTE" "$TARGET_BRANCH"

  # Read manifest after sync — a stale local file before pull caused wrong tags.
  version="$(read_manifest_version)"
  tag="v${version}"

  if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
    echo "Refusing to tag: manifest version ${version!r} -> ${tag!r} (expected SemVer)" >&2
    exit 1
  fi

  if git rev-parse "refs/tags/${tag}" >/dev/null 2>&1; then
    echo "Tag ${tag} already exists locally." >&2
    exit 1
  fi
  if git ls-remote --exit-code --tags "$RELEASE_GIT_REMOTE" "refs/tags/${tag}" >/dev/null 2>&1; then
    echo "Tag ${tag} already exists on ${RELEASE_GIT_REMOTE}." >&2
    exit 1
  fi

  echo "Tagging ${tag} from ${MANIFEST_FILE} at $(git rev-parse --short HEAD) on ${TARGET_BRANCH} (${RELEASE_GIT_REMOTE})"
  git tag "$tag"
  git push "$RELEASE_GIT_REMOTE" "$tag"
  echo "Pushed ${tag} to ${RELEASE_GIT_REMOTE}. release.yml should start on GitHub Actions."
}

run_release_please() {
  if command -v release-please >/dev/null 2>&1; then
    release-please "$@"
    return
  fi
  if ! command -v npx >/dev/null 2>&1; then
    echo "Install Node.js/npm (for npx) or release-please globally: npm i -g release-please" >&2
    exit 1
  fi
  npx --yes "release-please@${RELEASE_PLEASE_VERSION}" "$@"
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 1
fi

cmd="$1"
shift

extra_args=()
if [[ $# -gt 0 ]]; then
  if [[ "$1" == "--" ]]; then
    shift
  fi
  extra_args=("$@")
fi

case "$cmd" in
  pr|release-pr)
    token="$(resolve_github_token || true)"
    if [[ -z "$token" ]]; then
      echo "release-pr needs GitHub API access." >&2
      echo "Run 'gh auth login' or set GITHUB_TOKEN / GH_TOKEN (contents + pull-requests write)." >&2
      exit 1
    fi
    repo_url="$(resolve_repo_url)"
    run_release_please release-pr \
      --token="$token" \
      --repo-url="$repo_url" \
      --target-branch="$TARGET_BRANCH" \
      --config-file="$CONFIG_FILE" \
      --manifest-file="$MANIFEST_FILE" \
      "${extra_args[@]}"
    ;;
  publish|github-release|tag)
    publish_git_tag
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: $cmd" >&2
    usage >&2
    exit 1
    ;;
esac
