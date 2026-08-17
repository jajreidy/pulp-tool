#!/usr/bin/env bash
# Extract Keep a Changelog section for a release tag into release-notes.md.
# Usage: extract-changelog-notes.sh <tag> [changelog-path]
set -euo pipefail

tag="${1:?usage: extract-changelog-notes.sh <tag> [changelog-path]}"
changelog="${2:-CHANGELOG.md}"
output="${3:-release-notes.md}"
version="${tag#v}"

if [[ ! -f "$changelog" ]]; then
  echo "::error::Changelog not found: ${changelog}"
  exit 1
fi

if ! grep -qE "^## \\[${version//./\\.}\\]" "$changelog"; then
  {
    echo "Release **${tag}**."
    echo
    if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
      echo "See [CHANGELOG.md](https://github.com/${GITHUB_REPOSITORY}/blob/${tag}/CHANGELOG.md)."
    else
      echo "See CHANGELOG.md in the repository at tag ${tag}."
    fi
  } >"$output"
  echo "::warning::No CHANGELOG section ## [${version}]; wrote fallback release notes"
  exit 0
fi

awk -v ver="$version" '
  /^## \[/ {
    if (found) {
      exit
    }
    if ($0 ~ "^## \\[" ver "\\]") {
      found = 1
      next
    }
  }
  found {
    print
  }
' "$changelog" >"$output"

if [[ ! -s "$output" ]]; then
  echo "Release **${tag}**." >"$output"
fi
