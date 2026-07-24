# Releasing pulp-tool

Maintainer guide for publishing **`pulp-tool`** to [PyPI](https://pypi.org/project/pulp-tool/). For day-to-day development and pull requests, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Overview

Releases are **tag-driven only**. Pushing a semver tag on **`main`** triggers the GitHub Actions workflow [`.github/workflows/release.yml`](../.github/workflows/release.yml), which builds and inspects the package, then uploads to PyPI.

## One-time setup

Upload uses [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/) (OpenID Connect from GitHub Actions). No `PYPI_API_TOKEN` or other PyPI repository secret is required.

### PyPI trusted publisher

At https://pypi.org/manage/account/publishing/, add a publisher for the **`pulp-tool`** project with values that **exactly** match this repository and workflow:

| Field | Value |
|-------|--------|
| PyPI project name | `pulp-tool` |
| Owner | `konflux-ci` |
| Repository name | `pulp-tool` |
| Workflow name | `release.yml` |
| Environment name | *(leave blank — the workflow does not set a GitHub Environment)* |

Register the publisher before the first tag-driven release (or use a pending publisher so the project is created on first successful upload).

### GitHub repository

- Ensure **Actions** is enabled for the repository.
- The upload job in [`.github/workflows/release.yml`](../.github/workflows/release.yml) grants **`id-token: write`** only on **`upload-to-pypi`** (required for trusted publishing). Do not add that permission elsewhere.
- If you previously stored **`PYPI_API_TOKEN`**, revoke the token on PyPI and remove the repository secret; it is no longer used.

Optional hardening: create a GitHub **Environment** (e.g. `pypi`) with required reviewers, add `environment: pypi` to the upload job, and set the same environment name on the PyPI trusted publisher. That is not configured in the workflow today.

## Version numbers

Package version comes from [setuptools-scm](https://setuptools-scm.readthedocs.io/) and the git tag (see `[tool.setuptools_scm]` in [`pyproject.toml`](../pyproject.toml)). Tag **`v1.2.3`** produces version **`1.2.3`**. The release workflow checks out full git history (`fetch-depth: 0`) so the tagged version resolves correctly at build time.

Build metadata in tags (e.g. **`v1.2.3+build.1`**) is accepted by the workflow; confirm how `setuptools_scm` maps that to the PyPI version string before publishing non-standard tags.

## Before you tag

1. Merge release changes to **`main`** and confirm CI is green (`make test`, `make test-diff-coverage`, `pre-commit run --all-files`).
2. Update [`CHANGELOG.md`](../CHANGELOG.md): move items from **`[Unreleased]`** into a new **`[X.Y.Z]`** section with the release date, and add compare links at the bottom per [Keep a Changelog](https://keepachangelog.com/).
3. Commit the changelog update on **`main`** if it is not already included in the release commit.

## Cut a release

Tags must match **`vMAJOR.MINOR.PATCH`** with optional SemVer pre-release and build metadata (e.g. **`v1.2.3`**, **`v1.2.3-rc1`**, **`v1.2.3+build.1`**, **`v1.2.3-rc1+build.1`**) and must point at a commit on **`main`**. Other tags are rejected by the workflow.

```bash
git checkout main
git pull origin main
git tag v1.2.3
git push origin v1.2.3
```

Pushing the tag starts the **Release python package** workflow:

1. **Build & inspect package** — builds sdist and wheel with Python **3.12** (same as CI), runs wheel/README checks via [`hynek/build-and-inspect-python-package`](https://github.com/hynek/build-and-inspect-python-package), and uploads artifacts for inspection in the run summary.
2. **Upload package to PyPI** — downloads the built artifacts and publishes with [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish) using PyPI trusted publishing (OIDC; no API token in GitHub Secrets).

## Verify the release

- In GitHub **Actions**, open the workflow run for the tag and confirm both jobs succeeded.
- On PyPI, confirm the new version appears at https://pypi.org/project/pulp-tool/
- Optionally install locally: `pip install pulp-tool==1.2.3`

## Troubleshooting

| Issue | Check |
|-------|-------|
| Workflow did not start | Tag must match `v*` and be pushed to GitHub (`git push origin vX.Y.Z`) |
| Tag format rejected | Use SemVer tags such as `v1.2.3`, `v1.2.3-rc1`, or `v1.2.3+build.1` (not `1.2.3` without the `v` prefix) |
| Not on `main` | Tag must point at a commit reachable from `main` (checked via GitHub compare API, not a separate `git fetch`) |
| Upload auth failed | PyPI trusted publisher must match `konflux-ci` / `pulp-tool` / workflow `release.yml` with a blank environment name; upload job needs `id-token: write` |
| Wrong package version | Tag name must match the intended release; rebuild requires a new tag |
