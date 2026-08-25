# Development Scripts

This directory contains helper scripts for common development tasks.

## Available Scripts

### `dev-setup.sh`

One-command development environment setup.

```bash
./scripts/dev-setup.sh
```

This script:
- Checks Python version
- Installs the package in development mode
- Installs pre-commit hooks

### `run-tests.sh`

Standardized test execution.

```bash
# Run all tests with coverage
./scripts/run-tests.sh

# Run specific tests
./scripts/run-tests.sh tests/cli/test_cli_core.py -v
```

### `check-all.sh`

Run all code quality checks (formatting, linting, type checking, tests).

```bash
./scripts/check-all.sh
```

This script runs:
1. Ruff lint and format check (`pulp_tool/`, `tests/`)
2. Pylint (errors only, `pulp_tool/`, `tests/`)
3. Mypy type checking
4. Pytest with coverage (options from `pyproject.toml`)
5. Diff coverage vs merge base when available (100%, same as PR CI)

For full CI parity (yamllint, shellcheck, hadolint, codespell, pip-audit, checkton), use `make pre-commit-ci`.

### `update-deps.sh`

Update all dependencies to latest versions.

```bash
./scripts/update-deps.sh
```

This script:
- Updates pip
- Updates build tools
- Updates package dependencies
- Updates pre-commit hooks

### `release-please.sh`

Run [Release Please](https://github.com/googleapis/release-please) locally to open release PRs and create semver tags. See [docs/releasing.md](../docs/releasing.md).

```bash
gh auth login

# Open or update the release PR (run on main after feature merges)
./scripts/release-please.sh pr
make release-please

# After merging the release PR — push v* tag (git credentials only)
./scripts/release-please.sh publish
make release-publish

# Canonical repo on upstream (fork on origin):
RELEASE_GIT_REMOTE=upstream make release-please
RELEASE_GIT_REMOTE=upstream make release-publish

# Preview without opening a PR
./scripts/release-please.sh pr -- --dry-run --debug
```

`make release-please` needs GitHub API access (`gh auth login` or a token). `make release-publish` uses plain `git tag` / `git push` only. Set **`RELEASE_GIT_REMOTE`** (default `origin`) when the release target is not `origin` — see [docs/releasing.md](../docs/releasing.md#fork-and-upstream-remotes).

## Usage

All scripts are executable and can be run directly:

```bash
chmod +x scripts/*.sh
./scripts/dev-setup.sh
```

Alternatively, use the Makefile targets:

```bash
make install-dev  # editable install + pre-commit (+ pre-push) hooks
make test         # Same as ./scripts/run-tests.sh (pytest with coverage)
make check        # lint + test (subset; use make pre-commit-ci for full CI lint gates)
make pre-commit-ci  # all pre-commit hooks (matches GitHub PR CI)
make lock         # Regenerate uv.lock (see also ./scripts/update-deps.sh for broader bumps)
make release-please   # Open/update release PR (maintainers; see docs/releasing.md)
make release-publish  # Tag release after merging release PR
```

Optional scripts (run directly):

```bash
./scripts/dev-setup.sh   # Python version check + install-dev (no commit-msg hook)
./scripts/run-tests.sh
./scripts/check-all.sh
./scripts/update-deps.sh
```
