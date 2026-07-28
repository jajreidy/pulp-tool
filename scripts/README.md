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
```

Optional scripts (run directly):

```bash
./scripts/dev-setup.sh   # Python version check + install-dev (no commit-msg hook)
./scripts/run-tests.sh
./scripts/check-all.sh
./scripts/update-deps.sh
```
