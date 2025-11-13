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
./scripts/run-tests.sh tests/test_cli.py -v
```

### `check-all.sh`

Run all code quality checks (formatting, linting, type checking, tests).

```bash
./scripts/check-all.sh
```

This script runs:
1. Black formatting check
2. Flake8 linting
3. Pylint (errors only)
4. Mypy type checking
5. Pytest with coverage

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

Alternatively, use the Makefile targets that wrap these scripts:

```bash
make setup        # Runs dev-setup.sh
make run-tests    # Runs run-tests.sh
make check-all    # Runs check-all.sh
make update-deps  # Runs update-deps.sh
```
