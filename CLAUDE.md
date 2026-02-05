# CLAUDE.md - LLM-Assisted Development Guidelines

This document provides essential guidelines for LLM-assisted development in the pulp-tool repository. It emphasizes code quality, linting, and pre-commit hooks to ensure changes pass CI/CD pipelines on the first attempt.

## Purpose

This file is specifically designed for LLM assistants (like Claude, ChatGPT, etc.) working on this codebase. It provides clear instructions on:

- Required workflow after making code changes
- Linting tools and their configurations
- Pre-commit hooks usage
- Quick reference commands
- Best practices to avoid pipeline failures

## Required Workflow After Making Changes

**CRITICAL**: Always follow this workflow after making any code changes:

1. **Make your code changes** - Implement the requested feature or fix
2. **Run linters immediately** - Don't wait, run linters right after changes:
   ```bash
   make lint
   ```
3. **Fix any linting errors** - Address all issues before proceeding
4. **Format code** - Ensure code is properly formatted:
   ```bash
   make format
   ```
5. **Run pre-commit hooks** - Verify hooks pass before considering changes complete:
   ```bash
   pre-commit run --all-files
   ```
6. **Run tests with coverage** - Ensure functionality works and coverage requirements are met:
   ```bash
   make test
   ```
7. **Verify 100% coverage for new changes** - New code must have 100% test coverage:
   ```bash
   # Check coverage for changed files
   python3 -m pytest --cov=pulp_tool --cov-report=term-missing
   # Review coverage report to ensure new code is fully covered
   ```

**Never skip linting, pre-commit checks, or coverage verification** - They will fail in CI/CD and waste pipeline resources.

## Linting Tools

This repository uses multiple linting tools to ensure code quality. All tools are configured in `.pre-commit-config.yaml` and `pyproject.toml`.

### Black (Code Formatting)

- **Purpose**: Automatic code formatting
- **Configuration**: Line length 120, Python 3.12 target
- **Run manually**: `make format` or `black pulp_tool/ tests/`
- **Check only**: `make lint-black` or `black --check pulp_tool/ tests/`
- **Config file**: `pyproject.toml` (`[tool.black]` section)

### Flake8 (Style Checking)

- **Purpose**: Python style guide enforcement (PEP 8)
- **Configuration**: Max line length 120, ignores E203 (conflicts with Black)
- **Run manually**: `make lint-flake8` or `flake8 pulp_tool/ tests/`
- **Config file**: `.flake8`

### Pylint (Error Checking)

- **Purpose**: Static code analysis for errors and code smells
- **Configuration**: Errors only mode, max line length 120
- **Run manually**: `make lint-pylint` or `pylint pulp_tool/ --errors-only`
- **Config file**: `pyproject.toml` (`[tool.pylint]` section) and `pylintrc`

### Mypy (Type Checking)

- **Purpose**: Static type checking
- **Configuration**: Python 3.12, ignore missing imports, show error codes
- **Run manually**: `make lint-mypy` or `mypy pulp_tool/ --show-error-codes`
- **Config file**: `pyproject.toml` (`[tool.mypy]` section)
- **Note**: Some modules have type checking disabled (see `[[tool.mypy.overrides]]`)

## Pre-commit Hooks

Pre-commit hooks are configured in `.pre-commit-config.yaml` and run automatically on `git commit`. However, **you should run them manually after making changes** to catch issues early.

### Available Hooks

The repository includes the following pre-commit hooks:

1. **General file checks** (trailing whitespace, end-of-file, YAML/TOML/JSON validation, large files, merge conflicts)
2. **Black** - Code formatting
3. **Flake8** - Style checking
4. **Mypy** - Type checking
5. **Pylint** - Error checking (errors only)

### Running Pre-commit Hooks

**After making changes, always run:**

```bash
pre-commit run --all-files
```

This runs all hooks on all files and ensures your changes will pass CI/CD checks.

**To run hooks automatically on commit:**

```bash
pre-commit install
```

This is already done during `make install-dev` setup.

**To run a specific hook:**

```bash
pre-commit run <hook-id> --all-files
```

Example: `pre-commit run black --all-files`

## Quick Reference Commands

### Linting Commands

```bash
# Run all linters
make lint

# Run individual linters
make lint-black      # Check formatting
make lint-flake8     # Style checking
make lint-pylint     # Error checking
make lint-mypy       # Type checking

# Format code (fixes formatting issues)
make format
```

### Pre-commit Commands

```bash
# Run all pre-commit hooks on all files
pre-commit run --all-files

# Install pre-commit hooks (run once)
pre-commit install

# Run specific hook
pre-commit run black --all-files
pre-commit run flake8 --all-files
```

### Testing Commands

```bash
# Run all tests with coverage (REQUIRED for new changes)
make test

# Run tests without coverage (faster, but won't verify coverage)
make test-fast

# Run specific test file
python3 -m pytest tests/path/to/test_file.py -v

# Check coverage for specific module/file
python3 -m pytest --cov=pulp_tool/path/to/module --cov-report=term-missing tests/
```

**IMPORTANT**: New changes require **100% test coverage**. Always run `make test` to verify coverage before considering changes complete.

### Comprehensive Checks

```bash
# Run all checks (lint + test)
make check

# Run comprehensive check script (lint + test + coverage)
./scripts/check-all.sh
```

### Development Setup

```bash
# Install package with dev dependencies
make install-dev

# Setup development environment
make setup
```

## Best Practices for LLM-Assisted Development

### 1. Always Run Linters After Changes

**Never commit code without running linters first.** The CI/CD pipeline runs the same checks, and failures waste resources and delay reviews.

```bash
# After making changes:
make lint          # Check all linters
make format        # Fix formatting if needed
pre-commit run --all-files  # Verify hooks pass
make test          # Verify tests pass and 100% coverage for new changes
```

### 2. Fix Linting Errors Immediately

Don't accumulate linting errors. Fix them as soon as they appear:

- **Black errors**: Run `make format` to auto-fix
- **Flake8 errors**: Fix manually based on error messages
- **Pylint errors**: Address the specific error (errors-only mode)
- **Mypy errors**: Fix type annotations or add type ignores if necessary

### 3. Use Makefile Targets

The `Makefile` provides convenient targets for common tasks. Use them instead of running tools directly:

- `make lint` - Better than running each linter individually
- `make format` - Consistent formatting across the codebase
- `make check` - Comprehensive check before committing

### 4. Verify Pre-commit Hooks Pass

Pre-commit hooks catch issues before they reach CI/CD. Always run:

```bash
pre-commit run --all-files
```

If hooks fail, fix the issues and run again until all pass.

### 5. Check Configuration Files

When in doubt, check the configuration files:

- `.pre-commit-config.yaml` - Pre-commit hook configuration
- `pyproject.toml` - Tool configurations (Black, Pylint, Mypy)
- `.flake8` - Flake8 configuration
- `Makefile` - Available make targets

### 6. Ensure 100% Coverage for New Changes

**CRITICAL**: All new code changes must have 100% test coverage. The CI/CD pipeline enforces this using diff coverage checks.

- **New code**: Must have 100% test coverage
- **Modified code**: Must maintain or improve coverage
- **Overall project**: Must maintain 85%+ coverage

Always run tests with coverage before completing changes:

```bash
make test    # Full test suite with coverage (REQUIRED)
```

Review the coverage report to ensure all new/modified lines are covered. If coverage is insufficient:

1. Write tests for uncovered code paths
2. Include edge cases and error conditions
3. Verify all branches and conditions are tested
4. Re-run `make test` until coverage is 100% for new changes

### 7. Use Scripts for Comprehensive Checks

The `scripts/check-all.sh` script runs all checks including tests:

```bash
./scripts/check-all.sh
```

This is useful before creating pull requests.

## Common Issues and Solutions

### Black Formatting Conflicts

**Issue**: Black and Flake8 conflict on E203 (whitespace before ':')

**Solution**: E203 is ignored in Flake8 config (`.flake8`). This is expected and correct.

### Mypy Type Errors

**Issue**: Mypy reports type errors in certain modules

**Solution**: Some modules have type checking disabled in `pyproject.toml` (`[[tool.mypy.overrides]]`). This is intentional for dynamic code patterns.

### Pre-commit Hook Failures

**Issue**: Pre-commit hooks fail on commit

**Solution**: Run `pre-commit run --all-files` manually first to catch and fix issues before committing.

### Coverage Failures

**Issue**: CI/CD fails with "diff coverage below 100%" error

**Solution**: New code changes require 100% test coverage. Ensure all new/modified code paths are tested:

1. Run `make test` to see coverage report
2. Identify uncovered lines in your changes
3. Write tests for all uncovered code paths
4. Include edge cases, error conditions, and all branches
5. Re-run `make test` until coverage is 100% for new changes

The CI/CD pipeline uses `diff-cover` to check only new/changed lines, so you must ensure complete coverage for your changes.

## References

- **Pre-commit configuration**: `.pre-commit-config.yaml`
- **Makefile targets**: `Makefile`
- **Tool configurations**: `pyproject.toml`
- **Flake8 config**: `.flake8`
- **Comprehensive check script**: `scripts/check-all.sh`
- **Contributing guidelines**: `CONTRIBUTING.md`

## Summary

**Remember**: After making any code changes:

1. ✅ Run `make lint` to check for linting errors
2. ✅ Run `make format` to ensure proper formatting
3. ✅ Run `pre-commit run --all-files` to verify hooks pass
4. ✅ Run `make test` to verify tests pass and **100% coverage for new changes**
5. ✅ Fix any issues before considering changes complete

**Coverage Requirement**: New code must have 100% test coverage. The CI/CD pipeline uses diff coverage to enforce this requirement.

Following this workflow ensures your changes will pass CI/CD checks and avoids unnecessary pipeline runs.
