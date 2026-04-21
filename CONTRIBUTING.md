# Contributing to Pulp Tool

Thank you for your interest in contributing to Pulp Tool! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.12 or higher
- Git
- pip

### Initial Setup

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/pulp-tool.git
   cd pulp-tool
   ```

3. Install the package in development mode and set up pre-commit hooks:
   ```bash
   make install-dev
   ```
   This runs `pip install -e ".[dev]"` and `pre-commit install`.

### Using Development Scripts

We provide scripts to help with common development tasks:

```bash
# Setup development environment
./scripts/dev-setup.sh

# Run all tests
./scripts/run-tests.sh

# Run all checks (linting, type checking, tests)
./scripts/check-all.sh

# Update dependencies
./scripts/update-deps.sh
```

Alternatively, use the Makefile:

```bash
make test      # Run tests
make lint      # Run linters
make format    # Format code
make check     # Run all checks
```

### Documentation for coding agents

- **[CLAUDE.md](CLAUDE.md)** — quick reference, architecture, Konflux/Tekton integration contracts, and links to the LLM workflow (`.cursor/rules/llm-development-guidelines.mdc`).
- Optional: [AgentReady](https://github.com/ambient-code/agentready) (`agentready assess .`) with [.agentready-config.yaml](.agentready-config.yaml).

### Dependency lock file

Pinned lockfiles:

- **`requirements.txt`** — from **`requirements.in`** via [pip-tools](https://github.com/jazzband/pip-tools) (`pip-compile`).
- **`uv.lock`** — from the same **`pyproject.toml`** via **[uv](https://github.com/astral-sh/uv)** (`uv lock` or `python3 -m uv lock`).

After changing dependencies in **`pyproject.toml`**, regenerate both:

```bash
pip install -e ".[dev]"   # includes pip-tools
make lock
```

Commit the updated **`requirements.txt`** and **`uv.lock`**. Normal installs remain **`pip install -e ".[dev]"`** from pyproject; lockfiles support reproducible CI and audits.

### Commit messages

Optional [**Conventional Commits**](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, …) are enforced if you install commit-msg hooks:

```bash
pre-commit install --hook-type commit-msg
```

## Code Style

### Formatting

We use [Black](https://black.readthedocs.io/) for code formatting with a line length of 120 characters.

```bash
# Format code
make format
# or: black pulp_tool/ tests/

# Check formatting without changes
make lint
# or: black --check pulp_tool/ tests/
```

### Linting

We use multiple linting tools:

- **Flake8**: Style guide enforcement
- **Pylint**: Code quality analysis (errors only in CI)
- **Mypy**: Static type checking

```bash
# Run all linters
make lint

# Or individually:
flake8 pulp_tool/ tests/
pylint pulp_tool/ --errors-only
mypy pulp_tool/ --show-error-codes
```

### Type Annotations

- All function arguments must have type annotations
- Return types should be specified for all functions
- Use `Optional[T]` for nullable types
- Use `Union[T1, T2]` for multiple possible types
- Use `TYPE_CHECKING` for forward references to avoid circular imports

Example:
```python
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .api.pulp_client import PulpClient

def my_function(client: "PulpClient", value: Optional[str] = None) -> bool:
    """Function with proper type annotations."""
    return value is not None
```

## Testing

### Running Tests

```bash
# Run all tests with coverage (preferred; enforces 85%+)
make test

# Before a PR: full suite + 100% coverage on git diff vs origin/main (matches CI)
# git fetch origin   # ensure compare branch exists
make test-diff-coverage
# Override base branch: make test-diff-coverage COMPARE_BRANCH=origin/my-base

# Or with pytest directly
pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-fail-under=85

# Run specific test file
pytest tests/cli/test_cli_core.py -v

# Run specific test markers
pytest -m unit
pytest -m integration
```

### Test Requirements

- All new code must include tests
- Overall test coverage must remain at or above 85%
- New and changed lines must have 100% coverage (enforced in CI via diff coverage)
- **Before opening or updating a PR**, run `make test-diff-coverage` after `git fetch origin` so your local report matches the merge gate (`diff-cover` vs `origin/main` by default). Use `make test-diff-coverage COMPARE_BRANCH=origin/<base>` if the PR targets another branch.
- Tests should be fast and isolated
- Use fixtures from `tests/conftest.py` when possible
- Mark slow tests with `@pytest.mark.slow`
- Mark integration tests with `@pytest.mark.integration`

### Writing Tests

- Follow the existing test structure
- Use descriptive test names: `test_function_name_scenario_expected_result`
- Use pytest fixtures for common setup
- Mock external dependencies (HTTP calls, file system, etc.)
- Test both success and failure cases
- Test edge cases and boundary conditions

Example:
```python
import pytest
from unittest.mock import Mock, patch

def test_upload_content_success():
    """Test successful content upload."""
    client = Mock()
    client.upload_content.return_value = "/api/v3/content/123/"

    result = upload_content(client, "/path/to/file.rpm", {})
    assert result == "/api/v3/content/123/"
    client.upload_content.assert_called_once()
```

## Pull Request Process

### Before Submitting

1. **Update CHANGELOG.md**: Add an entry describing your changes
2. **Update documentation**: If adding features, update README.md as needed
3. **Run all checks**: Ensure `make check` passes (or run `make lint`, then `pre-commit run --all-files` twice, then `make test`)
4. **Verify PR diff coverage**: Run `make test-diff-coverage` (requires `git fetch origin` so `origin/main` or your `COMPARE_BRANCH` exists); CI fails below 100% on the diff
5. **Write tests**: Add tests for new functionality (new/changed code requires 100% diff coverage)
6. **Update type hints**: Ensure all functions have proper type annotations

### PR Checklist

- [ ] Code follows the project's style guidelines
- [ ] All tests pass (`make test`)
- [ ] PR diff coverage is 100% (`make test-diff-coverage` after `git fetch origin`)
- [ ] All linters pass (`make lint`)
- [ ] Pre-commit hooks pass (`pre-commit run --all-files`, run twice after fixing issues)
- [ ] Test coverage is maintained or improved (85%+ overall, 100% for new/changed lines)
- [ ] CHANGELOG.md is updated
- [ ] Documentation (e.g. README.md) is updated if needed
- [ ] Commits follow the commit message conventions

### Commit Messages

Follow these conventions for commit messages:

```
type(scope): short description

Longer description if needed, explaining what and why.

Fixes #123
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test additions or changes
- `chore`: Maintenance tasks

Examples:
```
feat(cli): add --dry-run option to upload command

Allows users to preview upload operations without actually
uploading files to Pulp.

fix(transfer): handle missing artifact metadata gracefully

Previously would crash if artifact JSON was missing expected
fields. Now logs warning and continues.

docs(readme): update installation instructions

Adds Python 3.12 support information.
```

#### AI-assisted commits

When the work was produced with an AI assistant, end the commit message with this trailer block (after a blank line following the subject or body):

```
<type(scope): short description>

Assisted-By: Cursor
Signed-off-by: Your Name <your-email@example.com>
```

- **Assisted-By:** The **agent or product** you used (e.g. `Cursor`, `Composer`, `Claude`).
- **Signed-off-by:** **You** as the human author, in [Developer Certificate of Origin](https://developercertificate.org/) form — typically `git config user.name` and `git config user.email`, often aligned with your [GitHub commit email](https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-email-preferences/setting-your-commit-email-address).

A blank layout you can copy is **`.github/commit-message-template.txt`**. To use it as the editor starter for every commit: `git config commit.template .github/commit-message-template.txt` (repo-relative path; adjust if you set it globally).

### PR Description Template

When creating a PR, include:

1. **Summary**: Brief description of changes
2. **Type**: Feature, Bug Fix, Documentation, etc.
3. **Changes**: Detailed list of changes
4. **Testing**: How to test the changes
5. **Related Issues**: Link to related issues

Example:
```markdown
## Summary
Adds --dry-run option to upload command for testing upload operations.

## Type
Feature

## Changes
- Added --dry-run flag to upload command
- Added dry-run mode that validates files without uploading
- Updated help text and documentation

## Testing
- Run `pulp-tool upload --dry-run ...` to test
- Verify files are validated but not uploaded
- Check logs show "DRY RUN" mode

## Related Issues
Closes #123
```

## Code Review

- All PRs require at least one approval before merging
- Address review comments promptly
- Be respectful and constructive in reviews
- Ask questions if something is unclear

## Questions?

- Open an issue for bug reports or feature requests
- Check existing issues and PRs before creating new ones
- Join discussions in existing issues/PRs

Thank you for contributing to Pulp Tool!
