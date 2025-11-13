# Development Guide

This guide provides detailed information for developers working on pulp-tool.

## Development Environment Setup

See `CONTRIBUTING.md` for initial setup instructions.

## Development Workflow

### Making Changes

1. Create a feature branch from `main`
2. Make your changes
3. Run tests and linters: `make check`
4. Update documentation if needed
5. Update CHANGELOG.md
6. Create a pull request

### Running Tests

```bash
# All tests
make test

# Fast tests (no coverage)
make test-fast

# Unit tests only
make test-unit

# Integration tests only
make test-integration

# Specific test file
pytest tests/test_cli.py -v
```

### Code Quality Checks

```bash
# Run all linters
make lint

# Format code
make format

# Run all checks (lint + test)
make check
```

### Pre-commit Hooks

Pre-commit hooks run automatically on commit. To run manually:

```bash
pre-commit run --all-files
```

## Code Organization

### Adding New Features

1. **API Changes**: Add to appropriate mixin in `pulp_tool/api/`
2. **CLI Commands**: Add to `pulp_tool/cli.py` or create new command module
3. **Models**: Add Pydantic models in `pulp_tool/models/`
4. **Utilities**: Add helper functions in `pulp_tool/utils/`

### Module Guidelines

- Keep modules focused on a single responsibility
- Use type annotations for all functions
- Add docstrings following Google style
- Export public APIs via `__all__` in `__init__.py`

### Testing Guidelines

- Write tests before or alongside code (TDD encouraged)
- Aim for high coverage but focus on meaningful tests
- Use descriptive test names
- Group related tests in classes
- Use fixtures for common setup

## Debugging

### Enable Debug Logging

```bash
# CLI
pulp-tool upload ... -dd  # DEBUG level
pulp-tool upload ... -ddd  # DEBUG with HTTP logs

# Python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Common Issues

**Import Errors**: Ensure package is installed in development mode: `pip install -e .`

**Type Errors**: Run `mypy pulp_tool/` to see type issues

**Test Failures**: Run with `-v` flag for verbose output: `pytest -v`

## Documentation

### Docstring Format

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """Brief description.

    Longer description explaining what the function does
    and any important details.

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value

    Raises:
        ValueError: When something goes wrong

    Example:
        >>> my_function("test", 42)
        True
    """
    pass
```

### Building Documentation

```bash
cd docs
make html
```

Documentation will be built in `docs/_build/html/`.

## Release Process

1. Update version in `pyproject.toml` (if not using setuptools_scm)
2. Update CHANGELOG.md with release notes
3. Create git tag: `git tag v1.0.0`
4. Push tag: `git push origin v1.0.0`
5. GitHub Actions will build and publish release

## Performance Profiling

```bash
# Profile a specific function
python -m cProfile -o profile.stats -m pulp_tool.cli upload ...

# Analyze results
python -m pstats profile.stats
```

## Memory Profiling

```bash
# Install memory_profiler
pip install memory-profiler

# Profile memory usage
python -m memory_profiler pulp_tool/cli.py upload ...
```
