# Dependency Management

## Overview

All dependencies are managed in `pyproject.toml` using PEP 621 standard. This provides a single source of truth for all project dependencies.

## Installation

### For Users

```bash
# Install runtime dependencies
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

### For CI/CD

```yaml
- name: Install dependencies
  run: |
    pip install -e ".[dev]"
```

## Benefits

1. **Single source of truth**: All dependencies in one place
2. **Standard compliance**: Uses PEP 621 standard
3. **Better tooling**: Improved integration with modern Python tools
4. **Clear separation**: Runtime vs development dependencies clearly defined
