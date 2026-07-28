#!/bin/bash
# Development environment setup script for pulp-tool

set -e

echo "Setting up pulp-tool development environment..."

# Check Python version (CI and container image use 3.12)
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"
if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 12) else 1)'; then
    echo "Warning: Python 3.12+ is required (matches CI and the Konflux container image)."
fi

# Install package in development mode
echo "Installing pulp-tool in development mode..."
pip install -e ".[dev]"

# Install pre-commit hooks (commit + pre-push; matches GitHub PR CI)
if command -v pre-commit &> /dev/null; then
    echo "Installing pre-commit hooks..."
    pre-commit install
    pre-commit install --hook-type pre-push || true
else
    echo "Warning: pre-commit not found. Install it with: pip install pre-commit"
fi

echo ""
echo "Development environment setup complete!"
echo ""
echo "Next steps:"
echo "  - Run tests: make test"
echo "  - Run linters: make lint"
echo "  - Format code: make format"
echo "  - Full CI parity: make pre-commit-ci (after green tests)"
echo "  - Run all checks: make check"
