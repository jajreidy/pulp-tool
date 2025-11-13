#!/bin/bash
# Dependency update helper script for pulp-tool

set -e

echo "Updating dependencies for pulp-tool..."
echo ""

# Update pip
echo "Updating pip..."
python3 -m pip install --upgrade pip

# Install/upgrade build tools first
echo "Installing build tools..."
pip install --upgrade setuptools wheel setuptools-scm[toml]

# Install/upgrade package in development mode (this will update dependencies)
echo "Installing/updating package dependencies..."
pip install -e ".[dev]"

# Update pre-commit hooks if installed
if command -v pre-commit &> /dev/null; then
    echo "Updating pre-commit hooks..."
    pre-commit autoupdate
fi

echo ""
echo "Dependencies updated!"
echo ""
echo "To check for outdated packages, run:"
echo "  pip list --outdated"
