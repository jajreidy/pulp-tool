#!/bin/bash
# Standardized test execution script for pulp-tool

set -e

echo "Running pulp-tool tests..."
echo ""

# Run pytest with coverage (options from pyproject.toml [tool.pytest.ini_options])
if [ "$#" -eq 0 ]; then
    echo "Running all tests with coverage..."
    python3 -m pytest --cov-report=xml
else
    echo "Running tests with arguments: $*"
    python3 -m pytest "$@"
fi

echo ""
echo "Tests completed!"
