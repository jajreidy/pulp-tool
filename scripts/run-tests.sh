#!/bin/bash
# Standardized test execution script for pulp-tool

set -e

# Default to running all tests
TEST_ARGS="${@:-}"

echo "Running pulp-tool tests..."
echo ""

# Run pytest with coverage
if [ -z "$TEST_ARGS" ]; then
    echo "Running all tests with coverage..."
    python3 -m pytest \
        -v \
        --tb=short \
        --cov=pulp_tool \
        --cov-report=term-missing \
        --cov-report=html \
        --cov-report=xml \
        --cov-fail-under=85
else
    echo "Running tests with arguments: $TEST_ARGS"
    python3 -m pytest -v --tb=short $TEST_ARGS
fi

echo ""
echo "Tests completed!"
