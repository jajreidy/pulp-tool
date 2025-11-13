#!/bin/bash
# Run all code quality checks for pulp-tool

set -e

echo "Running all code quality checks..."
echo ""

# Black formatting check
echo "1. Checking code formatting (Black)..."
python3 -m black --check pulp_tool/ tests/ || {
    echo "❌ Black formatting check failed. Run 'make format' to fix."
    exit 1
}
echo "✅ Black formatting check passed"
echo ""

# Flake8 linting
echo "2. Running Flake8 linting..."
python3 -m flake8 pulp_tool/ tests/ || {
    echo "❌ Flake8 linting failed."
    exit 1
}
echo "✅ Flake8 linting passed"
echo ""

# Pylint (errors only)
echo "3. Running Pylint (errors only)..."
python3 -m pylint pulp_tool/ --errors-only || {
    echo "❌ Pylint check failed."
    exit 1
}
echo "✅ Pylint check passed"
echo ""

# Mypy type checking
echo "4. Running Mypy type checking..."
python3 -m mypy pulp_tool/ --show-error-codes || {
    echo "❌ Mypy type checking failed."
    exit 1
}
echo "✅ Mypy type checking passed"
echo ""

# Run tests
echo "5. Running tests..."
python3 -m pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-fail-under=85 || {
    echo "❌ Tests failed or coverage below threshold."
    exit 1
}
echo "✅ Tests passed"
echo ""

echo "🎉 All checks passed!"
