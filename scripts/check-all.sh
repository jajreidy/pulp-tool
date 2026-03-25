#!/bin/bash
# Run all code quality checks for pulp-tool
#
# After tests, runs diff-cover vs origin/main when available (100% on PR diff, same as CI).
# Set DIFF_COVER_COMPARE_BRANCH to override the base (e.g. origin/release-1.0).
# Requires dev deps (diff-cover). If the compare ref is missing: git fetch origin

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

# Run tests (XML for diff-cover; same pytest run as Makefile test minus html)
echo "5. Running tests..."
python3 -m pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-report=xml --cov-fail-under=85 || {
    echo "❌ Tests failed or coverage below threshold."
    exit 1
}
echo "✅ Tests passed"
echo ""

# PR merge gate: 100% coverage on changed lines vs merge base (optional if branch missing)
COMPARE_BRANCH="${DIFF_COVER_COMPARE_BRANCH:-origin/main}"
if command -v diff-cover >/dev/null 2>&1; then
    if git rev-parse --verify "$COMPARE_BRANCH" >/dev/null 2>&1; then
        echo "6. Diff coverage vs $COMPARE_BRANCH (100% required in PR CI)..."
        diff-cover coverage.xml --compare-branch="$COMPARE_BRANCH" --fail-under=100 || {
            echo "❌ Diff coverage below 100%. Fix tests or run: make test-diff-coverage COMPARE_BRANCH=$COMPARE_BRANCH"
            exit 1
        }
        echo "✅ Diff coverage OK"
    else
        echo "⚠️  Skipping diff-cover: $COMPARE_BRANCH not found. Run: git fetch origin"
        echo "    Then re-run this script or: make test-diff-coverage COMPARE_BRANCH=$COMPARE_BRANCH"
    fi
else
    echo "⚠️  diff-cover not on PATH; install dev deps (make install-dev). Run: make test-diff-coverage"
fi
echo ""

echo "🎉 All checks passed!"
