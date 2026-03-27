# Makefile for pulp-tool development tasks

COMPARE_BRANCH ?= origin/main
AUDIT_VENV ?= .audit-venv

.PHONY: help install install-dev test test-diff-coverage lint format check clean audit

# Default target
help:
	@echo "Available targets:"
	@echo "  make install      - Install package"
	@echo "  make install-dev  - Install package with dev dependencies"
	@echo "  make test         - Run tests with coverage"
	@echo "  make test-diff-coverage - make test + diff-cover 100% vs COMPARE_BRANCH (same gate as PR CI)"
	@echo "  make lint         - Run all linters"
	@echo "  make format       - Format code with Black"
	@echo "  make check        - Run all checks (lint + test)"
	@echo "  make audit        - Run pip-audit in a throwaway venv (only this project's dev deps)"
	@echo "  make clean        - Clean build artifacts"
	@echo ""
	@echo "  Diff coverage base: COMPARE_BRANCH=origin/main (override for e.g. origin/release-1.0)"

# Installation
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install || echo "pre-commit not available, skipping"

# Testing
test:
	python3 -m pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-report=html --cov-report=xml --cov-fail-under=85

# Same as GitHub Actions: 100% coverage on lines changed vs merge base (requires coverage.xml from test).
# Requires diff-cover (included in pip install -e ".[dev]" / make install-dev).
test-diff-coverage: test
	@command -v diff-cover >/dev/null 2>&1 || { echo "diff-cover not found. Run: make install-dev"; exit 1; }
	@echo "Diff coverage vs $(COMPARE_BRANCH) (fail under 100%)..."
	diff-cover coverage.xml --compare-branch=$(COMPARE_BRANCH) --fail-under=100

test-fast:
	python3 -m pytest -v --tb=short

test-unit:
	python3 -m pytest -v -m unit

test-integration:
	python3 -m pytest -v -m integration

# Linting
lint: lint-black lint-flake8 lint-pylint lint-mypy

lint-black:
	python3 -m black --check pulp_tool/ tests/

lint-flake8:
	python3 -m flake8 pulp_tool/ tests/

lint-pylint:
	python3 -m pylint pulp_tool/ --errors-only

lint-mypy:
	python3 -m mypy pulp_tool/ --show-error-codes

# Formatting
format:
	python3 -m black pulp_tool/ tests/

# Run all checks
check: lint test

# Pygments CVE-2026-4539: no wheel >2.19.2 on PyPI yet (transitive via pytest/diff-cover). Drop when pinning pygments>=2.19.3.
AUDIT_IGNORES := --ignore-vuln CVE-2026-4539 --ignore-vuln GHSA-5239-wwwm-4pmq

audit:
	@echo "pip-audit: creating $(AUDIT_VENV), installing .[dev]..."
	@rm -rf "$(AUDIT_VENV)" && python3 -m venv "$(AUDIT_VENV)" && \
	 "$(AUDIT_VENV)/bin/python" -m pip install -q -U pip && \
	 "$(AUDIT_VENV)/bin/python" -m pip install -q -e ".[dev]" && \
	 "$(AUDIT_VENV)/bin/pip-audit" -l --desc on $(AUDIT_IGNORES)
	@rm -rf "$(AUDIT_VENV)"
	@echo "pip-audit: OK"

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov/
	rm -f coverage.xml
	rm -f .coverage
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# Development helpers
setup: install-dev
	@echo "Development environment setup complete!"

update-deps:
	@./scripts/update-deps.sh

run-tests:
	@./scripts/run-tests.sh $(ARGS)

check-all:
	@./scripts/check-all.sh
