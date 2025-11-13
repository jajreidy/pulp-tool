# Makefile for pulp-tool development tasks

.PHONY: help install install-dev test lint format check clean docs

# Default target
help:
	@echo "Available targets:"
	@echo "  make install      - Install package"
	@echo "  make install-dev  - Install package with dev dependencies"
	@echo "  make test         - Run tests with coverage"
	@echo "  make lint         - Run all linters"
	@echo "  make format       - Format code with Black"
	@echo "  make check        - Run all checks (lint + test)"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make docs         - Build documentation (if Sphinx configured)"

# Installation
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install || echo "pre-commit not available, skipping"

# Testing
test:
	python3 -m pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-report=html --cov-report=xml --cov-fail-under=85

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

# Documentation
docs:
	cd docs && $(MAKE) html

docs-clean:
	cd docs && $(MAKE) clean

docs-serve:
	cd docs/_build/html && python3 -m http.server 8000

# Development helpers
setup: install-dev
	@echo "Development environment setup complete!"

update-deps:
	@./scripts/update-deps.sh

run-tests:
	@./scripts/run-tests.sh $(ARGS)

check-all:
	@./scripts/check-all.sh
