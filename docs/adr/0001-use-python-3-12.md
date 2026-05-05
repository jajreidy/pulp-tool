# 1. Use Python 3.12 as the minimum version

Date: 2026-04-21

## Status

Accepted

## Context

The project needs a single baseline for typing (`typing` / `collections.abc`), asyncio, and toolchains (mypy, Black, pytest).

## Decision

Require **Python ≥ 3.12** as declared in `pyproject.toml` (`requires-python`).

## Consequences

- Contributors and CI standardize on 3.12+.
- Syntax and stdlib features from 3.12 are available without backports.
