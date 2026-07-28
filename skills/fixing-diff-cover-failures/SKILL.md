---
name: fixing-diff-cover-failures
description: >-
  Use when make test-diff-coverage fails, CI reports diff coverage below 100%,
  or a GitHub PR is blocked on uncovered changed lines.
---

# Fixing diff-cover failures

## Overview

GitHub merge requires **100% test coverage on the PR diff** (branch vs base). Overall project coverage (e.g. 85%) is separate and does not unblock merge.

## Loop until green

```bash
git fetch origin
make test-diff-coverage
# Non-main base: make test-diff-coverage COMPARE_BRANCH=origin/<base>
```

1. Read uncovered lines printed by `diff-cover` (same signal as CI).
2. Add or extend tests so every **changed** line executes in tests (branches, errors, edge cases).
3. Re-run `make test` (full suite), then `make test-diff-coverage`.
4. Repeat until both pass.

Use `python3 -m pytest --cov=pulp_tool --cov-report=term-missing` only to debug specific files; **`make test-diff-coverage` is the authoritative gate.**

## Red flags

- Declaring PR-ready after `make test` alone (without diff-cover)
- Relying on "nearby" coverage from unrelated tests
- Stopping after one green run before a later fix broke coverage
- Using `# pragma: no cover` to avoid writing tests (only if truly unreachable and justified)

## Common rationalizations

| Excuse | Reality |
|--------|---------|
| "Overall coverage is high" | Merge gate is 100% on the diff only |
| "That line is hard to test" | Add a focused unit test or refactor for testability |
| "CI might pass anyway" | diff-cover locally matches CI; fix before push |

## References

- Merge policy: [CONTRIBUTING.md](../../CONTRIBUTING.md)
- Full check script: `git fetch origin && make pre-commit-ci` (or `./scripts/check-all.sh` for Python lint + tests + diff-cover)
