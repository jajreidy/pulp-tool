---
name: troubleshooting-pulp-tool-ci
description: >-
  Use when pre-commit hooks fail repeatedly, lint errors persist after make format,
  or local checks disagree with CI on pulp-tool.
---

# Troubleshooting pulp-tool CI / lint

## Pre-commit loop

Pre-commit mirrors GitHub PR CI (`.github/workflows/python-diff-lint.yml` + `security-scan.yml`):

| Stage | Hooks |
|-------|--------|
| **commit** | lock-check, ruff, pylint, mypy, pip-audit, yamllint, shellcheck (`scripts/*.sh`), codespell, hadolint |
| **pre-push** | `make test-diff-coverage`, checkton (Tekton embedded scripts) |

```bash
make install-dev   # installs commit + pre-push hooks
make pre-commit-ci # both stages (run before opening a PR)
```

Or step by step:

```bash
pre-commit run --all-files
git fetch origin
pre-commit run --hook-stage pre-push --all-files
```

Fix **every** failure, then re-run until one full run passes with zero failures.

Single hook: `pre-commit run <hook-id> --all-files`

## Lint quick reference

```bash
make lint          # Python linters (matches CI python-lint job)
make format        # Auto-fix with Ruff (run before re-checking ruff hooks)
make lint-ruff
make lint-pylint
make lint-mypy
make audit         # pip-audit (matches security-scan.yml)
```

Prefer Makefile targets over invoking tools directly.

## Common issues

| Issue | Solution |
|-------|----------|
| Ruff import order (I001) | Run `make format` or `ruff check --fix` |
| Ruff S105 on OAuth/token URLs | Use `RED_HAT_SSO_TOKEN_URL` constant or `# noqa: S105` |
| Mypy errors in specific modules | Check `[[tool.mypy.overrides]]` in `pyproject.toml` — may be intentional |
| Hooks fail on commit | Loop `pre-commit run --all-files` locally until clean |
| Yamllint truthy on `on:` | Add `# yamllint disable-line rule:truthy` (see release workflow) |
| Hadolint DL3041/DL3013 on Dockerfile | Ignored in pre-commit/CI (UBI microdnf + pip install pattern) |
| checkton: docker/podman required | Install podman/docker, or rely on CI tekton-lint job |
| checkton: `unknown revision or path not in the working tree` / `main` | Run `git fetch origin main` (or set `CHECKTON_DIFF_BASE` to an existing ref). `scripts/run-checkton.sh` fetches only when defaulting the base ref and exits with instructions if `origin/main` is missing. CI tekton-lint runs on pull requests only (needs PR base/head SHAs). |
| test-diff-coverage on push hook | Run `git fetch origin` so `origin/main` exists |

## Config files

- `.pre-commit-config.yaml` — hooks (parity with GitHub Actions)
- `pyproject.toml` — Ruff, Pylint, Mypy
- `.yamllint.yml` — YAML lint rules
- `Makefile` — targets (`make pre-commit-ci`)

## Diff coverage

If the failure is uncovered changed lines, use **fixing-diff-cover-failures** skill instead of guessing from lint output.
