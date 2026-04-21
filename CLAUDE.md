# pulp-tool

Python **CLI and library** for [Pulp](https://pulpproject.org/) REST API operations (RPM and file content, repositories, uploads, pulls). The code runs interactively and inside **Konflux** (Tekton) tasks—changes to `upload`, global CLI flags, SBOM or artifact-result handling, or the container image affect downstream pipelines.

For platform background (tenants, Tekton, trusted artifacts, releases), see the **[Konflux documentation](https://konflux-ci.dev/docs/)** (e.g. *Building → Image management*). For upstream **Pulp** (REST API, plugins, docs), see **[pulpproject.org](https://pulpproject.org/)**. The **Konflux / Tekton** sections below are **pulp-tool–specific** integration contracts.

## Quick reference

| Item | Value |
|------|------|
| **Python** | >= 3.12 (see `pyproject.toml`) |
| **Package** | `pulp_tool` |
| **CLI entry** | `pulp-tool` → `pulp_tool.cli:main` |
| **Konflux contracts** | **This file** — required reading for upload / CI–related PRs |
| **Dev lockfile** | **`requirements.txt`** (from `requirements.in`; `make lock` after `pyproject.toml` dep changes) |
| **Cursor / LLM workflow** | **[`.cursor/rules/llm-development-guidelines.mdc`](.cursor/rules/llm-development-guidelines.mdc)** |
| **GitHub PR template** | **[`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md)** — use when opening a PR |

## Commands

```bash
make install-dev          # pip install -e ".[dev]" + pre-commit install
make test                 # Full suite, coverage (85%+ project threshold)
make test-diff-coverage   # After: git fetch origin — 100% on PR diff vs COMPARE_BRANCH (default origin/main)
make lint                 # black --check, flake8, pylint, mypy
make format               # black pulp_tool/ tests/
pre-commit run --all-files
make check                # lint + test
```

Before a PR: loop `pre-commit` until clean; `git fetch origin` and `make test-diff-coverage`. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Architecture (high level)

```
pulp_tool/
├── api/              # PulpClient, mixins, httpx session (transient HTTP retries in utils/session.py)
├── cli/              # Click commands: upload, upload_files, pull, search_by, create_repository
├── models/           # Pydantic models
├── pull/             # Pull / download helpers
├── services/         # Upload orchestration (UploadService → PulpHelper)
└── utils/            # PulpHelper, repository/upload helpers, session, validation
tests/
├── support/          # Shared fixtures, TLS helpers
└── …                 # Mirrors package layout where practical; see tests/README.md
```

**Data flow (upload):** CLI → `PulpHelper.setup_repositories` / `process_uploads` → `UploadOrchestrator` + `upload_service` / `upload_collect`.

## Conventions

- **Types:** Prefer type hints; mypy enabled for `pulp_tool/` (see `pyproject.toml` overrides).
- **Tests:** New or changed lines need **100% diff coverage** for GitHub merge (not only overall %). See `.cursor/rules/llm-development-guidelines.mdc`.
- **Konflux:** Do not change `upload` flags or artifact paths without re-reading the **Konflux** sections below and linked Tekton YAMLs.

## Key files

| Purpose | Location |
|---------|----------|
| Maintainer + Konflux context | **This file (`CLAUDE.md`)** |
| User README | [README.md](README.md) |
| Contributing / style / checks | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Test layout and patterns | [tests/README.md](tests/README.md) |
| HTTP retries (5xx/429) | `pulp_tool/utils/session.py` |
| Upload CLI | `pulp_tool/cli/upload.py` |
| Container build (Konflux image) | `.tekton/pulp-tool-container-build-push.yaml` |

### Using this doc in a PR review

If you are new here: **pulp-tool** is mainly a **Click** CLI and Python client for Pulp’s HTTP API. Konflux runs the **`upload`** subcommand with flags and mounted config paths that differ between pipelines—**both** integrations below must keep working after a change.

- When reviewing or authoring a PR that touches **`upload`**, global CLI options, SBOM/artifact handling, or the **container image**, open the **linked task YAMLs** in the sections below and check that behavior (flags, paths, skip vs failure) still matches.
- This documentation is **descriptive**: it was written against linked revisions of upstream Tekton tasks. Those files can change; if something looks off, compare against the current YAML on GitHub.

## Konflux / Tekton downstream integrations

Two managed flows invoke the **`pulp-tool-container`** image (`quay.io/redhat-user-workloads/artifact-storage-tenant/tooling/pulp-tool-container:latest`; digest in each task YAML may pin a specific build). Both use a workspace rooted at **`/var/workdir/results`** for RPM-related input in the `upload` step. Paths and flags differ.

### 1. RPM build pipeline — `import-to-quay`

- **Repo:** [konflux-ci/rpmbuild-pipeline](https://github.com/konflux-ci/rpmbuild-pipeline)
- **Task:** [`task/import-to-quay.yaml`](https://github.com/konflux-ci/rpmbuild-pipeline/blob/main/task/import-to-quay.yaml)
- **Step:** `push-to-pulp-select-auth`

**Invocation (illustrative; Tekton substitutes params/results):**

```bash
pulp-tool --config /pulp-access/cli.toml \
  --build-id "<pipelinerun-id>" \
  --namespace "<taskRun namespace>" \
  upload \
  --parent-package "<package-name>" \
  --rpm-path "/var/workdir/results" \
  --sbom-path "/var/workdir/results/oras-staging/sbom-merged.json" \
  --artifact-results "<PULP-IMAGE_URL result path>,<PULP-IMAGE_DIGEST result path>"
```

**Environment and guardrails:**

- Config: secret volume **`pulp-access`** (optional) mounted at **`/pulp-access`**, file **`/pulp-access/cli.toml`**. If that file is missing, the step **skips** Pulp upload and writes **empty** `PULP-IMAGE_URL` / `PULP-IMAGE_DIGEST` Tekton result files.
- Earlier steps (`gather-rpms`, Quay push, `merge-syft-sbom`) populate **`/var/workdir/results`** and **`/var/workdir/results/oras-staging/`** (including `sbom-merged.json`). Changes to RPM or SBOM layout on disk can break this task.

### 2. Release service — `push-artifacts-to-storage`

- **Repo:** [konflux-ci/release-service-catalog](https://github.com/konflux-ci/release-service-catalog)
- **Managed pipeline (wires this task):** [`pipelines/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml`](https://github.com/konflux-ci/release-service-catalog/blob/development/pipelines/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml) (branch `development`)
- **Task:** [`tasks/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml`](https://github.com/konflux-ci/release-service-catalog/blob/development/tasks/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml) (branch `development` as referenced in managed pipelines)
- **Step:** `push-build-to-artifact-storage`

**Invocation (illustrative):**

```bash
pulp-tool --config /etc/rok-access/cli.toml \
  --build-id "<snapshotBuildId>" \
  --namespace "<snapshotNamespace>" \
  upload \
  --rpm-path "/var/workdir/results"
```

**Environment and guardrails:**

- Config: **`/etc/rok-access/cli.toml`** (mounted **rok-access** secret). If that file is absent, the step **exits successfully (0) without calling `pulp-tool`**—not a failing exit—so the task run still succeeds with no upload.
- The pipeline may **skip** artifact storage when **`koji_import_draft`** in merged data JSON is **`false`** (draft-build path).

### Comparison

| Aspect | import-to-quay | push-artifacts-to-storage |
|--------|----------------|---------------------------|
| Config path | `/pulp-access/cli.toml` | `/etc/rok-access/cli.toml` |
| `upload` flags | `--parent-package`, `--sbom-path`, `--artifact-results` | `--rpm-path` only (in task script) |
| SBOM | Task always passes `--sbom-path` to `…/oras-staging/sbom-merged.json` when Pulp runs | No `--sbom-path` in this task’s `pulp-tool` invocation |

## In-repo code tied to Konflux behavior

### Canonical upload entry (read this first)

- **Primary path for CLI and Konflux:** `pulp_tool/cli/upload.py` uses **`PulpHelper`** (`pulp_tool/utils/pulp_helper.py`) — `setup_repositories` and `process_uploads`, which delegate to **`UploadOrchestrator`** and the helpers in **`upload_service`** / **`upload_collect`** for results JSON and Konflux artifacts.
- **`UploadService`** (`pulp_tool/services/upload_service.py`) is the same orchestration exposed as a small class for tests and programmatic callers; it delegates to **`PulpHelper`**, not a parallel implementation.

- **CLI:** `pulp_tool/cli/upload.py`, `pulp_tool/cli/upload_files.py` — global options; `--artifact-results` as `url_path,digest_path` for Konflux result files.
- **Upload / artifact + SBOM integration:** `pulp_tool/services/upload_service.py` and `pulp_tool/services/upload_collect.py` — e.g. `_write_konflux_results`, `_handle_artifact_results`, SBOM handling.
- **Pull:** `pulp_tool/pull/download.py` — `konflux-` domain prefix behavior for pull.
- **Container image build:** `.tekton/pulp-tool-container-build-push.yaml` — publishes the Quay image used by the tasks above.

## Regression checklist (before merging risky changes)

When touching any of the following, re-read the two task YAMLs above and consider adding or extending tests:

- `upload` / `upload-files` semantics, defaults, or required options
- Global flags: `--config`, `--build-id`, `--namespace`
- `--rpm-path`, `--parent-package`, `--sbom-path`, `--artifact-results`
- Config loading, TLS, or paths assumed in containers
- Container entrypoint, image contents, or `pulp-tool` invocation
- RPM discovery or directory layout under `--rpm-path`
- **Upstream pipeline layout:** tasks may change how RPMs and SBOMs are staged (e.g. **ORAS** / trusted-artifacts steps, `oras-staging/`, or replacements). Re-verify **konflux-ci/rpmbuild-pipeline** and **konflux-ci/release-service-catalog** when altering anything that assumes workspace paths; refresh **this doc** if call sites move.

## Hypothesis Ghostwriter (optional test scaffolding)

[Hypothesis Ghostwriter](https://hypothesis.readthedocs.io/en/latest/reference/integrations.html#ghostwriter) (`hypothesis.extra.ghostwriter`) can generate **starter** property-based tests from the CLI (`hypothesis write …`). Use it to bootstrap `@given` tests for pure helpers, then edit and harden them before merging.

- **Setup:** `pip install -e ".[dev]"` (Hypothesis is in optional `dev` extras). The ghostwriter expects **[Black](https://pypi.org/project/black/)** to be available to format emitted code; this repo already uses Black in `make format` / pre-commit.
- **Examples:** `hypothesis write --help` lists modes (`--roundtrip`, `--equivalent`, `--idempotent`, `--except`, pytest vs unittest style). You can target a dotted path such as `hypothesis write pulp_tool.utils.correlation.resolve_correlation_id` or pass a module for `magic`-style generation per the docs.
- **Scope in pulp-tool:** Prefer **small, pure** functions (parsers, sanitizers, dict-in/dict-out helpers)—same boundaries as in [tests/README.md](tests/README.md). Ghostwriting **full `PulpClient` or CLI flows** usually produces tests that need large follow-up (HTTP mocking, fixtures, slow runs); treat that as a manual design task unless you plan to invest in strategies and shrinking.
- **Before merge:** Treat generated code as a draft—fix imports, align with `tests/` layout and `tests/support/`, cap `@settings(max_examples=…)`, avoid function-scoped pytest fixtures inside `@given` without understanding [Hypothesis health checks](https://hypothesis.readthedocs.io/en/latest/reference/api.html#hypothesis.HealthCheck), and satisfy this repo’s **100% PR diff coverage** gate (`make test-diff-coverage`).

## Related docs

- **[docs/cli-reference.md](docs/cli-reference.md)** — full CLI flags and examples (README links here for a shorter landing page).
- **[docs/adr/0000-record-architecture-decisions.md](docs/adr/0000-record-architecture-decisions.md)** — when and how we add ADRs.
- **[`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md)** — GitHub PR body template (Summary, How to test, Checklist).
- **[`.cursor/rules/konflux-ecosystem.mdc`](.cursor/rules/konflux-ecosystem.mdc)** — short Konflux pointers.
- **[`.cursor/rules/llm-development-guidelines.mdc`](.cursor/rules/llm-development-guidelines.mdc)** — lint, pre-commit, tests, diff coverage, PR/changelog conventions; suggested commits use **`Assisted-By:`** (agent used) and **`Signed-off-by:`** (human / Git identity). See also **`.github/commit-message-template.txt`** and [CONTRIBUTING.md](CONTRIBUTING.md#ai-assisted-commits).
- Optional repo readiness scan: [AgentReady](https://github.com/ambient-code/agentready) — `agentready assess .` (see [README.md](README.md#ai-assisted-development)).
