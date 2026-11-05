---
name: changing-pulp-upload
description: >-
  Use when changing upload, pull, transfer, global CLI flags (--config, --build-id,
  --namespace), --rpm-path, --parent-package, --sbom-path, --artifact-results,
  --source-config, Tekton workspace paths, or Konflux downstream integration.
  Container image *build*: changing-pulp-container.
---

# Changing pulp upload / Konflux integration

## Overview

Downstream Tekton pipelines invoke `pulp-tool` with fixed paths and flags. Changes must stay compatible with **documented call sites** and with the **`pull` vs `transfer` split**:

| Command | Role |
|---------|------|
| **`upload-build`** / **`upload`** | Build pipeline: write artifacts + `pulp_results.json` (+ optional ORAS attach) |
| **`pull`** | Download only (verify build output, read OCI-backed metadata) |
| **`transfer`** | Download from build Pulp + promote to **release Pulp** (artifact storage target); side-tag, ORAS, GPG, snapshot flags |
| **`update-build`** | Mutate `pulp_results` after signing/BTS in-place |

**Source of truth:** [CLAUDE.md](../../docs/CLAUDE.md). **Code map:** [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) — upload under `pulp_tool/upload/`; pull/transfer via **`PullService`**, **`ArtifactPullRunOptions`**, and `pulp_tool/cli/artifact_pull_run.py`.

## Workflow

1. Read [CLAUDE.md](../../docs/CLAUDE.md) (pipelines, **`transfer`** target contract, regression checklist).
2. Re-open current upstream task YAML on GitHub (pipelines evolve):
   - [import-to-quay.yaml](https://github.com/konflux-ci/rpmbuild-pipeline/blob/main/task/import-to-quay.yaml) — step `push-to-pulp-select-auth`
   - [push-artifacts-to-storage.yaml](https://github.com/konflux-ci/release-service-catalog/blob/development/tasks/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml) — step `push-artifacts-to-storage` ([release-service-utils](https://github.com/konflux-ci/release-service-utils) → `pulp-tool upload` today)
3. Implement + extend tests for every checklist item touched.
4. New multi-field CLI or workflow inputs: **`KonfluxBaseModel`** in `pulp_tool/models/` — [pydantic-data-models](../pydantic-data-models/SKILL.md) (not dataclasses in `cli/`).
5. Update [CLAUDE.md](../../docs/CLAUDE.md) if invocation, paths, or flags change.
6. Update [docs/cli-reference.md](../../docs/cli-reference.md) and [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) when behavior or layout changes.

## Regression checklist

Before merge, verify (extend tests where applicable):

- [ ] `upload` / `upload-files` behavior, defaults, required options
- [ ] Global: `--config`, `--build-id`, `--namespace`
- [ ] `--rpm-path`, `--parent-package`, `--sbom-path`, `--artifact-results`, **`--oci-storage`** (Konflux `ociStorage`; no `cli.oci_storage` in config)
- [ ] **`pull`**: download-only; no destination promotion flags on this command
- [ ] **`transfer`**: required destination `--config`; **`--source-config`** when build ≠ destination; side-tag / ORAS / GPG / snapshot flags
- [ ] Config/TLS/paths assumed in containers; image entrypoint and invocation (image *build*: **changing-pulp-container**)
- [ ] RPM discovery under `--rpm-path`
- [ ] Linked Tekton tasks still match (config path, flags, workspace `/var/workdir/results`, `oras-staging/` where used)
- [ ] If changing `PulpClient` pools, GET cache, or upload concurrency: update [`pulp_tool/utils/client_limits.py`](../../pulp_tool/utils/client_limits.py), tests under `tests/utils/test_client_limits.py`, and [ARCHITECTURE.md § Memory and Konflux pods](../../docs/ARCHITECTURE.md)

## Red flags — stop and re-read CLAUDE.md

- Changing a default flag without checking **both** Tekton scripts (and future **`transfer`** release tasks)
- Putting side-tag, ORAS publish, or `--transfer-dest`-style promotion back on **`pull`**
- Assuming one pipeline’s flags apply to the other (`--sbom-path` is import-to-quay only)
- Duplicating upload logic outside `PulpHelper` → `pulp_tool/upload/`
- Skipping upstream YAML review (“we only changed tests” on upload paths)

## Common rationalizations

| Excuse | Reality |
|--------|---------|
| "Only affects rpmbuild-pipeline" | Release path uses different config and fewer upload flags — check CLAUDE §2–3 |
| "Tests pass locally" | Container paths and Tekton result files need explicit coverage |
| "Upstream YAML is stable" | ORAS/staging and release-service-utils wrappers change; re-verify linked tasks |

## Quick reference

| | import-to-quay | push-artifacts-to-storage (today) |
|--|----------------|-----------------------------------|
| Config | `/pulp-access/cli.toml` (build Pulp) | `/etc/rok-access/cli.toml` (release Pulp) |
| Command | `upload-build` (+ ORAS flags) | `upload` after ORAS bundle pull (utils module) |
| Missing config | Skip upload; empty Tekton results | Skip `pulp-tool` upload |

| | pull | transfer |
|--|------|----------|
| Destination upload | No | Yes (requires global `--config`) |
| `--source-config` | N/A | When build Pulp ≠ release Pulp |
| Side-tag / ORAS / GPG | No | Yes |

Full command examples and secrets behavior: [CLAUDE.md](../../docs/CLAUDE.md) · flags: [cli-reference.md](../../docs/cli-reference.md).
