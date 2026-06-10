---
name: changing-pulp-container
description: >-
  Use when changing the Dockerfile, .tekton/ PipelineRuns, pulp-tool-container
  image, Fedora/Python base, or Konflux container build integration. Container
  images are built by Konflux Tekton — not GitHub Actions.
---

# Changing pulp-tool container image / Konflux build

## Overview

The **`pulp-tool-container`** image is built and published by **Konflux Tekton** (Pipelines as Code). GitHub Actions runs unit tests and lint only; it does **not** build the container.

**In-repo:** [Dockerfile](../../Dockerfile), [`.tekton/`](../../.tekton/). **Remote pipeline:** [single-arch-build-pipeline.yaml](https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/.tekton/single-arch-build-pipeline.yaml) (git resolver @ `main`). Full task chain: [reference.md](reference.md).

**Downstream consumers** of the published image: **changing-pulp-upload** + [CLAUDE.md](../../CLAUDE.md).

## In-repo PipelineRuns

| File | PAC trigger | Output image |
|------|-------------|--------------|
| [pulp-tool-container-build-push.yaml](../../.tekton/pulp-tool-container-build-push.yaml) | `push` → `main` | `quay.io/.../pulp-tool-container:latest` |
| [pulp-tool-container-build-pull-request.yaml](../../.tekton/pulp-tool-container-build-pull-request.yaml) | `pull_request` → `main` | `…/pulp-tool-container:on-pr-{{revision}}` (`image-expires-after: 5d`) |

Shared: namespace `artifact-storage-tenant`, app/component `tooling` / `pulp-tool-container`, SA `build-pipeline-pulp-tool-container`, workspace `git-auth`, params `git-url` + `revision`.

## What the remote pipeline does

1. **`init`** — gate build; optional cache proxy.
2. **`git-clone-oci-ta`** — checkout repo at `revision`.
3. **`prefetch-dependencies-oci-ta`** — Cachi2 (empty for pulp-tool; no prefetch config in-repo).
4. **`buildah-oci-ta` (`build-container`)** — **Buildah builds [Dockerfile](../../Dockerfile) at repo root (`path-context: .`) and pushes `output-image`.** Dockerfile `RUN` steps need network (`hermetic` defaults `false`).
5. **Post-build checks** (unless `skip-checks`) — deprecated base image, Clair, cert preflight, Snyk SAST, ClamAV, SBOM JSON.
6. **Finally** — `show-sbom`, `show-summary`.

**Debug tip:** Dockerfile errors appear in Konflux **`build-container`** logs, not GitHub Actions.

## Workflow

1. Read this skill, [Dockerfile](../../Dockerfile), and [reference.md](reference.md).
2. Re-open `.tekton/pulp-tool-container-build-*.yaml` and upstream **single-arch-build-pipeline** on GitHub (bundles evolve).
3. Edit `Dockerfile` / `pyproject.toml` install deps as needed.
4. Edit `.tekton/` for publish paths/triggers only — do not vendor the remote pipeline in-repo.
5. Do **not** add GitHub Actions `docker build` as a merge gate.
6. Optional local check: `make test-container`.
7. Confirm Konflux PipelineRun on PR; after merge, **pulp-tool-container-on-push** on App Studio.
8. If runtime Tekton invocation changes, load **changing-pulp-upload**.

## Regression checklist

- [ ] `Dockerfile` builds (`make test-container` or Konflux PR `build-container` task)
- [ ] `pulp-tool --version` / `--help` in built image
- [ ] Python matches Fedora base (currently **3.15**); transient `gcc` for `pydantic-core` if no cp315 wheel
- [ ] `.tekton/` image refs and PAC CEL expressions correct
- [ ] No duplicate GHA container workflow
- [ ] Downstream tasks ([CLAUDE.md](../../CLAUDE.md)) unchanged

## Red flags

- GHA `docker build` as CI gate — Konflux builds on every PR/push to `main`
- Hermetic build without prefetch — would break `dnf`/`pip` in Dockerfile
- Stale Python pin vs Fedora base image
- `gcc` or `/root/.cache` left in final image layers
- Quay path / component label changes without tenant coordination

## Quick reference

| Concern | Where |
|---------|--------|
| Image recipe | `Dockerfile` |
| Triggers / Quay tags | `.tekton/pulp-tool-container-build-*.yaml` |
| Build implementation | Upstream `single-arch-build-pipeline` → `buildah-oci-ta` |
| Task details | [reference.md](reference.md) |
| Local smoke test | `make test-container` |
| Runtime usage | **changing-pulp-upload** + [CLAUDE.md](../../CLAUDE.md) |
