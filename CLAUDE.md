# pulp-tool — Konflux & Pulp integration (agents)

**Scope:** Downstream **Tekton / Konflux** contracts and **Pulp** client context that agents need when changing `upload`, global flags, SBOM/artifact handling, or the container image.

**Shared agent scaffold** (commands, conventions, PR workflow, repo layout): **[AGENTS.md](AGENTS.md)**. In Cursor, you can reference **`@AGENTS.md`**.

Platform: [Konflux documentation](https://konflux-ci.dev/docs/). Upstream Pulp: [pulpproject.org](https://pulpproject.org/).

---

## Quick facts

| Item | Value |
|------|------|
| **Python** | >= 3.12 (`pyproject.toml`); CI and container image use **3.15** (Fedora 45) |
| **Package / CLI** | `pulp_tool` / `pulp-tool` → `pulp_tool.cli:main` |
| **Konflux image** | `pulp-tool-container` (see `.tekton/pulp-tool-container-build-push.yaml`) |
| **Agent workflow** | Always-on [.cursor/rules/llm-development-guidelines.mdc](.cursor/rules/llm-development-guidelines.mdc); on-demand [skills/](skills/) ([skill index](.cursor/rules/llm-development-guidelines-deep.mdc)) |
| **Upload changes** | **changing-pulp-upload** skill ([`skills/changing-pulp-upload/SKILL.md`](skills/changing-pulp-upload/SKILL.md)) + this file |
| **Container image build** | **changing-pulp-container** skill ([`skills/changing-pulp-container/SKILL.md`](skills/changing-pulp-container/SKILL.md)); Konflux Tekton builds via [`.tekton/`](.tekton/) — not GitHub Actions |

**PR review rule:** Any PR touching **`upload`**, global options, SBOM/artifact paths, TLS/config paths in containers, or the image must still match the **two pipelines** below — open the linked YAML on GitHub; upstream tasks evolve.

**Structure, invariants, glossary:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Container image build (Konflux)

- **In-repo:** [Dockerfile](Dockerfile), [`.tekton/pulp-tool-container-build-push.yaml`](.tekton/pulp-tool-container-build-push.yaml) (main), [`.tekton/pulp-tool-container-build-pull-request.yaml`](.tekton/pulp-tool-container-build-pull-request.yaml) (PR).
- **Remote pipeline:** [single-arch-build-pipeline.yaml](https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/.tekton/single-arch-build-pipeline.yaml) — clone → prefetch (no-op) → **`buildah-oci-ta`** builds `Dockerfile` at `.` → image scans → push to `output-image`. Defaults: `dockerfile=Dockerfile`, `hermetic=false`, `path-context=.`.
- **Agent skill:** [changing-pulp-container](skills/changing-pulp-container/SKILL.md) · task catalog: [reference.md](skills/changing-pulp-container/reference.md). Optional local check: `make test-container`.

---

## Konflux downstream: two call sites

Workspace for RPM-related `upload` input: **`/var/workdir/results`**. Config paths and flags **differ**.

### 1. `import-to-quay` (rpmbuild-pipeline)

- **Task:** [`task/import-to-quay.yaml`](https://github.com/konflux-ci/rpmbuild-pipeline/blob/main/task/import-to-quay.yaml) — step **`push-to-pulp-select-auth`**

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

- Optional secret **`pulp-access`** → `/pulp-access/cli.toml`. **Missing file:** step skips Pulp upload, writes **empty** Tekton result files for URL/digest.
- **`/var/workdir/results`** and **`oras-staging/`** (e.g. merged SBOM) are populated by earlier steps; layout changes break the task.

### 2. `push-artifacts-to-storage` (release-service-catalog)

- **Pipeline/Task (branch `development`):** [`push-artifacts-to-storage.yaml`](https://github.com/konflux-ci/release-service-catalog/blob/development/tasks/managed/push-artifacts-to-storage/push-artifacts-to-storage.yaml) — step **`push-build-to-artifact-storage`**

```bash
pulp-tool --config /etc/rok-access/cli.toml \
  --build-id "<snapshotBuildId>" \
  --namespace "<snapshotNamespace>" \
  upload \
  --rpm-path "/var/workdir/results"
```

- **`/etc/rok-access/cli.toml`** (**rok-access** secret). **Missing file:** step exits **0** **without** invoking `pulp-tool`.
- Pipeline may skip storage when merged data has **`koji_import_draft`** **`false`**.

| | import-to-quay | push-artifacts-to-storage |
|--|----------------|---------------------------|
| **Config** | `/pulp-access/cli.toml` | `/etc/rok-access/cli.toml` |
| **`upload` flags (in task)** | `--parent-package`, `--sbom-path`, `--artifact-results` | `--rpm-path` only |
| **SBOM** | `--sbom-path` … `oras-staging/sbom-merged.json` when Pulp runs | no `--sbom-path` in script |

**Where to change code:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (code map, data flow, Konflux diagram). Do not duplicate upload logic outside `PulpHelper` / `UploadService`.

---

## Regression checklist

Before merging changes that affect the above, re-read both task YAMLs and extend tests if needed:

- `upload` / `upload-files` behavior, defaults, required options
- Global: `--config`, `--build-id`, `--namespace`
- `--rpm-path`, `--parent-package`, `--sbom-path`, `--artifact-results`
- Config/TLS/paths assumed in containers; image entrypoint and `pulp-tool` invocation
- RPM discovery under `--rpm-path`
- Upstream may change staging (ORAS, `oras-staging/`, etc.) — re-verify **rpmbuild-pipeline** and **release-service-catalog**; update this file if call sites move

---

## Pointers (not duplicated here)

- Architecture overview: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- User overview: [README.md](README.md)
- Contributing / checks: [CONTRIBUTING.md](CONTRIBUTING.md)
- CLI reference: [docs/cli-reference.md](docs/cli-reference.md)
- Konflux shorthand: [.cursor/rules/konflux-ecosystem.mdc](.cursor/rules/konflux-ecosystem.mdc)
- Upload change workflow: [skills/changing-pulp-upload/SKILL.md](skills/changing-pulp-upload/SKILL.md)
- Optional Hypothesis / Ghostwriter: [tests/README.md](tests/README.md)
