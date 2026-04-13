# Agent and maintainer context: Pulp Tool

This repository is **pulp-tool**: a Python CLI and library for Pulp API operations (RPM and file content, repositories, uploads, pulls). Much of the code is used both interactively and inside **Konflux** (Tekton) tasks. When you change the `upload` command, global CLI flags, SBOM or artifact-result handling, or the container image, treat the contracts below as integration tests for downstream pipelines.

For platform background (tenants, Tekton, trusted artifacts, releases), see the **[Konflux documentation](https://konflux-ci.dev/docs/)** (e.g. *Building → Image management* for Pulp-related topics). This file focuses on **pulp-tool–specific** behavior and downstream call sites.

### Using this doc in a PR review

If you are new here: **pulp-tool** is mainly a **Click** CLI and Python client for Pulp’s HTTP API. Konflux runs the **`upload`** subcommand with flags and mounted config paths that differ between pipelines—**both** integrations below must keep working after a change.

- When reviewing or authoring a PR that touches **`upload`**, global CLI options, SBOM/artifact handling, or the **container image**, open the **linked task YAMLs** in the sections below and check that behavior (flags, paths, skip vs failure) still matches.
- This documentation is **descriptive**: it was written against the linked revisions of upstream Tekton tasks. Those files can change; if something looks off, compare against the current YAML on GitHub.

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

- **CLI:** `pulp_tool/cli/upload.py`, `pulp_tool/cli/upload_files.py` — global options; `--artifact-results` as `url_path,digest_path` for Konflux result files.
- **Upload / artifact + SBOM integration:** `pulp_tool/services/upload_service.py` — e.g. `_write_konflux_results`, `_handle_artifact_results`, SBOM handling.
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
- **Upstream pipeline layout:** tasks may change how RPMs and SBOMs are staged (e.g. **ORAS** / trusted-artifacts steps, `oras-staging/`, or replacements). Re-verify **konflux-ci/rpmbuild-pipeline** and **konflux-ci/release-service-catalog** when altering anything that assumes workspace paths; refresh this doc if call sites move.

## LLM workflow in this repository

For lint, pre-commit, tests, diff coverage, and PR/changelog conventions, see [.cursor/rules/llm-development-guidelines.mdc](.cursor/rules/llm-development-guidelines.mdc).
