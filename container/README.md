# pulp-tool-container (Konflux)

| File | Purpose |
|------|---------|
| [`Dockerfile`](Dockerfile) | Multi-stage UBI 10 image for Tekton (`pulp-tool`, ORAS helpers) |
| [`pulp-tool-container.build-args`](pulp-tool-container.build-args) | `VERSION` / `RELEASE` build args (synced by `scripts/sync-container-build-args.sh`) |

Build context is the **repository root** (`.`), not this directory. Local smoke test: **`make test-container`**. PipelineRuns: [`.tekton/pulp-tool-container-build-*.yaml`](../.tekton/).
