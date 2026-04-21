# Pulp Tool

[![Unit tests](https://github.com/konflux/pulp-tool/actions/workflows/gh-action-testsuite.yaml/badge.svg)](https://github.com/konflux/pulp-tool/actions/workflows/gh-action-testsuite.yaml)
[![codecov](https://codecov.io/gh/konflux/pulp-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/konflux/pulp-tool)

A Python client for Pulp API operations including RPM and file management.

**Overview:** [Setup](#setup) · [Usage and API](#usage-and-api) · [Development](#development) · [License](#license)

## Setup

Clone and install:

```bash
git clone https://github.com/konflux/pulp-tool.git
cd pulp-tool
pip install -e .
```

For development (dev dependencies and pre-commit):

```bash
pip install -e ".[dev]"
```

Create `~/.config/pulp/cli.toml` with `base_url`, `api_root`, OAuth or Basic Auth fields, `domain`, `verify_ssl`, `format`, `dry_run`, `timeout`, `verbose`, and optionally `correlation_id` for `X-Correlation-ID` (or set `PULP_TOOL_CORRELATION_ID`). See [CONTRIBUTING.md](CONTRIBUTING.md) and `pulp-tool --help` for defaults.

When `--build-id` and `--namespace` are set and no correlation id is configured, the client sends `X-Correlation-ID: {namespace}/{build_id}` (or the build id alone if namespace is omitted), similar to [pulp-cli](https://github.com/pulp/pulp-cli).

**packages.redhat.com:** use Basic Auth; `[cli]` can set `base_url = "https://packages.redhat.com"`, `api_root = "/api/pulp/"`, `username`, `password`, `domain`, `verify_ssl`. OAuth2 (`client_id` / `client_secret`) is also supported. For distribution pull, add `cert` and `key` paths for pull/transfer operations.

## Usage and API

```bash
pulp-tool --config ~/.config/pulp/cli.toml \
  --build-id my-build-123 \
  --namespace my-namespace \
  upload \
  --parent-package my-package \
  --rpm-path /path/to/rpms \
  --sbom-path /path/to/sbom.json
```

```bash
pulp-tool --config ~/.config/pulp/cli.toml \
  upload \
  --results-json /path/to/pulp_results.json \
  --signed-by key-id-123
```

```bash
pulp-tool pull \
  --artifact-location /path/to/artifacts.json \
  --transfer-dest ~/.config/pulp/cli.toml

pulp-tool --config ~/.config/pulp/cli.toml search-by --checksums <sha256>
```

```bash
pulp-tool --help
pulp-tool upload --help
pulp-tool search-by --help
```

Full command tables, examples, and logging: **[docs/cli-reference.md](docs/cli-reference.md)**.

```python
from pulp_tool import PulpClient, PulpHelper
from pulp_tool.models import RepositoryRefs

client = PulpClient.create_from_config_file(path="~/.config/pulp/cli.toml")
try:
    helper = PulpHelper(client)
    repos: RepositoryRefs = helper.setup_repositories("my-build-123")

    response = client.upload_rpm_package(
        "/path/to/package.rpm",
        labels={"build_id": "my-build-123"},
        arch="x86_64",
    )
finally:
    client.close()
```

```python
from pulp_tool import DistributionClient

dist = DistributionClient(cert="/path/to/cert.pem", key="/path/to/key.pem")
dist = DistributionClient(username="user", password="pass")

metadata = dist.pull_artifact("https://pulp.example.com/artifacts.json").json()
dist.pull_data(filename="pkg.rpm", file_url="...", arch="x86_64", artifact_type="rpm")
```

**Models:** `RepositoryRefs`, `UploadContext`, `PullContext`, `ArtifactMetadata`, `PulpResultsModel`, `PulledArtifacts`.

## Development

**Konflux / Tekton:** pulp-tool runs in RPM build (`import-to-quay`) and release (`push-artifacts-to-storage`) tasks. If you change `upload`, SBOM/artifact behavior, or the container image, read **[CLAUDE.md](CLAUDE.md)** for contracts and regression checks; re-verify **konflux-ci/rpmbuild-pipeline** (`task/import-to-quay.yaml`) and **konflux-ci/release-service-catalog** (`tasks/managed/push-artifacts-to-storage/`). Pipelines evolve (e.g. ORAS or `oras-staging/`); update **CLAUDE.md** when upstream staging changes.

```bash
make install-dev
make test
make test-diff-coverage
make lint
make format
make check
```

Before committing: `pre-commit run --all-files`. Before a PR: `git fetch origin` and `make test-diff-coverage`. See [CONTRIBUTING.md](CONTRIBUTING.md), [tests/README.md](tests/README.md), and [CLAUDE.md](CLAUDE.md) for tests, Hypothesis, and AI-assisted workflows. Optional [AgentReady](https://github.com/ambient-code/agentready): `pip install agentready && agentready assess .` ([.agentready-config.yaml](.agentready-config.yaml); reports under `.agentready/`, gitignored).

**Troubleshooting**

| Issue | Check |
|-------|-------|
| Command not found | `pip install -e .` or `pip install pulp-tool` |
| Authentication errors | Verify `~/.config/pulp/cli.toml` credentials |
| SSL/TLS errors | Verify cert/key paths and permissions |
| Permission denied | Check file permissions on artifacts and key |

**Contributing:** fork, branch, change with tests, then `make test`, `make test-diff-coverage` (after `git fetch origin`), and `pre-commit run --all-files`, and open a pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE).
