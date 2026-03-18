# Pulp Tool

[![codecov](https://codecov.io/gh/konflux/pulp-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/konflux/pulp-tool)

A Python client for Pulp API operations including RPM and file management.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
  - [upload](#upload-command)
  - [upload-files](#upload-files-command)
  - [pull](#pull-command)
  - [create-repository](#create-repository-command)
  - [search-by](#search-by-command)
- [Python API](#python-api)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

Pulp Tool provides a Python client for interacting with Pulp API to manage RPM repositories, file repositories, and content uploads. It supports OAuth2 and Basic Auth, and is built on httpx, Pydantic, and Click for type-safe, robust operations.

## Installation

```bash
git clone https://github.com/konflux/pulp-tool.git
cd pulp-tool
pip install -e .
```

For development (includes dev dependencies and pre-commit):

```bash
pip install -e ".[dev]"
```

## Configuration

Create `~/.config/pulp/cli.toml`:

```toml
[cli]
base_url = "https://your-pulp-instance.com"
api_root = "/pulp/api/v3"
client_id = "your-client-id"
client_secret = "your-client-secret"
domain = "your-domain"
verify_ssl = true
format = "json"
dry_run = false
timeout = 0
verbose = 0
```

### packages.redhat.com (Hosted Pulp)

Use Basic Auth for [packages.redhat.com](https://packages.redhat.com):

```toml
[cli]
base_url = "https://packages.redhat.com"
api_root = "/api/pulp/"
username = "your-username"
password = "your-password"
domain = "konflux-jreidy-tenant"
verify_ssl = true
```

OAuth2 (`client_id` / `client_secret`) is also supported.

### Certificate (for distribution access)

Add `cert` and `key` paths to the config for pull/transfer operations.

## Quick Start

```bash
# Upload RPMs and SBOM
pulp-tool --config ~/.config/pulp/cli.toml \
  --build-id my-build-123 \
  --namespace my-namespace \
  upload \
  --parent-package my-package \
  --rpm-path /path/to/rpms \
  --sbom-path /path/to/sbom.json

# Upload from pulp_results.json (e.g. from --artifact-results folder)
pulp-tool --config ~/.config/pulp/cli.toml \
  upload \
  --results-json /path/to/pulp_results.json \
  --signed-by key-id-123

# Download artifacts
pulp-tool pull \
  --artifact-location /path/to/artifacts.json \
  --transfer-dest ~/.config/pulp/cli.toml

# Search RPMs by checksum
pulp-tool --config ~/.config/pulp/cli.toml search-by --checksums <sha256>
```

```bash
pulp-tool --help
pulp-tool upload --help
pulp-tool search-by --help
```

## CLI Reference

### upload

Upload RPM packages, logs, and SBOM files.

| Argument | Required | Description |
|----------|----------|-------------|
| `--build-id` | No | Build identifier |
| `--namespace` | No | Namespace (e.g., org or project) |
| `--parent-package` | No | Parent package name |
| `--rpm-path` | No | Path to RPM directory (default: current dir) |
| `--sbom-path` | No | Path to SBOM file |
| `--results-json` | No | Path to `pulp_results.json`; upload artifacts from this file (files resolved from its directory or `--files-base-path`). When used, `--build-id` and `--namespace` are optional (extracted from artifact labels in the JSON) |
| `--files-base-path` | No | Base path for resolving artifact keys to file paths (default: directory of `--results-json`; requires `--results-json`) |
| `--signed-by` | No | Add `signed_by` pulp_label and upload to separate signed repos/distributions |
| `--artifact-results` | No | Comma-separated paths or folder for local `pulp_results.json` |
| `--sbom-results` | No | Path to write SBOM results |
| `-d, --debug` | No | Verbosity: `-d` INFO, `-dd` DEBUG, `-ddd` HTTP logs |

**Upload from results JSON:** When `--results-json` is used, artifact keys from the JSON are resolved to file paths (default: same directory as the JSON; override with `--files-base-path`). Files are classified by extension (`.rpm` → rpms, `.log` → logs, SBOM extensions → sbom, else → artifacts) and uploaded to the appropriate repository. `--rpm-path` and `--sbom-path` are ignored in this mode.

**Signed-by:** When `--signed-by` is set, a `signed_by` label is added to RPMs only, and RPMs are stored in a separate `rpms-signed` repository with its own distribution. Logs and SBOMs are never signed and always go to the standard repositories.

### upload-files

Upload individual files (RPMs, logs, SBOMs, generic files).

| Argument | Required | Description |
|----------|----------|-------------|
| `--build-id` | Yes | Build identifier |
| `--namespace` | Yes | Namespace |
| `--parent-package` | Yes | Parent package name |
| `--rpm` / `--file` / `--log` / `--sbom` | At least one | File paths (repeatable) |
| `--arch` | No | Architecture (e.g., x86_64) |
| `--artifact-results` | No | Output paths or folder |
| `--sbom-results` | No | SBOM output path |

### pull

Download artifacts from Pulp distributions.

| Argument | Required | Description |
|----------|----------|-------------|
| `--artifact-location` | Yes* | Path or URL to artifact metadata JSON |
| `--build-id` + `--namespace` | Yes* | Alternative to artifact-location |
| `--transfer-dest` | Conditional | Config path for cert/key and upload destination |
| `--cert-path` / `--key-path` | Conditional | SSL cert/key (or from config) |
| `--content-types` | No | Filter: rpm, log, sbom (comma-separated) |
| `--archs` | No | Filter: x86_64, aarch64, etc. |
| `--max-workers` | No | Concurrent downloads (default: 4) |

\* Use `--artifact-location` OR `--build-id` + `--namespace`. For remote URLs, cert/key required.

**File layout:** RPMs/SBOMs → current folder; logs → `logs/<arch>/`.

### create-repository

Create a repository with specified packages.

| Argument | Required | Description |
|----------|----------|-------------|
| `--repository-name` | Yes* | Repository name |
| `--packages` | Yes* | Comma-separated Pulp content HREFs |
| `--base-path` | Yes* | Base path for published URL |
| `--compression-type` | No | `zstd` or `gz` |
| `--checksum-type` | No | sha256, sha384, etc. |
| `--skip-publish` | No | Disable autopublish |
| `--generate-repo-config` | No | Generate .repo files |
| `-j, --json-data` | No | JSON input (overrides CLI options) |

**JSON example:**
```bash
pulp-tool create-repository --json-data '{
  "name": "my-repo",
  "packages": [{"pulp_href": "/api/pulp/.../"}],
  "repository_options": {"autopublish": true, "checksum_type": "sha256", "compression_type": "zstd"},
  "distribution_options": {"name": "my-repo", "base_path": "my-repo/path", "generate_repo_config": true}
}'
```

### search-by

Search RPM content by checksum, filename, or signed_by label. Output is JSON.

**Constraints:**
- `checksums` and `filenames` are mutually exclusive
- `--signed-by` accepts a single value
- When `signed_by` is used with checksums or filenames, it is applied server-side in one API call

**Direct search (JSON output):**

```bash
# By checksum(s)
pulp-tool --config ~/.config/pulp/cli.toml search-by --checksums <sha256>
pulp-tool --config ~/.config/pulp/cli.toml search-by --checksums <sha256_1>,<sha256_2>

# By filename (artifact keys, e.g. pkg-1.0-1.x86_64.rpm)
pulp-tool --config ~/.config/pulp/cli.toml search-by --filenames pkg1.rpm,pkg2.rpm

# By signed_by label (single key)
pulp-tool --config ~/.config/pulp/cli.toml search-by --signed-by key-id-123
```

**Filter results.json** (remove RPMs found in Pulp, write filtered file):

```bash
# Default: extract checksums from file
pulp-tool --config ~/.config/pulp/cli.toml search-by \
  --results-json /path/to/pulp_results.json \
  --output-results /path/to/filtered_results.json

# Explicit: use checksums from file (--checksum flag)
pulp-tool --config ~/.config/pulp/cli.toml search-by \
  --checksum --results-json /path/to/pulp_results.json \
  --output-results /path/to/filtered_results.json

# Use filename from file (--filename flag)
pulp-tool --config ~/.config/pulp/cli.toml search-by \
  --filename --results-json /path/to/pulp_results.json \
  --output-results /path/to/filtered_results.json

# With signed_by filter (server-side)
pulp-tool --config ~/.config/pulp/cli.toml search-by \
  --results-json /path/to/pulp_results.json \
  --output-results /path/to/filtered_results.json \
  --signed-by key-id-123
```

### Environment & Logging

**Environment:** `SSL_CERT_FILE`, `SSL_CERT_DIR`, `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` are supported.

**Verbosity:** `-d` INFO, `-dd` DEBUG, `-ddd` HTTP logs. Default: WARNING.

## Python API

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

**DistributionClient** (certificate auth for downloads):

```python
from pulp_tool import DistributionClient

dist = DistributionClient(cert="/path/to/cert.pem", key="/path/to/key.pem")
metadata = dist.pull_artifact("https://pulp.example.com/artifacts.json").json()
dist.pull_data(filename="pkg.rpm", file_url="...", arch="x86_64", artifact_type="rpm")
```

**Models:** `RepositoryRefs`, `UploadContext`, `PullContext`, `ArtifactMetadata`, `PulpResultsModel`, `PulledArtifacts`.

## Development

```bash
make install-dev   # Install with dev deps + pre-commit
make test          # Run tests with coverage (85%+ required)
make lint          # Black, flake8, pylint, mypy
make format        # Format with Black
make check         # Lint + test
```

Before committing: `pre-commit run --all-files` (run twice after fixes). See [CONTRIBUTING.md](CONTRIBUTING.md).

## Troubleshooting

| Issue | Check |
|-------|-------|
| Command not found | `pip install -e .` or `pip install pulp-tool` |
| Authentication errors | Verify `~/.config/pulp/cli.toml` credentials |
| SSL/TLS errors | Verify cert/key paths and permissions |
| Permission denied | Check file permissions on artifacts and key |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Run `make test` and `pre-commit run --all-files`
5. Submit a pull request

## License

Apache License 2.0. See [LICENSE](LICENSE).
