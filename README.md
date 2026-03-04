# Pulp Tool

[![codecov](https://codecov.io/gh/konflux/pulp-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/konflux/pulp-tool)

A Python client for Pulp API operations including RPM and file management.

## Overview

Pulp Tool provides a comprehensive, modern Python client for interacting with Pulp API to manage RPM repositories, file repositories, and content uploads with OAuth2 authentication. Built on a foundation of httpx, Pydantic, and Click, this package delivers type-safe, robust operations for Red Hat's Pulp infrastructure with support for uploading, downloading, and managing various types of artifacts.

The package emphasizes developer experience with comprehensive type safety, intuitive CLI commands, and a modular architecture that makes it easy to integrate into automated workflows.

## Installation

### From Source

```bash
git clone https://github.com/konflux/pulp-tool.git
cd pulp-tool
pip install -e .
```

### Development Installation

```bash
git clone https://github.com/konflux/pulp-tool.git
cd pulp-tool
pip install -e ".[dev]"
```

## Quick Start

### Using the CLI

The `pulp-tool` command provides a modern Click-based CLI with subcommands:

#### Upload RPMs and Artifacts

```bash
pulp-tool \
  --config ~/.config/pulp/cli.toml \
  --build-id my-build-123 \
  --namespace my-namespace \
  upload \
  --parent-package my-package \
  --rpm-path /path/to/rpms \
  --sbom-path /path/to/sbom.json
```

```bash
pulp-tool \
  --build-id my-build-123 \
  --namespace my-namespace \
  upload-files \
  --parent-package my-package \
  --rpm /path/to/rpm1 \
  --rpm /path/to/rpm2 \
  --file /path/to/generic/file \
  --log /path/to/log \
  --sbom /path/to/sbom \
  --arch x86_64 \
  --artifact-results '/konflux/artifact1/path,/konflux/artifact2/path' \
  --sbom-results /path/to/write/sbom-results
```

#### Download Artifacts

```bash
# Using --transfer-dest for cert/key and upload destination
pulp-tool pull \
  --artifact-location /path/to/artifacts.json \
  --transfer-dest ~/.config/pulp/cli.toml

# Using CLI options for cert/key
pulp-tool pull \
  --artifact-location /path/to/artifacts.json \
  --cert-path /path/to/cert.pem \
  --key-path /path/to/key.pem
```

#### Create Repository

```bash
# Using CLI options
pulp-tool create-repository \
  --repository-name my-new-repo \
  --packages '<comma separated list of content HREFs>' \
  --compression-type gz \
  --checksum-type sha256 \
  --skip-publish \
  --base-path bear-demo/path \
  --generate-repo-config

# Using JSON config
pulp-tool create-repository \
  --json-data \
  '{
    "name": "my-new-repo",
    "packages": [
      {
        "pulp_href": "<pulp content HREF>"
      }
    ],
    "repository_options":{
      "autopublish": true,
      "checksum_type": <"unknown", "md5", "sha1", "sha224", "sha256", "sha384", "sha512">,
      "compression_type": <"zstd", "gz">
    },
    "distribution_options": {
      "name": "my-new-repo",
      "base_path": "my-new-repo/path",
      "generate_repo_config": true
    }
  }'
```

#### Get Help

```bash
pulp-tool --help              # General help
pulp-tool upload --help       # Upload command help
pulp-tool pull --help         # Pull command help
pulp-tool create-repository --help  # Create repository help
pulp-tool --version           # Show version
```

### Using the Python API

#### Direct Client Usage

```python
from pulp_tool import PulpClient, PulpHelper
from pulp_tool.models import RepositoryRefs
from pulp_tool.models.pulp_api import RpmRepositoryRequest

# Create a client from configuration file
client = PulpClient.create_from_config_file(path="~/.config/pulp/cli.toml")

try:
    # Use the helper for high-level operations
    helper = PulpHelper(client)
    repositories: RepositoryRefs = helper.setup_repositories("my-build-123")

    # Upload RPM package - client handles authentication automatically
    response = client.upload_rpm_package(
        "/path/to/package.rpm",
        labels={"build_id": "my-build-123"},
        arch="x86_64"
    )

    # Get task href if upload returns async task
    task_href = response.json().get("task")
    if task_href:
        task = client.wait_for_finished_task(task_href)
finally:
    # Always close the client to clean up resources
    client.close()
```

#### Working with Pydantic Models

The package uses Pydantic for type-safe data handling:

```python
from pulp_tool.models import (
    RepositoryRefs,
    UploadContext,
    PullContext,
    ArtifactMetadata,
    PulpResultsModel
)

# Context objects for type-safe parameter passing
upload_ctx = UploadContext(
    build_id="my-build-123",
    namespace="my-namespace",
    parent_package="my-package",
    rpm_path="/path/to/rpms",
    sbom_path="/path/to/sbom.json",
    config="~/.config/pulp/cli.toml",
    debug=1
)

# Models provide validation and IDE autocomplete
print(upload_ctx.build_id)  # Type-safe access
```

#### Distribution Client for Downloads

```python
from pulp_tool import DistributionClient

# Initialize with certificate authentication
# Certificate and key paths are required
dist_client = DistributionClient(
    cert="/path/to/cert.pem",
    key="/path/to/key.pem"
)

# Download artifact metadata
response = dist_client.pull_artifact("https://pulp.example.com/path/to/artifacts.json")
metadata = response.json()

# Download artifact files
# Files are saved with the following structure:
# - RPM files: current folder (e.g., "package.rpm")
# - SBOM files: current folder (e.g., "artifact.sbom")
# - Log files: logs/<arch>/ directory (e.g., "logs/x86_64/build.log")
file_path = dist_client.pull_data(
    filename="package.rpm",
    file_url="https://pulp.example.com/path/to/package.rpm",
    arch="x86_64",
    artifact_type="rpm"  # "rpm", "log", or "sbom"
)
```

#### Programmatic CLI Usage

You can invoke the Click-based CLI programmatically:

```python
from pulp_tool import cli_main

# Call with custom arguments (uses Click command parsing)
import sys
sys.argv = ['pulp-tool', 'upload',
            '--build-id', 'test',
            '--namespace', 'ns',
            '--parent-package', 'pkg',
            '--rpm-path', '/path',
            '--sbom-path', '/path/sbom.json']
cli_main()
```

## Configuration

### Pulp CLI Configuration

Create a configuration file at `~/.config/pulp/cli.toml`:

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

### Certificate Configuration (Optional)

For distribution access, you may need a certificate configuration file:

```toml
[cli]
base_url = "https://your-pulp-instance.com"
api_root = "/pulp/api/v3"
cert = "path/to/cert"
key = "path/to/key"
domain = "your-domain"
verify_ssl = true
format = "json"
dry_run = false
timeout = 0
verbose = 0
```

## CLI Reference

The `pulp-tool` command provides a modern Click-based interface with four main subcommands: `upload`, `upload-files`, `pull`, and `create-repository`.

### Upload Command

Upload RPM packages, logs, and SBOM files to Pulp repositories.

**Required Arguments:**
- `--build-id`: Unique build identifier for organizing content
- `--namespace`: Namespace for the build (e.g., organization or project name)

**Optional Arguments:**
- `--parent-package`: Parent package name (optional, will not be added to labels if not provided)
- `--rpm-path`: Path to directory containing RPM files (optional, defaults to current directory if not provided)
- `--sbom-path`: Path to SBOM file (optional, SBOM upload will be skipped if not provided)
- `--config`: Path to Pulp CLI config file (default: `~/.config/pulp/cli.toml`)
- `--artifact-results`: Comma-separated paths for Konflux artifact results (url_path,digest_path)
- `--sbom-results`: Path to write SBOM results
- `-d, --debug`: Increase verbosity (use `-d` for INFO, `-dd` for DEBUG, `-ddd` for DEBUG with HTTP logs)

**Example:**
```bash
# Basic upload with all parameters
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  -dd \
  upload \
  --parent-package my-application \
  --rpm-path ./build/rpms \
  --sbom-path ./build/sbom.json

# Upload using current directory for RPMs (rpm-path omitted)
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  upload \
  --parent-package my-application \
  --sbom-path ./build/sbom.json

# Upload without SBOM (sbom-path omitted)
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  upload \
  --parent-package my-application \
  --rpm-path ./build/rpms

# Upload without parent-package (will not be added to labels)
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  upload \
  --rpm-path ./build/rpms \
  --sbom-path ./build/sbom.json

# Minimal upload (using current directory, no SBOM, no parent-package)
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  upload

# With result file outputs
pulp-tool \
  --build-id konflux-build-12345 \
  --namespace konflux-team \
  upload \
  --parent-package my-application \
  --rpm-path ./build/rpms \
  --sbom-path ./build/sbom.json \
  --sbom-results ./sbom_url.txt \
  --artifact-results ./artifact_url.txt,./artifact_digest.txt
```

### Upload Files Command

Upload individual files to pulp repositories.

**Required Arguments:**
- `--build-id`: Unique build identifier for organizing content
- `--namespace`: Namespace for the build (e.g., organization or project name)
- `--parent-package`: Parent package name

  ***And 1 or more of:***

- `--rpm`: Path to RPM file (can be specified multiple times)
- `--file`: Path to generic file (can be specified multiple times)
- `--log`: Path to log file (can be specified multiple times)
- `--sbom`: Path to SBOM file (can be specified multiple times)

**Optional Arguments:**
- `--arch`: Architecture for RPM files (e.g., 'x86_64', 'aarch64'). If not provided, will try to detect from RPM file
- `--artifact-results`: Comma-separated paths for Konflux artifact results location (url_path,digest_path)
- `--sbom-results`: Path to write SBOM results

**Example:**
```bash
pulp-tool \
  --build-id my-build-123 \
  --namespace my-namespace \
  upload-files \
  --parent-package my-package \
  --rpm /path/to/rpm1 \
  --rpm /path/to/rpm2 \
  --file /path/to/generic/file \
  --log /path/to/log \
  --sbom /path/to/sbom \
  --arch x86_64 \
  --artifact-results '/konflux/artifact1/path,/konflux/artifact2/path' \
  --sbom-results /path/to/write/sbom-results
```

### Pull Command

Download artifacts from Pulp distributions and optionally re-upload to Pulp repositories.

**Required Arguments:**
- `--artifact-location`: Path to local artifact metadata JSON file or HTTP URL. Mutually exclusive with `--build-id` + `--namespace`.

**Optional Arguments (conditionally required):**
- `--build-id` + `--namespace`: Alternative to `--artifact-location`. Both must be provided together. Either `--transfer-dest` or `--config` is required. The artifact location will be auto-generated from these values.
- `--cert-path`: Path to SSL certificate file for authentication (optional, can come from --transfer-dest or --config)
- `--key-path`: Path to SSL private key file for authentication (optional, can come from --transfer-dest or --config)
- `--transfer-dest`: Path to Pulp config file for transfer destination (required when using `--build-id` + `--namespace` unless `--config` is used, optional otherwise. If supplied, will upload to this config domain and use cert/key from config)
- `--content-types`: Comma-separated list of content types to pull (rpm, log, sbom). If not specified, all types are pulled.
- `--archs`: Comma-separated list of architectures to pull (e.g., x86_64,aarch64,noarch). If not specified, all architectures are pulled.
- `--max-workers`: Maximum number of concurrent download threads (default: 4)
- `-d, --debug`: Increase verbosity (use `-d` for INFO, `-dd` for DEBUG, `-ddd` for DEBUG with HTTP logs)

**Note:** For remote URLs (`http://` or `https://`), both `--cert-path` and `--key-path` are required (or must be provided via `--transfer-dest` or `--config`).

**File Path Behavior:**
When downloading artifacts, files are saved with the following structure:
- **RPM files**: Saved to current folder (e.g., `package.rpm`)
- **SBOM files**: Saved to current folder (e.g., `artifact.sbom`)
- **Log files**: Saved to `logs/<arch>/` directory (e.g., `logs/x86_64/build.log`)

**Example:**
```bash
# Download all artifacts (cert/key from --transfer-dest config)
pulp-tool pull \
  --artifact-location https://example.com/artifacts.json \
  --transfer-dest ~/.config/pulp/cli.toml \
  --max-workers 4 \
  -dd

# Download all artifacts (cert/key from CLI options)
pulp-tool pull \
  --artifact-location https://example.com/artifacts.json \
  --cert-path /etc/pki/tls/certs/client.cert \
  --key-path /etc/pki/tls/private/client.key \
  --max-workers 4 \
  -dd

# Download only RPMs for specific architectures
pulp-tool pull \
  --artifact-location https://example.com/artifacts.json \
  --transfer-dest ~/.config/pulp/cli.toml \
  --cert-path /etc/pki/tls/certs/client.cert \
  --key-path /etc/pki/tls/private/client.key \
  --content-types rpm \
  --archs x86_64,aarch64

# Download using build-id + namespace (artifact location auto-generated)
# Using --transfer-dest
pulp-tool \
  --build-id my-build-123 \
  --namespace my-namespace \
  pull \
  --transfer-dest ~/.config/pulp/cli.toml \
  --max-workers 4

# Or using group-level --config
pulp-tool \
  --config ~/.config/pulp/cli.toml \
  --build-id my-build-123 \
  --namespace my-namespace \
  pull \
  --max-workers 4
```

### Create Repository Command

Create a custom defined repository in Pulp with specified packages and configuration options.

**Required Arguments (when not using JSON):**
- `--repository-name`: A unique name for this repository
- `--packages`: Comma-separated list of package Pulp hrefs to be added to the newly created repository
- `--base-path`: The base (relative) path component of the published URL

**Optional Arguments:**
- `--config`: Path to Pulp CLI config file (default: `~/.config/pulp/cli.toml`)
- `--compression-type`: The compression type to use for metadata files (`zstd` or `gz`)
- `--checksum-type`: The preferred checksum type during repo publish (`unknown`, `md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512`)
- `--skip-publish`: Disables autopublish for a repository (flag)
- `--generate-repo-config`: Whether Pulp should generate `.repo` files (flag, ignored for non-RPM distributions)
- `-j, --json-data`: JSON string input. CLI options are ignored when JSON data is provided
- `-d, --debug`: Increase verbosity (use `-d` for INFO, `-dd` for DEBUG, `-ddd` for DEBUG with HTTP logs)

**Note:** When using `--json-data`, all other CLI options are ignored. The JSON must conform to the `CreateRepository` model structure.

**Example:**
```bash
# Create repository with CLI options
pulp-tool \
  --config ~/.config/pulp/cli.toml \
  create-repository \
  --repository-name my-repo \
  --packages /api/pulp/konflux-test/api/v3/content/file/packages/019b1338-f265-7ad6-a278-8bead86e5c1d/ \
  --base-path my-repo-path \
  --compression-type zstd \
  --checksum-type sha256 \
  --generate-repo-config

# Create repository with JSON input
pulp-tool \
  --config ~/.config/pulp/cli.toml \
  create-repository \
  --json-data '{"name": "my-repo", "packages": [{"pulp_href": "/api/pulp/..."}], "repository_options": {...}, "distribution_options": {...}}'

# Create repository without autopublish
pulp-tool \
  --config ~/.config/pulp/cli.toml \
  create-repository \
  --repository-name my-repo \
  --packages /api/pulp/.../ \
  --base-path my-repo-path \
  --skip-publish
```

### Environment Variables

The commands respect standard environment variables for SSL/TLS and HTTP configuration:

**SSL/TLS:**
- `SSL_CERT_FILE`: Path to CA bundle for verifying SSL certificates (httpx standard)
- `SSL_CERT_DIR`: Directory containing CA certificates
- `REQUESTS_CA_BUNDLE`: Also supported for compatibility
- `CURL_CA_BUNDLE`: Alternative CA bundle path

**HTTP Configuration:**
- `HTTP_PROXY` / `HTTPS_PROXY`: Proxy server URLs (supported by httpx)
- `NO_PROXY`: Comma-separated list of hosts to exclude from proxying

**Debug:**
- Use `-d`, `-dd`, or `-ddd` flags for progressive verbosity levels

### Logging

All commands support progressive verbosity levels with the `-d` / `--debug` flag:

**Verbosity Levels:**
- No flag: `WARNING` level (errors and warnings only)
- `-d`: `INFO` level (general progress information)
- `-dd`: `DEBUG` level (detailed operation information)
- `-ddd`: `DEBUG` level with HTTP request/response logging

**Log Format:**
- Timestamp
- Log level (INFO, DEBUG, WARNING, ERROR)
- Message with context
- Progress indicators for long-running operations
- Detailed error tracebacks in debug modes

**Example:**
```bash
# Minimal output
pulp-tool --build-id test upload ...

# Progress information
pulp-tool --build-id test -d upload ...

# Detailed debugging
pulp-tool --build-id test -dd upload ...

# Full HTTP debugging
pulp-tool --build-id test -ddd upload ...
```

### Troubleshooting

#### Command not found

Ensure the package is installed and your PATH includes the installation location:

```bash
pip install -e .
# or
pip install pulp-tool
```

#### Authentication errors

Check your Pulp CLI configuration file and ensure credentials are correct:

```bash
cat ~/.config/pulp/cli.toml
```

#### SSL/TLS errors

Verify certificate paths and permissions:

```bash
ls -la /path/to/cert.pem /path/to/key.pem
```

#### Permission denied

Ensure the user has read access to source files and write access to destination:

```bash
chmod 644 /path/to/artifacts.json
chmod 600 /path/to/key.pem
```

## API Reference

### Core Components

#### PulpClient

The main client class for interacting with Pulp API. Built with httpx for modern async-capable HTTP operations and organized by resource type to match Pulp's API structure.

**Architecture:**
- Resource-based organization matching Pulp's API documentation
- Mixins organized by resource type: repositories, distributions, content, artifacts, tasks
- Automatic OAuth2 authentication with token refresh
- Context manager support for proper resource cleanup

**Resource-Based Mixins:**
- `RpmRepositoryMixin`, `FileRepositoryMixin`: Repository operations
- `RpmDistributionMixin`, `FileDistributionMixin`: Distribution operations
- `RpmPackageContentMixin`, `FileContentMixin`: Content upload and management
- `ArtifactMixin`: Artifact operations
- `TaskMixin`: Task monitoring with exponential backoff

**Key Methods:**
- `create_rpm_repository()`, `create_file_repository()`: Create repositories
- `create_rpm_distribution()`, `create_file_distribution()`: Create distributions
- `upload_rpm_package()`, `create_file_content()`: Upload content
- `get_task()`, `wait_for_finished_task()`: Monitor async operations
- `get_artifact()`, `list_artifacts()`: Query artifacts
- `close()`: Clean up HTTP session and resources

**Usage:**
```python
from pulp_tool import PulpClient
from pulp_tool.models.pulp_api import RpmRepositoryRequest

client = PulpClient.create_from_config_file()
try:
    # Create repository
    repo_response, task_href = client.create_rpm_repository(
        RpmRepositoryRequest(name="my-repo")
    )

    # Upload content
    response = client.upload_rpm_package(
        "/path/to/package.rpm",
        labels={"build_id": "123"},
        arch="x86_64"
    )
finally:
    client.close()
```

#### PulpHelper

High-level helper class for common workflow operations. Orchestrates multiple PulpClient operations.

**Key Methods:**
- `setup_repositories()`: Create or get repositories and distributions (returns `RepositoryRefs`)
- `process_uploads()`: Handle complete upload workflows with progress tracking
- `get_distribution_urls()`: Get distribution URLs for repositories

#### DistributionClient

Specialized client for downloading artifacts from Pulp distributions using certificate authentication.

**Key Methods:**
- `pull_artifact()`: Download artifact metadata JSON with certificate-based authentication
- `pull_data()`: Download and save artifact files to local filesystem
  - RPM files: saved to current folder
  - SBOM files: saved to current folder
  - Log files: saved to `logs/<arch>/` directory
- `pull_data_async()`: Asynchronously download artifacts (used internally by pull command)
- Handles SSL/TLS with client certificates
- Returns httpx Response objects for metadata, file paths for downloaded files

### Data Models (Pydantic)

#### API Response Models (`pulp_tool.models.pulp_api`)

- `TaskResponse`, `TaskListResponse`: Pulp task status and results
- `RepositoryResponse`, `RpmRepositoryResponse`, `FileRepositoryResponse`: Repository metadata
- `DistributionResponse`, `RpmDistributionResponse`, `FileDistributionResponse`: Distribution configuration
- `RpmPackageResponse`: RPM-specific metadata
- `FileResponse`: File content metadata
- `ArtifactResponse`, `ArtifactListResponse`: Artifact information
- `OAuthTokenResponse`: OAuth token data

#### Domain Models (`pulp_tool.models`)

- `RepositoryRefs`: References to repositories (rpms_href, logs_href, sbom_href, etc.)
- `UploadContext`: Type-safe context for upload operations
- `PullContext`: Type-safe context for pull operations
- `ArtifactMetadata`: Artifact metadata with labels and digests
- `PulpResultsModel`: Unified upload tracking and results building
- `PulledArtifacts`: Downloaded artifact organization

## Development

### Setting Up Development Environment

```bash
git clone https://github.com/konflux/pulp-tool.git
cd pulp-tool
make install-dev
```

This installs the package with dev dependencies and sets up pre-commit hooks.

### Makefile Targets

Common tasks use the project Makefile:

```bash
make test          # Run all tests with coverage (required 85%+)
make lint          # Run all linters (black, flake8, pylint, mypy)
make format        # Format code with Black
make check         # Lint and test
make clean         # Remove build artifacts and caches
```

Before committing, run `pre-commit run --all-files` (and fix any issues, then run again). See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow.

### Running Tests

```bash
# Run all tests with coverage (preferred)
make test

# Or with pytest directly
pytest -v --tb=short --cov=pulp_tool --cov-report=term-missing --cov-fail-under=85

# Run specific test file
pytest tests/test_cli.py -v

# Run by marker
pytest -m unit
pytest -m integration
```

### Code Coverage

Code coverage is tracked using [Codecov](https://codecov.io/gh/konflux/pulp-tool). Coverage reports are automatically uploaded to Codecov when tests run in CI.

- **Coverage Target**: 85% overall coverage
- **Diff Coverage**: 100% coverage required for new/changed lines in pull requests
- **View Coverage**: Check the [Codecov dashboard](https://codecov.io/gh/konflux/pulp-tool) for detailed coverage reports

```bash
make test          # Generates htmlcov/ and coverage.xml
# View HTML report: open htmlcov/index.html
```

### Code Formatting and Linting

```bash
make format        # Format with Black
make lint          # Check formatting and run flake8, pylint, mypy
```

Individual tools: `black`, `flake8`, `pylint --errors-only`, `mypy` (see `Makefile` and `pyproject.toml`).

### Building the Package

```bash
python -m build
pip install -e .
```

### Dependencies

**Core:**
- `httpx>=0.24.0` - Modern HTTP client
- `pydantic>=2.0.0` - Data validation
- `click>=8.0.0` - CLI framework

**Development:**
- `pytest>=6.0` - Testing framework
- `pytest-cov>=2.0` - Coverage reporting
- `black>=21.0` - Code formatting
- `flake8>=3.8` - Linting
- `mypy>=0.800` - Type checking
- `pylint>=2.8` - Code analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.
