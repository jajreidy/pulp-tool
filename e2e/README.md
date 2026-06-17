# End-to-End Test Suite

This directory contains the end-to-end test suite for `pulp-tool`, designed to validate the complete CLI workflow.

## Overview

The e2e test suite follows a four-phase lifecycle:

1. **Pre-test setup** — Build test RPM packages
2. **Test execution** — Run comprehensive CLI tests
3. **Post-test validation** — Verify repositories and distributions
4. **Post-test cleanup** — Remove test resources from Pulp

## Files

### [`pre-test.py`](pre-test.py)

**Purpose:** Generate test RPM packages for the test suite.

**What it does:**
- Creates 5 test packages (`test.0` through `test.4`)
- Builds each package for 3 architectures: `x86_64`, `aarch64`, `noarch`
- Outputs 15 total RPM files organized by package number and architecture
- Each RPM contains a minimal test executable at `/user/bin/test.<N>-bin`

**Usage:**
```bash
./pre-test.py --build-dir <directory>
```

**Arguments:**
- `--build-dir`: Directory where test packages will be built (default: current directory)

**Output structure:**
```
<build-dir>/
└── test_pkgs/
    ├── 0/
    │   ├── x86_64/test.0-1.0.0-1.x86_64.rpm
    │   ├── aarch64/test.0-1.0.0-1.aarch64.rpm
    │   └── noarch/test.0-1.0.0-1.noarch.rpm
    ├── 1/
    │   └── ...
    └── 4/
        └── ...
```

**Dependencies:** `rpm-rs<0.25`

---

### [`test-pulp-tool.py`](test-pulp-tool.py)

**Purpose:** Comprehensive end-to-end test suite for the `pulp-tool` CLI.

**What it tests:**
- **Global options:** `--config`, `--build-id`, `--namespace`, `--dry-run`
- **Commands:**
  - `upload` — Upload RPMs to Pulp
  - `upload-files` — Upload arbitrary files to Pulp
  - `publish` — Create/update distributions
  - `create-repo` — Create repositories
  - `list-repos` — List repositories with filtering
  - `list-distributions` — List distributions with filtering
  - `search-by` — Filter artifacts by repository/distribution existence
- **Error handling:** Invalid paths, missing config, invalid options
- **Result file generation:** JSON output for Konflux integration

**Usage:**
```bash
./test-pulp-tool.py \
  --config <path-to-cli.toml> \
  --rpm-dir <test-rpms-directory> \
  --pulp-results <fixture-json-file> \
  [--test-dir <working-directory>] \
  [--real-server] \
  [--skip-setup]
```

**Arguments:**
- `--config`: Path to Pulp CLI config file (`cli.toml`) — **required**
- `--rpm-dir`: Directory containing test RPM packages — **required**
- `--pulp-results`: Path to fixture `pulp_results.json` file referencing existing test repositories/distributions in Pulp — **required** (used for `pull` and `search-by` tests)
- `--test-dir`: Working directory for test execution (default: temp directory)
- `--real-server`: Test against a real Pulp server (default: dry-run mode)
- `--skip-setup`: Skip creating fresh test directory (reuse existing)

**The `pulp-results` fixture:**

This is an **input file** (not output) containing references to pre-existing test repositories and distributions in Pulp. This file is used to test `pull --artifact-location`. The test suite also copies and modifies this file to test the `search-by --results-json` filtering functionality. In CI, this file is provided via the `pulp-results` Kubernetes secret.

**Output:**
- Test results printed to stdout with color-coded pass/fail indicators
- Summary statistics at the end
- Various command outputs written to `--test-dir` for verification
- Exit code: 0 on success, 1 if any test fails

**Example:**
```bash
# Dry-run mode (no actual Pulp operations)
./test-pulp-tool.py --config /path/to/cli.toml --rpm-dir ./test_pkgs --pulp-results /path/to/fixture.json

# Against real Pulp server
./test-pulp-tool.py --config /etc/pulp-access/cli.toml --rpm-dir ./test_pkgs --pulp-results /etc/pulp-results/pulp-results.json --real-server
```

---

### [`post-test-validation.py`](post-test-validation.py)

**Purpose:** Verify that test repositories and distributions contain the expected content after test execution.

**What it validates:**
- **RPM repositories:** 10 repositories with specific RPM packages
- **File repositories:** 7 repositories with artifacts, logs, and SBOMs
- Uses `pulp` CLI to query repository content
- Checks for both missing and unexpected content

**Usage:**
```bash
./post-test-validation.py --config <path-to-cli.toml>
```

**Arguments:**
- `--config`: Path to Pulp CLI config file (`cli.toml`) — **required**

**Dependencies:** `pulp-cli` (Pulp CLI tool)

**Output:**
- Validates each repository against expected content
- Prints verification results for each repository
- Summary of repositories verified vs. failed
- Exit code: 0 if all repositories verified, 1 otherwise

**Expected repositories:**

**RPM repositories:**
- `aarch64`, `noarch`, `x86_64` — architecture-specific single RPMs
- `test-build-123/rpms` — 3 RPMs (all architectures for `test.0`)
- `test-build-456/rpms` — 3 RPMs (all architectures for `test.1`)
- `test-build-456/rpms-signed` — empty (signed RPMs repository)
- `test-build-files/rpms` — single x86_64 RPM
- `test-repo` — `duck-0.6-1.noarch.rpm`
- `test-repo-json` — 2 RPMs from JSON input
- `test-upload-results/rpms` — single noarch RPM

**File repositories:**
- `test-build-123/artifacts`, `test-build-789/artifacts`, `test-upload-results/artifacts` — `pulp_results.json`
- `test-build-456/sbom` — `sbom.json`
- `test-build-files/artifacts` — `pulp_results.json`, `test.md`
- `test-build-files/logs` — `x86_64/build.log`
- `test-build-files/sbom` — `sbom.json`

---

### [`post-test-cleanup.py`](post-test-cleanup.py)

**Purpose:** Clean up all test repositories and distributions created during e2e tests.

**What it does:**
- Destroys all test RPM repositories and distributions
- Destroys all test file repositories and distributions
- Runs `pulp orphan cleanup` to remove orphaned content
- Supports dry-run mode to preview what would be destroyed

**Usage:**
```bash
./post-test-cleanup.py --config <path-to-cli.toml> [--dry-run]
```

**Arguments:**
- `--config`: Path to Pulp CLI config file (`cli.toml`) — **required**
- `--dry-run`: Show what would be destroyed without executing (optional)

**Dependencies:** `pulp-cli` (Pulp CLI tool)

**Output:**
- Progress indicator for each resource being destroyed
- Summary of successful vs. failed deletions
- Exit code: 0 if all deletions successful, 1 otherwise

**Example:**
```bash
# Preview cleanup without executing
./post-test-cleanup.py --config /etc/pulp-access/cli.toml --dry-run

# Actually clean up test resources
./post-test-cleanup.py --config /etc/pulp-access/cli.toml
```

---

## CI/CD Integration (Tekton)

The e2e test suite runs automatically on pull requests via Konflux Tekton pipelines.

### Pipeline: [`pulp-e2e-testing`](../.tekton/pipelines/pulp-e2e-testing.yaml)

**Trigger:** Pull requests and pushes to `main` branch

**Pipeline steps:**

1. **init** — Initialize build context
2. **clone-repository** — Clone the PR branch
3. **run-e2e-test-suite** — Execute the test suite (see task below)
4. **post-test-cleanup** (finally) — Clean up test resources even if tests fail

### Task: [`run-e2e-test-suite`](../.tekton/tasks/run-e2e-test-suite.yaml)

**Steps:**

1. **pre-test-setup**
   - Install `python3`, `python3-pip`, `rpm-rs<0.25`
   - Run `pre-test.py` to build test RPMs
2. **pulp-tool-test**
   - Install `pulp-tool` from source
   - Run `test-pulp-tool.py --real-server` against the real Pulp server
   - Uses secrets:
     - `pulp-access` → `/etc/pulp-access/cli.toml` (Pulp config)
     - `pulp-results` → `/etc/pulp-results/pulp-results.json` (fixture file with test repo/dist references)
3. **post-test-validation**
   - Install `pulp-cli`
   - Run `post-test-validation.py` to verify repository content

### Task: [`post-test-cleanup`](../.tekton/tasks/post-test-cleanup.yaml)

**Runs in `finally` block** (always executes, even if tests fail)

- Installs `pulp-cli`
- Runs `post-test-cleanup.py` to destroy test resources

---

## Local Testing

### Prerequisites

You'll need:

1. **Test RPM packages** (generated by `pre-test.py`)
2. **Pulp CLI config** (`cli.toml`) with valid credentials
3. **Fixture file** (`pulp_results.json`) referencing existing repos/distributions (for `search-by` and `pull` tests)

### Quick start

```bash
# 1. Build test RPMs
./e2e/pre-test.py --build-dir ./build

# 2. Create a valid fixture file (or use one from CI) ***yours will look different than this***
cat > fixture.json << 'EOF'
{
  "artifacts": {},
  "distributions": {}
}
EOF

# 3. Run tests (dry-run mode, no real Pulp operations)
./e2e/test-pulp-tool.py \
  --config /path/to/cli.toml \
  --rpm-dir ./build/test_pkgs \
  --pulp-results ./fixture.json

# 4. Run against real Pulp server (requires valid config and credentials)
./e2e/test-pulp-tool.py \
  --config /path/to/cli.toml \
  --rpm-dir ./build/test_pkgs \
  --pulp-results ./fixture.json \
  --real-server

# 5. Validate repositories (if using real server)
./e2e/post-test-validation.py --config /path/to/cli.toml

# 6. Clean up (if using real server)
./e2e/post-test-cleanup.py --config /path/to/cli.toml
```

### Using the container image

The e2e test suite can also be run using the `pulp-tool:test` container:

```bash
# Build the container
make test-container

# The container includes pulp-tool pre-installed
# Mount your config and test data, then run tests
```

See [`skills/changing-pulp-container/SKILL.md`](../skills/changing-pulp-container/SKILL.md) for details on the container build process.

---

## Dependencies

**Runtime dependencies:**

- **Python 3.12+** (as specified in `pyproject.toml`)
- **rpm-rs < 0.25** (for RPM generation in `pre-test.py`)
- **pulp-cli** (for validation and cleanup scripts)
- **pulp-tool** (installed from source)

**CI environment:**

- Fedora 45 base image (`registry.fedoraproject.org/fedora:45`)
- Konflux secrets:
  - `pulp-access` → Pulp CLI config (`cli.toml`)
  - `pulp-results` → Fixture file with test repo/distribution references
- Workspace persistence for test artifacts

---

## Test Coverage

The test suite validates:

- ✅ All `pulp-tool` commands and subcommands
- ✅ Global options (`--config`, `--build-id`, `--namespace`, `--dry-run`)
- ✅ RPM upload workflows (single files, directories, architectures)
- ✅ File upload workflows (artifacts, logs, SBOMs)
- ✅ Repository and distribution creation/publishing
- ✅ JSON input formats for batch operations
- ✅ Artifact filtering with `search-by --results-json`
- ✅ Result file generation (`pulp_results.json` output)
- ✅ Error handling and validation
- ✅ List and filter operations

---

## Troubleshooting

### Test failures

1. Check the test output for specific failure messages
2. Verify Pulp config file exists and is valid (`cli.toml`)
3. Ensure test RPMs were built successfully (`pre-test.py` output)
4. For `--real-server` tests, verify Pulp server connectivity
5. For `search-by` tests, verify the `pulp-results` fixture file exists and is valid JSON

### Validation failures

If `post-test-validation.py` fails:

1. Check repository content manually: `pulp rpm repository content list --repository <name>`
2. Verify test execution completed without errors

### Cleanup issues

If `post-test-cleanup.py` fails:

1. Run with `--dry-run` to preview what would be destroyed
2. Manually destroy stuck resources: `pulp rpm repository destroy --name <name>`
3. Force orphan cleanup: `pulp orphan cleanup`

### CI/CD failures

1. Check Tekton PipelineRun logs in Konflux UI
2. Verify workspace mounts are correct
3. Check secret availability (`pulp-access`, `pulp-results`)
4. Review task step outputs in order (debug → pre-test → test → validation)
5. Verify the `pulp-results` secret contains valid fixture data

---

## Further Reading

- [ARCHITECTURE.md](../docs/ARCHITECTURE.md) — Code structure and data flow
- [CLI Reference](../docs/cli-reference.md) — Complete command documentation
- [CONTRIBUTING.md](../CONTRIBUTING.md) — Development workflow and checks
- [Konflux documentation](https://konflux-ci.dev/docs/) — Tekton pipeline platform
