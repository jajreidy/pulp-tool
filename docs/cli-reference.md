# CLI reference

Detailed command-line documentation for **pulp-tool**. For installation, configuration, and the Python API, see [README.md](../README.md).

## upload

Upload RPM packages, logs, and SBOM files.

| Argument | Required | Description |
|----------|----------|-------------|
| `--build-id` | No | Build identifier |
| `--namespace` | No | Namespace (e.g. org or project) |
| `--parent-package` | No | Parent package name |
| `--rpm-path` | No | Path to RPM directory (default: current dir) |
| `--sbom-path` | No | Path to SBOM file |
| `--results-json` | No | Path to `pulp_results.json`; upload artifacts from this file (files resolved from its directory or `--files-base-path`). When used, `--build-id` and `--namespace` are optional (extracted from artifact labels in the JSON) |
| `--files-base-path` | No | Base path for resolving artifact keys to file paths (default: directory of `--results-json`; requires `--results-json`) |
| `--signed-by` | No | Add `signed_by` pulp_label and upload to separate signed repos/distributions |
| `--overwrite` | No | RPM only: before upload, find packages in the target RPM repo by each local file’s SHA256 (and `signed_by` when set) and remove them via `remove_content_units` |
| `--target-arch-repo` | No | RPM only: use each architecture as the RPM repo/distribution base path (e.g. `…/pulp-content/{namespace}/x86_64/`) instead of `{build}/rpms`; logs, SBOM, and generic artifacts stay `{build}/…`. With `--signed-by`, paths stay `{arch}/` only (`signed_by` is a label). Repos are created per arch at upload time. Works with `--results-json` |
| `--artifact-results` | No | Comma-separated paths or folder for local `pulp_results.json` |
| `--sbom-results` | No | Path to write SBOM results |
| `-d, --debug` | No | Verbosity: `-d` INFO, `-dd` DEBUG, `-ddd` HTTP logs |

**Upload from results JSON:** When `--results-json` is used, artifact keys from the JSON are resolved to file paths (default: same directory as the JSON; override with `--files-base-path`). Files are classified by extension (`.rpm` → rpms, `.log` → logs, SBOM extensions → sbom, else → artifacts) and uploaded to the appropriate repository. `--rpm-path` and `--sbom-path` are ignored in this mode.

**Signed-by:** When `--signed-by` is set, a `signed_by` label is added to RPMs only, and RPMs are stored in a separate `rpms-signed` repository with its own distribution. Logs and SBOMs are never signed and always go to the standard repositories.

**Overwrite:** When `--overwrite` is set, for each RPM about to be uploaded the tool searches Pulp by the file’s SHA256 (same mechanism as `search-by` checksum mode), keeps only matches that exist in the target RPM repository’s latest version, then calls the repository modify API with `remove_content_units` before uploading and adding the new RPMs. Use with `--signed-by` to scope the search to signed packages.

**Target-arch-repo:** When `--target-arch-repo` is set, RPM repositories and distributions are named by architecture only (`{arch}`), including when `--signed-by` is set (no separate `rpms-signed` path). Published paths look like `…/pulp-content/{namespace}/{arch}/`. The aggregate `{build}/rpms` repo is not created; RPM repos are created when each arch is uploaded. `pulp_results.json` `distributions` maps string names to base URLs (sorted keys when serialized); per-arch RPM bases use keys `rpm_<arch>` (e.g. `rpm_x86_64`). Logs, SBOM, and generic artifacts still use `{build}/logs`, `{build}/sbom`, and `{build}/artifacts`.

## upload-files

Upload individual files (RPMs, logs, SBOMs, generic files).

| Argument | Required | Description |
|----------|----------|-------------|
| `--build-id` | Yes | Build identifier |
| `--namespace` | Yes | Namespace |
| `--parent-package` | Yes | Parent package name |
| `--rpm` / `--file` / `--log` / `--sbom` | At least one | File paths (repeatable) |
| `--arch` | No | Architecture (e.g. x86_64) |
| `--artifact-results` | No | Output paths or folder |
| `--sbom-results` | No | SBOM output path |

## pull

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

## create-repository

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

## search-by

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

## Environment and logging

**Environment:** `SSL_CERT_FILE`, `SSL_CERT_DIR`, `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` are supported.

**Verbosity:** `-d` INFO, `-dd` DEBUG, `-ddd` HTTP logs. Default: WARNING.

**JSON logs (structured):** set `PULP_TOOL_JSON_LOG=1` (or `true` / `yes`) for newline-delimited JSON on stdout (via [python-json-logger](https://github.com/madzak/python-json-logger)); useful for aggregators. Default remains plain text.
