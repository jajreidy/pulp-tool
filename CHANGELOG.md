# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`changing-pulp-container` agent skill:** documents in-repo `.tekton/` PipelineRuns, upstream Konflux `single-arch-build-pipeline` (`buildah-oci-ta` task chain), and [reference.md](skills/changing-pulp-container/reference.md); `make test-container` for optional local Dockerfile smoke-test
- **`docs/ARCHITECTURE.md`:** living architecture doc (overview, mermaid flow, code map, invariants, external integrations, glossary); complements `AGENTS.md` / `CLAUDE.md`
- **`AGENTS.md`:** canonical agent entry with § **Bootstrap** (read-first order to reduce context thrash); pointers to `docs/ARCHITECTURE.md`, `CLAUDE.md`, and on-demand skills under `skills/`
- **Agent skills (`skills/`):** portable on-demand workflows (upload/Konflux, PR drafting, diff-cover, CI troubleshooting); `skills/README.md` and verification scenarios; `.cursor/skills` and `.agents/skills` symlinks for Cursor and other agentskills.io clients
- **AgentReady:** `file_size_limits` and `type_annotations` are checked again in `.agentready-config.yaml` (removed from `excluded_attributes`); oversized test modules were split into smaller files; `scripts/split_agentready_tests.py` encodes slice boundaries for maintaining those splits
- **Test layout and Hypothesis:** `tests/support/` for shared helpers (`tempfile_config`, `make_rpm_list_response`, TLS PEM generation, checksum constants); `hypothesis` in optional `dev` dependencies; `tests/utils/test_hypothesis_properties.py` exercises small pure functions (correlation ID resolution, build ID strip/sanitize, RPM filename parsing, `versions_from_status_payload`); test tree split to mirror `pulp_tool/` (`tests/pull/`, multiple `tests/cli/test_*.py`, `tests/services/test_upload_*.py`, `tests/api/pulp_client/`); `conftest` uses `tests.support.tls_certs` for mock TLS material
- **Konflux downstream documentation for contributors and agents:** `CLAUDE.md` documents Tekton call sites (konflux-ci/rpmbuild-pipeline `import-to-quay` / `push-to-pulp-select-auth`; konflux-ci/release-service-catalog `push-artifacts-to-storage` / `push-build-to-artifact-storage` and managed pipeline YAML), config mounts (`/pulp-access` vs `/etc/rok-access`), illustrative `upload` flags, `rok-access` missing-config behavior (success exit without `pulp-tool`), comparison table, in-repo code map, regression checklist (including re-verifying upstream YAML when paths or ORAS/trusted-artifact staging such as `oras-staging/` may change), and a **PR review** section; `.cursor/rules/konflux-ecosystem.mdc` (`alwaysApply: true`) summarizes the same contracts; README Development links to `CLAUDE.md` and instructs re-checking those task/pipeline YAMLs before merging changes to how `pulp-tool` is used
- `make test-diff-coverage` runs `diff-cover` at 100% vs `COMPARE_BRANCH` (default `origin/main`) after `make test`, matching the PR merge gate; `scripts/check-all.sh` also generates `coverage.xml` and runs `diff-cover` when the tool and compare ref exist (`DIFF_COVER_COMPARE_BRANCH` optional)
- `upload --target-arch-repo`: `pulp_results.json` includes per-architecture RPM distribution base URLs under `distributions` with keys `rpm_<arch>` (e.g. `rpm_x86_64`); serialized `distributions` uses sorted keys for stable `{name: url}` output, alongside build-scoped entries when those repos exist
- Upload optionally skips creating logs and SBOM repositories when no log or SBOM uploads are expected; `skip_logs_repo` / `skip_sbom_repo` on `UploadContext` and `PulpHelper.setup_repositories` (defaults preserve creating all repos for programmatic callers who omit the flags)
- `upload --target-arch-repo`: per-architecture RPM repos/distributions (``{namespace}/{arch}/Packages/...``); logs/SBOM/artifacts stay build-scoped; lazy repo creation at upload; works with `--results-json`, `--signed-by`, and `--overwrite`; with `--signed-by`, same arch repo and `signed_by` is label-only
- `upload --overwrite`: RPM-only; remove existing RPM package units in the target repo that match local RPM NVRA filename (and `signed_by` when set) via `remove_content_units` before upload
- `upload --results-json`: Upload artifacts from pulp_results.json; files resolved from JSON directory or --files-base-path; --build-id and --namespace optional (extracted from artifact labels)
- DistributionClient username/password (Basic Auth) support; use `username` and `password` in config as alternative to cert/key for pull downloads
- `pull --distribution-config`: Path to config file for distribution auth (cert/key or username/password); overrides --transfer-dest/--config for auth when set
- Skip artifacts repository and distribution when `--artifact-results` is a local folder path (no comma); Konflux mode (url_path,digest_path) still creates artifacts repo
- `upload --signed-by`: Add signed_by pulp_label to RPMs only; use separate rpms-signed repo (logs/SBOMs never signed)
- `search-by` command: search RPM content in Pulp by checksum, filename, and/or signed_by; filter results.json by removing found artifacts (--results-json, --output-results); supports --filename/--filenames, --checksum/--checksums, --signed-by, --keep-files; NVR-based queries with incremental API call reduction; --keep-files keeps logs and sboms in output-results (default: only RPM artifacts)
- `codecov.yml` configuration file with `unit-tests` flag and carryforward enabled
- packages.redhat.com configuration section in README with OAuth2 setup
- Username/password (Basic Auth) support for packages.redhat.com
- **`create_file_content_and_wait`** in `pulp_tool.utils.pulp_tasks`: single helper for file content POST, response check, task wait, and optional URL extraction; used from upload orchestration, `upload_collect`, `uploads`, and pull re-upload paths
- **Upload gather split:** `pulp_tool.services.upload_collect` and `pulp_tool.services.upload_common` hold results JSON / Konflux helpers; `upload_service` re-exports the same public names for stable imports
- **`RepositoryApiOps`** (`pulp_tool.utils.repository_manager`): frozen dataclass binding `get` / `create` / `distro` / `get_distro` / `update_distro` / `wait_for_finished_task` to `PulpClient` for a given API type (replaces a dict of lambdas)
- **Pulp client package:** `pulp_tool/api/pulp_client/` (`cache`, `chunked_get`, `repository`, `content_query`, `results`, `helpers`, `client`); `PulpClient` delegates without changing mixin order or Tekton-visible behavior

### Fixed
- **`upload` / `search-by` / Pulp RPM queries — `signed_by`:** Pulpcore rejects label values with comma or parentheses (400 on upload). The tool substitutes `,`→`:` and `(`/`)`→`[`/`]` via `pulp_tool.models.pulp_label_values` on `UploadRpmContext`, `SearchByRequest`, and at `PulpClient` query time so storage and lookups stay aligned. `search-by` applies the same mapping when building requests and when removing RPMs from `pulp_results.json` (artifact labels may still be pre-substitution). `pulp_label_select` is included in the primary GET `q=` with checksum or NVR constraints when possible; paginated list + client label filtering remains a forced fallback only when a query cannot be expressed safely.
- **Container certification:** Address ecosystem-cert-preflight failures (`BasedOnUbi`, `HasLicense`, `HasRequiredLabel`, `RunAsNonRoot`) by migrating the Konflux image from Fedora 45 to UBI 10 minimal with required labels, `/licenses/LICENSE`, and non-root `USER 1001`

### Changed
- **Container image:** Dockerfile uses UBI 10 minimal with Python 3.12; adds OpenShift preflight labels, `/licenses/LICENSE`, and non-root `USER 1001`; `gcc` no longer required (`pydantic-core` cp312 wheels)
- **CI:** GitHub Actions unit/lint and security workflows run on Python 3.12 (no transient `gcc`); container image build remains on Konflux Tekton only (no GitHub Actions `docker build`)
- **Agent skills and Cursor rules:** on-demand workflows extracted to `skills/`; `llm-development-guidelines-deep.mdc` is a skill index (lint quick-ref); `AGENTS.md`, `CLAUDE.md`, and `CONTRIBUTING.md` point at `skills/` as the canonical path
- **`.cursor/rules/llm-development-guidelines.mdc`:** slim always-on essentials (workflow, diff coverage, PR/CHANGELOG rules); lengthy PR/lint/troubleshooting detail moved to `skills/`
- **`CLAUDE.md`:** scoped to Konflux/Tekton contracts and regression checklist; system/code-map narrative in `docs/ARCHITECTURE.md`
- **Agent documentation links:** `README.md`, `CONTRIBUTING.md`, `docs/adr/0000-record-architecture-decisions.md`, `docs/cli-reference.md`, `.cursor/rules/konflux-ecosystem.mdc`, and `docs/ARCHITECTURE.md` updated for the split
- **`docs/cli-reference.md` / `docs/ARCHITECTURE.md`:** `signed_by` substitution, server-side vs fallback Pulp queries, and `search-by` / `pulp_results.json` filtering; **`upload --signed-by` / `search-by --signed-by`:** help text aligned
- **Mypy (tests):** `[[tool.mypy.overrides]]` for `tests.*` disables `return-value`, `var-annotated`, `assignment`, `arg-type`, and `call-arg` so pre-commit mypy on the test suite tolerates mocks, fixtures, and intentional invalid inputs; `make lint` still type-checks **`pulp_tool/`** only
- Removed **`ensure_pulp_capabilities`** (pre-flight `GET …/status/` and minimum pulpcore / `pulp_rpm` version checks) from **`upload`**, **`upload-files`**, **`create-repository`**, **`search-by`**, and pull repository setup; **`versions_from_status_payload`** remains in **`pulp_tool.utils.pulp_capabilities`** for callers that parse status JSON. Upload and search flows no longer fail early when the status endpoint is missing, returns non-JSON, or sits behind routing that does not expose it like a stock Pulp deployment.
- **Testing docs and examples:** `tests/README.md` adds a directory map (mirrors `pulp_tool/`) and a short Hypothesis section; root `README` links to it; `CONTRIBUTING.md` and `scripts/README.md` pytest examples point at `tests/cli/test_cli_core.py` instead of the former monolithic `tests/test_cli.py`
- **`RepositoryManager.get_repository_methods`** now returns **`RepositoryApiOps`** instead of a `dict` of callables (call sites use attribute access, e.g. `ops.get(name)`)
- **Removed** `pulp_tool.api.task_manager` (documentation-only `TaskManagerMixin` Protocol); **`TaskMixin`** in `pulp_tool.api.tasks.operations` is the live implementation (module docstring notes the removal)
- Pulp HTTP client: validate response status on more code paths before returning or parsing JSON—chunked GET (all branches, including the aggregated-results fallback), repository/distribution GET-by-name (still allows **404** for “not found” lookups), create-resource POST, distribution PATCH (`update_distro`), file content POST, task GET parsing (single `_check_response`), post-task distribution fetch in `RepositoryManager`, and `DistributionClient.pull_artifact` (`raise_for_status` on error status).
- `pull`: create destination repositories/distributions and re-upload downloaded content only when `--transfer-dest` is set; group-level `--config` alone still supplies auth (and `base_url` for `--build-id` + `--namespace`) but does not create destination repos or upload
- `pull`: download URLs use only per-artifact `url` fields in artifact results JSON; `distributions` in that file are not used to build download URLs (artifacts without `url` are skipped)
- `upload` and `upload-files` again exit with code 1 on authentication-related failures (HTTP 401/403, OAuth “failed to obtain access token”, and similar); the previous temporary non-fatal workaround (warning and exit 0) has been removed
- Raised minimum versions for runtime (`httpx`, `pydantic`, `click`) and dev tooling in `pyproject.toml` / `setup.py`; build-system uses newer `setuptools`/`setuptools-scm`
- Removed Sphinx and sphinx-rtd-theme from optional `dev` extras (in-tree docs build was removed earlier); Pygments may still be installed transitively (e.g. `pytest`, `diff-cover`)
- Local `--artifact-results` folder path: `distributions` in `pulp_results.json` no longer includes a synthetic `artifacts` pulp-content URL (artifacts repo was already skipped; URL map now aligns)
- `upload --target-arch-repo`: `pulp_results.json` `distributions` keys for per-arch RPM bases are `rpm_<arch>` instead of bare architecture names (e.g. `rpm_x86_64` not `x86_64`)
- `upload` / `upload-files`: infer whether log and SBOM repos are needed before repository setup (directory `*.log` scan or `--results-json` artifact keys; SBOM via `--sbom-path` or SBOM-classified keys); omitted types are excluded from results `distributions`; clear errors if uploads are attempted without the matching repository
- Upload orchestration uses `RpmUploadResult` per architecture instead of ad-hoc dicts; gather/collect uses `PulpContentRow`, `ExtraArtifactRef`, and `FileInfoMap` for clearer typed data flow
- Upload flow populates `pulp_results.json` artifact entries incrementally as RPMs, logs, SBOMs, and generic files finish; final gather still reconciles via merge (keeps incremental entries when keys already exist)
- Repository setup logs use the concrete repo slug (e.g. ``rpms-signed``) instead of a generic ``Rpms`` label; distribution creation logs state that ``name`` and ``base_path`` match the repository name on one line
- `upload --target-arch-repo` with `--signed-by`: RPM paths remain `{arch}/` only (no `{arch}/rpms-signed`); signing is via `signed_by` label on content
### Security
- Added **`pip-audit`** to optional `dev` dependencies, **`make audit`** (isolated **`.audit-venv`** with **`pip-audit -l`**, same **CVE-2026-4539** / **GHSA-5239-wwwm-4pmq** ignores as CI until Pygments **>2.19.2** is on PyPI), and **`pip-audit -l`** in **`security-scan.yml`**; when a fixed Pygments is released, pin **`pygments>=…`** under `dev` in `pyproject.toml` / `setup.py` and drop the workflow/Makefile ignores
- Optional docs stack (Sphinx) remains removed from `dev` extras; **CVE-2026-4539** still applies to transitive Pygments from **`pytest`** and **`diff-cover`** until a patched wheel is published

### Fixed
- **Tests (lint):** Removed stray split-artifact string lines left at the top of some `test_all_models_*.py` files; wrapped long mock URLs and trimmed unused imports in split test modules so `flake8` passes
- **`@cached_get`** cache keys now include the decorated method name plus full positional and keyword arguments, so **`_get_single_resource(endpoint, name)`** cannot return a cached response for a different **`name`** when the endpoint string matches (regression test added)
- **Synchronous `_chunked_get`:** when an event loop is already running, the method now raises **`RuntimeError`** with a clear message instead of incorrectly treating that case like “no loop” and swallowing the error
- Content search (`GET /api/v3/content/`, including gather-by-`build_id`): empty or non-JSON bodies no longer surface as a bare `JSONDecodeError`; errors include HTTP status, URL, and a short body preview when JSON is invalid (`content_find_results_from_response`). `find_content` rejects non-success HTTP responses before parsing the body.
- When `cert`/`key` are set for mTLS but PEM files are missing (wrong path in containers, etc.), `PulpClient` now fails fast with a clear error instead of opening a TLS connection without a client certificate (which often surfaced only as HTTP 403)
- `create_session_with_retry` logs an error when a `cert` tuple is given but the PEM paths do not exist (defensive; `PulpClient` normally validates paths first)
- Generic `/api/v3/content/` responses that are a bare JSON array (not `{"results": [...]}`) no longer crash gather-by-href or `_find_artifact_content` with `TypeError: list indices must be integers or slices, not str`
- Results JSON RPM URLs with `--signed-by`: use the `rpms-signed` distribution base (`distributions.rpms_signed` / correct artifact `url`) instead of the unsigned `rpms` path
- RPM distribution URLs: ``Packages/<letter>/`` uses the lowercase first character of the RPM **basename** only (correct for paths like ``Packages/W/foo.rpm``, ``arch/pkg.rpm``, or plain ``foo.rpm``)
- Clear error when no auth credentials provided (client_id/client_secret or username/password)

### Added
- `--artifact-results` folder mode: pass a folder path to save pulp_results.json locally instead of uploading to Pulp
- Comprehensive type annotations for all function arguments
- Pre-commit hooks for code quality checks
- CHANGELOG.md following Keep a Changelog format
- CONTRIBUTING.md with development guidelines
- Developer scripts for common tasks
- Makefile with common development targets
- .editorconfig for consistent formatting
- Dockerfile for containerized deployments
- Initial release of pulp-tool
- CLI commands: upload, transfer, get-repo-md
- PulpClient for API interactions
- PulpHelper for high-level operations
- DistributionClient for artifact downloads
- Support for RPM, log, and SBOM file management
- OAuth2 authentication with automatic token refresh
- Comprehensive test suite with 85%+ coverage

### Changed
- Renamed `transfer` command to `pull`; added `--transfer-dest` option for transfer destination. When using `--build-id` + `--namespace`, either `--transfer-dest` or group-level `--config` can be used
- Renamed file structure from `transfer` to `pull`: `cli/transfer.py` → `cli/pull.py`, `pulp_tool/transfer/` → `pulp_tool/pull/`, `TransferContext` → `PullContext`, `TransferService` → `PullService`, `tests/test_transfer.py` → `tests/test_pull.py`
- Upload progress messages (e.g. "Uploading SBOM: X", "Uploading RPM: X") now use logging.warning instead of info
- Consolidated all dependencies into pyproject.toml
- Improved type safety across the codebase
- Enhanced error handling and logging
- Per-file upload progress: "Uploading X: filename" now logged at INFO so progress is visible at default verbosity
- README: Makefile-first development workflow, pre-commit, fixed typos and duplicate Create Repository section
- CONTRIBUTING: recommend `make install-dev`, pre-commit run twice, `make test` and 100% diff coverage for new code

### Removed
- `transfer` command (replaced by `pull`; use `pulp-tool pull` with `--transfer-dest` instead of `--config`)
- Documentation GitHub workflow (`.github/workflows/docs.yml`)
- Makefile targets: `docs`, `docs-clean`, `docs-serve`

### Fixed
- Fixed type annotation issues in transfer.py
- Fixed import order issues in cli.py
- Fixed Optional import missing in content_query.py

[Unreleased]: https://github.com/konflux/pulp-tool/compare/v1.0.0...HEAD
