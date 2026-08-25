# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0](https://github.com/jajreidy/pulp-tool/compare/v1.0.0...v1.1.0) (2026-08-25)


### Added

* add i686 arch support and pr-description drafting ([edfe7a1](https://github.com/jajreidy/pulp-tool/commit/edfe7a12bd84a36542fabe530e5c0d8623685a0f))
* add i686 arch support and pr-description drafting ([e06cf41](https://github.com/jajreidy/pulp-tool/commit/e06cf415bc6a6e6557d5bd0459a5c6c155e61013))
* **e2e:** reusable test image and concurrent run isolation ([a7e776f](https://github.com/jajreidy/pulp-tool/commit/a7e776fd5df6666c50f034a7705c60b2e93b545a))
* **e2e:** reusable test image and concurrent run isolation ([c060d79](https://github.com/jajreidy/pulp-tool/commit/c060d791fd5c952a84c6ff376eb5aa55e6beced0))
* **release:** add local Release Please workflow and GitHub Release o… ([43e3e48](https://github.com/jajreidy/pulp-tool/commit/43e3e4890929470914d213918bdf980a83c9b6eb))
* **release:** add local Release Please workflow and GitHub Release on tag ([c02a88b](https://github.com/jajreidy/pulp-tool/commit/c02a88b1cadf53752d9723f4b93c70edd30c3574))
* **upload:** add --overwrite for RPMs (remove matching repo content before upload) ([01c9750](https://github.com/jajreidy/pulp-tool/commit/01c9750511a16de1e32662099dca74945ecc8ed6))


### Fixed

* **api:** validate HTTP responses and clarify content-search parse errors ([c565851](https://github.com/jajreidy/pulp-tool/commit/c565851187308ebff2bf5522076298128d6c284a))
* **api:** validate HTTP responses and clarify content-search parse errors ([e1bd66b](https://github.com/jajreidy/pulp-tool/commit/e1bd66b93cfdfeb1b4120b32219c2d300a8af4d1))
* **ci:** correct codecov flag name and add codecov.yml config ([2311b82](https://github.com/jajreidy/pulp-tool/commit/2311b82024eeed62152ce20af1ac5bde8bdef2dc))
* **ci:** correct codecov flag name and add codecov.yml config ([fd91a71](https://github.com/jajreidy/pulp-tool/commit/fd91a71d99d85d471bdd5d567a07c60fa3b36507))
* **ci:** restrict Checkton to pull requests and fix local diff base ([3902add](https://github.com/jajreidy/pulp-tool/commit/3902add34b9f8b7be6c81ea1cdee763124cb96fc))
* **ci:** restrict Checkton to pull requests and fix local diff base ([0e60822](https://github.com/jajreidy/pulp-tool/commit/0e608226b258863dcddce6f2b8aa37b56a21c347))
* **ci:** run pip-audit before safety and migrate e2e init task ([60eebcb](https://github.com/jajreidy/pulp-tool/commit/60eebcb256cbee8cbd26e01afc8a66c6d2c7d36c))
* **ci:** run pip-audit before safety and migrate e2e init task ([3833b01](https://github.com/jajreidy/pulp-tool/commit/3833b015c5a4eed882b312f1594b467e4783674d))
* **container:** align CI and agents with Python 3.15 Konflux image ([c2d23bc](https://github.com/jajreidy/pulp-tool/commit/c2d23bcf1dc979cc6bce29e70728d45991a28020))
* **container:** align CI and agents with Python 3.15 Konflux image ([f7bc1a6](https://github.com/jajreidy/pulp-tool/commit/f7bc1a6b7b4292dd9548bfd3ef9554769a9bf3e9))
* **container:** build image on Fedora 45 Python 3.15 ([17d680e](https://github.com/jajreidy/pulp-tool/commit/17d680eabe79bcf55aa2e0215f24703eadfbc523))
* **container:** build image on Fedora 45 Python 3.15 ([dfc460d](https://github.com/jajreidy/pulp-tool/commit/dfc460d419ec595692954117e92af1b5b7e7ef73))
* **container:** move pulp-tool image to UBI 10 and Python 3.12 ([d9f1257](https://github.com/jajreidy/pulp-tool/commit/d9f1257f73d671c402aa45b6fbcc96576168b1ed))
* **container:** move pulp-tool image to UBI 10 and Python 3.12 ([0d6c302](https://github.com/jajreidy/pulp-tool/commit/0d6c302ca8e47107579ed5ac52d8ffe4a09ca4e0))
* correct GitHub URLs and align package metadata ([9a04339](https://github.com/jajreidy/pulp-tool/commit/9a04339397e638e994138329228ea3bb4b760ccc))
* correct GitHub URLs and align package metadata ([fcf528c](https://github.com/jajreidy/pulp-tool/commit/fcf528cda897b08ada6e5dc0891be39065b0ab90))
* **deps:** pin pip&gt;=26.2 for CVE-2026-13346 in pip-audit ([57d3f81](https://github.com/jajreidy/pulp-tool/commit/57d3f81a7d44ff0410c4ac02f063e434964e5eab))
* **deps:** pin pip&gt;=26.2 for CVE-2026-13346 in pip-audit ([b313813](https://github.com/jajreidy/pulp-tool/commit/b313813d4b06f1b9f626e8b20cec31c7a3979415))
* detect artifact type by extension before substring matching ([51f3c3f](https://github.com/jajreidy/pulp-tool/commit/51f3c3fbdc66edd4e4c06030f12bad914eaa560c))
* detect artifact type by extension before substring matching ([0fcd7c6](https://github.com/jajreidy/pulp-tool/commit/0fcd7c64db6213a2461831ec91de4583b1dadbf4))
* **pulp:** normalize signed_by for uploads and Pulp queries ([e6b6ca3](https://github.com/jajreidy/pulp-tool/commit/e6b6ca3feec3af7d7b933d5eb6826fef4c83c4e7))
* **pulp:** normalize signed_by for uploads and Pulp queries ([ebf366e](https://github.com/jajreidy/pulp-tool/commit/ebf366e3d0474dbf59a7988bf4cc317958116a7d))
* **upload:** defer distribution waits and cap task polling at 30 minutes ([ee5c226](https://github.com/jajreidy/pulp-tool/commit/ee5c226a8a49df6f4a175602b5b304f5a22a7dfd))
* **upload:** defer distribution waits and cap task polling at 30 minutes ([5a2076d](https://github.com/jajreidy/pulp-tool/commit/5a2076d644b6940bca44416774d8444ef0c8e4c4))
* **upload:** handle signed_by labels with commas; refresh agent docs ([352c308](https://github.com/jajreidy/pulp-tool/commit/352c30862b657a5667527b220aba737878848def))
* **upload:** handle signed_by labels with commas; refresh agent docs ([c45eb5b](https://github.com/jajreidy/pulp-tool/commit/c45eb5b687ed5b02c1144eb5ac542bb92e654425))
* **upload:** harden Konflux digest, paths, and RPM upload reliability ([84998cf](https://github.com/jajreidy/pulp-tool/commit/84998cf3c906b7fd79b146b25ae9f66e1997dbc6))
* **upload:** harden Konflux digest, paths, and RPM upload reliability ([7fdeb58](https://github.com/jajreidy/pulp-tool/commit/7fdeb58edd43dafa88d833d211406be153a401f4))
* **upload:** idempotent distribution setup after 504 retries ([a68c443](https://github.com/jajreidy/pulp-tool/commit/a68c4436deb78cb301b7223e78f3d2aa8cc8823c))
* **upload:** idempotent distribution setup after 504 retries ([3cee8bb](https://github.com/jajreidy/pulp-tool/commit/3cee8bbcabe8d93905e8bf7e2da6a6a9ac59fde6))
* **upload:** omit artifacts distribution URL for local --artifact-results folder ([91b7bd1](https://github.com/jajreidy/pulp-tool/commit/91b7bd173dd0fa8e8611ba0b805e50803072b435))
* **upload:** omit artifacts distribution URL for local --artifact-results folder ([cb4603e](https://github.com/jajreidy/pulp-tool/commit/cb4603e66ec54b37f41d1580eb62ab4193ca3c65))

## [Unreleased]

### Added
- **Release automation (maintainers):** local [Release Please](https://github.com/googleapis/release-please) via [`scripts/release-please.sh`](scripts/release-please.sh) — `make release-please` opens/updates the release PR (`gh auth login` or token); `make release-publish` pushes a `v*` tag from [`.release-please-manifest.json`](.release-please-manifest.json) with git only (triggers [`.github/workflows/release.yml`](.github/workflows/release.yml) → PyPI trusted publishing and GitHub Release notes from `CHANGELOG.md`); config in [`release-please-config.json`](release-please-config.json); documented in [`docs/releasing.md`](docs/releasing.md). Merging the release PR rebuilds the Konflux container on `main` (no on-tag image PipelineRun).
- **E2e reusable test image:** [`Dockerfile.e2e`](Dockerfile.e2e) pre-installs `python3`, `gcc`, `pulp-cli`, and `rpm-rs`; [`pulp-e2e-testing`](.tekton/pipelines/pulp-e2e-testing.yaml) builds it once per PipelineRun via `task-buildah` and reuses it in all e2e Tekton steps; `make test-e2e-container` for local smoke-test
- **E2e concurrent run isolation:** Tekton sets `E2E_RUN_ID` from `$(context.pipelineRun.uid)`; build-scoped Pulp resource names are suffixed via [`e2e/names.py`](e2e/names.py); per-arch `--target-arch-repo` repos remain global names and may contend when runs overlap
- **Lint toolchain expansion:** Ruff (replaces Black and Flake8), yamllint, ShellCheck, hadolint, codespell, and Checkton (Tekton embedded scripts) in pre-commit and/or CI; pip-audit added to pre-commit via `make audit`; `.yamllint.yml` and `.codespell-ignore-words.txt`
- **`docs/releasing.md`:** maintainer guide for PyPI releases (trusted publishing setup, semver tagging on `main`, workflow steps, verification, troubleshooting); linked from `CONTRIBUTING.md`, `README.md`, and `.github/workflows/release.yml`
- **PyPI release workflow (`.github/workflows/release.yml`):** builds and publishes `pulp-tool` on semver `v*` tags that point at `main`; uses `hynek/build-and-inspect-python-package` for wheel/sdist inspection and PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/) via `pypa/gh-action-pypi-publish`; maintainer steps in [docs/releasing.md](docs/releasing.md)
- **`make lock-check`:** fails when `pyproject.toml` and `uv.lock` are out of sync (`uv lock --check`); CI runs it in `python-diff-lint.yml`
- **`tests/utils/test_iteration_utils.py`:** unit tests for `pulp_tool.utils.iteration_utils`
- **`i686` architecture support:** `SUPPORTED_ARCHITECTURES` includes 32-bit x86; RPM filename and path detection, upload orchestration, and content queries treat `i686` like other supported arches
- **`drafting-pulp-tool-pr` skill:** writes gitignored local `pr-description.md` at repo root for paste-ready PR body drafts
- **`changing-pulp-container` agent skill:** documents in-repo `.tekton/` PipelineRuns, upstream Konflux `single-arch-build-pipeline` (`buildah-oci-ta` task chain), and [reference.md](skills/changing-pulp-container/reference.md); `make test-container` for optional local Dockerfile smoke-test
- **`docs/ARCHITECTURE.md`:** living architecture doc (overview, mermaid flow, code map, invariants, external integrations, glossary); complements `AGENTS.md` / `CLAUDE.md`
- **`AGENTS.md`:** canonical agent entry with § **Bootstrap** (read-first order to reduce context thrash); pointers to `docs/ARCHITECTURE.md`, `CLAUDE.md`, and on-demand skills under `skills/`
- **Agent skills (`skills/`):** portable on-demand workflows (upload/Konflux, PR drafting, diff-cover, CI troubleshooting); `skills/README.md` and verification scenarios; `.cursor/skills` and `.agents/skills` symlinks for Cursor and other agentskills.io clients
- **AgentReady:** `file_size_limits` and `type_annotations` are checked again in `.agentready-config.yaml` (removed from `excluded_attributes`); oversized test modules were split into smaller files; `scripts/split_agentready_tests.py` encodes slice boundaries for maintaining those splits
- **Test layout and Hypothesis:** `tests/support/` for shared helpers (`tempfile_config`, `make_rpm_list_response`, TLS PEM generation, checksum constants); `hypothesis` in optional `dev` dependencies; `tests/utils/test_hypothesis_properties.py` exercises small pure functions (correlation ID resolution, build ID strip/sanitize, RPM filename parsing); test tree split to mirror `pulp_tool/` (`tests/pull/`, multiple `tests/cli/test_*.py`, `tests/services/test_upload_*.py`, `tests/api/pulp_client/`); `conftest` uses `tests.support.tls_certs` for mock TLS material
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
- CLI commands: upload, upload-files, pull, search-by, create-repository
- PulpClient for API interactions
- PulpHelper for high-level operations
- DistributionClient for artifact downloads
- Support for RPM, log, and SBOM file management
- OAuth2 authentication with automatic token refresh
- Comprehensive test suite with 85%+ coverage

### Changed
- **Lint toolchain:** replace Black and Flake8 with Ruff (lint + format, S security ruleset); expand CI lint workflow (`python-diff-lint.yml`) with yamllint, ShellCheck, hadolint, codespell, Checkton, and **`make test-diff-coverage`** on pull requests; consolidate pytest options in `pyproject.toml` (`Makefile`, `scripts/check-all.sh`, `scripts/run-tests.sh` defer to `[tool.pytest.ini_options]`); update `CONTRIBUTING.md`, `README.md`, and troubleshooting skill
- **Documentation audit:** align README, `docs/cli-reference.md`, `CONTRIBUTING.md`, `SECURITY.md`, `scripts/README.md`, and `docs/ARCHITECTURE.md` with current CLI flags, config keys, PyPI trusted publishing, and Makefile targets
- **`docs/cli-reference.md`:** normalize `search-by` section to the same options-table layout as other commands; document `--keep-files`
- **`setup.py`:** thin `setup()` shim only; dependency ranges and package metadata live in **`pyproject.toml`**
- **Container image (`Dockerfile`):** install runtime deps from **`uv.lock`** (`uv export --frozen --no-dev`) then `pip install --no-deps .`; documented in **`CONTRIBUTING.md`** and **`README.md`**
- **`renovate.json`:** enable **`lockFileMaintenance`** and group pep621 dependency bumps with **`uv.lock`** refresh in one Mintmaker PR
- **`CHANGELOG.md`:** consolidate duplicate `[Unreleased]` subsections; fix compare URL org
- **`pyproject.toml`:** Use setuptools package discovery instead of incomplete explicit package list
- **`setup.py`:** Sync runtime and dev dependencies with `pyproject.toml` (including `python-json-logger`); drop hardcoded version (use `setuptools_scm` via `pyproject.toml`); container builds pass `SETUPTOOLS_SCM_PRETEND_VERSION` from the `VERSION` build-arg; exclude scm-generated `_version.py` from Black
- **`pulp_tool` package:** `__init__` imports `__version__` from `_version` so CLI and library agree
- **Container image:** Dockerfile uses UBI 10 minimal with Python 3.12; adds OpenShift preflight labels, `/licenses/LICENSE`, and non-root `USER 1001`; `gcc` no longer required (`pydantic-core` cp312 wheels)
- **CI:** GitHub Actions unit/lint and security workflows run on Python 3.12 (no transient `gcc`); container image build remains on Konflux Tekton only (no GitHub Actions `docker build`)
- **Agent skills and Cursor rules:** on-demand workflows extracted to `skills/`; `llm-development-guidelines-deep.mdc` is a skill index (lint quick-ref); `AGENTS.md`, `CLAUDE.md`, and `CONTRIBUTING.md` point at `skills/` as the canonical path
- **`.cursor/rules/llm-development-guidelines.mdc`:** slim always-on essentials (workflow, diff coverage, PR/CHANGELOG rules); lengthy PR/lint/troubleshooting detail moved to `skills/`
- **`CLAUDE.md`:** scoped to Konflux/Tekton contracts and regression checklist; system/code-map narrative in `docs/ARCHITECTURE.md`
- **Agent documentation links:** `README.md`, `CONTRIBUTING.md`, `docs/adr/0000-record-architecture-decisions.md`, `docs/cli-reference.md`, `.cursor/rules/konflux-ecosystem.mdc`, and `docs/ARCHITECTURE.md` updated for the split
- **`docs/cli-reference.md` / `docs/ARCHITECTURE.md`:** `signed_by` substitution, server-side vs fallback Pulp queries, and `search-by` / `pulp_results.json` filtering; **`upload --signed-by` / `search-by --signed-by`:** help text aligned
- **Mypy (tests):** `[[tool.mypy.overrides]]` for `tests.*` disables `return-value`, `var-annotated`, `assignment`, `arg-type`, and `call-arg` so mypy tolerates mocks, fixtures, and intentional invalid inputs; `make lint` type-checks both **`pulp_tool/`** and **`tests/`**
- Removed **`ensure_pulp_capabilities`** (pre-flight `GET …/status/` and minimum pulpcore / `pulp_rpm` version checks) from **`upload`**, **`upload-files`**, **`create-repository`**, **`search-by`**, and pull repository setup. Upload and search flows no longer fail early when the status endpoint is missing, returns non-JSON, or sits behind routing that does not expose it like a stock Pulp deployment.
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
- Renamed `transfer` command to `pull`; added `--transfer-dest` option for transfer destination. When using `--build-id` + `--namespace`, either `--transfer-dest` or group-level `--config` can be used
- Renamed file structure from `transfer` to `pull`: `cli/transfer.py` → `cli/pull.py`, `pulp_tool/transfer/` → `pulp_tool/pull/`, `TransferContext` → `PullContext`, `TransferService` → `PullService`, `tests/test_transfer.py` → `tests/pull/`
- Upload progress messages (e.g. "Uploading SBOM: X", "Uploading RPM: X") now use logging.warning instead of info
- Consolidated all dependencies into pyproject.toml
- Improved type safety across the codebase
- Enhanced error handling and logging
- Per-file upload progress: "Uploading X: filename" now logged at INFO so progress is visible at default verbosity
- README: Makefile-first development workflow, pre-commit, fixed typos and duplicate Create Repository section
- CONTRIBUTING: recommend `make install-dev`, pre-commit run twice, `make test` and 100% diff coverage for new code

### Removed
- **Lint tooling:** `.flake8`, Black and Flake8 dev dependencies; deprecated `safety check` and non-blocking bandit steps from `security-scan.yml` (pip-audit and Ruff S rules remain blocking)
- Unused status/capability helpers (`pulp_tool.utils.pulp_capabilities`, `StatusResponse` / `VersionInfo` models), test-only RPM batch/NVRA helpers, unused `_parse_list_response`, and unused `get_rpm_by_unsigned_checksums` API
- Unused model properties and classes (`UploadResult`, `FileSizeStats`, per-type count helpers on `PulledArtifacts`/`ContentData`, `RpmPackageResponse` NVRA/NEVRA helpers)
- **`pulp_tool.utils.predicates`** and test-only helpers trimmed from `logging_utils`, `path_utils`, and `iteration_utils` (production call sites unchanged)
- **`requirements.in`** and **`requirements.txt`** (superseded by **`uv.lock`**)
- `transfer` command (replaced by `pull`; use `pulp-tool pull` with `--transfer-dest` instead of `--config`)
- Documentation GitHub workflow (`.github/workflows/docs.yml`)
- Makefile targets: `docs`, `docs-clean`, `docs-serve`

### Fixed
- **Async repository setup tests:** mock `_create_or_get_repository_impl` by repository name suffix instead of call-order `side_effect` lists so concurrent `asyncio.gather` + `run_in_executor` setup is stable on Python 3.14 (fixes `test_setup_repositories_impl_async_success` flake seen in CI when upgrading GitHub Actions Python) treat Pulp `name`/`base_path` uniqueness errors (HTTP 400 or failed create task) as idempotent success when the distribution already exists—covers gateway 504 retries that create the distribution server-side before the client retries; always check for an existing distribution before POST
- **Checkton CI (`tekton-lint`):** run the job on pull requests only; branch `push` events left Checkton without PR base/head SHAs, so it fell back to `git rev-parse main` and failed when `main` was not fetched. **`scripts/run-checkton.sh`** fetches `origin/main`, sets `CHECKTON_DIFF_BASE` / `CHECKTON_DIFF_HEAD`, and uses Podman `:Z` volume labeling for local SELinux.
- **`make audit` / pre-commit pip-audit:** install dev dependencies from `uv.lock` (`uv export --extra dev --no-emit-project`) instead of `pip install -e ".[dev]"`, so setuptools-scm does not regenerate tracked `pulp_tool/_version.py` during commits
- **Konflux `--artifact-results` digest:** write `sha256:<hex>` from the uploaded artifact (via Pulp artifact API) instead of leaving the digest file empty when the pulp-content URL has no OCI `@sha256:` suffix
- **Partial RPM uploads:** increment `uploaded_counts.rpms` by successful uploads only; record failures in `upload_errors`; raise when any RPM in a batch fails instead of exiting 0 with incomplete repo content
- **Missing `--sbom-path`:** raise `FileNotFoundError` when the SBOM file is explicitly requested but absent (upload no longer continues silently)
- **SBOM classification in `--results-json`:** bare `.json` keys are no longer treated as SBOM unless the key contains `sbom` (`.spdx` / `.spdx.json` unchanged)
- **Parallel upload results model:** lock `PulpResultsModel` mutations during concurrent per-architecture uploads
- **Repository URLs:** Correct GitHub org links from `konflux/pulp-tool` (404) to `konflux-ci/pulp-tool` across README, package metadata, issue templates, Dockerfile, and SECURITY.md
- **CI security scan:** Run `pip-audit` before installing optional `safety`/`bandit` in `security-scan.yml`; `safety>=3.6` transitively installs `nltk` (PYSEC-2026-597), which is not a pulp-tool dependency and was failing the audit step
- **E2e Tekton pipeline:** Apply Konflux `task-init` 0.3 migration in `pulp-e2e-testing` — remove stale `build` result `when` on `clone-repository` and obsolete init params (`image-url`, `rebuild`, `skip-checks`) for `task-init:0.4`
- **`upload` / `search-by` / Pulp RPM queries — `signed_by`:** Pulpcore rejects label values with comma or parentheses (400 on upload). The tool substitutes `,`→`:` and `(`/`)`→`[`/`]` via `pulp_tool.models.pulp_label_values` on `UploadRpmContext`, `SearchByRequest`, and at `PulpClient` query time so storage and lookups stay aligned. `search-by` applies the same mapping when building requests and when removing RPMs from `pulp_results.json` (artifact labels may still be pre-substitution). `pulp_label_select` is included in the primary GET `q=` with checksum or NVR constraints when possible; paginated list + client label filtering remains a forced fallback only when a query cannot be expressed safely.
- **Container certification:** Address ecosystem-cert-preflight failures (`BasedOnUbi`, `HasLicense`, `HasRequiredLabel`, `RunAsNonRoot`) by migrating the Konflux image from Fedora 45 to UBI 10 minimal with required labels, `/licenses/LICENSE`, and non-root `USER 1001`
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
- Fixed type annotation issues in transfer.py
- Fixed import order issues in cli.py
- Fixed Optional import missing in content_query.py

### Security
- **`pip-audit` / security-scan:** pin **`pip>=26.2`** in the `dev` extra (lockfile **26.2.1**) so CVE-2026-13346 (doubly-encoded index URLs; GHSA-qwm4-qh6w-59xr) is not flagged; `pip-audit` → `pip-api` previously resolved **26.1.2**
- **Path traversal via `--results-json`:** reject artifact keys whose resolved path escapes `files_base_path` (`resolve_path_under_base`)
- **Path traversal via pull log `arch` labels:** validate architecture against `SUPPORTED_ARCHITECTURES` before writing under `logs/<arch>/`
- **Invalid `arch` in Pulp file content paths:** `_build_file_relative_path` rejects unsupported architecture values
- Added **`pip-audit`** to optional `dev` dependencies, **`make audit`** (isolated **`.audit-venv`** with **`pip-audit -l`**, same **CVE-2026-4539** / **GHSA-5239-wwwm-4pmq** ignores as CI until Pygments **>2.19.2** is on PyPI), and **`pip-audit -l`** in **`security-scan.yml`**; when a fixed Pygments is released, pin **`pygments>=…`** under `dev` in `pyproject.toml` / `setup.py` and drop the workflow/Makefile ignores
- Optional docs stack (Sphinx) remains removed from `dev` extras; **CVE-2026-4539** still applies to transitive Pygments from **`pytest`** and **`diff-cover`** until a patched wheel is published

[Unreleased]: https://github.com/konflux-ci/pulp-tool/compare/v1.0.0...HEAD
