# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- E2e large RPM upload: `pre-test.py` builds a **> 300 MiB** RPM (`--large-rpm-size-mb`, default 301 MiB incompressible payload); e2e uploads to Pulp and verifies via `search-by --checksums`
- `UPLOAD_CONTENT_TIMEOUT` (30 minutes) for multipart RPM and file uploads

### Fixed

- Large RPM uploads no longer fail with `httpx.WriteTimeout` at the previous 120-second write limit (e.g. large debuginfo packages in sign-and-verify pipelines)

## [1.1.0] - 2026-08-25

### Added

- `i686` architecture support in `SUPPORTED_ARCHITECTURES`, RPM path detection, upload orchestration, and content queries
- `upload --overwrite`: remove matching RPM content units in the target repo before upload (RPM-only; respects `signed_by` when set)
- E2e reusable test image ([`Dockerfile.e2e`](Dockerfile.e2e)) and concurrent run isolation via `E2E_RUN_ID` / [`e2e/names.py`](e2e/names.py)
- Release automation for maintainers: local [Release Please](https://github.com/googleapis/release-please) ([`scripts/release-please.sh`](scripts/release-please.sh), `make release-please`, `make release-publish`), [`.github/workflows/release.yml`](.github/workflows/release.yml), and [`docs/releasing.md`](docs/releasing.md)
- `make lock-check` (`uv lock --check`) in CI
- `drafting-pulp-tool-pr` agent skill for paste-ready PR drafts
- Expanded lint toolchain: Ruff (replaces Black and Flake8), yamllint, ShellCheck, hadolint, codespell, and Checkton in pre-commit and/or CI

### Changed

- Container image on UBI 10 minimal with Python 3.12; OpenShift preflight labels, `/licenses/LICENSE`, and non-root `USER 1001`
- GitHub Actions unit/lint and security workflows on Python 3.12; container image build remains Konflux Tekton only
- Agent documentation split: on-demand workflows under `skills/`; `CLAUDE.md` scoped to Konflux contracts; architecture narrative in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Documentation audit across README, `docs/cli-reference.md`, `CONTRIBUTING.md`, `SECURITY.md`, and related files
- `setup.py` is a thin shim; dependency ranges and package metadata live in `pyproject.toml`
- Container `Dockerfile` installs runtime deps from `uv.lock` (`uv export --frozen --no-dev`)
- Removed `ensure_pulp_capabilities` pre-flight status/version checks from upload, search, and pull flows
- Mypy `[[tool.mypy.overrides]]` for `tests.*` to tolerate mocks and intentional invalid inputs

### Removed

- Black, Flake8, and `.flake8`; deprecated `safety check` and non-blocking bandit steps from `security-scan.yml`

### Fixed

- HTTP response validation and clearer content-search parse errors across more Pulp client paths
- Checkton CI on pull requests only; `scripts/run-checkton.sh` fetches `origin/main` and sets diff base/head
- `make audit` / pre-commit pip-audit installs from `uv.lock` so setuptools-scm does not rewrite `_version.py` during commits
- Async repository setup tests stable under concurrent `asyncio.gather` (mock by repo name suffix, not call order)
- Idempotent distribution setup after gateway 504 retries and Pulp name/base_path uniqueness errors
- `signed_by` label normalization for uploads and Pulp queries (commas and parentheses)
- Konflux `--artifact-results` digest writes `sha256:<hex>` from the uploaded artifact
- Partial RPM upload accounting, missing `--sbom-path` errors, and SBOM classification in `--results-json`
- Konflux artifact type detection by file extension before substring matching
- Repository and package metadata URLs corrected to `konflux-ci/pulp-tool`
- E2e Tekton `task-init` 0.3 migration in `pulp-e2e-testing`
- Codecov flag name and `codecov.yml` configuration
- Container certification preflight failures (UBI base, license, labels, non-root user)

### Security

- Pin `pip>=26.2` in the `dev` extra for CVE-2026-13346 (doubly-encoded index URLs)

## [1.0.0] - 2026-08-25

### Added

- Initial release of pulp-tool: CLI commands `upload`, `upload-files`, `pull`, `search-by`, and `create-repository`
- `PulpClient`, `PulpHelper`, and `DistributionClient` for Pulp API interactions
- RPM, log, and SBOM file management; OAuth2 authentication with automatic token refresh
- `upload --target-arch-repo`: per-architecture RPM repos/distributions (`{namespace}/{arch}/Packages/...`); logs/SBOM/artifacts stay build-scoped
- `upload --signed-by`: `signed_by` pulp label on RPMs only; separate `rpms-signed` repo
- `upload --results-json`: upload artifacts listed in `pulp_results.json`
- `search-by`: search RPM content by checksum, filename, and/or `signed_by`; filter `pulp_results.json` output
- `pull --distribution-config` and DistributionClient username/password (Basic Auth) support
- `--artifact-results` folder mode for local `pulp_results.json` output
- packages.redhat.com configuration (OAuth2 and Basic Auth) documented in README
- `make test-diff-coverage` at 100% vs `origin/main` (PR merge gate)
- Konflux downstream documentation in `CLAUDE.md` and `.cursor/rules/konflux-ecosystem.mdc`
- `AGENTS.md`, `docs/ARCHITECTURE.md`, and portable agent skills under `skills/`
- `changing-pulp-container` agent skill; Hypothesis property tests; test tree mirroring `pulp_tool/`
- Pulp client package refactor (`pulp_tool/api/pulp_client/`); `RepositoryApiOps`; upload gather split (`upload_collect`, `upload_common`)
- `create_file_content_and_wait` helper; comprehensive type annotations; pre-commit hooks; `CONTRIBUTING.md`; `Makefile`; `Dockerfile`
- `codecov.yml`; developer scripts; `.editorconfig`; test suite with high coverage

### Changed

- Renamed `transfer` command to `pull` (`cli/pull.py`, `pulp_tool/pull/`, `PullContext`, `PullService`)
- `pull`: re-upload to destination repos only when `--transfer-dest` is set; download URLs use per-artifact `url` fields only
- Upload orchestration uses `RpmUploadResult` and typed gather models; incremental `pulp_results.json` population
- `pulp_results.json` `distributions` keys for per-arch RPM bases are `rpm_<arch>` (e.g. `rpm_x86_64`)
- Upload infers log/SBOM repo needs before setup; optional skip when no uploads expected
- `RepositoryManager.get_repository_methods` returns `RepositoryApiOps` instead of a dict of callables
- Consolidated dependencies into `pyproject.toml`; raised minimum versions for runtime and dev tooling
- Upload progress messages at INFO; authentication failures exit with code 1 again

### Removed

- `pulp_tool.api.task_manager`; unused status/capability helpers and trimmed test-only utilities
- `requirements.in` and `requirements.txt` (superseded by `uv.lock`)
- `transfer` command (use `pull`); docs GitHub workflow; Makefile `docs` targets
- Sphinx and sphinx-rtd-theme from optional `dev` extras

### Fixed

- Path traversal via `--results-json` artifact keys and pull log `arch` labels
- Invalid `arch` in Pulp file content paths rejected
- `@cached_get` cache keys include method name and full arguments
- Synchronous `_chunked_get` raises `RuntimeError` when an event loop is already running
- Content search empty/non-JSON bodies surface HTTP status, URL, and body preview
- `PulpClient` fails fast when mTLS cert/key paths are missing
- Generic `/api/v3/content/` bare JSON array responses handled without `TypeError`
- Results JSON RPM URLs with `--signed-by` use the `rpms-signed` distribution base
- RPM distribution `Packages/<letter>/` uses lowercase first character of RPM basename
- Clear error when no auth credentials are provided
- Konflux `--artifact-results` digest, partial RPM upload accounting, and parallel upload locking
- CI security scan runs `pip-audit` before optional `safety`/`bandit` installs

### Security

- `pip-audit` in optional `dev` dependencies, `make audit`, and `security-scan.yml`
- Path traversal fixes for `--results-json` and pull log architecture labels

[Unreleased]: https://github.com/konflux-ci/pulp-tool/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/konflux-ci/pulp-tool/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/konflux-ci/pulp-tool/releases/tag/v1.0.0
