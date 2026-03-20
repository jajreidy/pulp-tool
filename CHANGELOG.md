# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `upload --results-json`: Upload artifacts from pulp_results.json; files resolved from JSON directory or --files-base-path; --build-id and --namespace optional (extracted from artifact labels)
- DistributionClient username/password (Basic Auth) support; use `username` and `password` in config as alternative to cert/key for pull downloads
- `pull --distribution-config`: Path to config file for distribution auth (cert/key or username/password); overrides --transfer-dest/--config for auth when set
- Skip artifacts repository and distribution when `--artifact-results` is a local folder path (no comma); Konflux mode (url_path,digest_path) still creates artifacts repo
- `upload --signed-by`: Add signed_by pulp_label to RPMs only; use separate rpms-signed repo (logs/SBOMs never signed)
- `search-by` command: search RPM content in Pulp by checksum, filename, and/or signed_by; filter results.json by removing found artifacts (--results-json, --output-results); supports --filename/--filenames, --checksum/--checksums, --signed-by, --keep-files; NVR-based queries with incremental API call reduction; --keep-files keeps logs and sboms in output-results (default: only RPM artifacts)
- `codecov.yml` configuration file with `unit-tests` flag and carryforward enabled
- packages.redhat.com configuration section in README with OAuth2 setup
- Username/password (Basic Auth) support for packages.redhat.com

### Fixed
- Change package url format from hardcoding "l" in path to using first letter of rpm name to fit Pulp standard
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
