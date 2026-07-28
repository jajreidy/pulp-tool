## Summary

<!-- What does this PR change and why? (This template is the default for new PRs in GitHub.) -->

## How to test

<!-- e.g. make pre-commit-ci, make lint, make test, git fetch origin && make test-diff-coverage -->

## Checklist

- [ ] `make test` and `make test-diff-coverage` (after `git fetch origin`) pass locally
- [ ] `pre-commit run --all-files` passes (commit stage); pre-push stage passes for PR checks (`make pre-commit-ci` runs both)
- [ ] If this changes `upload`, global CLI flags, SBOM/artifact handling, or the container image: [CLAUDE.md](CLAUDE.md) Konflux sections and linked Tekton YAMLs were considered

## Notes for reviewers

<!-- Risks, follow-ups, or design choices (optional) -->
