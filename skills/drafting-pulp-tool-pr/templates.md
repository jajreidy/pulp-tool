# PR and commit templates (pulp-tool)

## PR body (mandatory structure)

Reproduce `.github/PULL_REQUEST_TEMPLATE.md` exactly:

```markdown
## Summary

<!-- Replace with what/why in complete sentences -->

## How to test

<!-- Concrete commands: make lint, make test, pre-commit run --all-files, etc. -->

## Checklist

- [ ] `make test` and `make test-diff-coverage` (after `git fetch origin`) pass locally
- [ ] `pre-commit run --all-files` passes
- [ ] If this changes `upload`, global CLI flags, SBOM/artifact handling, or the container image: [CLAUDE.md](CLAUDE.md) Konflux sections and linked Tekton YAMLs were considered

## Notes for reviewers

<!-- Optional: risks, follow-ups, design choices -->
```

Rules:

- Keep all three `- [ ]` checklist lines **unchanged**.
- Do not rename sections or substitute a different outline unless the user explicitly overrides.
- Write Summary / How to test / Notes in complete sentences.

## Local file: `pr-description.md`

On confirmation, write **only the PR body** (sections above) to `pr-description.md` at the repo root. The file is gitignored — a local paste-ready draft, not committed.

- Include `## Summary`, `## How to test`, `## Checklist`, and `## Notes for reviewers` when there is reviewer context.
- Use concrete commands under How to test (e.g. `make lint`, `make test`, `pre-commit run --all-files`, `make test-diff-coverage`).
- Do **not** put the commit message or CHANGELOG entry in this file; those stay in chat / `CHANGELOG.md`.
- Overwrite the file on each PR-drafting turn.

## Suggested commit message

```text
<type(scope): short description>

Assisted-By: <agent or product — e.g. Cursor, Composer, Claude>
Signed-off-by: <human author — contributor GitHub/git identity>
```

- **Assisted-By:** The agent or product used, not a person's name unless specified.
- **Signed-off-by:** The human commit owner. Do not invent emails; use a clear placeholder and tell the author to replace it.

## CHANGELOG.md

**When:** Only in the PR-drafting turn (not each debug iteration), unless the user asks earlier.

**Where:** `## [Unreleased]` at top of `CHANGELOG.md`.

**Format:** [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) subsections as needed: Added, Changed, Deprecated, Removed, Fixed, Security. One line per change.

**Skip** for trivial typo-only or internal refactors with no user-visible effect.

Example:

```markdown
### Added
- New --dry-run option for upload command

### Fixed
- Handle missing artifact metadata in transfer without crashing
```
