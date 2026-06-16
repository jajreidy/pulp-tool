---
name: drafting-pulp-tool-pr
description: >-
  Use when the user confirms paste-ready PR materials, or asks for a PR title/body,
  commit message, or CHANGELOG entry after substantive pulp-tool repo changes.
disable-model-invocation: true
---

# Drafting pulp-tool PR materials

## Overview

Provide paste-ready GitHub PR text **only after the user confirms**. Do not dump PR boilerplate on every turn while debugging.

## When to ask first

After substantive code or repo changes that could ship as a PR, end with a short question (e.g. *"Ready for a paste-ready GitHub PR description?"*).

**Skip** when: no repo edits (Q&A only), user wants a small fix with no PR, or user already asked for PR materials.

## Workflow (on confirmation — same turn)

1. Draft PR body using [templates.md](templates.md) — skeleton from `.github/PULL_REQUEST_TEMPLATE.md`.
2. **Write the PR body to `pr-description.md`** at the repo root (gitignored). Use complete fences for any code blocks; do not wrap the file in an outer ` ```markdown ` fence.
3. Provide suggested commit message (`Assisted-By:` + `Signed-off-by:` per `.github/commit-message-template.txt`).
4. Edit `CHANGELOG.md` under `[Unreleased]` when the change is user-facing or notable (see templates.md).
5. Point the user to `pr-description.md` for copy/paste or `gh pr create --body-file pr-description.md`.

Chat may prefix with `### PR description (GitHub)`; the file content starts with `## Summary`.

## Red flags — do not proceed

- Pasting full PR body without user confirmation
- Renaming template sections or omitting checklist items
- Editing `CHANGELOG.md` on every debug iteration
- Inventing a real `Signed-off-by` email

## Common rationalizations

| Excuse | Reality |
|--------|---------|
| "They'll want a PR soon" | Ask once; unsolicited PR text adds noise |
| "Custom outline is clearer" | Use `.github/PULL_REQUEST_TEMPLATE.md` unless user overrides |
| "Changelog can wait" | Notable changes get `[Unreleased]` entries in the PR-drafting turn |

## Templates

Verbatim PR skeleton, commit shape, and CHANGELOG rules: [templates.md](templates.md).
