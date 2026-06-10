# Agent skills (pulp-tool)

Portable [Agent Skills](https://agentskills.io/specification) for this repository. Each skill is a directory with a `SKILL.md` (YAML frontmatter + instructions). Optional supporting files live alongside it.

## Layout

```
skills/
  README.md
  verification-scenarios.md
  <skill-name>/
    SKILL.md
    …optional templates, reference.md, scripts/
```

## Tool discovery

| Path | Tools |
|------|--------|
| **`skills/`** (this directory) | Canonical location — any agent or human workflow |
| [`.cursor/skills/`](../.cursor/skills/) | Symlink → `skills/` (Cursor) |
| [`.agents/skills/`](../.agents/skills/) | Symlink → `skills/` (Codex, Claude Code, other agentskills.io clients) |

Point your agent at `skills/<name>/SKILL.md` or rely on auto-discovery via the symlinks above.

## Skills

| Skill | Load when |
|-------|-----------|
| [changing-pulp-upload](changing-pulp-upload/SKILL.md) | Changing `upload`, CLI flags, SBOM, artifacts, Tekton paths that *run* pulp-tool |
| [changing-pulp-container](changing-pulp-container/SKILL.md) | Changing `Dockerfile`, `.tekton/` PipelineRuns, or Konflux image build ([reference.md](changing-pulp-container/reference.md) — remote pipeline tasks) |
| [drafting-pulp-tool-pr](drafting-pulp-tool-pr/SKILL.md) | User confirms paste-ready PR body, commit message, or CHANGELOG |
| [fixing-diff-cover-failures](fixing-diff-cover-failures/SKILL.md) | `make test-diff-coverage` fails or CI diff coverage below 100% |
| [troubleshooting-pulp-tool-ci](troubleshooting-pulp-tool-ci/SKILL.md) | Pre-commit or lint loops failing; local vs CI mismatch |

Index and lint quick-ref: [`.cursor/rules/llm-development-guidelines-deep.mdc`](../.cursor/rules/llm-development-guidelines-deep.mdc).

Verification notes (RED/GREEN scenarios): [verification-scenarios.md](verification-scenarios.md).

## Related docs

- [AGENTS.md](../AGENTS.md) — bootstrap and conventions
- [CLAUDE.md](../CLAUDE.md) — Konflux / upload contracts (source of truth for upload skill)
- [CONTRIBUTING.md](../CONTRIBUTING.md) — contributor and agent workflow
