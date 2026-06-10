# Agent context (pulp-tool)

Canonical **README for AI agents** in this repo. Human-oriented setup and user docs live in [README.md](README.md).

## Bootstrap (read first; fewer wasted context round-trips)

Do this **in order** when starting work in a **new thread** or with a **new** model:

1. **This file** — commands, conventions, and links below. In Cursor, attach **`@AGENTS.md`** at session start when practical.
2. **`docs/ARCHITECTURE.md`** — code map, mermaid flow, invariants, glossary (open if you will touch more than one area or are unfamiliar with the layout).
3. **changing-pulp-upload** + **`CLAUDE.md`** — if changing `upload`, global CLI flags, SBOM/artifact results, or Tekton tasks that *run* pulp-tool ([`skills/changing-pulp-upload/SKILL.md`](skills/changing-pulp-upload/SKILL.md)). **changing-pulp-container** — if changing `Dockerfile` or `.tekton/` image build PipelineRuns ([`skills/changing-pulp-container/SKILL.md`](skills/changing-pulp-container/SKILL.md)).
4. **On-demand skills** — see [`.cursor/rules/llm-development-guidelines-deep.mdc`](.cursor/rules/llm-development-guidelines-deep.mdc) (skill index) and [`skills/README.md`](skills/README.md): PR drafting, diff-cover failures, lint/CI troubleshooting.

Do **not** read all of [CONTRIBUTING.md](CONTRIBUTING.md) up front unless you are changing process, dependencies, or release workflow.

**Konflux (Tekton) contracts** (same as item 3): [CLAUDE.md](CLAUDE.md).

---

## Build, test, lint

Copy-paste **`make` / pre-commit** flow, **`make lock`** for dependencies, and PR reminders: **[README.md § Development](README.md#development)**.

---

## Key conventions (do not skip)

1. **GitHub merge:** every changed line in a PR needs executing test coverage — run `make test-diff-coverage` (same as CI diff-cover), not only `make test`.
2. **Konflux:** never change `upload` / artifact paths / Tekton assumptions without the **changing-pulp-upload** skill, [CLAUDE.md](CLAUDE.md), and the linked task YAMLs there.
3. **Documentation:** Keep **relevant** in-repo docs and core `.md` files in sync with your change in the **same PR** (do not leave stale references):
   - **CLI** (`pulp_tool/cli/`): [docs/cli-reference.md](docs/cli-reference.md) — flags, behavior, examples; should match `pulp-tool <command> --help`.
   - **Architecture** (layout, data flow, boundaries, integrations): [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
   - **Konflux / Tekton contracts** (upload paths, flags, container): [CLAUDE.md](CLAUDE.md) — together with item 2 above.
   - **User-facing** install, config, overview: [README.md](README.md); deeper CLI detail stays in `docs/cli-reference.md`.
   - **Contributor workflow** (checks, deps, process): [CONTRIBUTING.md](CONTRIBUTING.md).
   - **Tests / layout:** [tests/README.md](tests/README.md) when test organization or conventions change.
   - **ADRs:** [docs/adr/](docs/adr/) when you record a new architectural decision (see [0000-record-architecture-decisions.md](docs/adr/0000-record-architecture-decisions.md)).
   - **Agent skills** (`skills/`): [skills/README.md](skills/README.md) — index and cross-tool discovery; update when adding or renaming skills.
   - **Cross-links:** If you move or rename files, update links in other `.md` and Cursor rules that pointed at the old path.
4. **Types:** prefer hints; `mypy` covers `pulp_tool/` (see `pyproject.toml` overrides).
5. **Changelog / PR text:** always-on [`.cursor/rules/llm-development-guidelines.mdc`](.cursor/rules/llm-development-guidelines.mdc); on confirmation load **drafting-pulp-tool-pr** skill ([`skills/drafting-pulp-tool-pr/`](skills/drafting-pulp-tool-pr/)). Skill index: [`llm-development-guidelines-deep.mdc`](.cursor/rules/llm-development-guidelines-deep.mdc). Ask before drafting PR boilerplate; update `CHANGELOG.md` when preparing the PR, not every debug iteration.

---

## PR and commit

- PR body: use [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) **as-is**—same section headings and checklist items; fill in content under each section only.
- AI-assisted commits: [.github/commit-message-template.txt](.github/commit-message-template.txt), `Assisted-By:` + `Signed-off-by:` — details in [CONTRIBUTING.md](CONTRIBUTING.md#ai-assisted-commits).
- Essentials (always-on): [`.cursor/rules/llm-development-guidelines.mdc`](.cursor/rules/llm-development-guidelines.mdc); on-demand skills: [`skills/`](skills/) ([`skills/README.md`](skills/README.md), [skill index](.cursor/rules/llm-development-guidelines-deep.mdc)); [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Where things live

High-level **structure, diagrams, invariants, and glossary:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Optional

- Full CLI flags: [docs/cli-reference.md](docs/cli-reference.md).
- Test patterns: [tests/README.md](tests/README.md).
- Agent skills (portable): [skills/README.md](skills/README.md) — Cursor/Claude Code/Codex discover via [`.cursor/skills/`](.cursor/skills/) and [`.agents/skills/`](.agents/skills/) symlinks.
