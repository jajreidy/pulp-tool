---
name: pydantic-data-models
description: >-
  Use when adding or refactoring structured data: multi-field CLI options, service
  contexts, batch upload plans, JSON document slices, or any bundle passed across
  CLI → services → utils. Prefer Pydantic models over dataclasses, TypedDict, or
  ad hoc dicts.
---

# Pydantic data models (pulp-tool)

## When to load this skill

- A change introduces **many related fields** (roughly **~5+** attributes, nested structures, or optional groups that vary by command).
- Data crosses **layer boundaries** (CLI → runner → service → Pulp client) and would otherwise be a growing parameter list or untyped `dict`.
- You are tempted to add **`@dataclass`**, **`NamedTuple`**, or **`dict[str, Any]`** for request/response or workflow state.
- You add **resolution / validation / mapping** helpers that take the same field bundle repeatedly (side-tag settings, auth paths, filter lists, etc.).

**Skip** for one-off two-tuple returns, tiny internal structs, or Pulp wire DTOs already covered in `pulp_tool/models/pulp_api.py`.

## Default pattern

1. **Base class:** subclass **`KonfluxBaseModel`** from `pulp_tool/models/base.py` (same as `PullContext`, `UploadContext`, `SearchByRequest`, `ArtifactPullRunOptions`).
2. **Location:** `pulp_tool/models/` — group by domain (`context.py`, `cli.py`, `artifact_pull_run.py`, `pulp_results/`, …). Do not define production models only under `cli/` unless they are truly Click-only (then still prefer `models/` for testability).
3. **Behavior on the model:** put **normalization, validation, and mapping** on methods or `@field_validator` / `@model_validator`, not scattered free functions. Examples in-repo:
   - `ArtifactPullRunOptions.resolve_side_tag()`, `to_pull_context()`
   - `SearchByRequest` validators and XOR rules
   - `PulpResultsDocument` mutation helpers
   - `FoundPackages.from_packages()`
4. **Tests:** add or extend **`tests/models/test_*.py`** for validators and methods (not only integration CLI tests).
5. **Exports:** export from the subpackage `__init__.py` or `pulp_tool/models/__init__.py` when other packages need the type.

## Anti-patterns

| Avoid | Prefer |
|-------|--------|
| Frozen `@dataclass` option bags in `cli/` | `KonfluxBaseModel` in `pulp_tool/models/` |
| Passing the same 10 kwargs through 3 functions | One model + `.to_*()` / `.resolve_*()` methods |
| Mutating raw JSON dicts across upload/pull flows | `PulpResultsDocument` or dedicated entry models |
| Validating only at the HTTP edge | Validators on the model at construction / assignment |

## Related code map

- **Architecture:** [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) — Models layer, invariants
- **Contexts:** `pulp_tool/models/context.py` (`PullContext`, `UploadContext`, …)
- **CLI bundles:** `pulp_tool/models/artifact_pull_run.py`, `pulp_tool/models/cli.py`
- **Canonical JSON:** `pulp_tool/models/pulp_results/`

After adding a model, run **`make lint`** and **`make test`**; new methods need coverage for **`make test-diff-coverage`**.

Migration backlog and completed conversions: [reference.md](reference.md).
