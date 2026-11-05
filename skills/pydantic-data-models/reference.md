# Pydantic migration scan (pulp-tool)

Living backlog from repo scans. **Done** items were converted in the same effort that added this file; use **Backlog** when picking up follow-up PRs.

## Done (KonfluxBaseModel)

| Former shape | New location | Notes |
|--------------|--------------|--------|
| `@dataclass` `DistributionAuthSettings` | `pulp_tool/models/distribution_auth.py` | `from_config()`, `with_pulp_api_base_url_from_configs()` |
| `@dataclass` `PullRunResult` | `pulp_tool/models/pull_workflow.py` | `arbitrary_types_allowed` for clients |
| `@dataclass` `PullDestinationSetup` | `pulp_tool/models/pull_workflow.py` | Same |
| `@dataclass` `SideTagUploadResult` | `pulp_tool/models/pull_workflow.py` | Uses `SideTagRpmTransfer` list |
| `@dataclass` `ExistingDistributionInfo` | `pulp_tool/models/repository.py` | Idempotent distribution recovery |
| `@dataclass` `ArtifactPullRunOptions` | `pulp_tool/models/artifact_pull_run.py` | (prior PR) |

## Backlog — good candidates

| Area | Current | Suggestion |
|------|---------|------------|
| `pulp_tool/upload/batch_json.py` | `UploadPlan` already Pydantic | Extend with methods for plan steps instead of free functions where bundles repeat |
| `pulp_tool/cli/runner_helpers.py` | `resolve_artifact_location_for_pull(...)` many kwargs | `PullArtifactLocationRequest` model with `.resolve()` |
| `pulp_tool/models/artifacts.py` | `ArtifactData` | Already Pydantic; ensure pull/upload paths stop passing parallel dict slices |
| `pulp_tool/upload/pipeline.py` | Large kwargs / local dicts | Stage context model (paths, refs, counts) if a refactor touches pipeline |
| `SearchByResultsJson` | Plain class wrapping dict | Optional: subclass/wrap `PulpResultsDocument` slice model for removal helpers |

## Backlog — keep as-is (for now)

| Area | Why |
|------|-----|
| `RepositoryApiOps` (`repository_manager.py`) | Behavior object bound to `PulpClient`, not a data transfer bundle |
| `pulp_tool/models/pulp_api.py` wire DTOs | Already Pydantic (`PulpBaseModel`) |
| `PullRunnerError` | Exception, not structured data |
| Small 2-tuple returns | Skill says skip |

## How to re-scan

```bash
rg '@dataclass|NamedTuple|TypedDict' pulp_tool --glob '*.py'
rg 'dict\[str, Any\]' pulp_tool --glob '*.py' | head
```

After each conversion: `tests/models/test_*.py`, `make lint`, `make test`, PR diff coverage.
