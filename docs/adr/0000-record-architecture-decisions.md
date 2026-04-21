# Record architecture decisions

We use **Architecture Decision Records** (ADRs) to capture significant technical choices. Format follows [Michael Nygard’s template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

## When to add an ADR

- A decision affects multiple modules, public CLI/API behavior, or Konflux downstream contracts
- The trade-offs are non-obvious and worth preserving for future maintainers

## File naming

`docs/adr/NNNN-title-with-dashes.md` where `NNNN` is a monotonic index (e.g. `0001-use-httpx-for-pulp-client.md`).

## Template

```markdown
# N. Title (active / superseded)

Date: YYYY-MM-DD

## Status

Proposed | Accepted | Superseded by ADR M | Deprecated

## Context

What is the issue we are seeing?

## Decision

What did we decide?

## Consequences

What becomes easier or harder?
```

See also [CLAUDE.md](../../CLAUDE.md) for Konflux-specific integration notes (Tekton tasks, paths, flags).
