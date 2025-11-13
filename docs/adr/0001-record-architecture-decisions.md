# ADR-0001: Record Architecture Decisions

## Status

Accepted

## Context

We need to record the architectural decisions made on this project to help future developers understand why certain decisions were made.

## Decision

We will use Architecture Decision Records, as described by Michael Nygard in this article: http://thinkrelevance.com/blog/2011/11/15/documenting-architecture-decisions

## Consequences

- Architecture decisions will be documented in markdown files in `docs/adr/`
- Each ADR will be numbered sequentially and monotonically
- ADRs will be named using the format: `NNNN-title-with-dashes.md`
- ADRs will include:
  - Status (Proposed, Accepted, Rejected, Deprecated, Superseded)
  - Context
  - Decision
  - Consequences

## Template

New ADRs should follow this template:

```markdown
# ADR-NNNN: Title

## Status

[Proposed | Accepted | Rejected | Deprecated | Superseded]

## Context

[Describe the issue motivating this decision]

## Decision

[Describe the change that we're proposing or have agreed to implement]

## Consequences

[Describe the consequences of this decision]
```
