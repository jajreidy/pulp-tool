# 2. Use httpx for HTTP to the Pulp API

Date: 2026-04-21

## Status

Accepted

## Context

The client must speak to Pulp’s REST API over HTTPS with timeouts, optional HTTP/2, and good error surfacing for CLI and library use.

## Decision

Use **[httpx](https://www.python-httpx.org/)** as the HTTP client (sync and async), with **[pydantic](https://docs.pydantic.dev/)** for request/response models where applicable.

## Consequences

- One stack for Tekton/long-running and interactive use; connection limits and retries are centralized (see `pulp_tool/utils/session.py`).
