"""Shared helpers for pulp_client mixins (synthetic httpx responses, RPM result dedupe)."""

from typing import Any, Dict, List

import httpx

# Placeholder request for synthetic responses (httpx.raise_for_status requires it)
EMPTY_RESPONSE_REQUEST = httpx.Request("GET", "https://placeholder/")


def dedupe_results_by_pulp_href(results: List[Any]) -> List[Any]:
    """Deduplicate RPM result dicts by pulp_href. Later entries overwrite earlier."""
    seen: Dict[str, Any] = {}
    for r in results:
        href = r.get("pulp_href") if isinstance(r, dict) else None
        if href:
            seen[href] = r
    return list(seen.values())
