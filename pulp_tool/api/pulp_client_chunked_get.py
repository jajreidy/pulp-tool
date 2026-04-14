"""
Chunked GET (large query param lists) for :class:`pulp_tool.api.pulp_client.PulpClient`.

Extracted to keep ``pulp_client.py`` smaller; behavior is unchanged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from typing import TYPE_CHECKING, Any, Dict, Optional

import httpx

from ..utils.constants import DEFAULT_CHUNK_SIZE

if TYPE_CHECKING:
    from .pulp_client import PulpClient  # pragma: no cover


async def chunked_get_async(
    client: "PulpClient",
    url: str,
    params: Optional[Dict[str, Any]] = None,
    chunk_param: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    **kwargs: Any,
) -> httpx.Response:
    """
    Perform a GET request with chunking for large parameter lists using async.

    See ``PulpClient._chunked_get_async`` docstring in historical revisions for full notes.
    """
    async_client = client._get_async_session()

    if not params or not chunk_param or chunk_param not in params:
        response = await async_client.get(url, params=params, **client._prepare_async_kwargs(**kwargs))
        client._check_response(response, "chunked GET")
        return response

    param_value = params[chunk_param]
    if not isinstance(param_value, str) or "," not in param_value:
        response = await async_client.get(url, params=params, **client._prepare_async_kwargs(**kwargs))
        client._check_response(response, "chunked GET")
        return response

    values = [v.strip() for v in param_value.split(",")]

    if len(values) <= chunk_size:
        response = await async_client.get(url, params=params, **client._prepare_async_kwargs(**kwargs))
        client._check_response(response, "chunked GET")
        return response

    chunks = [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]

    logging.debug(
        "Chunking parameter '%s' with %d values into %d chunks (async concurrent)",
        chunk_param,
        len(values),
        len(chunks),
    )

    if hasattr(client, "_metrics"):
        client._metrics.log_chunked_request(parallel=True)

    async def fetch_chunk(chunk: list, chunk_index: int) -> tuple:
        chunk_params = params.copy()
        chunk_params[chunk_param] = ",".join(chunk)

        try:
            response = await async_client.get(url, params=chunk_params, **client._prepare_async_kwargs(**kwargs))
            client._check_response(response, f"chunked request {chunk_index}")

            chunk_data = response.json()
            results = chunk_data.get("results", [])
            logging.debug("Completed chunk %d/%d with %d results", chunk_index, len(chunks), len(results))
            return response, results

        except Exception as e:
            logging.error("Failed to process chunk %d: %s", chunk_index, e)
            logging.error("Traceback: %s", traceback.format_exc())
            raise

    tasks = [fetch_chunk(chunk, i) for i, chunk in enumerate(chunks, 1)]
    results = await asyncio.gather(*tasks)

    all_results = []
    last_response = None
    for response, chunk_results in results:
        last_response = response
        all_results.extend(chunk_results)

    if last_response:
        aggregated_data = {"count": len(all_results), "results": all_results}
        last_response._content = json.dumps(aggregated_data).encode("utf-8")
        return last_response

    response = await async_client.get(url, params={chunk_param: ""}, **client._prepare_async_kwargs(**kwargs))
    client._check_response(response, "chunked GET (fallback)")
    return response


def chunked_get(
    client: "PulpClient",
    url: str,
    params: Optional[Dict[str, Any]] = None,
    chunk_param: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    **kwargs: Any,
) -> httpx.Response:
    """Synchronous wrapper for :func:`chunked_get_async`."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("_chunked_get called from async context. Use _chunked_get_async instead.")

    return client._run_async(chunked_get_async(client, url, params, chunk_param, chunk_size, **kwargs))


__all__ = ["chunked_get", "chunked_get_async"]
