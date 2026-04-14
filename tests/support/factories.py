"""Reusable mock HTTP / response builders for tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import httpx


def make_rpm_list_response(results: list) -> httpx.Response:
    """Build a mock httpx.Response for RPM packages list API."""
    response = Mock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {
        "count": len(results),
        "next": None,
        "previous": None,
        "results": results,
    }
    response.raise_for_status = Mock()
    return response


def mock_httpx_response_json(status_code: int, payload: dict[str, Any]) -> Mock:
    """Minimal httpx.Response mock with .json() and status fields."""
    response = Mock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = payload
    response.text = ""
    response.headers = httpx.Headers({"content-type": "application/json"})
    return response
