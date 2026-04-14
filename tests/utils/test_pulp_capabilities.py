"""Tests for pulp_capabilities (version gate)."""

from unittest.mock import MagicMock

import httpx
import pytest

from pulp_tool.utils import pulp_capabilities as pulp_capabilities_mod
from pulp_tool.utils.pulp_capabilities import ensure_pulp_capabilities, versions_from_status_payload


def test_versions_from_status_payload() -> None:
    data = {
        "versions": [{"component": "core", "version": "3.50.1"}, {"component": "rpm", "version": "3.25.0"}],
        "database_connection": {"connected": True},
    }
    assert versions_from_status_payload(data)["core"] == "3.50.1"
    assert versions_from_status_payload(data)["rpm"] == "3.25.0"


def test_ensure_pulp_capabilities_success() -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    payload = {
        "versions": [{"component": "core", "version": "3.50.0"}, {"component": "rpm", "version": "3.25.0"}],
        "database_connection": {"connected": True},
        "online_workers": [],
        "online_content_apps": [],
    }
    client.session.get.return_value = httpx.Response(200, json=payload)
    ensure_pulp_capabilities(client, operation="test")


def test_ensure_pulp_capabilities_http_error() -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    client.session.get.return_value = httpx.Response(503, text="no")
    with pytest.raises(RuntimeError, match="status returned 503"):
        ensure_pulp_capabilities(client, operation="test")


def test_ensure_pulp_capabilities_core_too_old() -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    payload = {
        "versions": [{"component": "core", "version": "3.0.0"}],
        "database_connection": {"connected": True},
        "online_workers": [],
        "online_content_apps": [],
    }
    client.session.get.return_value = httpx.Response(200, json=payload)
    with pytest.raises(RuntimeError, match=r"Pulpcore version 3\.0\.0 is below minimum"):
        ensure_pulp_capabilities(client, operation="test")


def test_ensure_pulp_capabilities_rpm_too_old() -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    payload = {
        "versions": [{"component": "core", "version": "3.50.0"}, {"component": "rpm", "version": "3.0.0"}],
        "database_connection": {"connected": True},
        "online_workers": [],
        "online_content_apps": [],
    }
    client.session.get.return_value = httpx.Response(200, json=payload)
    with pytest.raises(RuntimeError, match="pulp_rpm version 3.0.0 is below minimum"):
        ensure_pulp_capabilities(client, operation="test")


def test_version_tuple_breaks_on_non_numeric_segment() -> None:
    """Leading segments are parsed; trailing non-numeric segment stops further parsing (``break``)."""
    assert pulp_capabilities_mod._version_tuple("3.foo.9") == (3,)


def test_ensure_pulp_capabilities_invalid_status_payload() -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    client.session.get.return_value = httpx.Response(200, json={"versions": "not-a-list"})
    with pytest.raises(RuntimeError, match="Cannot parse Pulp status response"):
        ensure_pulp_capabilities(client, operation="test")


def test_ensure_pulp_capabilities_no_core_version_skips_check(caplog: pytest.LogCaptureFixture) -> None:
    client = MagicMock()
    client._url.return_value = "https://pulp/api/v3/status/"
    client.timeout = 30
    client.request_params = {}
    payload = {
        "versions": [{"component": "rpm", "version": "3.25.0"}],
        "database_connection": {"connected": True},
        "online_workers": [],
        "online_content_apps": [],
    }
    client.session.get.return_value = httpx.Response(200, json=payload)
    with caplog.at_level("WARNING"):
        ensure_pulp_capabilities(client, operation="test")
    assert "no core version" in caplog.text
