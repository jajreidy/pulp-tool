"""Tests for pulp_capabilities helpers."""

from pulp_tool.utils.pulp_capabilities import versions_from_status_payload


def test_versions_from_status_payload() -> None:
    data = {
        "versions": [{"component": "core", "version": "3.50.1"}, {"component": "rpm", "version": "3.25.0"}],
        "database_connection": {"connected": True},
    }
    assert versions_from_status_payload(data)["core"] == "3.50.1"
    assert versions_from_status_payload(data)["rpm"] == "3.25.0"
