"""Tests for pulp_tool.utils.rpm_pulp_search."""

from unittest.mock import MagicMock

import httpx

from pulp_tool.models.pulp_api import RpmPackageResponse
from pulp_tool.utils.rpm_pulp_search import (
    parse_rpm_response,
    search_rpms_by_checksums_for_overwrite,
)


def test_parse_rpm_response_skips_invalid_items():
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {
                "pulp_href": "/ok/",
                "name": "p",
                "version": "1",
                "release": "1",
                "arch": "x86_64",
                "sha256": "a" * 64,
            },
            {"broken": True},
        ]
    }
    pkgs = parse_rpm_response(response)
    assert len(pkgs) == 1
    assert pkgs[0].pulp_href == "/ok/"


def test_search_rpms_by_checksums_for_overwrite_empty():
    client = MagicMock()
    assert search_rpms_by_checksums_for_overwrite(client, [], None) == []
    assert search_rpms_by_checksums_for_overwrite(client, [], "sig") == []
    client.get_rpm_by_pkgIDs.assert_not_called()


def test_search_rpms_by_checksums_for_overwrite_unsigned_branch():
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {
                "pulp_href": "/u/",
                "name": "u",
                "version": "1",
                "release": "1",
                "arch": "x86_64",
                "sha256": "c" * 64,
            }
        ]
    }
    client.get_rpm_by_pkgIDs.return_value = response
    out = search_rpms_by_checksums_for_overwrite(client, ["c" * 64], None)
    assert len(out) == 1
    client.get_rpm_by_pkgIDs.assert_called_once_with(["c" * 64])
    client.get_rpm_by_checksums_and_signed_by.assert_not_called()


def test_search_rpms_by_checksums_for_overwrite_signed_branch():
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {
                "pulp_href": "/p/",
                "name": "p",
                "version": "1",
                "release": "1",
                "arch": "x86_64",
                "sha256": "b" * 64,
            }
        ]
    }
    client.get_rpm_by_checksums_and_signed_by.return_value = response
    out = search_rpms_by_checksums_for_overwrite(client, ["b" * 64], "my-key")
    assert len(out) == 1
    assert isinstance(out[0], RpmPackageResponse)
    client.get_rpm_by_checksums_and_signed_by.assert_called_once_with(["b" * 64], "my-key")


def test_search_rpms_by_checksums_blank_signed_by_uses_unsigned():
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {"results": []}
    client.get_rpm_by_pkgIDs.return_value = response
    search_rpms_by_checksums_for_overwrite(client, ["d" * 64], "   ")
    client.get_rpm_by_pkgIDs.assert_called_once()
    client.get_rpm_by_checksums_and_signed_by.assert_not_called()
