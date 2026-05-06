"""Tests for pulp_tool.utils.rpm_pulp_search."""

from unittest.mock import MagicMock
import httpx
from pulp_tool.models.pulp_api import RpmPackageResponse
from pulp_tool.utils.rpm_pulp_search import parse_rpm_response, search_rpms_by_filenames_for_overwrite


def test_parse_rpm_response_skips_invalid_items() -> None:
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {"pulp_href": "/ok/", "name": "p", "version": "1", "release": "1", "arch": "x86_64", "sha256": "a" * 64},
            {"broken": True},
        ]
    }
    pkgs = parse_rpm_response(response)
    assert len(pkgs) == 1
    assert pkgs[0].pulp_href == "/ok/"


def test_search_rpms_by_filenames_for_overwrite_empty() -> None:
    client = MagicMock()
    assert search_rpms_by_filenames_for_overwrite(client, [], None) == []
    assert search_rpms_by_filenames_for_overwrite(client, [], "sig") == []
    client.get_rpm_by_filenames.assert_not_called()


def test_search_rpms_by_filenames_for_overwrite_unsigned_branch() -> None:
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {"pulp_href": "/u/", "name": "u", "version": "1", "release": "1", "arch": "x86_64", "sha256": "c" * 64}
        ]
    }
    client.get_rpm_by_filenames.return_value = response
    out = search_rpms_by_filenames_for_overwrite(client, ["u-1-1.x86_64.rpm"], None)
    assert len(out) == 1
    client.get_rpm_by_filenames.assert_called_once_with(["u-1-1.x86_64.rpm"])
    client.get_rpm_by_filenames_and_signed_by.assert_not_called()


def test_search_rpms_by_filenames_for_overwrite_signed_branch() -> None:
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "results": [
            {"pulp_href": "/p/", "name": "p", "version": "1", "release": "1", "arch": "x86_64", "sha256": "b" * 64}
        ]
    }
    client.get_rpm_by_filenames_and_signed_by.return_value = response
    out = search_rpms_by_filenames_for_overwrite(client, ["p-1-1.x86_64.rpm"], "my-key")
    assert len(out) == 1
    assert isinstance(out[0], RpmPackageResponse)
    client.get_rpm_by_filenames_and_signed_by.assert_called_once_with(["p-1-1.x86_64.rpm"], "my-key")


def test_search_rpms_by_filenames_blank_signed_by_uses_unsigned() -> None:
    client = MagicMock()
    response = MagicMock(spec=httpx.Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {"results": []}
    client.get_rpm_by_filenames.return_value = response
    search_rpms_by_filenames_for_overwrite(client, ["d-1-1.x86_64.rpm"], "   ")
    client.get_rpm_by_filenames.assert_called_once()
    client.get_rpm_by_filenames_and_signed_by.assert_not_called()
