"""Tests for Pulp content HTTP fetch allowlist."""

import pytest

from pulp_tool.utils.fetch_url_policy import (
    _default_port,
    _origin_key,
    assert_pulp_content_fetch_url,
)

PULP_API = "https://pulp.example.com"
GOOD = f"{PULP_API}/api/pulp-content/ns/build/artifacts/pulp_results.json"


def test_allows_matching_host_and_pulp_content_path() -> None:
    assert_pulp_content_fetch_url(GOOD, PULP_API)


def test_rejects_wrong_host() -> None:
    with pytest.raises(ValueError, match="host other than"):
        assert_pulp_content_fetch_url("https://evil.example/api/pulp-content/x/y", PULP_API)


def test_rejects_non_pulp_content_path() -> None:
    with pytest.raises(ValueError, match="/api/pulp-content/"):
        assert_pulp_content_fetch_url(f"{PULP_API}/artifacts.json", PULP_API)


def test_rejects_metadata_service_style_url() -> None:
    with pytest.raises(ValueError, match="/api/pulp-content/"):
        assert_pulp_content_fetch_url(
            "http://169.254.169.254/latest/meta-data/",
            PULP_API,
        )


def test_default_port_helpers() -> None:
    assert _default_port("https") == 443
    assert _default_port("http") == 80
    assert _default_port("ftp") == 0


def test_origin_key_rejects_bad_scheme_and_missing_host() -> None:
    with pytest.raises(ValueError, match="scheme"):
        _origin_key("ftp://pulp.example/api/pulp-content/x")
    with pytest.raises(ValueError, match="host"):
        _origin_key("https:///api/pulp-content/x")


def test_rejects_empty_pulp_api_base_url() -> None:
    with pytest.raises(ValueError, match="pulp_api_base_url is required"):
        assert_pulp_content_fetch_url(GOOD, "  ")


def test_rejects_non_http_fetch_scheme() -> None:
    with pytest.raises(ValueError, match="non-HTTP"):
        assert_pulp_content_fetch_url("file:///api/pulp-content/x/y", PULP_API)


def test_rejects_invalid_base_url_for_origin_compare() -> None:
    with pytest.raises(ValueError, match="scheme"):
        assert_pulp_content_fetch_url(GOOD, "ftp://pulp.example.com")
