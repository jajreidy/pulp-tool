"""Tests for Pulp content HTTP fetch allowlist."""

import pytest

from pulp_tool.utils.fetch_url_policy import assert_pulp_content_fetch_url

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
