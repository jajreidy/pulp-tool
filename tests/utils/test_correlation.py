"""Tests for correlation ID resolution."""

import pytest

from pulp_tool.utils.correlation import CORRELATION_HEADER, ENV_CORRELATION, resolve_correlation_id


def test_resolve_config_wins_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CORRELATION, "from-env")
    cid = resolve_correlation_id(config_value="from-config", namespace="ns", build_id="b")
    assert cid == "from-config"


def test_resolve_env_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CORRELATION, "env-only")
    cid = resolve_correlation_id(config_value="", namespace="ns", build_id="b")
    assert cid == "env-only"


def test_resolve_namespace_build_id_derived(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_CORRELATION, raising=False)
    cid = resolve_correlation_id(config_value=None, namespace="ns1", build_id="bid1")
    assert cid == "ns1/bid1"


def test_resolve_build_id_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_CORRELATION, raising=False)
    cid = resolve_correlation_id(config_value=None, env_value="", namespace=None, build_id="only-bid")
    assert cid == "only-bid"


def test_pulp_client_headers_merge(monkeypatch: pytest.MonkeyPatch, mock_config: dict) -> None:
    from pulp_tool.api import PulpClient

    monkeypatch.delenv(ENV_CORRELATION, raising=False)
    client = PulpClient(
        mock_config,
        correlation_namespace="n",
        correlation_build_id="b",
    )
    assert client.headers == {CORRELATION_HEADER: "n/b"}
