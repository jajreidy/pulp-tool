"""Property-based tests for small pure helpers (Hypothesis)."""

from __future__ import annotations
import os
import string
from hypothesis import given, settings
from hypothesis import strategies as st
from pulp_tool.utils.correlation import ENV_CORRELATION, resolve_correlation_id
from pulp_tool.utils.rpm_operations import parse_rpm_filename_to_nvra, parse_rpm_filename_to_nvr
from pulp_tool.utils.validation.build_id import sanitize_build_id_for_repository, strip_namespace_from_build_id


def _env_safe_text(max_size: int) -> st.SearchStrategy[str]:
    """Environment values cannot contain NUL (POSIX / ``os.environ``)."""
    return st.text(min_size=0, max_size=max_size).filter(lambda s: "\x00" not in s)


@settings(max_examples=40)
@given(
    cfg=st.one_of(st.none(), st.text(min_size=0, max_size=64)),
    env=st.one_of(st.none(), _env_safe_text(64)),
    ns=st.one_of(st.none(), st.text(min_size=0, max_size=32)),
    bid=st.one_of(st.none(), st.text(min_size=0, max_size=32)),
)
def test_resolve_correlation_id_prefers_config_then_env(cfg, env, ns, bid) -> None:
    """Config ``correlation_id`` wins; then env; then derived ``ns/bid`` or ``bid``."""
    prev = os.environ.get(ENV_CORRELATION)
    try:
        os.environ.pop(ENV_CORRELATION, None)
        if env is not None:
            os.environ[ENV_CORRELATION] = env
        out = resolve_correlation_id(config_value=cfg, env_value=None, namespace=ns, build_id=bid)
        cfg_s = (str(cfg).strip() if cfg is not None else None) or None
        if cfg_s:
            assert out == cfg_s
            return
        env_read = os.environ.get(ENV_CORRELATION)
        if env_read and str(env_read).strip():
            assert out == str(env_read).strip()
            return
        ns_s = str(ns).strip() if ns is not None else ""
        bid_s = str(bid).strip() if bid is not None else ""
        if ns_s and bid_s:
            assert out == f"{ns_s}/{bid_s}"
        elif bid_s:
            assert out == bid_s
        else:
            assert out is None
    finally:
        if prev is None:
            os.environ.pop(ENV_CORRELATION, None)
        else:
            os.environ[ENV_CORRELATION] = prev


@settings(max_examples=50)
@given(
    parts=st.lists(
        st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=12), min_size=1, max_size=4
    )
)
def test_strip_namespace_from_build_id_roundtrip(parts) -> None:
    """Strip keeps suffix after first slash; no slash returns input."""
    raw = "/".join(parts)
    out = strip_namespace_from_build_id(raw)
    if "/" in raw:
        assert out == raw.split("/", 1)[1]
    else:
        assert out == raw


@settings(max_examples=50)
@given(seg=st.text(alphabet=string.ascii_letters + string.digits + "-", min_size=1, max_size=24))
def test_sanitize_build_id_for_repository_no_invalid_chars(seg) -> None:
    """Sanitized id has no path separators or glob-like chars from the invalid set."""
    invalid = {"/", "\\", ":", "*", "?", '"', "<", ">", "|"}
    sanitized = sanitize_build_id_for_repository(seg)
    assert not any((c in sanitized for c in invalid))
    assert sanitized == sanitized.strip("-").lower() or sanitized == "default-build"


@settings(max_examples=40)
@given(
    name=st.text(alphabet=string.ascii_lowercase + string.digits, min_size=1, max_size=16),
    ver=st.from_regex("[0-9]+(\\.[0-9]+)?", fullmatch=True),
    rel=st.from_regex("[0-9]+[a-z0-9.]*", fullmatch=True),
    arch=st.sampled_from(["x86_64", "i686", "noarch", "src", "aarch64"]),
)
def test_parse_rpm_filename_nvra_roundtrip(name, ver, rel, arch) -> None:
    """Synthetic ``name-ver-rel.arch.rpm`` parses to consistent NVR and NVRA."""
    filename = f"{name}-{ver}-{rel}.{arch}.rpm"
    nvr = parse_rpm_filename_to_nvr(filename)
    nvra = parse_rpm_filename_to_nvra(filename)
    assert nvr is not None
    assert nvra is not None
    assert nvra[:3] == nvr
    assert nvra[3] == arch
