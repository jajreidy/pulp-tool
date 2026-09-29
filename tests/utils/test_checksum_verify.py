"""Tests for SHA256 verification helpers."""

import pytest

from pulp_tool.exceptions import PulpToolChecksumError
from pulp_tool.utils.checksum_verify import (
    normalize_sha256_hex,
    validate_sha256_hex,
    verify_bytes_sha256,
    verify_file_sha256,
)


def test_normalize_sha256_hex_strips_prefix() -> None:
    assert normalize_sha256_hex("sha256:ABC") == "abc"


def test_validate_sha256_hex_rejects_short() -> None:
    with pytest.raises(ValueError, match="64 hex"):
        validate_sha256_hex("abc")


def test_verify_bytes_sha256_match() -> None:
    body = b"hello"
    import hashlib

    expected = hashlib.sha256(body).hexdigest()
    verify_bytes_sha256(body, expected, label="test")


def test_verify_bytes_sha256_mismatch() -> None:
    with pytest.raises(PulpToolChecksumError, match="mismatch"):
        verify_bytes_sha256(b"data", "a" * 64, label="test")


def test_verify_file_sha256(tmp_path) -> None:
    import hashlib

    path = tmp_path / "blob.bin"
    path.write_bytes(b"rpm-bytes")
    digest = hashlib.sha256(b"rpm-bytes").hexdigest()
    verify_file_sha256(path, digest, label="rpm")


def test_verify_file_sha256_mismatch(tmp_path) -> None:
    path = tmp_path / "blob.bin"
    path.write_bytes(b"rpm-bytes")
    with pytest.raises(PulpToolChecksumError, match="mismatch"):
        verify_file_sha256(path, "a" * 64, label="rpm")
