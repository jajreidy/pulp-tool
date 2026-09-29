"""SHA256 verification helpers for downloaded artifacts."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from ..exceptions import PulpToolChecksumError
from ..models.cli import SHA256_HEX_LENGTH

_SHA256_HEX_RE = re.compile(rf"^[0-9a-f]{{{SHA256_HEX_LENGTH}}}$")


def normalize_sha256_hex(value: str) -> str:
    """Strip optional ``sha256:`` prefix and return lowercase hex."""
    normalized = value.strip().lower()
    if normalized.startswith("sha256:"):
        normalized = normalized.removeprefix("sha256:")
    return normalized


def validate_sha256_hex(value: str, *, field_name: str = "sha256") -> str:
    """Return normalized SHA256 hex or raise ``ValueError``."""
    normalized = normalize_sha256_hex(value)
    if not _SHA256_HEX_RE.match(normalized):
        raise ValueError(f"Invalid {field_name} (expected {SHA256_HEX_LENGTH} hex chars): {value!r}")
    return normalized


def verify_bytes_sha256(body: bytes, expected_sha256: str, *, label: str) -> None:
    """Compare ``body`` to ``expected_sha256`` from artifact metadata."""
    expected = validate_sha256_hex(expected_sha256, field_name=f"{label} sha256")
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise PulpToolChecksumError(
            f"{label}: SHA256 mismatch (expected {expected}, got {actual})",
        )


def verify_file_sha256(path: Path, expected_sha256: str, *, label: str) -> None:
    """Hash a file on disk and compare to ``expected_sha256``."""
    expected = validate_sha256_hex(expected_sha256, field_name=f"{label} sha256")
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    actual = hasher.hexdigest()
    if actual != expected:
        raise PulpToolChecksumError(
            f"{label}: SHA256 mismatch for {path} (expected {expected}, got {actual})",
        )


__all__ = [
    "normalize_sha256_hex",
    "validate_sha256_hex",
    "verify_bytes_sha256",
    "verify_file_sha256",
]
