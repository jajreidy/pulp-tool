"""Unit tests for e2e large upload helpers (no Pulp server or rpm-rs required)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
sys.path.insert(0, str(E2E_DIR))

from large_upload import (  # noqa: E402
    DEFAULT_LARGE_RPM_PAYLOAD_MB,
    LARGE_UPLOAD_MIN_SIZE_BYTES,
    LARGE_UPLOAD_MIN_SIZE_MB,
    write_incompressible_payload,
)


def test_large_upload_minimum_is_300mb() -> None:
    assert LARGE_UPLOAD_MIN_SIZE_MB == 300
    assert LARGE_UPLOAD_MIN_SIZE_BYTES == 300 * 1024 * 1024
    assert DEFAULT_LARGE_RPM_PAYLOAD_MB > LARGE_UPLOAD_MIN_SIZE_MB


def test_write_incompressible_payload_size(tmp_path: Path) -> None:
    payload = tmp_path / "payload.bin"
    write_incompressible_payload(payload, 2, chunk_mb=1)
    assert payload.stat().st_size == 2 * 1024 * 1024


def test_write_incompressible_payload_rejects_invalid_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="size_mb must be positive"):
        write_incompressible_payload(tmp_path / "bad.bin", 0)
