"""Large upload test constants and payload helpers (no rpm-rs dependency)."""

from __future__ import annotations

import os
from pathlib import Path

# Match production failures (e.g. cmake-debuginfo sign-and-verify uploads).
LARGE_UPLOAD_MIN_SIZE_MB = 300
LARGE_UPLOAD_MIN_SIZE_BYTES = LARGE_UPLOAD_MIN_SIZE_MB * 1024 * 1024
# Payload strictly above the minimum so the built RPM exceeds 300 MiB on disk.
DEFAULT_LARGE_RPM_PAYLOAD_MB = LARGE_UPLOAD_MIN_SIZE_MB + 1

LARGE_RPM_PACKAGE = "test.large"
LARGE_RPM_VERSION = "1.0.0"
LARGE_RPM_RELEASE = "1"
LARGE_RPM_ARCH = "x86_64"
LARGE_RPM_FILENAME = f"{LARGE_RPM_PACKAGE}-{LARGE_RPM_VERSION}-{LARGE_RPM_RELEASE}.{LARGE_RPM_ARCH}.rpm"


def write_incompressible_payload(path: Path, size_mb: int, *, chunk_mb: int = 1) -> None:
    """Write ``size_mb`` MiB of high-entropy data (poor gzip compression) to ``path``."""
    if size_mb <= 0:
        raise ValueError("size_mb must be positive")
    if chunk_mb <= 0 or chunk_mb > size_mb:
        raise ValueError("chunk_mb must be positive and not greater than size_mb")

    path.parent.mkdir(parents=True, exist_ok=True)
    chunk = os.urandom(chunk_mb * 1024 * 1024)
    full_chunks, remainder_mb = divmod(size_mb, chunk_mb)
    with open(path, "wb") as out:
        for _ in range(full_chunks):
            out.write(chunk)
        if remainder_mb:
            out.write(os.urandom(remainder_mb * 1024 * 1024))
