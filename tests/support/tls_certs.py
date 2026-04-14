"""TLS material for tests (real PEM pairs for ssl.load_cert_chain)."""

from __future__ import annotations

import subprocess
from pathlib import Path


def write_self_signed_pem_pair(cert_path: Path, key_path: Path) -> None:
    """Create a minimal valid cert/key pair so ssl.load_cert_chain succeeds in tests."""
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=pulp-tool-test",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
