"""Unit tests for e2e GPG signing helpers."""

from __future__ import annotations

import sys
from pathlib import Path

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
sys.path.insert(0, str(E2E_DIR))

from gpg_sign_rpm import GPG_KEY_EMAIL, GPG_KEY_REAL_NAME, gpg_user_id  # noqa: E402


def test_gpg_user_id_format() -> None:
    assert gpg_user_id() == f"{GPG_KEY_REAL_NAME} <{GPG_KEY_EMAIL}>"
