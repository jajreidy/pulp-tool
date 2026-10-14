"""Create a test GPG keyring and sign RPMs for ``pull --verify-rpm-gpg`` e2e."""

from __future__ import annotations

import os
import shutil
import subprocess
import textwrap
from pathlib import Path

GPG_KEY_REAL_NAME = "pulp-tool e2e"
GPG_KEY_EMAIL = "pulp-tool-e2e@example.invalid"


def gpg_user_id() -> str:
    """User-id string for ``rpm --define _gpg_name`` (must match ``gpg --list-keys``)."""
    return f"{GPG_KEY_REAL_NAME} <{GPG_KEY_EMAIL}>"


def _gpg_has_secret_key(gnupg_home: Path) -> bool:
    if not gnupg_home.is_dir():
        return False
    result = subprocess.run(
        [
            "gpg",
            "--homedir",
            str(gnupg_home),
            "--list-secret-keys",
            "--with-colons",
            GPG_KEY_EMAIL,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return any(line.startswith("sec:") for line in result.stdout.splitlines())


def _secure_gnupg_home(gnupg_home: Path) -> None:
    """GnuPG refuses to use secret keys when the homedir is group/world accessible."""
    gnupg_home.mkdir(parents=True, exist_ok=True)
    os.chmod(gnupg_home, 0o700)


def ensure_gpg_keyring(gnupg_home: Path) -> None:
    """Create a passwordless test key under ``gnupg_home`` if one is not already present."""
    _secure_gnupg_home(gnupg_home)
    if _gpg_has_secret_key(gnupg_home):
        return

    batch = textwrap.dedent(
        f"""
        Key-Type: RSA
        Key-Length: 2048
        Subkey-Type: RSA
        Subkey-Length: 2048
        Name-Real: {GPG_KEY_REAL_NAME}
        Name-Email: {GPG_KEY_EMAIL}
        Expire-Date: 0
        %no-protection
        %commit
        """
    ).strip()
    subprocess.run(
        ["gpg", "--batch", "--homedir", str(gnupg_home), "--gen-key"],
        input=f"{batch}\n",
        text=True,
        check=True,
        capture_output=True,
    )
    if not _gpg_has_secret_key(gnupg_home):
        raise RuntimeError(f"GPG key generation failed under {gnupg_home}")


def sign_rpm(rpm_path: Path, gnupg_home: Path) -> None:
    """Sign ``rpm_path`` in place using the e2e test key in ``gnupg_home``."""
    rpm_path = rpm_path.resolve()
    if not rpm_path.is_file():
        raise FileNotFoundError(f"RPM not found for signing: {rpm_path}")

    ensure_gpg_keyring(gnupg_home)
    _secure_gnupg_home(gnupg_home)
    env = os.environ.copy()
    env["GNUPGHOME"] = str(gnupg_home.resolve())
    gpg_name = gpg_user_id()
    gpg_bin = shutil.which("gpg") or "gpg"
    defines = [
        "--define",
        "_signature gpg",
        "--define",
        f"_gpg_name {gpg_name}",
        "--define",
        f"_gpgbin {gpg_bin}",
    ]
    rpmsign = shutil.which("rpmsign")
    if rpmsign:
        cmd = [rpmsign, "--addsign", *defines, str(rpm_path)]
    else:
        cmd = ["rpm", "--addsign", *defines, str(rpm_path)]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"RPM signing failed ({' '.join(cmd[:2])}): {detail or 'non-zero exit'}")
