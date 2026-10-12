"""RPM GPG signature verification via the system ``rpm`` CLI."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ..exceptions import PulpToolError


class RpmGpgVerifyError(PulpToolError):
    """RPM signature verification failed."""


def _rpm_checksig_argv(rpm_file: str, *, rpmdb_path: Path | None) -> list[str]:
    """Build ``rpm --checksig`` argv; optional isolated ``%_dbpath`` for imported keys."""
    cmd = [
        "rpm",
        "--checksig",
        "--verbose",
        "--define",
        "_pkgverify_level signature",
    ]
    if rpmdb_path is not None:
        cmd.extend(["--define", f"_dbpath {rpmdb_path}"])
    cmd.append(rpm_file)
    return cmd


def _checksig_indicates_valid_signature(combined: str) -> bool:
    """Return True when verbose ``rpm --checksig`` output shows an acceptable package signature."""
    for line in combined.splitlines():
        upper = line.upper()
        if "NOT OK" in upper or "NOKEY" in upper:
            return False
        if "SIGNATURE" in upper and ": OK" in upper:
            return True
    return False


def _rpmkeys_import_argv(asc_path: Path, *, rpmdb_path: Path) -> list[str]:
    return ["rpmkeys", "--define", f"_dbpath {rpmdb_path}", "--import", str(asc_path)]


def _export_pubkey_armor(gnupg_home: Path, asc_path: Path) -> None:
    gpg = shutil.which("gpg") or shutil.which("gpg2")
    if gpg is None:
        raise RpmGpgVerifyError("gpg CLI not found on PATH (install gnupg2 in the image to use --gpg-keyring)")
    result = subprocess.run(
        [gpg, "--batch", "--homedir", str(gnupg_home), "--export", "-a", "-o", str(asc_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RpmGpgVerifyError(f"gpg --export failed for keyring {gnupg_home}: {detail or 'non-zero exit'}")
    if not asc_path.is_file() or asc_path.stat().st_size == 0:
        raise RpmGpgVerifyError(f"No public keys exported from GPG homedir: {gnupg_home}")


@contextmanager
def _ephemeral_rpmdb_for_gnupg_home(gnupg_home: Path) -> Iterator[Path]:
    """Import exported pubkeys into a temporary rpmdb for ``rpm --checksig``."""
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        rpmdb_path = work / "rpmdb"
        rpmdb_path.mkdir()
        asc_path = work / "keys.asc"
        _export_pubkey_armor(gnupg_home, asc_path)
        import_cmd = _rpmkeys_import_argv(asc_path, rpmdb_path=rpmdb_path)
        try:
            import_result = subprocess.run(
                import_cmd,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            raise RpmGpgVerifyError(
                "rpmkeys CLI not found on PATH (install rpm package in the container image)"
            ) from None
        if import_result.returncode != 0:
            detail = (import_result.stderr or import_result.stdout or "").strip()
            raise RpmGpgVerifyError(f"rpmkeys --import failed: {detail or 'non-zero exit'}")
        yield rpmdb_path


def _run_rpm_checksig(path: Path, *, rpmdb_path: Path | None, env: dict[str, str]) -> None:
    cmd = _rpm_checksig_argv(str(path), rpmdb_path=rpmdb_path)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
    except FileNotFoundError:
        raise RpmGpgVerifyError("rpm CLI not found on PATH (install rpm package in the container image)") from None

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RpmGpgVerifyError(f"rpm --checksig failed for {path.name}: {detail or 'non-zero exit'}")

    combined = f"{result.stdout}\n{result.stderr}"
    if not _checksig_indicates_valid_signature(combined):
        raise RpmGpgVerifyError(
            f"rpm --checksig reported missing or invalid GPG signature for {path.name}: {combined.strip()}"
        )

    logging.debug("RPM GPG check OK: %s", path.name)


def verify_rpm_gpg_signature(
    rpm_path: str | Path,
    *,
    gpg_keyring: str | None = None,
    rpmdb_path: Path | None = None,
) -> None:
    """
    Verify an RPM package signature using ``rpm --checksig``.

    Args:
        rpm_path: Path to the ``.rpm`` file on disk.
        gpg_keyring: Optional GnuPG homedir; public keys are exported and imported into a
            temporary rpmdb (RHEL ``rpm`` does not honor ``GNUPGHOME`` for ``--checksig``).
        rpmdb_path: Internal: reuse an ephemeral rpmdb from ``_ephemeral_rpmdb_for_gnupg_home``.

    Raises:
        RpmGpgVerifyError: When verification fails or ``rpm`` is unavailable.
    """
    path = Path(rpm_path).resolve()
    if not path.is_file():
        raise RpmGpgVerifyError(f"RPM not found for signature check: {path}")

    env = os.environ.copy()
    if gpg_keyring:
        gnupg_home = Path(gpg_keyring).expanduser().resolve()
        if not gnupg_home.exists():
            raise RpmGpgVerifyError(f"GPG keyring path does not exist: {gnupg_home}")
        env["GNUPGHOME"] = str(gnupg_home)
        if rpmdb_path is None:
            with _ephemeral_rpmdb_for_gnupg_home(gnupg_home) as ephemeral_db:
                _run_rpm_checksig(path, rpmdb_path=ephemeral_db, env=env)
            return

    _run_rpm_checksig(path, rpmdb_path=rpmdb_path, env=env)


def verify_pulled_rpms_gpg(rpm_paths: list[str | Path], *, gpg_keyring: str | None) -> None:
    """Verify every path in ``rpm_paths``; raises on first failure."""
    if not gpg_keyring:
        for rpm_path in rpm_paths:
            verify_rpm_gpg_signature(rpm_path, gpg_keyring=None)
        return

    gnupg_home = Path(gpg_keyring).expanduser().resolve()
    if not gnupg_home.exists():
        raise RpmGpgVerifyError(f"GPG keyring path does not exist: {gnupg_home}")
    env = os.environ.copy()
    env["GNUPGHOME"] = str(gnupg_home)
    with _ephemeral_rpmdb_for_gnupg_home(gnupg_home) as rpmdb_path:
        for rpm_path in rpm_paths:
            verify_rpm_gpg_signature(rpm_path, gpg_keyring=gpg_keyring, rpmdb_path=rpmdb_path)


__all__ = ["RpmGpgVerifyError", "verify_pulled_rpms_gpg", "verify_rpm_gpg_signature"]
