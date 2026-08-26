"""Build large RPM packages for e2e upload timeout coverage (requires rpm-rs)."""

from __future__ import annotations

from pathlib import Path

from large_upload import (
    DEFAULT_LARGE_RPM_PAYLOAD_MB,
    LARGE_RPM_ARCH,
    LARGE_RPM_FILENAME,
    LARGE_RPM_PACKAGE,
    LARGE_UPLOAD_MIN_SIZE_BYTES,
    LARGE_UPLOAD_MIN_SIZE_MB,
    write_incompressible_payload,
)
from rpm_rs import BuildConfig, CompressionType, FileOptions, PackageBuilder

__all__ = [
    "DEFAULT_LARGE_RPM_PAYLOAD_MB",
    "LARGE_RPM_FILENAME",
    "LARGE_UPLOAD_MIN_SIZE_BYTES",
    "LARGE_UPLOAD_MIN_SIZE_MB",
    "build_large_rpm",
]


def build_large_rpm(test_pkgs_dir: Path, payload_mb: int) -> bool:
    """Build a large RPM with an on-disk size above ``LARGE_UPLOAD_MIN_SIZE_MB``."""
    if payload_mb <= 0:
        return True

    arch_dir = test_pkgs_dir.resolve() / "large" / LARGE_RPM_ARCH
    arch_dir.mkdir(parents=True, exist_ok=True)
    out_rpm = arch_dir / LARGE_RPM_FILENAME
    payload_path = arch_dir / f"{LARGE_RPM_PACKAGE}-{payload_mb}mb.payload.bin"

    print(f"Writing {payload_mb} MiB incompressible payload to {payload_path}")
    write_incompressible_payload(payload_path, payload_mb)

    config = BuildConfig(compression=CompressionType.Gzip)
    builder = PackageBuilder(LARGE_RPM_PACKAGE, "1.0.0", "MIT", LARGE_RPM_ARCH, "Large upload test RPM")
    builder.using_config(config)
    builder.with_file(
        str(payload_path),
        FileOptions.new(f"/usr/share/test/{LARGE_RPM_PACKAGE}-{payload_mb}mb.dat", permissions=0o100644),
    )
    pkg = builder.build()

    written = Path(pkg.write_to(str(out_rpm)))
    rpm_path = written if written.is_file() else out_rpm
    if not rpm_path.is_file():
        print(f"Failed to build large RPM: {out_rpm}")
        return False

    on_disk = rpm_path.stat().st_size
    if on_disk < LARGE_UPLOAD_MIN_SIZE_BYTES:
        print(
            f"Large RPM on-disk size {on_disk} bytes is below minimum "
            f"{LARGE_UPLOAD_MIN_SIZE_BYTES} bytes ({LARGE_UPLOAD_MIN_SIZE_MB} MiB)"
        )
        return False

    print(f"Built large RPM ({on_disk / (1024 * 1024):.1f} MiB on disk): {rpm_path}")
    try:
        payload_path.unlink()
    except OSError:
        pass
    return True
