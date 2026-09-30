"""Build NEVRA-stable test RPM variants (same filename, different payload) for update-build e2e."""

from __future__ import annotations

from pathlib import Path

from rpm_rs import BuildConfig, CompressionType, FileOptions, PackageBuilder

TEST2_NOARCH_RPM_NAME = "test.2-1.0.0-1.noarch.rpm"


def build_test2_noarch_rpm(workspace: Path, *, variant: str) -> Path:
    """
    Write ``test.2-1.0.0-1.noarch.rpm`` under ``workspace`` with a unique payload ``variant``.

    Same NEVRA as pre-test package ``2/noarch`` so ``update-build`` can replace content by filename.
    """
    workspace.mkdir(parents=True, exist_ok=True)
    out_rpm = workspace / TEST2_NOARCH_RPM_NAME
    payload = f"#!/bin/sh\n# e2e-update-build-variant={variant}\nexit 0\n".encode()
    config = BuildConfig(compression=CompressionType.Gzip)
    builder = PackageBuilder("test.2", "1.0.0", "MIT", "noarch", "Test package variant")
    builder.using_config(config)
    builder.with_file_contents(payload, FileOptions.new("/usr/bin/test.2-bin", permissions=0o100755))
    pkg = builder.build()
    written = Path(pkg.write_to(str(out_rpm)))
    if written.is_file():
        return written
    if out_rpm.is_file():
        return out_rpm
    raise RuntimeError(f"Failed to build variant RPM at {out_rpm}")
