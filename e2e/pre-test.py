#!/usr/bin/env python3
"""Build test RPM packages for e2e tests."""

import argparse
import sys
from pathlib import Path

from rpm_rs import BuildConfig, CompressionType, FileOptions, PackageBuilder

PACKAGE_COUNT = 5
ARCHITECTURES = ["x86_64", "aarch64", "noarch"]
TEST_EXECUTABLE = b"#!/bin/sh\nexit 0\n"
LARGE_RPM_PACKAGE = "test.large"
LARGE_RPM_VERSION = "1.0.0"
LARGE_RPM_RELEASE = "1"
LARGE_RPM_ARCH = "x86_64"
DEFAULT_LARGE_RPM_SIZE_MB = 25


def build_rpm(test_pkgs_dir: Path, pkg_num: str, arch: str) -> bool:
    """Build a test RPM package.

    Args:
        executable: Path to the executable to include in the RPM
        test_pkgs_dir: Directory where test packages will be organized
        pkg_num: Package number identifier
        arch: Target architecture

    Returns:
        True if successful, False otherwise
    """
    pkg_dir = test_pkgs_dir.resolve() / pkg_num
    arch_dir = pkg_dir / arch
    arch_dir.mkdir(parents=True, exist_ok=True)
    out_rpm = arch_dir / f"test.{pkg_num}-1.0.0-1.{arch}.rpm"

    config = BuildConfig(compression=CompressionType.Gzip)
    builder = PackageBuilder(f"test.{pkg_num}", "1.0.0", "MIT", arch, "Test package")
    builder.using_config(config)
    builder.with_file_contents(TEST_EXECUTABLE, FileOptions.new(f"/user/bin/test.{pkg_num}-bin", permissions=0o100755))
    pkg = builder.build()

    written = Path(pkg.write_to(str(out_rpm)))
    if written.is_file():
        print(f"Built: {out_rpm}")
        return True
    if out_rpm.is_file():
        print(f"Built: {out_rpm}")
        return True
    else:
        print(f"Failed to build: {out_rpm}")
        return False


def build_large_rpm(test_pkgs_dir: Path, size_mb: int) -> bool:
    """Build a single large RPM to exercise multipart upload timeouts against Pulp.

    Args:
        test_pkgs_dir: Root directory for test packages (``test_pkgs``)
        size_mb: Payload size in megabytes embedded in the RPM

    Returns:
        True if successful, False otherwise
    """
    if size_mb <= 0:
        print("Skipping large RPM build (--large-rpm-size-mb <= 0)")
        return True

    arch_dir = test_pkgs_dir.resolve() / "large" / LARGE_RPM_ARCH
    arch_dir.mkdir(parents=True, exist_ok=True)
    out_rpm = arch_dir / f"{LARGE_RPM_PACKAGE}-{LARGE_RPM_VERSION}-{LARGE_RPM_RELEASE}.{LARGE_RPM_ARCH}.rpm"

    payload = b"\0" * (size_mb * 1024 * 1024)
    config = BuildConfig(compression=CompressionType.Gzip)
    builder = PackageBuilder(LARGE_RPM_PACKAGE, LARGE_RPM_VERSION, "MIT", LARGE_RPM_ARCH, "Large upload test RPM")
    builder.using_config(config)
    builder.with_file_contents(
        payload,
        FileOptions.new(f"/usr/share/test/{LARGE_RPM_PACKAGE}-{size_mb}mb.dat", permissions=0o100644),
    )
    pkg = builder.build()

    written = Path(pkg.write_to(str(out_rpm)))
    if written.is_file():
        print(f"Built large RPM ({size_mb} MiB payload): {out_rpm}")
        return True
    if out_rpm.is_file():
        print(f"Built large RPM ({size_mb} MiB payload): {out_rpm}")
        return True
    print(f"Failed to build large RPM: {out_rpm}")
    return False


def build_test_packages(build_dir: Path, *, large_rpm_size_mb: int = DEFAULT_LARGE_RPM_SIZE_MB) -> int:
    """Build all test RPM packages.

    Args:
        build_dir: Base directory for build artifacts

    Returns:
        Exit code (0 for success, 1 if any failures occurred)
    """
    build_dir = build_dir.resolve()
    build_dir.mkdir(parents=True, exist_ok=True)

    test_pkgs_dir = build_dir / "test_pkgs"
    test_pkgs_dir.mkdir(parents=True, exist_ok=True)

    print(f"Build directory: {build_dir}")
    print(f"Test packages directory: {test_pkgs_dir}")

    failures = 0
    total_packages = PACKAGE_COUNT * len(ARCHITECTURES)
    built = 0

    for pkg in range(PACKAGE_COUNT):
        for arch in ARCHITECTURES:
            if build_rpm(test_pkgs_dir, str(pkg), arch):
                built += 1
            else:
                failures += 1

    print(f"\nSummary: {built}/{total_packages} packages built successfully")

    if not build_large_rpm(test_pkgs_dir, large_rpm_size_mb):
        failures += 1

    return 1 if failures > 0 else 0


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build test RPM packages for e2e tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--build-dir",
        type=Path,
        default=Path("."),
        help="Directory where test packages will be built (default: current directory)",
    )
    parser.add_argument(
        "--large-rpm-size-mb",
        type=int,
        default=DEFAULT_LARGE_RPM_SIZE_MB,
        help=(
            "Payload size in MiB for the large upload test RPM (default: "
            f"{DEFAULT_LARGE_RPM_SIZE_MB}; use 0 to skip)"
        ),
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    return build_test_packages(args.build_dir, large_rpm_size_mb=args.large_rpm_size_mb)


if __name__ == "__main__":
    sys.exit(main())
