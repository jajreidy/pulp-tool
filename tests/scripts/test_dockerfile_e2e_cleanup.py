"""Ensure the e2e cleanup image Dockerfile installs pulp-cli from requirements."""

from pathlib import Path


def test_dockerfile_e2e_cleanup_installs_pulp_cli() -> None:
    root = Path(__file__).resolve().parents[2]
    dockerfile = (root / "e2e" / "Dockerfile.e2e-cleanup").read_text(encoding="utf-8")
    assert "e2e/requirements-cleanup.txt" in dockerfile
    assert "-r /opt/e2e/requirements-cleanup.txt" in dockerfile
    assert "pulp --version" in dockerfile

    requirements = (root / "e2e" / "requirements-cleanup.txt").read_text(encoding="utf-8")
    runner_requirements = (root / "e2e" / "requirements.txt").read_text(encoding="utf-8")
    for line in requirements.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            assert stripped in runner_requirements
