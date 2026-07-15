"""Tests for pulp_tool.utils.iteration_utils."""

from pulp_tool.models.artifacts import ArtifactFile, PulledArtifacts
from pulp_tool.utils.iteration_utils import (
    ARTIFACT_TYPES,
    count_artifacts,
    iterate_all_artifacts,
    iterate_artifact_types,
)


def _sample_pulled() -> PulledArtifacts:
    return PulledArtifacts(
        rpms={
            "pkg.rpm": ArtifactFile(
                file="/tmp/pkg.rpm",
                labels={"arch": "x86_64", "build_id": "build-1"},
            ),
            "pkg2.rpm": ArtifactFile(
                file="/tmp/pkg2.rpm",
                labels={"arch": "aarch64", "build_id": "build-1"},
            ),
        },
        sboms={
            "sbom.json": ArtifactFile(
                file="/tmp/sbom.json",
                labels={"build_id": "build-2"},
            ),
        },
        logs={
            "build.log": ArtifactFile(file="/tmp/build.log", labels={}),
        },
    )


def test_iterate_artifact_types_yields_non_empty_collections() -> None:
    pulled = _sample_pulled()
    types = list(iterate_artifact_types(pulled))
    assert {t for t, _ in types} == {"rpms", "sboms", "logs"}
    assert len(types[0][1]) >= 1


def test_iterate_artifact_types_respects_types_filter() -> None:
    pulled = _sample_pulled()
    types = list(iterate_artifact_types(pulled, types=["rpms"]))
    assert types == [("rpms", pulled.rpms)]


def test_iterate_all_artifacts_yields_every_item() -> None:
    pulled = _sample_pulled()
    items = list(iterate_all_artifacts(pulled))
    assert len(items) == pulled.total_count
    assert all(isinstance(data, ArtifactFile) for _, _, data in items)


def test_count_artifacts() -> None:
    pulled = _sample_pulled()
    assert count_artifacts(pulled) == {"rpms": 2, "sboms": 1, "logs": 1}


def test_artifact_types_constant() -> None:
    assert ARTIFACT_TYPES == ["rpms", "sboms", "logs"]
