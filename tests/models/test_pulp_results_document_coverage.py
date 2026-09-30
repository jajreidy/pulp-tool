"""Extra coverage for pulp_results_document edge paths."""

from datetime import date
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pulp_tool.models import pulp_results as prd
from pulp_tool.models.artifacts import ArtifactJsonResponse
from pulp_tool.models.pulp_results import BUILD_SIGN_OPERATION, PULP_RESULTS_SCHEMA_VERSION, PulpResultsDocument
from pulp_tool.models.repository import RepositoryRefs
from pulp_tool.models.results import PulpResultsModel


def test_coerce_schema_version_blank_and_digit_strings() -> None:
    assert prd.schema_version({"version": "  "}) == PULP_RESULTS_SCHEMA_VERSION
    assert prd.schema_version({"version": "3"}) == "3.0.0"


def test_parse_last_updated_non_string_returns_none() -> None:
    assert prd.parse_last_updated(None) is None
    assert prd.parse_last_updated(12345) is None


def test_normalize_href_history_sets_default_schema_version() -> None:
    doc = prd.normalize_document(
        {
            "artifacts": {
                "a.rpm": {
                    "href_history": [{"href": "/h/", "operation": "build_sign"}],
                }
            }
        }
    )
    entry = doc["artifacts"]["a.rpm"]["href_history"][0]
    assert entry["schema_version"] == PULP_RESULTS_SCHEMA_VERSION


def test_merge_upload_outcomes_coerces_non_dict_artifacts_in_place() -> None:
    repos = RepositoryRefs(
        rpms_href="/r/",
        rpms_prn="r",
        logs_href="/l/",
        logs_prn="l",
        sbom_href="/s/",
        sbom_prn="s",
        artifacts_href="/a/",
        artifacts_prn="a",
    )
    upload = PulpResultsModel(build_id="b1", repositories=repos)
    target: dict[str, Any] = {
        "version": "1.0.0",
        "last_updated": "2026-01-01",
        "artifacts": [],
    }

    def identity(doc: dict[str, Any]) -> dict[str, Any]:
        return doc

    with patch("pulp_tool.models.pulp_results.normalize_document", side_effect=identity):
        prd.merge_upload_outcomes_into_document(
            target,
            upload,
            operation=BUILD_SIGN_OPERATION,
            signed_by=None,
            replace_signed_by=False,
            verified_distribution_slots=set(),
        )
    assert isinstance(target["artifacts"], dict)


class TestPulpResultsDocumentMethods:
    def test_model_validator_passthrough_non_dict(self) -> None:
        doc = PulpResultsDocument.from_raw({"artifacts": {}})
        again = PulpResultsDocument.model_validate(doc)
        assert again.build_id == doc.build_id
        with pytest.raises(ValidationError):
            PulpResultsDocument.model_validate("not-a-dict")

    def test_from_artifact_json_dict_and_unsupported(self) -> None:
        from_dict = PulpResultsDocument.from_artifact_json({"version": 1, "artifacts": {}})
        assert from_dict.version == "1.0.0"
        from_model = PulpResultsDocument.from_artifact_json(ArtifactJsonResponse(artifacts={}))
        assert from_model.artifacts == {}
        with pytest.raises(TypeError, match="Unsupported artifact_json"):
            PulpResultsDocument.from_artifact_json(42)

    def test_empty_shell(self) -> None:
        doc = PulpResultsDocument.empty_shell(build_id="b", namespace="ns", cluster="c1")
        assert doc.build_id == "b"
        assert doc.cluster == "c1"

    def test_mutable_dict_includes_cluster(self) -> None:
        doc = PulpResultsDocument.from_raw({"artifacts": {}, "cluster": "cluster-a"})
        assert doc._mutable_dict()["cluster"] == "cluster-a"

    def test_prepare_for_mutation_on_model(self) -> None:
        prior = "2024-06-01"
        doc = PulpResultsDocument.from_raw(
            {
                "version": "1.0.0",
                "last_updated": prior,
                "artifacts": {"p.rpm": {"href": "/old/", "sha256": "abc"}},
            }
        )
        old = doc.prepare_for_mutation(operation=BUILD_SIGN_OPERATION)
        assert old == prior
        assert doc.last_updated == date.today().isoformat()

    def test_oci_manifest_ref_instance_method(self) -> None:
        doc = PulpResultsDocument.from_raw({"artifacts": {}})
        assert doc.oci_manifest_ref() == ""


def test_parse_oci_manifest_unsupported_type() -> None:
    assert prd.parse_oci_manifest_field(42) is None


def test_oci_manifest_ref_legacy_embedded_only() -> None:
    doc = {"oci_manifest": {"ref": "quay.io/r", "digest": ""}}
    assert prd.oci_manifest_ref(doc) == "quay.io/r"


def test_normalize_strips_empty_oci_manifest() -> None:
    doc = prd.normalize_document({"oci_manifest": "", "artifacts": {}})
    assert "oci_manifest" not in doc


def test_normalize_skips_non_dict_artifact_rows() -> None:
    doc = prd.normalize_document({"artifacts": {"ok": {"labels": {"a": "1"}}, "bad": "x"}})
    assert doc["artifacts"]["ok"]["pulp_labels"]["a"] == "1"


def test_normalize_invalid_top_level_distributions() -> None:
    doc = prd.normalize_document({"distributions": "not-a-dict", "artifacts": {}})
    assert doc["distributions"] == {}


def test_document_to_canonical_dict_includes_shell_fields() -> None:
    raw = {
        "build_id": "  b1 ",
        "namespace": "ns",
        "cluster": "c1",
        "oci_manifest": {"ref": "quay.io/r", "digest": "sha256:aa"},
        "oci_manifest_history": [{"version": 1}],
        "distributions": {"rpms": "https://example/rpms/"},
        "artifacts": {},
    }
    out = prd.document_to_canonical_dict(raw)
    assert out["build_id"] == "b1"
    assert out["namespace"] == "ns"
    assert out["cluster"] == "c1"
    assert "oci_manifest" not in out
    assert "oci_manifest_history" not in out
    assert "last_updated" in out
    assert out["version"] == prd.PULP_RESULTS_SCHEMA_VERSION
    assert out["distributions"] == {"rpms": "https://example/rpms/"}


def test_merge_signed_by_replace() -> None:
    assert prd.merge_signed_by("a;b", "c", replace=True) == "c"


def test_merge_signed_by_empty_new_keeps_existing() -> None:
    assert prd.merge_signed_by("a@x.com", "") == "a@x.com"


def test_aggregate_top_level_distributions_merges_rows_and_document() -> None:
    doc = {
        "artifacts": {
            "a.rpm": {"distributions": {"rpms": "https://from-artifact/"}},
            "bad": "skip",
        },
        "distributions": {"logs": "https://from-doc/logs/"},
    }
    agg = prd.aggregate_top_level_distributions(doc)
    assert agg["rpms"] == "https://from-artifact/"
    assert agg["logs"] == "https://from-doc/logs/"


def test_aggregate_top_level_distributions_non_dict_artifacts() -> None:
    assert prd.aggregate_top_level_distributions({"artifacts": []}) == {}


def test_append_all_artifact_href_histories_skips_bad_rows() -> None:
    doc = {
        "artifacts": {
            "x.rpm": {"href": "/h/", "sha256": "abc"},
            "y": None,
        }
    }
    prd.append_all_artifact_href_histories(doc, last_updated="2026-01-01", operation="build_sign")
    artifacts = doc["artifacts"]
    assert isinstance(artifacts, dict)
    row = artifacts["x.rpm"]
    assert isinstance(row, dict)
    history = row["href_history"]
    assert isinstance(history, list)
    assert history[0]["href"] == "/h/"


def test_append_all_artifact_href_histories_non_dict_artifacts() -> None:
    doc: dict = {"artifacts": "nope"}
    prd.append_all_artifact_href_histories(doc, last_updated="2026-01-01", operation="build_sign")
    assert doc["artifacts"] == "nope"


def test_prepare_document_no_manifest_still_touches() -> None:
    prior = "2025-12-01"
    doc = {"version": "1.0.0", "last_updated": prior, "artifacts": {}}
    old = prd.prepare_document_for_mutation(doc, operation="build_sign")
    assert old == prior
    assert doc["version"] == "1.0.0"
    assert doc["last_updated"] == date.today().isoformat()


def test_prepare_document_invalid_version_uses_default_schema() -> None:
    doc: dict[str, Any] = {
        "version": "not-int",
        "oci_manifest": {"ref": "quay.io/r", "digest": "sha256:aa"},
        "artifacts": {"p.rpm": {"href": "/old/"}},
    }
    old = prd.prepare_document_for_mutation(doc, operation="build_sign")
    assert isinstance(old, str)
    assert doc.get("version") == "not-int"
    assert doc["last_updated"] == date.today().isoformat()


def test_normalize_href_history_migrates_document_version() -> None:
    doc = prd.normalize_document(
        {
            "artifacts": {
                "a.rpm": {
                    "href_history": [{"href": "/h/", "document_version": 2, "operation": "build_sign"}],
                }
            }
        }
    )
    entry = doc["artifacts"]["a.rpm"]["href_history"][0]
    assert "document_version" not in entry
    assert entry["schema_version"] == "2.0.0"


def test_apply_side_tag_coerces_non_dict_artifacts_after_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_deepcopy(doc: dict) -> dict:
        out = dict(doc)
        out["artifacts"] = "not-a-dict"
        return out

    monkeypatch.setattr(prd.copy, "deepcopy", fake_deepcopy)
    merged = prd.apply_side_tag_transfer_to_document(
        {"artifacts": {}, "last_updated": "2020-01-01"},
        [],
        side_tag="t",
        top_level_side_tag_distribution_url="",
    )
    assert isinstance(merged["artifacts"], dict)


def test_apply_side_tag_invalid_prior_last_updated(monkeypatch: pytest.MonkeyPatch) -> None:
    real_normalize = prd.normalize_document

    def normalize_keep_bad_last_updated(raw: dict) -> dict:
        doc = real_normalize(raw)
        doc["last_updated"] = "not-a-date"
        return doc

    monkeypatch.setattr(prd, "normalize_document", normalize_keep_bad_last_updated)
    merged = prd.apply_side_tag_transfer_to_document(
        {"artifacts": {}},
        [],
        side_tag="t",
        top_level_side_tag_distribution_url="",
    )
    assert merged["last_updated"] == date.today().isoformat()


def test_document_from_artifact_json_normalizes() -> None:
    doc = prd.document_from_artifact_json({"artifacts": {"x": {"labels": {"a": "1"}}}})
    assert doc["artifacts"]["x"]["pulp_labels"]["a"] == "1"
