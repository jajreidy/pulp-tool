"""
Canonical ``pulp_results.json`` model and document operations (upload through update-build).

Schema ``version`` as x.y.z, ``last_updated`` date, href_history, per-artifact distributions, origin_* pulp_labels.
"""

from __future__ import annotations

import copy
import json
from datetime import date
from threading import Lock
from typing import Any, Self

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, PrivateAttr, TypeAdapter, model_validator

from .artifacts import ArtifactMetadata
from .base import KonfluxBaseModel
from .repository import RepositoryRefs
from .statistics import UploadCounts

PULP_RESULTS_ORAS_MEDIA_TYPE = "application/vnd.pulp.results.v0"

# Top-level ``version`` is the JSON **schema** semver (bump only on format changes).
PULP_RESULTS_SCHEMA_VERSION = "1.0.0"

BUILD_UPLOAD_OPERATION = "build_upload"
BUILD_SIGN_OPERATION = "build_sign"
RELEASE_SIGN_OPERATION = "release_sign"
BTS_UPDATE_OPERATION = "bts_update"
SIDE_TAG_TRANSFER_OPERATION = "transfer"


class HrefHistoryEntry(BaseModel):
    """One prior Pulp content href for an artifact."""

    model_config = ConfigDict(extra="ignore")

    href: str
    sha256: str = ""
    operation: str = SIDE_TAG_TRANSFER_OPERATION
    last_updated: str = ""
    schema_version: str = PULP_RESULTS_SCHEMA_VERSION
    signed_by: str = ""


class OciManifestObject(BaseModel):
    """Legacy in-memory shape for embedded ``oci_manifest`` on read (not serialized)."""

    model_config = ConfigDict(extra="ignore")

    ref: str
    digest: str = ""


class SideTagRpmTransfer(KonfluxBaseModel):
    """Upload outcome for one RPM promoted to a side-tag repository."""

    model_config = ConfigDict(extra="ignore")

    artifact_key: str
    pulp_href: str
    sha256: str
    distribution_url: str


def artifact_pulp_labels(artifact: dict[str, Any]) -> dict[str, str]:
    """Return pulp_labels dict from an artifact row (legacy ``labels`` accepted)."""
    raw = artifact.get("pulp_labels")
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if v is not None}
    raw = artifact.get("labels")
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if v is not None}
    return {}


def _normalize_sha256_digest(digest: str) -> str:
    d = (digest or "").strip()
    if not d:
        return ""
    return d if d.startswith("sha256:") else f"sha256:{d}"


def parse_oci_manifest_field(raw: Any) -> OciManifestObject | None:
    """Parse oci_manifest from string or object form."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        ref = str(raw.get("ref") or "").strip()
        digest = _normalize_sha256_digest(str(raw.get("digest") or ""))
        if ref:
            return OciManifestObject(ref=ref, digest=digest)
        return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        ref, digest = s, ""
        if "@" in s:
            ref, digest = s.rsplit("@", 1)
        return OciManifestObject(ref=ref, digest=_normalize_sha256_digest(digest))
    return None


def _coerce_schema_version(raw: Any) -> str:
    if raw is None:
        return PULP_RESULTS_SCHEMA_VERSION
    if isinstance(raw, int):
        return f"{raw}.0.0" if raw >= 1 else PULP_RESULTS_SCHEMA_VERSION
    text = str(raw).strip()
    if not text:
        return PULP_RESULTS_SCHEMA_VERSION
    if text.isdigit():
        return f"{text}.0.0"
    return text


def _try_schema_version(document: dict[str, Any]) -> str:
    return _coerce_schema_version(document.get("version"))


def _parse_last_updated_value(raw: Any) -> date | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    fragment = raw.strip()[:10]
    try:
        return date.fromisoformat(fragment)
    except ValueError:
        return None


def _try_document_last_updated(document: dict[str, Any]) -> str:
    raw = document.get("last_updated")
    if isinstance(raw, str) and raw.strip():
        parsed = _parse_last_updated_value(raw)
        if parsed is not None:
            return parsed.isoformat()
    return date.today().isoformat()


def _optional_str_field(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _normalize_href_history_entry(entry: dict[str, Any]) -> None:
    if "document_version" in entry:
        dv = entry.pop("document_version")
        if "schema_version" not in entry:
            entry["schema_version"] = _coerce_schema_version(dv)
    entry.pop("revision", None)
    if "schema_version" in entry:
        entry["schema_version"] = _coerce_schema_version(entry.get("schema_version"))
    elif "schema_version" not in entry:
        entry["schema_version"] = PULP_RESULTS_SCHEMA_VERSION


def document_last_updated(document: dict[str, Any]) -> str:
    """Return document ``last_updated`` (ISO date), defaulting to today when unset."""
    return _try_document_last_updated(document)


def schema_version(document: dict[str, Any]) -> str:
    """Return JSON schema ``version`` semver (default ``PULP_RESULTS_SCHEMA_VERSION``)."""
    return _try_schema_version(document)


def parse_last_updated(raw: Any) -> date | None:
    """Parse ``last_updated`` or ISO datetime prefix to ``date``."""
    return _parse_last_updated_value(raw)


def touch_document_last_updated(document: dict[str, Any], *, when: date | None = None) -> str:
    """Set ``last_updated`` to today (or ``when``) and return the ISO date string."""
    value = (when or date.today()).isoformat()
    document["last_updated"] = value
    document.pop("revision", None)
    return value


def oci_manifest_ref(document: dict[str, Any]) -> str:
    """
    Legacy helper: digest-pinned ``ref@sha256:…`` from embedded ``oci_manifest`` on old JSON only.

    Canonical serialized JSON does not include ``oci_manifest`` or OCI history fields.
    """
    parsed = parse_oci_manifest_field(document.get("oci_manifest"))
    if not parsed:
        return ""
    if parsed.digest:
        return f"{parsed.ref}@{parsed.digest}"
    return parsed.ref


def normalize_document(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Deep-copy and normalize to canonical in-memory shape (``pulp_labels``, no OCI registry fields).

    Legacy ``labels`` and embedded ``oci_manifest`` / ``oci_manifest_history`` are stripped on read.
    """
    document = copy.deepcopy(raw)
    if document.get("version") is None:
        document["version"] = PULP_RESULTS_SCHEMA_VERSION
    document.pop("revision", None)
    if document.get("last_updated") is None:
        document["last_updated"] = date.today().isoformat()

    document.pop("oci_manifest", None)
    document.pop("oci_manifest_history", None)
    document["version"] = _try_schema_version(document)
    document["last_updated"] = _try_document_last_updated(document)

    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        document["artifacts"] = {}
        artifacts = document["artifacts"]

    for key, row in list(artifacts.items()):
        if not isinstance(row, dict):
            continue
        labels = artifact_pulp_labels(row)
        if labels:
            row["pulp_labels"] = labels
        row.pop("labels", None)
        if not isinstance(row.get("href_history"), list):
            row["href_history"] = []
        else:
            for entry in row["href_history"]:
                if isinstance(entry, dict):
                    _normalize_href_history_entry(entry)

    if document.get("distributions") is not None and not isinstance(document.get("distributions"), dict):
        document["distributions"] = {}

    return document


def replace_document_from_normalized(document: dict[str, Any], normalized: dict[str, Any]) -> None:
    """Replace ``document`` contents with a normalized deep copy."""
    document.clear()
    document.update(normalized)


def document_to_canonical_dict(document: dict[str, Any]) -> dict[str, Any]:
    """Return a canonical dict suitable for JSON serialization (write shape)."""
    doc = normalize_document(document)
    out: dict[str, Any] = {
        "version": _try_schema_version(doc),
        "last_updated": _try_document_last_updated(doc),
        "artifacts": doc.get("artifacts") or {},
    }
    for field in ("build_id", "namespace", "cluster"):
        val = doc.get(field)
        if isinstance(val, str) and val.strip():
            out[field] = val.strip()
    distributions = doc.get("distributions")
    if isinstance(distributions, dict) and distributions:
        out["distributions"] = {str(k): str(v) for k, v in distributions.items() if v}
    return out


def document_to_canonical_json(document: dict[str, Any]) -> str:
    """Serialize canonical document to indented JSON."""
    return json.dumps(document_to_canonical_dict(document), indent=2)


def resolve_predecessor_href(artifact: dict[str, Any]) -> str:
    """Return the immediate predecessor content href for lineage (artifact href or labels)."""
    href = (artifact.get("href") or "").strip()
    if href:
        return href
    labels = artifact_pulp_labels(artifact)
    for key in ("source_pulp_href", "pulp_href"):
        value = labels.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def merge_signed_by(existing: str, new: str, *, replace: bool = False) -> str:
    """Merge semicolon-separated signed_by values (default) or replace when ``replace`` is True."""
    new_val = (new or "").strip()
    if replace or not (existing or "").strip():
        return new_val
    if not new_val:
        return (existing or "").strip()
    parts = [p.strip() for p in existing.split(";") if p.strip()]
    if new_val not in parts:
        parts.append(new_val)
    return ";".join(parts)


def merge_origin_pulp_labels(
    labels: dict[str, str],
    *,
    namespace: str | None,
    build_id: str | None,
    cluster: str | None,
) -> dict[str, str]:
    """Set origin_* labels once; never overwrite existing origin_* keys."""
    merged = dict(labels)
    if namespace and not merged.get("origin_namespace"):
        merged["origin_namespace"] = namespace
    if build_id and not merged.get("origin_build_id"):
        merged["origin_build_id"] = build_id
    if cluster and not merged.get("origin_cluster"):
        merged["origin_cluster"] = cluster
    return merged


def side_tag_upload_labels(
    labels: dict[str, str],
    *,
    side_tag: str,
    source_pulp_href: str,
    namespace: str | None,
    build_id: str | None,
    cluster: str | None,
) -> dict[str, str]:
    """Build pulp_labels for content uploaded to a side-tag RPM repository."""
    merged = merge_origin_pulp_labels(labels, namespace=namespace, build_id=build_id, cluster=cluster)
    merged["side_tag"] = side_tag
    if source_pulp_href:
        merged["source_pulp_href"] = source_pulp_href
    return merged


def _artifact_distributions_dict(artifact: dict[str, Any]) -> dict[str, str]:
    raw = artifact.get("distributions")
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items() if v}


def recompute_distributions(
    artifact: dict[str, Any],
    verified_slots: set[str],
    *,
    slot_updates: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Keep per-artifact distribution slots in ``verified_slots``; prune stale slots.

    ``slot_updates`` merges new/updated slot URLs after pruning.
    """
    merged = {k: v for k, v in _artifact_distributions_dict(artifact).items() if k in verified_slots}
    if slot_updates:
        for slot, url in slot_updates.items():
            if slot in verified_slots and url:
                merged[slot] = url
    return merged


def aggregate_top_level_distributions(document: dict[str, Any]) -> dict[str, str]:
    """Optional aggregate ``distributions`` from per-artifact slots (for pull backward compat)."""
    aggregate: dict[str, str] = {}
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        return aggregate
    for row in artifacts.values():
        if not isinstance(row, dict):
            continue
        for slot, url in _artifact_distributions_dict(row).items():
            if url and slot not in aggregate:
                aggregate[slot] = url
    existing = document.get("distributions")
    if isinstance(existing, dict):
        for slot, url in existing.items():
            if url and slot not in aggregate:
                aggregate[str(slot)] = str(url)
    return aggregate


def _retain_distribution_slots(
    artifact: dict[str, Any],
    prior_href: str,
    merged_distributions: dict[str, str],
) -> dict[str, str]:
    """
    Keep per-artifact distribution slots that still point at the same content href.

    Slots are retained when the artifact href is unchanged from ``prior_href``.
    """
    current_href = (artifact.get("href") or "").strip() or prior_href
    if prior_href and current_href != prior_href:
        return merged_distributions
    for slot, url in _artifact_distributions_dict(artifact).items():
        if slot not in merged_distributions and url:
            merged_distributions[slot] = url
    return merged_distributions


def bump_document_version(document: dict[str, Any]) -> str:
    """Deprecated alias: updates ``last_updated`` to today (schema ``version`` unchanged)."""
    return touch_document_last_updated(document)


def append_href_history(
    artifact: dict[str, Any],
    *,
    prior_href: str,
    prior_sha256: str,
    last_updated: str,
    schema_version_value: str = PULP_RESULTS_SCHEMA_VERSION,
    operation: str = SIDE_TAG_TRANSFER_OPERATION,
) -> None:
    """Append a href_history entry when prior_href is set."""
    if not prior_href:
        return
    labels = artifact_pulp_labels(artifact)
    signed_by = (labels.get("signed_by") or "").strip()
    entry = HrefHistoryEntry(
        href=prior_href,
        sha256=prior_sha256 or "",
        operation=operation,
        last_updated=last_updated,
        schema_version=schema_version_value,
        signed_by=signed_by,
    )
    history = artifact.get("href_history")
    if not isinstance(history, list):
        history = []
    history.append(entry.model_dump())
    artifact["href_history"] = history


def append_all_artifact_href_histories(
    document: dict[str, Any],
    *,
    last_updated: str,
    schema_version_value: str = PULP_RESULTS_SCHEMA_VERSION,
    operation: str,
) -> None:
    """Append href_history for every artifact that has a prior href."""
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        return
    for row in artifacts.values():
        if not isinstance(row, dict):
            continue
        prior_href = resolve_predecessor_href(row)
        prior_sha256 = (row.get("sha256") or "").strip()
        append_href_history(
            row,
            prior_href=prior_href,
            prior_sha256=prior_sha256,
            last_updated=last_updated,
            schema_version_value=schema_version_value,
            operation=operation,
        )


def prepare_document_for_mutation(document: dict[str, Any], *, operation: str) -> str:
    """
    Before an ORAS republish: record prior artifact hrefs, set ``last_updated`` to today.

    Schema ``version`` is unchanged. Returns ``last_updated`` **before** touch (for history entries).
    """
    normalized = normalize_document(document)
    replace_document_from_normalized(document, normalized)
    schema_ver = _try_schema_version(document)
    old_last_updated = _try_document_last_updated(document)
    append_all_artifact_href_histories(
        document,
        last_updated=old_last_updated,
        schema_version_value=schema_ver,
        operation=operation,
    )
    touch_document_last_updated(document)
    return old_last_updated


def apply_side_tag_transfer_to_document(
    source_document: dict[str, Any],
    transfers: list[SideTagRpmTransfer],
    *,
    side_tag: str,
    top_level_side_tag_distribution_url: str,
) -> dict[str, Any]:
    """
    Merge side-tag RPM transfer outcomes into a copy of the source pulp_results document.

    Mainline top-level distribution slots are preserved. Per-artifact RPM rows get updated
    href/url/sha256, href_history, and a distributions slot keyed by side_tag name.
    """
    document = normalize_document(source_document)
    document = copy.deepcopy(document)
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        artifacts = {}
        document["artifacts"] = artifacts

    old_last_updated = document_last_updated(document)
    schema_ver = _try_schema_version(document)

    touch_document_last_updated(document)

    top_distributions = document.get("distributions")
    if not isinstance(top_distributions, dict):
        top_distributions = {}
    top_distributions = {str(k): str(v) for k, v in top_distributions.items() if v}
    if top_level_side_tag_distribution_url:
        top_distributions[side_tag] = top_level_side_tag_distribution_url.rstrip("/") + "/"
    document["distributions"] = top_distributions

    for transfer in transfers:
        key = transfer.artifact_key
        existing = artifacts.get(key)
        if not isinstance(existing, dict):
            existing = {"pulp_labels": {}}
        prior_href = resolve_predecessor_href(existing)
        prior_sha256 = (existing.get("sha256") or "").strip()

        append_href_history(
            existing,
            prior_href=prior_href,
            prior_sha256=prior_sha256,
            last_updated=old_last_updated,
            schema_version_value=schema_ver,
            operation=SIDE_TAG_TRANSFER_OPERATION,
        )

        existing["href"] = transfer.pulp_href
        existing["sha256"] = transfer.sha256
        existing["url"] = transfer.distribution_url

        per_dist = _artifact_distributions_dict(existing)
        per_dist[side_tag] = transfer.distribution_url
        per_dist = _retain_distribution_slots(existing, prior_href, per_dist)
        existing["distributions"] = per_dist

        artifacts[key] = existing

    return document


def document_from_artifact_json(artifact_json: Any) -> dict[str, Any]:
    """Serialize ArtifactJsonResponse (or dict) to a normalized plain document dict."""
    if hasattr(artifact_json, "model_dump"):
        raw = artifact_json.model_dump(mode="json", exclude_none=True)
        return normalize_document(raw)
    if isinstance(artifact_json, dict):
        return normalize_document(artifact_json)
    raise TypeError(f"Unsupported artifact_json type: {type(artifact_json)}")


def initial_document_shell(
    *,
    build_id: str,
    namespace: str,
    cluster: str | None = None,
) -> dict[str, Any]:
    """Empty schema v1 document shell for first upload-build publish."""
    doc: dict[str, Any] = {
        "version": PULP_RESULTS_SCHEMA_VERSION,
        "last_updated": date.today().isoformat(),
        "build_id": build_id,
        "namespace": namespace,
        "artifacts": {},
    }
    if cluster and cluster.strip():
        doc["cluster"] = cluster.strip()
    return doc


def merge_upload_outcomes_into_document(
    document: dict[str, Any],
    upload: PulpResultsDocument,
    *,
    operation: str,
    signed_by: str | None,
    replace_signed_by: bool,
    verified_distribution_slots: set[str],
) -> None:
    """Apply upload session artifacts onto an existing results document (in place)."""
    normalize_document(document)
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        artifacts = {}
        document["artifacts"] = artifacts
    old_last_updated = document_last_updated(document)
    schema_ver = schema_version(document)

    for key, info in upload.artifacts.items():
        row = artifacts.get(key)
        if not isinstance(row, dict):
            row = {}
            artifacts[key] = row
        prior_href = resolve_predecessor_href(row)
        prior_sha256 = (row.get("sha256") or "").strip()
        new_href = (info.href or "").strip()
        if new_href and new_href != prior_href:
            append_href_history(
                row,
                prior_href=prior_href or (row.get("href") or "").strip(),
                prior_sha256=prior_sha256,
                last_updated=old_last_updated,
                schema_version_value=schema_ver,
                operation=operation,
            )
        if info.url:
            row["url"] = info.url
        if info.sha256:
            row["sha256"] = info.sha256
        if new_href:
            row["href"] = new_href
        labels = artifact_pulp_labels(row)
        labels.update({k: v for k, v in info.labels.items() if k != "signed_by"})
        new_signed = (info.labels.get("signed_by") or "").strip()
        if new_signed:
            labels["signed_by"] = merge_signed_by(
                labels.get("signed_by", ""),
                new_signed,
                replace=replace_signed_by,
            )
        elif signed_by:
            labels["signed_by"] = merge_signed_by(
                labels.get("signed_by", ""),
                signed_by,
                replace=replace_signed_by,
            )
        row["pulp_labels"] = labels
        slot_updates = {str(k): str(v) for k, v in (info.distributions or {}).items() if v}
        if slot_updates or row.get("distributions"):
            row["distributions"] = recompute_distributions(row, verified_distribution_slots, slot_updates=slot_updates)

    document["distributions"] = aggregate_top_level_distributions(document)


def _artifact_row_to_dict(meta: ArtifactMetadata) -> dict[str, Any]:
    row = meta.model_dump(mode="json", by_alias=True)
    if meta.href:
        row["href"] = meta.href
    if meta.distributions:
        row["distributions"] = dict(meta.distributions)
    if meta.href_history:
        row["href_history"] = list(meta.href_history)
    return row


class PulpResultsDocument(KonfluxBaseModel):
    """
    Unified ``pulp_results.json`` document and upload session state.

    Canonical JSON fields serialize via :meth:`to_canonical_dict`. Session-only fields
    (``repositories``, ``uploaded_counts``, ``upload_errors``) are excluded from export.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    version: str = PULP_RESULTS_SCHEMA_VERSION
    last_updated: str = Field(default_factory=lambda: date.today().isoformat())
    build_id: str = ""
    namespace: str | None = None
    cluster: str | None = None
    artifacts: dict[str, ArtifactMetadata] = Field(default_factory=dict)
    distributions: dict[str, str] = Field(default_factory=dict)
    repositories: RepositoryRefs | None = None
    uploaded_counts: UploadCounts = Field(default_factory=UploadCounts)
    upload_errors: list[str] = Field(default_factory=list)

    _lock: Lock = PrivateAttr(default_factory=Lock)

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_document_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            d = dict(data)
            d["version"] = _coerce_schema_version(d.get("version"))
            dist = d.get("distributions")
            if isinstance(dist, dict):
                d["distributions"] = {str(k): str(v) for k, v in dist.items() if v is not None}
            return d
        return data

    @classmethod
    def from_raw(cls, raw: dict[str, Any], *, repositories: RepositoryRefs | None = None) -> Self:
        normalized = normalize_document(raw)
        artifacts: dict[str, ArtifactMetadata] = {}
        raw_arts = normalized.get("artifacts")
        if isinstance(raw_arts, dict):
            for key, row in raw_arts.items():
                if isinstance(row, dict):
                    artifacts[key] = ArtifactMetadata.model_validate(row)
        dist_raw = normalized.get("distributions")
        distributions = {str(k): str(v) for k, v in dist_raw.items() if v} if isinstance(dist_raw, dict) else {}
        return cls(
            version=_try_schema_version(normalized),
            last_updated=_try_document_last_updated(normalized),
            build_id=str(normalized.get("build_id") or "").strip(),
            namespace=_optional_str_field(normalized.get("namespace")),
            cluster=_optional_str_field(normalized.get("cluster")),
            artifacts=artifacts,
            distributions=distributions,
            repositories=repositories,
        )

    @classmethod
    def from_artifact_json(cls, artifact_json: Any, *, repositories: RepositoryRefs | None = None) -> Self:
        if hasattr(artifact_json, "model_dump"):
            raw = artifact_json.model_dump(mode="json", exclude_none=True)
            return cls.from_raw(raw, repositories=repositories)
        if isinstance(artifact_json, dict):
            return cls.from_raw(artifact_json, repositories=repositories)
        raise TypeError(f"Unsupported artifact_json type: {type(artifact_json)}")

    @classmethod
    def empty_shell(
        cls,
        *,
        build_id: str,
        namespace: str,
        cluster: str | None = None,
        repositories: RepositoryRefs | None = None,
    ) -> Self:
        return cls.from_raw(
            initial_document_shell(build_id=build_id, namespace=namespace, cluster=cluster),
            repositories=repositories,
        )

    def _mutable_dict(self) -> dict[str, Any]:
        arts: dict[str, Any] = {}
        for key, meta in self.artifacts.items():
            arts[key] = _artifact_row_to_dict(meta)
        out: dict[str, Any] = {
            "version": self.version,
            "last_updated": self.last_updated,
            "build_id": self.build_id,
            "artifacts": arts,
            "distributions": dict(self.distributions),
        }
        if self.namespace:
            out["namespace"] = self.namespace
        if self.cluster:
            out["cluster"] = self.cluster
        return out

    def _apply_mutable_dict(self, data: dict[str, Any]) -> None:
        updated = self.from_raw(data, repositories=self.repositories)
        self.version = updated.version
        self.last_updated = updated.last_updated
        self.build_id = updated.build_id
        self.namespace = updated.namespace
        self.cluster = updated.cluster
        self.artifacts = updated.artifacts
        self.distributions = updated.distributions

    def normalize_in_place(self) -> None:
        self._apply_mutable_dict(self._mutable_dict())

    def prepare_for_mutation(self, *, operation: str) -> str:
        data = self._mutable_dict()
        old = prepare_document_for_mutation(data, operation=operation)
        self._apply_mutable_dict(data)
        return old

    def touch_last_updated(self, *, when: date | None = None) -> str:
        data = self._mutable_dict()
        value = touch_document_last_updated(data, when=when)
        self._apply_mutable_dict(data)
        return value

    def apply_side_tag_transfer(
        self,
        transfers: list[SideTagRpmTransfer],
        *,
        side_tag: str,
        top_level_side_tag_distribution_url: str,
    ) -> PulpResultsDocument:
        merged = apply_side_tag_transfer_to_document(
            self._mutable_dict(),
            transfers,
            side_tag=side_tag,
            top_level_side_tag_distribution_url=top_level_side_tag_distribution_url,
        )
        return PulpResultsDocument.from_raw(merged, repositories=self.repositories)

    def merge_upload_outcomes(
        self,
        upload: PulpResultsDocument,
        *,
        operation: str,
        signed_by: str | None,
        replace_signed_by: bool,
        verified_distribution_slots: set[str],
    ) -> None:
        data = self._mutable_dict()
        merge_upload_outcomes_into_document(
            data,
            upload,
            operation=operation,
            signed_by=signed_by,
            replace_signed_by=replace_signed_by,
            verified_distribution_slots=verified_distribution_slots,
        )
        self._apply_mutable_dict(data)

    def to_canonical_dict(
        self,
        *,
        namespace: str | None = None,
        cluster: str | None = None,
    ) -> dict[str, Any]:
        ns = namespace if namespace is not None else self.namespace
        cl = cluster if cluster is not None else self.cluster
        artifacts_out: dict[str, Any] = {}
        for key, info in self.artifacts.items():
            labels = merge_origin_pulp_labels(
                dict(info.labels),
                namespace=ns,
                build_id=self.build_id or None,
                cluster=cl,
            )
            row: dict[str, Any] = {
                "pulp_labels": labels,
                "url": info.url,
                "sha256": info.sha256 or "",
                "href_history": list(info.href_history or []),
            }
            if info.href:
                row["href"] = info.href
            if info.distributions:
                row["distributions"] = dict(info.distributions)
            artifacts_out[key] = row
        dist_map = dict(self.distributions)
        if not dist_map and artifacts_out:
            dist_map = aggregate_top_level_distributions({"artifacts": artifacts_out})
        out: dict[str, Any] = {
            "version": self.version or PULP_RESULTS_SCHEMA_VERSION,
            "last_updated": self.last_updated or date.today().isoformat(),
            "build_id": self.build_id,
            "artifacts": artifacts_out,
            "distributions": dict(sorted(dist_map.items())),
        }
        if ns:
            out["namespace"] = ns
        if cl and str(cl).strip():
            out["cluster"] = str(cl).strip()
        out["distributions"] = dict(sorted(dist_map.items()))
        return out

    def to_canonical_json(
        self,
        *,
        namespace: str | None = None,
        cluster: str | None = None,
    ) -> str:
        return json.dumps(self.to_canonical_dict(namespace=namespace, cluster=cluster), indent=2)

    def to_json_dict(
        self,
        *,
        namespace: str | None = None,
        cluster: str | None = None,
    ) -> dict[str, Any]:
        """Alias for :meth:`to_canonical_dict` (upload-collect compatibility)."""
        return self.to_canonical_dict(namespace=namespace, cluster=cluster)

    def oci_manifest_ref(self) -> str:
        return oci_manifest_ref(self._mutable_dict())

    def validate_for_pull(self) -> None:
        if not self.artifacts:
            raise ValueError("artifacts must contain at least one entry")
        for name, meta in self.artifacts.items():
            if not meta.url:
                raise ValueError(f"artifact {name!r} must include a non-empty http(s) url for pull")
            lower = meta.url.lower()
            if not (lower.startswith("http://") or lower.startswith("https://")):
                raise ValueError(f"artifact {name!r} url must be an http or https URL for pull")
            if not meta.sha256:
                raise ValueError(f"artifact {name!r} must include sha256 for pull")
            from ..utils.checksum_verify import validate_sha256_hex

            validate_sha256_hex(meta.sha256, field_name=f"artifact {name!r} sha256")

    def add_artifact(self, key: str, url: str, sha256: str, labels: dict[str, str]) -> None:
        with self._lock:
            self.artifacts[key] = ArtifactMetadata(labels=labels, url=url, sha256=sha256)

    def add_distribution(self, repo_type: str, url: str) -> None:
        with self._lock:
            coerced = TypeAdapter(AnyHttpUrl).validate_python(url)
            self.distributions = {**self.distributions, repo_type: str(coerced)}

    def increment_counts(self, *, rpms: int = 0, logs: int = 0, sboms: int = 0, files: int = 0) -> None:
        with self._lock:
            if rpms:
                self.uploaded_counts.rpms += rpms
            if logs:
                self.uploaded_counts.logs += logs
            if sboms:
                self.uploaded_counts.sboms += sboms
            if files:
                self.uploaded_counts.files += files

    def add_error(self, error: str) -> None:
        with self._lock:
            self.upload_errors.append(error)

    @property
    def total_uploaded(self) -> int:
        return self.uploaded_counts.total

    @property
    def has_errors(self) -> bool:
        return len(self.upload_errors) > 0

    @property
    def error_count(self) -> int:
        return len(self.upload_errors)

    @property
    def artifact_count(self) -> int:
        return len(self.artifacts)

    @property
    def has_distributions(self) -> bool:
        return bool(self.distributions)

    def get_artifact(self, name: str) -> ArtifactMetadata | None:
        return self.artifacts.get(name)

    @property
    def rpms_distribution_url(self) -> str | None:
        return self.distributions.get("rpms")

    @property
    def logs_distribution_url(self) -> str | None:
        return self.distributions.get("logs")

    @property
    def sbom_distribution_url(self) -> str | None:
        return self.distributions.get("sbom")


# Backward-compatible alias during migration (prefer PulpResultsDocument).
PulpResultsModel = PulpResultsDocument
ArtifactJsonResponse = PulpResultsDocument


__all__ = [
    "PULP_RESULTS_ORAS_MEDIA_TYPE",
    "BUILD_UPLOAD_OPERATION",
    "BUILD_SIGN_OPERATION",
    "RELEASE_SIGN_OPERATION",
    "BTS_UPDATE_OPERATION",
    "SIDE_TAG_TRANSFER_OPERATION",
    "HrefHistoryEntry",
    "OciManifestObject",
    "SideTagRpmTransfer",
    "artifact_pulp_labels",
    "parse_oci_manifest_field",
    "oci_manifest_ref",
    "normalize_document",
    "replace_document_from_normalized",
    "document_to_canonical_dict",
    "document_to_canonical_json",
    "resolve_predecessor_href",
    "merge_signed_by",
    "merge_origin_pulp_labels",
    "side_tag_upload_labels",
    "recompute_distributions",
    "aggregate_top_level_distributions",
    "PULP_RESULTS_SCHEMA_VERSION",
    "schema_version",
    "document_last_updated",
    "parse_last_updated",
    "touch_document_last_updated",
    "bump_document_version",
    "append_href_history",
    "append_all_artifact_href_histories",
    "prepare_document_for_mutation",
    "apply_side_tag_transfer_to_document",
    "document_from_artifact_json",
    "initial_document_shell",
    "merge_upload_outcomes_into_document",
    "PulpResultsDocument",
    "PulpResultsModel",
    "ArtifactJsonResponse",
]
