"""Tests for Pulp label normalization."""

from pulp_tool.models.pulp_label_values import normalize_signed_by_value_for_pulp


def test_normalize_signed_by_passthrough_simple() -> None:
    assert normalize_signed_by_value_for_pulp("key-123") == "key-123"
    assert normalize_signed_by_value_for_pulp("Signer Name") == "Signer Name"


def test_normalize_signed_by_parentheses() -> None:
    raw = "Some Org (signing key) <keys@example.com>"
    assert normalize_signed_by_value_for_pulp(raw) == "Some Org [signing key] <keys@example.com>"


def test_normalize_signed_by_comma() -> None:
    assert normalize_signed_by_value_for_pulp("org, fingerprint-abc") == "org: fingerprint-abc"


def test_normalize_signed_by_utf8_stable() -> None:
    assert normalize_signed_by_value_for_pulp("Ключ (релиз)") == "Ключ [релиз]"
