"""Normal forms for Pulp content label strings (pulpcore validation rules)."""

from __future__ import annotations


def normalize_signed_by_value_for_pulp(value: str) -> str:
    """
    Return a value safe to store in ``pulp_labels.signed_by``.

    Pulpcore rejects label values that contain comma or parentheses (Kubernetes-style
    label selector parsing). Commas are replaced with colons. Parentheses are replaced
    with square brackets so typical GnuPG user-id shapes still fit in one readable string.
    The same substitution is applied for ``search-by`` so queries match uploaded content.
    """
    if "," not in value and "(" not in value and ")" not in value:
        return value
    return value.replace(",", ":").replace("(", "[").replace(")", "]")
