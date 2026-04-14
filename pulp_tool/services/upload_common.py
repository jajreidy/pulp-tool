"""Shared helpers for upload service modules (avoids circular imports)."""

from typing import Dict

from ..models.context import UploadContext
from ..utils import PulpHelper


def _distribution_urls_for_context(helper: PulpHelper, build_id: str, context: UploadContext) -> Dict[str, str]:
    """Resolve distribution URL map for results JSON (per-arch vs signed aggregate RPM base)."""
    return helper.get_distribution_urls_for_upload_context(build_id, context)


__all__ = ["_distribution_urls_for_context"]
