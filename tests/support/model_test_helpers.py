"""Shared helpers for model tests."""

from typing import Dict, cast

from pydantic import AnyHttpUrl


def _http_url(url: str) -> AnyHttpUrl:
    """String literal where models expect AnyHttpUrl (Pydantic still validates on construct)."""

    return cast(AnyHttpUrl, url)


def _dist_map(urls: Dict[str, str]) -> Dict[str, AnyHttpUrl]:
    """Dict literal for ArtifactJsonResponse.distributions (values are coerced at runtime)."""

    return cast(Dict[str, AnyHttpUrl], urls)
