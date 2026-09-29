"""HTTP fetch allowlist for Pulp distribution downloads (SSRF mitigation)."""

from __future__ import annotations

from urllib.parse import urlparse


def _default_port(scheme: str) -> int:
    if scheme == "https":
        return 443
    if scheme == "http":
        return 80
    return 0


def _origin_key(url: str) -> tuple[str, str, int]:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"URL scheme must be http or https, got {parsed.scheme!r}")
    if not parsed.hostname:
        raise ValueError(f"URL must include a host: {url}")
    port = parsed.port if parsed.port is not None else _default_port(parsed.scheme)
    return parsed.scheme, parsed.hostname.lower(), port


def assert_pulp_content_fetch_url(file_url: str, pulp_api_base_url: str) -> None:
    """
    Require ``file_url`` to target the configured Pulp API host with a pulp-content path.

    Args:
        file_url: Remote URL to fetch (metadata or artifact bytes)
        pulp_api_base_url: ``cli.base_url`` from config (Pulp API root, no path suffix required)

    Raises:
        ValueError: When the URL is not allowed
    """
    if not pulp_api_base_url or not pulp_api_base_url.strip():
        raise ValueError("pulp_api_base_url is required to validate remote fetch URLs")
    target = urlparse(file_url.strip())
    if target.scheme not in ("http", "https"):
        raise ValueError(f"Refusing non-HTTP(S) fetch URL: {file_url}")
    path = target.path or ""
    if not path.startswith("/api/pulp-content/"):
        raise ValueError(f"Refusing fetch outside Pulp content API (expected path /api/pulp-content/...): {file_url}")
    if _origin_key(file_url) != _origin_key(pulp_api_base_url.rstrip("/") + "/"):
        raise ValueError(f"Refusing fetch URL on a host other than cli.base_url: {file_url}")


__all__ = ["assert_pulp_content_fetch_url"]
