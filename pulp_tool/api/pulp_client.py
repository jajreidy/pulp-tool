"""
Pulp API client for managing repositories and content.

This module provides the main PulpClient class, which is composed using the
mixin pattern to provide specialized functionality:

Mixins:
    - RpmRepositoryMixin, FileRepositoryMixin: Repository creation and management
    - RpmDistributionMixin, FileDistributionMixin: Distribution management
    - RpmPackageContentMixin, FileContentMixin: Content upload and management
    - ArtifactMixin: Artifact operations
    - TaskMixin: Task monitoring with exponential backoff

The PulpClient class combines all resource-based mixins to provide a complete Pulp API interface
organized by resource type, matching Pulp's API documentation structure.

HTTP helpers (cache, chunked GET, repository_operation body) live in ``pulp_client_cache``,
``pulp_client_chunked_get``, and ``pulp_client_repository`` and are composed by this class.

Key Features:
    - OAuth2 authentication with automatic token refresh
    - Exponential backoff for task polling
    - Context-based error handling with @with_error_handling decorator
    - Type-safe operations using Pydantic models
    - Proper resource cleanup with context managers
"""

# Standard library imports
import asyncio
import json
import logging
import os
import ssl
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Third-party imports
import httpx

# Local imports
from ..exceptions import PulpToolConfigError, PulpToolHTTPError
from ..utils import create_session_with_retry
from ..utils.correlation import CORRELATION_HEADER, resolve_correlation_id
from ..utils.response_utils import content_find_results_from_response
from ..utils.artifact_detection import rpm_packages_letter_and_basename
from ..utils.constants import DEFAULT_CHUNK_SIZE, SUPPORTED_ARCHITECTURES
from ..utils.validation import sanitize_build_id_for_repository, validate_build_id
from ..utils.rpm_operations import calculate_sha256_checksum, parse_rpm_filename_to_nvr
from ..models.artifacts import ContentData, ExtraArtifactRef, FileInfoMap, PulpContentRow
from .auth import OAuth2ClientCredentialsAuth

# Resource-based mixins
from .repositories.rpm import RpmRepositoryMixin
from .repositories.file import FileRepositoryMixin
from .distributions.rpm import RpmDistributionMixin
from .distributions.file import FileDistributionMixin
from .content.rpm_packages import RpmPackageContentMixin
from .content.file_files import FileContentMixin
from .artifacts.operations import ArtifactMixin
from .tasks.operations import TaskMixin
from .pulp_client_cache import CACHE_TTL, PerformanceMetrics, TTLCache, cached_get
from .pulp_client_chunked_get import chunked_get, chunked_get_async
from .pulp_client_repository import get_single_resource_by_name, repository_operation as repository_operation_impl

import tomllib

# ============================================================================
# Constants
# ============================================================================

# Default timeout for HTTP requests (seconds)
# Increased to 120 seconds to handle slow operations like bulk content queries
DEFAULT_TIMEOUT = 120

# Placeholder request for synthetic responses (httpx.raise_for_status requires it)
_EMPTY_RESPONSE_REQUEST = httpx.Request("GET", "https://placeholder/")


def _dedupe_results_by_pulp_href(results: List[Any]) -> List[Any]:
    """Deduplicate RPM result dicts by pulp_href. Later entries overwrite earlier."""
    seen: Dict[str, Any] = {}
    for r in results:
        href = r.get("pulp_href") if isinstance(r, dict) else None
        if href:
            seen[href] = r
    return list(seen.values())


# ============================================================================
# Main Client Class
# ============================================================================


class PulpClient(
    RpmRepositoryMixin,
    FileRepositoryMixin,
    RpmDistributionMixin,
    FileDistributionMixin,
    RpmPackageContentMixin,
    FileContentMixin,
    ArtifactMixin,
    TaskMixin,
):
    """
    A client for interacting with Pulp API.

    API documentation:
    - https://docs.pulpproject.org/pulp_rpm/restapi.html
    - https://docs.pulpproject.org/pulpcore/restapi.html

    A note regarding PUT vs PATCH:
    - PUT changes all data and therefore all required fields need to be sent
    - PATCH changes only the data that we are sending

    Many methods require repository, distribution, publication, etc,
    to be the full API endpoint (called "pulp_href"), not simply their name.
    If method argument doesn't have "name" in its name, assume it expects
    pulp_href. It looks like this:
    /pulp/api/v3/publications/rpm/rpm/5e6827db-260f-4a0f-8e22-7f17d6a2b5cc/
    """

    def __init__(
        self,
        config: Dict[str, Union[str, int]],
        domain: Optional[str] = None,
        config_path: Optional[Path] = None,
        *,
        correlation_namespace: Optional[str] = None,
        correlation_build_id: Optional[str] = None,
    ) -> None:
        """Initialize the Pulp client.

        Args:
            config: Configuration dictionary from the TOML file
            domain: Optional explicit domain override
            config_path: Path to config file for resolving relative cert/key paths
            correlation_namespace: Optional CLI namespace for ``X-Correlation-ID`` derivation
            correlation_build_id: Optional CLI build id for correlation ID derivation
        """
        self.domain = domain
        self.config = config
        # Set namespace from domain or config file's domain field
        self.namespace = domain if domain else config.get("domain")
        self.config_path = config_path  # Store config path for resolving relative cert/key paths
        self._correlation_namespace = correlation_namespace
        self._correlation_build_id = correlation_build_id
        self.timeout = DEFAULT_TIMEOUT  # Used by Protocol mixins
        self._auth = None
        self._async_session: Optional[httpx.AsyncClient] = None
        self._cert_temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self._cert_paths: Optional[Tuple[str, str]] = None
        self.session = self._create_session()
        # Initialize cache for GET requests
        self._get_cache = TTLCache(ttl=CACHE_TTL)
        # Initialize performance metrics tracker
        self._metrics = PerformanceMetrics()
        logging.debug("PulpClient initialized with request caching enabled (TTL: %ds)", CACHE_TTL)

    def _require_client_cert_files_if_configured(self) -> None:
        """
        If cert+key are set for mTLS, both PEM paths must exist after resolution.

        Otherwise create_session_with_retry would silently skip loading the client cert,
        and requests would use default TLS verification only—often resulting in HTTP 403
        from gateways that require mTLS.
        """
        cert_cfg = self.config.get("cert")
        key_cfg = self.config.get("key")
        if not cert_cfg and not key_cfg:
            return
        if not cert_cfg or not key_cfg:
            logging.error(
                "Pulp config sets only one of cert/key; provide both for mTLS or omit both and use OAuth2/Basic auth."
            )
            raise ValueError(
                "Invalid Pulp TLS config: both `cert` and `key` must be set for client certificate auth, or "
                "remove both and use client_id/client_secret or username/password."
            )
        cert_path_str, key_path_str = self.cert
        cert_ok = Path(cert_path_str).is_file()
        key_ok = Path(key_path_str).is_file()
        if not cert_ok or not key_ok:
            logging.error(
                "Client TLS is configured (cert/key in config) but certificate file(s) are missing or not readable. "
                "cert=%s (ok=%s), key=%s (ok=%s). "
                "Without these files the HTTP client will not send a client certificate; the API may return HTTP 403.",
                cert_path_str,
                cert_ok,
                key_path_str,
                key_ok,
            )
            raise ValueError(
                f"Certificate or key file not found or not a file: cert={cert_path_str!s}, key={key_path_str!s}. "
                "Use paths that exist in this environment (Konflux/Tekton pods must mount TLS material where "
                "the config points)."
            )

    def _create_session(self) -> httpx.Client:
        """Create a requests session with retry strategy and connection pool configuration."""
        self._require_client_cert_files_if_configured()
        # Pass cert to Client constructor if available, otherwise auth will be added per-request
        cert = self.cert if self.config.get("cert") else None
        return create_session_with_retry(cert=cert, extra_headers=self.headers)

    def _get_async_session(self) -> httpx.AsyncClient:
        """Get or create async session with optimized configuration."""
        if self._async_session is None or self._async_session.is_closed:
            self._require_client_cert_files_if_configured()
            cert = self.cert if self.config.get("cert") else None

            # Create async client with same configuration as sync client
            # Increased limits for concurrent chunked requests
            limits = httpx.Limits(
                max_keepalive_connections=20, max_connections=100  # Match sync client's connection pool
            )
            timeout = httpx.Timeout(self.timeout, connect=10.0)

            # Add compression headers and optional correlation ID (pulp-cli ``cid`` pattern)
            default_headers: Dict[str, str] = {
                "Accept-Encoding": "gzip, deflate, br",
            }
            ch = self.headers
            if ch:
                default_headers.update(ch)

            # Try to enable HTTP/2 if available, but don't fail if not
            try:
                import importlib.util  # pylint: disable=import-outside-toplevel

                use_http2 = importlib.util.find_spec("h2") is not None
            except (ImportError, AttributeError):
                use_http2 = False

            if not use_http2:
                logging.debug("HTTP/2 support not available for async client (h2 package not installed)")

            # Configure SSL context for client certificates if provided
            verify: Union[bool, ssl.SSLContext] = True
            if cert:
                # Only create SSL context if certificate files actually exist
                # This allows tests to pass fake paths without FileNotFoundError
                if os.path.exists(cert[0]) and os.path.exists(cert[1]):
                    ssl_context = ssl.create_default_context()
                    ssl_context.load_cert_chain(certfile=cert[0], keyfile=cert[1])
                    verify = ssl_context
                # If cert paths provided but files don't exist, just use default verification
                # (useful for testing where we mock the actual HTTP calls)

            # Prepare client kwargs
            client_kwargs: Dict[str, Any] = {
                "limits": limits,
                "timeout": timeout,
                "follow_redirects": True,
                "headers": default_headers,
                "http2": use_http2,
                "verify": verify,
            }

            # Add auth for non-cert authentication
            if not self.config.get("cert"):
                client_kwargs["auth"] = self.auth

            self._async_session = httpx.AsyncClient(**client_kwargs)  # type: ignore[arg-type]
        return self._async_session

    def close(self) -> None:
        """Close the session and release all connections."""
        if hasattr(self, "session") and self.session:
            self.session.close()
            logging.debug("PulpClient session closed and connections released")
        # Clear cache on close
        if hasattr(self, "_get_cache"):
            cache_size = self._get_cache.size()
            self._get_cache.clear()
            logging.debug("Cleared cache (%d entries)", cache_size)
        # Log performance metrics summary
        if hasattr(self, "_metrics"):
            self._metrics.log_summary()
        # Clean up temp cert/key dir if we created one for base64-decoded certs
        if hasattr(self, "_cert_temp_dir") and self._cert_temp_dir is not None:
            try:
                self._cert_temp_dir.cleanup()
            except OSError:
                pass
            self._cert_temp_dir = None
            self._cert_paths = None

    async def async_close(self) -> None:
        """Close the async session and release all connections."""
        if self._async_session and not self._async_session.is_closed:
            await self._async_session.aclose()
            logging.debug("PulpClient async session closed and connections released")

    def _run_async(self, coro) -> Any:
        """
        Run async coroutine and clear cached async session afterward.

        Prevents 'Event loop is closed' when multiple sync wrappers call asyncio.run()
        in sequence (e.g. search-by with checksums + filenames + signed_by).
        Each asyncio.run() creates and closes a loop; the cached httpx.AsyncClient
        becomes bound to a closed loop. Clearing the session ensures the next call
        creates a fresh client for the new loop.
        """

        async def _run_with_cleanup() -> Any:
            try:
                return await coro
            finally:
                if self._async_session and not self._async_session.is_closed:
                    await self._async_session.aclose()
                self._async_session = None

        return asyncio.run(_run_with_cleanup())

    def __enter__(self) -> "PulpClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        """Context manager exit - ensures session is closed."""
        self.close()

    async def _chunked_get_async(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_param: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs,
    ) -> httpx.Response:
        """
        Perform a GET request with chunking for large parameter lists using async.

        This is a workaround for the fact that requests with large parameter
        values using "GET" method fails with "Request Line is too large".
        Hence, this splits the parameter value into chunks of the given size,
        and makes separate async requests for each chunk concurrently.
        The results are aggregated into a single response.

        Note: - chunks are created on only one parameter at a time.
              - response object of the last chunk is returned with the aggregated results.
              - chunks are processed concurrently using asyncio for optimal performance
        """
        return await chunked_get_async(self, url, params, chunk_param, chunk_size, **kwargs)

    def _chunked_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_param: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs,
    ) -> httpx.Response:
        """
        Synchronous wrapper for _chunked_get_async.

        Provides a synchronous interface while using async implementation
        underneath for better performance.
        """
        return chunked_get(self, url, params, chunk_param, chunk_size, **kwargs)

    @classmethod
    def create_from_config_file(
        cls,
        path: Optional[str] = None,
        domain: Optional[str] = None,
        *,
        correlation_namespace: Optional[str] = None,
        correlation_build_id: Optional[str] = None,
    ) -> "PulpClient":
        """
        Create a Pulp client from a standard configuration file that is
        used by the `pulp` CLI tool.

        The namespace/domain will be read from the config file's 'domain' field.

        Args:
            path: Path to config file or base64-encoded config content. If None, uses default path.
            domain: Optional domain override
            correlation_namespace: Optional CLI namespace for ``X-Correlation-ID`` derivation
            correlation_build_id: Optional CLI build id for correlation ID derivation
        """
        from ..utils.config_utils import load_config_content

        config_source = path or "~/.config/pulp/cli.toml"
        config_bytes, is_base64 = load_config_content(config_source)

        # Parse TOML from bytes
        try:
            config = tomllib.loads(config_bytes.decode("utf-8"))
        except tomllib.TOMLDecodeError as e:
            source_desc = "base64 config" if is_base64 else str(Path(config_source).expanduser())
            raise PulpToolConfigError(f"Invalid TOML in configuration {source_desc}: {e}") from e

        # For base64 config, config_path is None (no file path)
        # For file path, use the expanded path
        config_path = None if is_base64 else Path(config_source).expanduser()

        return cls(
            config["cli"],
            domain,
            config_path=config_path,
            correlation_namespace=correlation_namespace,
            correlation_build_id=correlation_build_id,
        )

    @property
    def headers(self) -> Optional[Dict[str, str]]:
        """
        Optional request headers (e.g. ``X-Correlation-ID`` for log correlation).

        Resolution order: ``cli.correlation_id`` in config > ``PULP_TOOL_CORRELATION_ID`` env >
        ``namespace/build_id`` from CLI kwargs > ``build_id`` alone.
        """
        cid = resolve_correlation_id(
            config_value=self.config.get("correlation_id"),
            namespace=self._correlation_namespace,
            build_id=self._correlation_build_id,
        )
        if not cid:
            return None
        return {CORRELATION_HEADER: cid}

    @property
    def auth(self) -> Union[OAuth2ClientCredentialsAuth, httpx.BasicAuth]:
        """
        Get authentication credentials.

        Supports OAuth2 (client_id/client_secret) or Basic Auth (username/password).
        OAuth2 is preferred when both credential types are present.

        Returns:
            OAuth2ClientCredentialsAuth or BasicAuth instance for API authentication
        """
        if not self._auth:
            client_id = self.config.get("client_id")
            client_secret = self.config.get("client_secret")
            username = self.config.get("username")
            password = self.config.get("password")

            # Prefer OAuth2 if both client_id and client_secret are set
            if client_id and client_secret:
                token_url = (
                    "https://sso.redhat.com/auth/realms/redhat-external/"
                    "protocol/openid-connect/token"  # nosec B105
                )
                self._auth = OAuth2ClientCredentialsAuth(  # type: ignore[assignment]
                    client_id=str(client_id),
                    client_secret=str(client_secret),
                    token_url=token_url,
                )
            # Fall back to Basic Auth (username/password) for packages.redhat.com
            elif username is not None and password is not None:
                self._auth = httpx.BasicAuth(str(username), str(password))  # type: ignore[assignment]
            else:
                missing = []
                if not (client_id and client_secret):
                    missing.append("client_id/client_secret (OAuth2)")
                if username is None or password is None:
                    missing.append("username/password (Basic Auth)")
                raise PulpToolConfigError(
                    f"Authentication credentials missing. Provide either: {', or '.join(missing)}. "
                    "See README for configuration."
                )
        return self._auth  # type: ignore[return-value]

    @property
    def cert(self) -> Tuple[str, str]:
        """
        Get client certificate information.

        If cert/key paths are not absolute and config_path is available,
        tries to resolve them relative to the config file's directory.
        If cert or key file content is base64-encoded, decodes it and uses
        temporary files so the SSL context receives valid paths.

        Returns:
            Tuple of (cert_path, key_path) for client certificate authentication
        """
        from ..utils.config_utils import load_file_content_maybe_base64

        # Return cached paths when we previously wrote base64-decoded content to temp files
        if self._cert_paths is not None:
            return self._cert_paths

        cert_path_str = str(self.config.get("cert"))
        key_path_str = str(self.config.get("key"))

        # Try to resolve relative paths if config_path is available
        if self.config_path and self.config_path.parent:
            cert_path = Path(cert_path_str)
            key_path = Path(key_path_str)

            # If cert path is not absolute and doesn't exist, try relative to config
            if not cert_path.is_absolute() and not cert_path.exists():
                potential_cert = self.config_path.parent / cert_path
                if potential_cert.exists():
                    cert_path_str = str(potential_cert)

            # If key path is not absolute and doesn't exist, try relative to config
            if not key_path.is_absolute() and not key_path.exists():
                potential_key = self.config_path.parent / key_path
                if potential_key.exists():
                    key_path_str = str(potential_key)

        cert_path = Path(cert_path_str)
        key_path = Path(key_path_str)
        if (
            cert_path_str in ("None", "")
            or key_path_str in ("None", "")
            or not cert_path.exists()
            or not key_path.exists()
        ):
            return (cert_path_str, key_path_str)

        cert_bytes, cert_was_base64 = load_file_content_maybe_base64(cert_path)
        key_bytes, key_was_base64 = load_file_content_maybe_base64(key_path)

        if cert_was_base64 or key_was_base64:
            self._cert_temp_dir = tempfile.TemporaryDirectory()
            temp_dir = Path(self._cert_temp_dir.name)
            temp_cert = temp_dir / "cert.pem"
            temp_key = temp_dir / "key.pem"
            temp_cert.write_bytes(cert_bytes)
            temp_key.write_bytes(key_bytes)
            self._cert_paths = (str(temp_cert), str(temp_key))
            logging.debug("Using temp cert/key from base64-decoded content")
            return self._cert_paths

        return (cert_path_str, key_path_str)

    @property
    def request_params(self) -> Dict[str, Any]:
        """
        Get default parameters for requests.

        Returns:
            Dictionary containing default request parameters including
            authentication information (headers and auth, but not cert which is in Client)
        """
        params = {}
        if self.headers:
            params["headers"] = self.headers
        # Note: cert is passed to Client constructor, not per-request
        # Only add auth if not using cert-based authentication
        if not self.config.get("cert"):
            params["auth"] = self.auth  # type: ignore[assignment]
        return params

    def _url(self, endpoint: str) -> str:
        """
        Build a fully qualified URL for a given API endpoint.

        Args:
            endpoint: API endpoint path (e.g., "api/v3/repositories/rpm/rpm/")

        Returns:
            Complete URL including base URL, API root, domain, and endpoint
        """
        domain = self._get_domain()

        relative = os.path.normpath(
            "/".join(
                [
                    str(self.config["api_root"]),
                    domain,
                    endpoint,
                ]
            )
        )

        # Normpath removes the trailing slash. If it was there, put it back
        if endpoint.endswith("/"):
            relative += "/"
        return str(self.config["base_url"]) + relative

    def _get_domain(self) -> str:
        """
        Get the domain name.

        Returns:
            Domain name as configured
        """
        if self.domain:
            return self.domain
        if self.config.get("domain"):
            return str(self.config["domain"])
        return ""

    def get_domain(self) -> str:
        """Public method to get the domain name."""
        return self._get_domain()

    @cached_get
    def _get_single_resource(self, endpoint: str, name: str) -> httpx.Response:
        """
        Helper method to get a single resource by name.

        This method is cached to avoid redundant lookups of repositories/distributions.

        Args:
            endpoint: API endpoint for the resource type
            name: Name of the resource to retrieve

        Returns:
            Response object containing the resource data
        """
        return get_single_resource_by_name(self, endpoint, name)

    def _log_request_headers(self, response: httpx.Response) -> None:
        """Log request headers with sensitive data redacted."""
        if response.request and response.request.headers:
            safe_headers = dict(response.request.headers)
            # Redact sensitive headers
            for sensitive_key in ["authorization", "cookie", "x-api-key"]:
                if sensitive_key in safe_headers:
                    safe_headers[sensitive_key] = "[REDACTED]"
            logging.error("  Request Headers: %s", safe_headers)

    def _log_request_body(self, response: httpx.Response) -> None:
        """Log request body, handling different content types."""
        try:
            if response.request and response.request.content:
                try:
                    # Try to decode as text for logging
                    content = response.request.content.decode("utf-8", errors="replace")
                    # Truncate if very long
                    if len(content) > 1000:
                        logging.error("  Request Body (truncated): %s...", content[:1000])
                    else:
                        logging.error("  Request Body: %s", content)
                except Exception:
                    logging.error("  Request Body: <binary data, %d bytes>", len(response.request.content))
        except (httpx.RequestNotRead, AttributeError):
            # For streaming/multipart requests, content has already been consumed
            content_type = response.request.headers.get("content-type", "") if response.request else ""
            if "multipart" in content_type:
                logging.error("  Request Body: <multipart/form-data - file upload>")
            else:
                logging.error("  Request Body: <streaming request - content already consumed>")

    def _log_response_details(self, response: httpx.Response) -> None:
        """Log response details including headers and body."""
        logging.error("RESPONSE DETAILS:")
        logging.error("  Status Code: %s", response.status_code)
        logging.error("  Response Headers: %s", dict(response.headers))

        # Try to parse error details
        try:
            error_data = response.json()
            logging.error("  Error Data: %s", error_data)
        except (ValueError, json.JSONDecodeError):
            # Log response body at error level for 5xx errors
            if len(response.text) > 500:
                logging.error("  Response Body (truncated): %s...", response.text[:500])
            else:
                logging.error("  Response Body: %s", response.text)

    def _log_server_error(self, response: httpx.Response, operation: str) -> None:
        """Log detailed information for server errors (5xx)."""
        logging.error("=" * 80)
        logging.error("SERVER ERROR (500) during %s", operation)
        logging.error("=" * 80)

        # Request details
        logging.error("REQUEST DETAILS:")
        logging.error("  Method: %s", response.request.method if response.request else "Unknown")
        logging.error("  URL: %s", response.url)

        self._log_request_headers(response)
        self._log_request_body(response)
        self._log_response_details(response)

        logging.error("=" * 80)

    def _check_response(self, response: httpx.Response, operation: str = "request") -> None:
        """Check if a response is successful, raise exception if not."""
        if not response.is_success:
            # Server errors (5xx) are critical and should be logged as ERROR
            if response.status_code >= 500:
                self._log_server_error(response, operation)
            elif response.status_code >= 400:
                # Client errors (4xx) are logged at debug level
                logging.debug("Client error during %s: %s - %s", operation, response.status_code, response.text)
            else:
                # Other non-success responses
                logging.debug("Failed to %s: %s - %s", operation, response.status_code, response.text)

            raise PulpToolHTTPError(
                f"Failed to {operation}: {response.status_code} - {response.text}",
                response=response,
            )

    def check_response(self, response: httpx.Response, operation: str = "request") -> None:
        """Public method to check if a response is successful, raise exception if not."""
        self._check_response(response, operation)

    # ============================================================================
    # Async Methods for Repository Setup
    # ============================================================================

    def _prepare_async_kwargs(self, **kwargs: Any) -> Dict[str, Any]:
        """Merge default request params (headers, auth) into async call kwargs."""
        rp = self.request_params
        out: Dict[str, Any] = dict(kwargs)
        if "headers" in rp:
            base_h = dict(rp["headers"])
            if out.get("headers"):
                base_h.update(out["headers"])
            out["headers"] = base_h
        if "auth" in rp and "auth" not in out and rp["auth"] is not None:
            out["auth"] = rp["auth"]
        return out

    async def async_get(self, url: str, **kwargs) -> httpx.Response:
        """Async GET request."""
        client = self._get_async_session()
        return await client.get(url, **self._prepare_async_kwargs(**kwargs))

    async def async_post(self, url: str, **kwargs) -> httpx.Response:
        """Async POST request."""
        client = self._get_async_session()
        return await client.post(url, **self._prepare_async_kwargs(**kwargs))

    # ============================================================================
    # Content Management Methods (migrated from ContentManagerMixin)
    # ============================================================================

    def upload_content(
        self, file_path: str, labels: Dict[str, str], *, file_type: str, arch: Optional[str] = None
    ) -> str:
        """
        Generic file upload function with validation and error handling.

        Args:
            file_path: Path to the file to upload
            labels: Labels to attach to the uploaded content
            file_type: Type of file (e.g., 'rpm', 'file') - determines upload method
            arch: Architecture for the uploaded content (required for RPM uploads)

        Returns:
            Pulp href of the uploaded content

        Raises:
            FileNotFoundError: If the file does not exist
            PermissionError: If the file cannot be read
            ValueError: If the file is empty or arch is missing for RPMs
        """
        from ..utils import validate_file_path

        # Validate file before upload
        validate_file_path(file_path, file_type)

        try:
            # Call the appropriate upload method based on file_type
            if file_type.lower() == "rpm":
                if not arch:
                    raise ValueError("arch parameter is required for RPM uploads")
                # Handle RPM upload directly
                url = self._url("api/v3/content/rpm/packages/upload/")
                with open(file_path, "rb") as fp:
                    file_name = os.path.basename(file_path)
                    build_id = labels.get("build_id", "")

                    # Build relative_path for RPMs
                    # RPMs use only the filename as the relative_path (no build_id, no arch prefix)
                    # The distribution base_path contains namespace/parent_package/rpms
                    relative_path = file_name

                    data = {
                        "pulp_labels": json.dumps(labels),
                        "relative_path": relative_path,
                    }
                    files = {"file": fp}

                    # Log upload attempt details for debugging
                    logging.debug("Attempting RPM upload:")
                    logging.debug("  URL: %s", url)
                    logging.debug("  File: %s", file_name)
                    logging.debug("  Relative Path: %s", relative_path)
                    logging.debug("  Build ID: %s", build_id)
                    logging.debug("  Arch: %s", arch)
                    logging.debug("  Labels: %s", labels)

                    response = self.session.post(
                        url, data=data, files=files, timeout=self.timeout, **self.request_params
                    )
            else:
                # For non-RPM files, use create_file_content from FileContentMixin
                response = self.create_file_content(
                    "", file_path, build_id=labels.get("build_id", ""), pulp_label=labels, arch=arch
                )

            # Include filename in operation for better error context
            operation_context = f"upload {file_type} ({os.path.basename(file_path)})"
            self._check_response(response, operation_context)
            return response.json()["pulp_href"]

        except httpx.HTTPError:
            logging.error("Request failed for %s %s", file_type, file_path, exc_info=True)
            raise
        except Exception:
            logging.error("Unexpected error uploading %s %s", file_type, file_path, exc_info=True)
            raise

    # Note: create_file_content and add_content are inherited from FileContentMixin
    # _build_file_relative_path is also inherited from FileContentMixin

    # ============================================================================
    # Repository Management Methods (migrated from RepositoryManagerMixin)
    # ============================================================================

    def _create_resource(self, endpoint: str, request_model: Any) -> httpx.Response:
        """
        Create a resource (repository or distribution).

        Args:
            endpoint: API endpoint for resource creation
            request_model: Request model (RepositoryRequest or DistributionRequest)

        Returns:
            Response object from the creation request
        """
        url = self._url(endpoint)
        data = request_model.model_dump(exclude_none=True)
        response = self.session.post(url, json=data, timeout=self.timeout, **self.request_params)
        self._check_response(response, "create resource")
        return response

    def _create_repository(self, endpoint: str, new_repository: Any) -> httpx.Response:
        """Create a repository (delegates to _create_resource)."""
        return self._create_resource(endpoint, new_repository)

    def _create_distribution(self, endpoint: str, new_distribution: Any) -> httpx.Response:
        """Create a distribution (delegates to _create_resource)."""
        return self._create_resource(endpoint, new_distribution)

    def repository_operation(
        self,
        operation: str,
        repo_type: str,
        *,
        name: Optional[str] = None,
        repository_data: Optional[Any] = None,
        distribution_data: Optional[Any] = None,
        publication: Optional[str] = None,
        distribution_href: Optional[str] = None,
    ) -> httpx.Response:
        """
        Perform repository or distribution operations.

        Args:
            operation: Operation to perform ('create_repo', 'get_repo',
                      'create_distro', 'get_distro', 'update_distro')
            repo_type: Type of repository/distribution ('rpm' or 'file')
            name: Name of the repository/distribution (for get resource operations)
            repository_data: RepositoryRequest model for the repository to create
            distribution_data: DistributionRequest model for the distribution to create
            publication: Publication href (for update operations)
            distribution_href: Full href of distribution (for update operations)

        Returns:
            Response object from the operation
        """
        return repository_operation_impl(
            self,
            operation,
            repo_type,
            name=name,
            repository_data=repository_data,
            distribution_data=distribution_data,
            publication=publication,
            distribution_href=distribution_href,
        )

    # ============================================================================
    # Content Query Methods (migrated from ContentQueryMixin)
    # ============================================================================

    @staticmethod
    @lru_cache(maxsize=256)
    def _get_content_type_from_href(pulp_href: str) -> str:
        """
        Determine content type from pulp_href (cached for performance).

        Args:
            pulp_href: The Pulp href path

        Returns:
            Content type string (e.g., "rpm.package", "file.file", "unknown")
        """
        if "/rpm/packages/" in pulp_href:
            return "rpm.package"
        elif "/file/files/" in pulp_href:
            return "file.file"
        return "unknown"

    def _rpm_distribution_base_url_from_labels(self, labels: Dict[str, str]) -> str:
        """Build RPM distribution base URL when using ``--target-arch-repo`` (per-arch base paths)."""
        base_url_str = str(self.config["base_url"]).rstrip("/")
        pulp_content = f"{base_url_str}/api/pulp-content/"
        ns = self.namespace if isinstance(self.namespace, str) else ""
        arch = (labels.get("arch") or "").strip() or "noarch"
        arch_seg = sanitize_build_id_for_repository(arch)
        if not validate_build_id(arch_seg):
            arch_seg = "noarch"
        return f"{pulp_content}{ns}/{arch_seg}/"

    def _build_rpm_packages_url_for_arch(self, arch: str, relative_path: str) -> str:
        """Build RPM content URL under per-arch distribution (namespace/arch/Packages/...)."""
        base_url_str = str(self.config["base_url"]).rstrip("/")
        pulp_content_base_url = f"{base_url_str}/api/pulp-content"
        ns = self.namespace if isinstance(self.namespace, str) else ""
        base = f"{pulp_content_base_url}/{ns}/{arch}/"
        filename, first_letter = rpm_packages_letter_and_basename(relative_path)
        if filename:
            return f"{base}Packages/{first_letter}/{filename}"
        return relative_path

    def _build_rpm_distribution_url(
        self,
        relative_path: str,
        distribution_urls: Dict[str, str],
        labels: Optional[Dict[str, str]] = None,
        *,
        target_arch_repo: bool = False,
    ) -> str:
        """Build distribution URL for RPM artifacts (Packages/<lowercase-first-of-basename>/<basename>)."""
        labels = labels or {}
        if target_arch_repo:
            arch = (labels.get("arch") or "").strip()
            if arch and arch in SUPPORTED_ARCHITECTURES:
                return self._build_rpm_packages_url_for_arch(arch, relative_path)
            rpms_url = self._rpm_distribution_base_url_from_labels(labels)
            filename, first_letter = rpm_packages_letter_and_basename(relative_path)
            if filename:
                return f"{rpms_url}Packages/{first_letter}/{filename}"
            return relative_path
        if labels.get("signed_by") and distribution_urls.get("rpms_signed"):
            rpms_url = distribution_urls["rpms_signed"]
            if rpms_url:
                filename, first_letter = rpm_packages_letter_and_basename(relative_path)
                if filename:
                    return f"{rpms_url}Packages/{first_letter}/{filename}"
        rpms_url = distribution_urls.get("rpms", "")
        if rpms_url:
            filename, first_letter = rpm_packages_letter_and_basename(relative_path)
            if filename:
                return f"{rpms_url}Packages/{first_letter}/{filename}"
        return relative_path

    @staticmethod
    def _build_file_distribution_url(
        relative_path: str, labels: Dict[str, str], distribution_urls: Dict[str, str]
    ) -> str:
        """Build distribution URL for file artifacts (logs, SBOM, etc.)."""
        # Check if relative_path contains arch prefix
        parts = relative_path.split("/", 1)
        if len(parts) == 2:
            arch, _ = parts
            if arch in SUPPORTED_ARCHITECTURES:
                logs_url = distribution_urls.get("logs", "")
                if logs_url:
                    return f"{logs_url}{relative_path}"

        # No arch prefix - determine type from filename
        filename_lower = relative_path.lower()
        if "sbom" in filename_lower:
            sbom_url = distribution_urls.get("sbom", "")
            if sbom_url:
                return f"{sbom_url}{relative_path}"
        else:
            # Likely a log file
            logs_url = distribution_urls.get("logs", "")
            if logs_url:
                arch = labels.get("arch", "")
                if arch:
                    return f"{logs_url}{arch}/{relative_path}"
                return f"{logs_url}{relative_path}"

        return relative_path

    def _build_artifact_distribution_url(
        self,
        relative_path: str,
        is_rpm: bool,
        labels: Dict[str, str],
        distribution_urls: Dict[str, str],
        *,
        target_arch_repo: bool = False,
    ) -> str:
        """
        Build distribution URL for an artifact based on its type and relative path.

        Args:
            relative_path: Relative path of the artifact (e.g., filename or arch/filename)
            is_rpm: Whether this is an RPM artifact
            labels: Labels from content (may contain arch, etc.)
            distribution_urls: Dictionary mapping repo_type to distribution base URL
            target_arch_repo: When True, RPM URLs use per-arch paths from labels (or sanitized fallback)

        Returns:
            Full distribution URL for the artifact
        """
        if is_rpm:
            return self._build_rpm_distribution_url(
                relative_path,
                distribution_urls,
                labels=labels,
                target_arch_repo=target_arch_repo,
            )
        return PulpClient._build_file_distribution_url(relative_path, labels, distribution_urls)

    def find_content(self, search_type: str, search_value: str) -> httpx.Response:
        """
        Find content by various criteria.

        Args:
            search_type: Type of search ('build_id' or 'href')
            search_value: Value to search for

        Returns:
            Response object containing content matching the search criteria
        """
        if search_type == "build_id":
            url = self._url(f"api/v3/content/?pulp_label_select=build_id~{search_value}")
        elif search_type == "href":
            url = self._url(f"api/v3/content/?pulp_href__in={search_value}")
        else:
            raise ValueError(f"Unknown search type: {search_type}")

        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        self._check_response(response, "find content")
        return response

    def get_file_locations(self, artifacts: List[Dict[str, str]]) -> httpx.Response:
        """
        Get file locations for artifacts using the Pulp artifacts API.

        Args:
            artifacts: List of artifact dictionaries containing hrefs

        Returns:
            Response object containing file location information
        """
        hrefs = [list(artifact.values())[0] for artifact in artifacts]
        url = self._url("api/v3/artifacts/")
        params = {"pulp_href__in": ",".join(hrefs)}

        logging.debug("Querying %d artifacts from Pulp", len(hrefs))

        return self._chunked_get(
            url, params=params, chunk_param="pulp_href__in", timeout=self.timeout, chunk_size=20, **self.request_params
        )

    def _build_rpm_packages_url(self) -> str:
        """Build URL for RPM packages endpoint."""
        return self._url("api/v3/content/rpm/packages/")

    def get_rpm_by_pkgIDs(self, pkg_ids: List[str]) -> httpx.Response:
        """
        Get RPMs by package IDs.

        Args:
            pkg_ids: List of package IDs (checksums) to search for

        Returns:
            Response object containing RPM information for matching package IDs
        """
        url = self._build_rpm_packages_url()
        params = {"pkgId__in": ",".join(pkg_ids)}
        return self._chunked_get(
            url, params=params, chunk_param="pkgId__in", timeout=self.timeout, **self.request_params
        )

    async def async_get_rpm_by_pkgIDs(self, pkg_ids: List[str]) -> httpx.Response:
        """
        Get RPMs by package IDs asynchronously.

        Args:
            pkg_ids: List of package IDs (checksums) to search for

        Returns:
            Response object containing RPM information for matching package IDs
        """
        url = self._build_rpm_packages_url()
        params = {"pkgId__in": ",".join(pkg_ids)}
        return await self.async_get(url, params=params)

    def get_rpm_by_unsigned_checksums(self, checksums: List[str]) -> httpx.Response:
        """
        Get signed RPMs by unsigned checksum (pulp_labels.unsigned_checksum).

        Uses Pulp complex filtering: q='pulp_label_select="unsigned_checksum=X" OR ...'

        Args:
            checksums: List of unsigned SHA256 checksums to search for

        Returns:
            Response object containing signed RPM packages matching the unsigned checksums
        """
        return self._run_async(self.async_get_rpm_by_unsigned_checksums(checksums))

    async def async_get_rpm_by_unsigned_checksums(self, checksums: List[str]) -> httpx.Response:
        """
        Get signed RPMs by unsigned checksum asynchronously.

        Args:
            checksums: List of unsigned SHA256 checksums to search for

        Returns:
            Response object containing signed RPM packages
        """
        url = self._build_rpm_packages_url()
        if not checksums:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )

        chunk_size = 20  # Pulp limits q expression complexity
        chunks = [checksums[i : i + chunk_size] for i in range(0, len(checksums), chunk_size)]

        if len(chunks) == 1:
            # Single request
            q_parts = [f'pulp_label_select="unsigned_checksum={c}"' for c in chunks[0]]
            q_expr = " OR ".join(q_parts)
            params = {"q": q_expr}
            return await self.async_get(url, params=params)

        # Multiple chunks: fetch concurrently and merge
        async def fetch_chunk(chunk: List[str]) -> tuple:
            q_parts = [f'pulp_label_select="unsigned_checksum={c}"' for c in chunk]
            q_expr = " OR ".join(q_parts)
            params = {"q": q_expr}
            response = await self._get_async_session().get(
                url, params=params, timeout=self.timeout, **self.request_params
            )
            self._check_response(response, "get RPM by unsigned checksums")
            return response.json().get("results", [])

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        raw: List[Any] = []
        for chunk_results in await asyncio.gather(*tasks):
            raw.extend(chunk_results)
        all_results = _dedupe_results_by_pulp_href(raw)

        # Return response with aggregated results
        aggregated = {"count": len(all_results), "results": all_results}
        return httpx.Response(
            200,
            content=json.dumps(aggregated).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    def get_rpm_by_filenames(self, filenames: List[str]) -> httpx.Response:
        """
        Get RPMs by filename (e.g. package-1.0-1.el10.x86_64.rpm).

        Parses filenames to name+version+release (NVR) and searches Pulp by those
        fields instead of filename, for compatibility with instances that do not
        support filename filtering.

        Args:
            filenames: List of RPM filenames or paths to search for

        Returns:
            Response object containing RPM packages matching the NVRs
        """
        return self._run_async(self.async_get_rpm_by_filenames(filenames))

    async def async_get_rpm_by_filenames(self, filenames: List[str]) -> httpx.Response:
        """
        Get RPMs by filename asynchronously. Parses filenames to NVR and queries by name+version+release.
        """
        nvrs = self._filenames_to_nvrs(filenames)
        if not nvrs:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )
        return await self.async_get_rpm_by_nvr([(n, v, r) for n, v, r in nvrs])

    def _filenames_to_nvrs(self, filenames: List[str]) -> List[Tuple[str, str, str]]:
        """Parse filenames to NVRs, skipping unparseable with warning. Deduplicates."""
        seen: set[Tuple[str, str, str]] = set()
        result: List[Tuple[str, str, str]] = []
        for fname in filenames:
            nvr = parse_rpm_filename_to_nvr(fname)
            if nvr is None:
                logging.warning("Skipping unparseable RPM filename: %s", fname)
                continue
            if nvr not in seen:
                seen.add(nvr)
                result.append(nvr)
        return result

    async def async_get_rpm_by_nvr(self, nvrs: List[Tuple[str, str, str]]) -> httpx.Response:
        """
        Get RPMs by name+version+release. Single NVR uses simple params; multiple use q expression.
        """
        url = self._build_rpm_packages_url()
        if not nvrs:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )

        if len(nvrs) == 1:
            n, v, r = nvrs[0]
            params = {"name": n, "version": v, "release": r}
            return await self.async_get(url, params=params)

        # Multiple NVRs: q=(name="a" AND version="1" AND release="2") OR ...
        # Use chunk_size=1 to stay under Pulp expression complexity limit (packages.redhat.com)
        chunk_size = 1
        chunks = [nvrs[i : i + chunk_size] for i in range(0, len(nvrs), chunk_size)]

        async def fetch_chunk(chunk: List[Tuple[str, str, str]]) -> list:
            nvr_parts = [f'(name="{n}" AND version="{v}" AND release="{r}")' for n, v, r in chunk]
            q_expr = " OR ".join(nvr_parts)
            params = {"q": q_expr}
            response = await self._get_async_session().get(
                url, params=params, timeout=self.timeout, **self.request_params
            )
            self._check_response(response, "get RPM by NVR")
            return response.json().get("results", [])

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        raw: List[Any] = []
        for chunk_results in await asyncio.gather(*tasks):
            raw.extend(chunk_results)
        all_results = _dedupe_results_by_pulp_href(raw)

        aggregated = {"count": len(all_results), "results": all_results}
        return httpx.Response(
            200,
            content=json.dumps(aggregated).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    def get_rpm_by_signed_by(self, signed_by_values: List[str]) -> httpx.Response:
        """
        Get RPMs by signed_by pulp label (pulp_labels.signed_by).

        Uses Pulp complex filtering: q='pulp_label_select="signed_by=X" OR ...'

        Args:
            signed_by_values: List of signed_by values to search for

        Returns:
            Response object containing RPM packages matching the signed_by values
        """
        return self._run_async(self.async_get_rpm_by_signed_by(signed_by_values))

    async def async_get_rpm_by_signed_by(self, signed_by_values: List[str]) -> httpx.Response:
        """
        Get RPMs by signed_by asynchronously.

        Args:
            signed_by_values: List of signed_by values to search for

        Returns:
            Response object containing RPM packages
        """
        url = self._build_rpm_packages_url()
        if not signed_by_values:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )

        # Pulp limits q expression complexity to 8. (A OR B OR C OR D) = 7.
        chunk_size = 4
        chunks = [signed_by_values[i : i + chunk_size] for i in range(0, len(signed_by_values), chunk_size)]

        if len(chunks) == 1:
            q_parts = [f'pulp_label_select="signed_by={v}"' for v in chunks[0]]
            q_expr = " OR ".join(q_parts)
            params = {"q": q_expr}
            return await self.async_get(url, params=params)

        async def fetch_chunk(chunk: List[str]) -> list:
            q_parts = [f'pulp_label_select="signed_by={v}"' for v in chunk]
            q_expr = " OR ".join(q_parts)
            params = {"q": q_expr}
            response = await self._get_async_session().get(
                url, params=params, timeout=self.timeout, **self.request_params
            )
            self._check_response(response, "get RPM by signed_by")
            return response.json().get("results", [])

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        raw: List[Any] = []
        for chunk_results in await asyncio.gather(*tasks):
            raw.extend(chunk_results)
        all_results = _dedupe_results_by_pulp_href(raw)

        aggregated = {"count": len(all_results), "results": all_results}
        return httpx.Response(
            200,
            content=json.dumps(aggregated).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    def get_rpm_by_checksums_and_signed_by(self, checksums: List[str], signed_by: str) -> httpx.Response:
        """
        Get RPMs by checksums AND signed_by in a single query (server-side filter).

        Uses q=(pkgId="x" OR pkgId="y") AND pulp_label_select="signed_by=key"
        """
        return self._run_async(self.async_get_rpm_by_checksums_and_signed_by(checksums, signed_by))

    async def async_get_rpm_by_checksums_and_signed_by(self, checksums: List[str], signed_by: str) -> httpx.Response:
        """Get RPMs by checksums and signed_by in a single query."""
        url = self._build_rpm_packages_url()
        if not checksums:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )

        # Pulp limits q expression complexity to 8. (A OR B OR C) AND pulp_label_select = 7.
        chunk_size = 3
        chunks = [checksums[i : i + chunk_size] for i in range(0, len(checksums), chunk_size)]
        signed_by_filter = f'pulp_label_select="signed_by={signed_by}"'

        if len(chunks) == 1:
            pkg_parts = [f'pkgId="{c}"' for c in chunks[0]]
            identity_expr = " OR ".join(pkg_parts)
            q_expr = f"({identity_expr}) AND {signed_by_filter}"
            params = {"q": q_expr}
            return await self.async_get(url, params=params)

        async def fetch_chunk(chunk: List[str]) -> list:
            pkg_parts = [f'pkgId="{c}"' for c in chunk]
            identity_expr = " OR ".join(pkg_parts)
            q_expr = f"({identity_expr}) AND {signed_by_filter}"
            params = {"q": q_expr}
            response = await self._get_async_session().get(
                url, params=params, timeout=self.timeout, **self.request_params
            )
            self._check_response(response, "get RPM by checksums and signed_by")
            return response.json().get("results", [])

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        raw: List[Any] = []
        for chunk_results in await asyncio.gather(*tasks):
            raw.extend(chunk_results)
        all_results = _dedupe_results_by_pulp_href(raw)
        aggregated = {"count": len(all_results), "results": all_results}
        return httpx.Response(
            200,
            content=json.dumps(aggregated).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    def get_rpm_by_filenames_and_signed_by(self, filenames: List[str], signed_by: str) -> httpx.Response:
        """
        Get RPMs by filenames AND signed_by. Parses filenames to NVR and queries by name+version+release.
        """
        return self._run_async(self.async_get_rpm_by_filenames_and_signed_by(filenames, signed_by))

    async def async_get_rpm_by_filenames_and_signed_by(self, filenames: List[str], signed_by: str) -> httpx.Response:
        """Get RPMs by filenames and signed_by. Parses to NVR, then tries combined query; falls back on 400/500."""
        nvrs = self._filenames_to_nvrs(filenames)
        if not nvrs:
            return httpx.Response(
                200,
                content=json.dumps({"count": 0, "results": []}).encode("utf-8"),
                request=_EMPTY_RESPONSE_REQUEST,
            )

        nvr_list = [(n, v, r) for n, v, r in nvrs]
        try:
            response = await self._fetch_rpm_by_nvr_and_signed_by_combined(nvr_list, signed_by)
            if response.status_code in (400, 500):
                raise httpx.HTTPStatusError(
                    f"Combined query returned {response.status_code}",
                    request=response.request,
                    response=response,
                )
            return response
        except (httpx.HTTPStatusError, httpx.HTTPError, ValueError):
            return await self._fetch_rpm_by_nvr_and_signed_by_fallback(nvr_list, signed_by)

    async def _fetch_rpm_by_nvr_and_signed_by_combined(
        self, nvrs: List[Tuple[str, str, str]], signed_by: str
    ) -> httpx.Response:
        """Single-query path: q=(name+version+release) OR ... AND pulp_label_select="signed_by=key"."""
        url = self._build_rpm_packages_url()
        # Use chunk_size=1 to stay under Pulp expression complexity limit (packages.redhat.com)
        chunk_size = 1
        chunks = [nvrs[i : i + chunk_size] for i in range(0, len(nvrs), chunk_size)]
        signed_by_filter = f'pulp_label_select="signed_by={signed_by}"'

        if len(nvrs) == 1:
            n, v, r = nvrs[0]
            params = {"name": n, "version": v, "release": r, "q": signed_by_filter}
            return await self.async_get(url, params=params)

        async def fetch_chunk(chunk: List[Tuple[str, str, str]]) -> list:
            nvr_parts = [f'(name="{n}" AND version="{v}" AND release="{r}")' for n, v, r in chunk]
            identity_expr = " OR ".join(nvr_parts)
            q_expr = f"({identity_expr}) AND {signed_by_filter}"
            params = {"q": q_expr}
            response = await self._get_async_session().get(
                url, params=params, timeout=self.timeout, **self.request_params
            )
            if response.status_code in (400, 500):
                raise httpx.HTTPStatusError(
                    f"Combined query returned {response.status_code}",
                    request=response.request,
                    response=response,
                )
            self._check_response(response, "get RPM by NVR and signed_by")
            return response.json().get("results", [])

        tasks = [fetch_chunk(chunk) for chunk in chunks]
        raw: List[Any] = []
        for chunk_results in await asyncio.gather(*tasks):
            raw.extend(chunk_results)
        all_results = _dedupe_results_by_pulp_href(raw)
        aggregated = {"count": len(all_results), "results": all_results}
        return httpx.Response(
            200,
            content=json.dumps(aggregated).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    async def _fetch_rpm_by_nvr_and_signed_by_fallback(
        self, nvrs: List[Tuple[str, str, str]], signed_by: str
    ) -> httpx.Response:
        """Fallback: when NVRs >= 5, get by signed_by first and filter by NVR (1 call vs N+1).
        Otherwise get by NVR, get by signed_by, intersect by pulp_href."""
        if len(nvrs) >= 5:
            return await self._fetch_rpm_by_signed_by_then_filter_nvr(nvrs, signed_by)
        by_nvr_resp = await self.async_get_rpm_by_nvr(nvrs)
        self._check_response(by_nvr_resp, "get RPM by NVR (fallback)")
        by_signed_resp = await self.async_get_rpm_by_signed_by([signed_by])
        self._check_response(by_signed_resp, "get RPM by signed_by (fallback)")

        by_hrefs = {r["pulp_href"]: r for r in by_nvr_resp.json().get("results", [])}
        by_signed_hrefs = {r["pulp_href"] for r in by_signed_resp.json().get("results", [])}
        intersected = [by_hrefs[href] for href in by_signed_hrefs if href in by_hrefs]
        return httpx.Response(
            200,
            content=json.dumps({"count": len(intersected), "results": intersected}).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    async def _fetch_rpm_by_signed_by_then_filter_nvr(
        self, nvrs: List[Tuple[str, str, str]], signed_by: str
    ) -> httpx.Response:
        """Fetch all packages by signed_by (paginated), filter by NVR locally. Reduces N+1 to 1 call."""
        url = self._build_rpm_packages_url()
        nvr_set = set(nvrs)
        params: dict[str, str | int] = {"q": f'pulp_label_select="signed_by={signed_by}"', "limit": 100}
        all_results: List[Any] = []
        next_url: Optional[str] = None

        while True:
            req_url = next_url if next_url else url
            req_params: dict[str, str | int] | None = None if next_url else params
            response = await self._get_async_session().get(
                req_url,
                params=req_params,
                timeout=self.timeout,
                **self.request_params,
            )
            self._check_response(response, "get RPM by signed_by (paginated)")
            data = response.json()
            results = data.get("results", [])
            for r in results:
                nvr = (r.get("name"), r.get("version"), r.get("release"))
                if nvr in nvr_set:
                    all_results.append(r)
            next_url = data.get("next")
            if not next_url:
                break

        filtered = _dedupe_results_by_pulp_href(all_results)
        return httpx.Response(
            200,
            content=json.dumps({"count": len(filtered), "results": filtered}).encode("utf-8"),
            request=_EMPTY_RESPONSE_REQUEST,
        )

    def gather_content_data(
        self, build_id: str, extra_artifacts: Optional[List[ExtraArtifactRef]] = None
    ) -> ContentData:
        """
        Gather content data and artifacts for a build ID.

        Args:
            build_id: Build identifier
            extra_artifacts: Optional extra artifacts to include (from created_resources)

        Returns:
            ContentData containing content results and artifacts
        """
        raw_results: List[Dict[str, Any]] = []
        artifacts: List[Dict[str, str]] = []

        # Always use bulk query by build_id for efficiency
        # This gets all content in a single API call instead of N individual calls
        if extra_artifacts:
            logging.info("Found %d created resources, querying all content by build_id", len(extra_artifacts))
        else:
            logging.debug("Searching for content by build_id")

        try:
            resp = self.find_content("build_id", build_id)
            raw_results = content_find_results_from_response(resp, "find content by build_id")
        except Exception:
            logging.error("Failed to get content by build ID", exc_info=True)
            raise

        # If no results from build_id query and we have extra_artifacts, try querying by href
        # This handles the case where content hasn't been indexed yet
        if not raw_results and extra_artifacts:
            logging.warning(
                "No content found by build_id, trying direct href query for %d artifacts", len(extra_artifacts)
            )
            try:
                # Extract content hrefs from extra_artifacts
                # Note: extra_artifacts contains content hrefs (not artifact hrefs)
                href_list = [a.pulp_href for a in extra_artifacts if a.pulp_href]
                if href_list:
                    href_query = ",".join(href_list)
                    resp = self.find_content("href", href_query)
                    raw_results = content_find_results_from_response(resp, "find content by href")
                    logging.info("Found %d content items by href query", len(raw_results))
            except Exception:
                logging.error("Failed to get content by href", exc_info=True)
                # Don't raise, just continue with empty results

        if not raw_results:
            logging.warning("No content found for build ID: %s", build_id)
            return ContentData()

        content_results = [PulpContentRow.model_validate(r) for r in raw_results]

        logging.info("Found %d content items for build_id: %s", len(content_results), build_id)

        # Log details about what content was found
        if content_results:
            logging.info("Content types found:")
            for idx, result in enumerate(content_results):
                pulp_href = result.pulp_href
                content_type = self._get_content_type_from_href(pulp_href)

                # Get relative paths from artifacts dict
                artifacts_dict = result.artifacts or {}
                if artifacts_dict:
                    relative_paths = list(artifacts_dict.keys())
                    logging.info("  - %s: %s", content_type, ", ".join(relative_paths))
                else:
                    logging.info("  - %s: no artifacts", content_type)

                # Log full structure for first item to help with debugging
                if idx == 0:
                    logging.debug(
                        "First content item full structure: %s",
                        json.dumps(result.model_dump(), indent=2, default=str),
                    )

        # Extract artifacts from content results
        # Content structure has "artifacts" (plural) field which is a dict: {relative_path: artifact_href}
        artifacts = [
            {"artifact": artifact_href}
            for result in content_results
            for artifact_href in (result.artifacts or {}).values()
            if artifact_href
        ]

        logging.info("Extracted %d artifact hrefs from content results", len(artifacts))
        return ContentData(content_results=content_results, artifacts=artifacts)

    def add_uploaded_artifact_to_results_model(
        self,
        results_model: Any,
        *,
        local_path: str,
        labels: Dict[str, str],
        is_rpm: bool,
        distribution_urls: Dict[str, str],
        target_arch_repo: bool = False,
        file_relative_path: Optional[str] = None,
    ) -> None:
        """
        Add one uploaded artifact to PulpResultsModel using the same keys and URLs as gather/build.

        Called after upload tasks succeed so results JSON can be built incrementally.
        """
        relative_path = os.path.basename(local_path) if is_rpm else (file_relative_path or os.path.basename(local_path))
        build_id = labels.get("build_id", "")
        if is_rpm:
            artifact_key = relative_path
        else:
            artifact_key = f"{build_id}/{relative_path}" if build_id else relative_path

        sha256_hex = calculate_sha256_checksum(local_path)
        artifact_url = self._build_artifact_distribution_url(
            relative_path,
            is_rpm,
            labels,
            distribution_urls,
            target_arch_repo=target_arch_repo,
        )
        results_model.add_artifact(key=artifact_key, url=artifact_url, sha256=sha256_hex, labels=labels)

    def build_results_structure(
        self,
        results_model: Any,
        content_results: List[PulpContentRow],
        file_info_map: FileInfoMap,
        distribution_urls: Optional[Dict[str, str]] = None,
        *,
        target_arch_repo: bool = False,
        merge: bool = False,
    ) -> Any:
        """
        Build the results structure from content and file info using optimized single-pass processing.

        Args:
            results_model: PulpResultsModel to populate with artifacts
            content_results: Content data from Pulp
            file_info_map: Mapping of artifact hrefs to file info models
            distribution_urls: Optional dictionary mapping repo_type to distribution base URL
            target_arch_repo: When True, RPM URLs use per-arch distribution paths from labels
            merge: When True, skip artifact keys already present (incremental upload + reconcile)

        Returns:
            Populated PulpResultsModel
        """
        logging.info("Building results structure:")
        logging.info("  - Content items: %d", len(content_results))
        logging.info("  - File info entries: %d", len(file_info_map))

        # Track statistics for logging
        missing_artifacts = 0
        missing_file_info = 0

        for idx, content in enumerate(content_results):
            labels = dict(content.pulp_labels or {})
            build_id = labels.get("build_id", "")
            pulp_href = content.pulp_href or "unknown"

            # Content structure has "artifacts" (plural) field which is a dict: {relative_path: artifact_href}
            artifacts_dict = content.artifacts or {}

            if not artifacts_dict:
                missing_artifacts += 1
                # Only log details for the first few items to avoid spam
                if idx < 3:
                    logging.warning(
                        "Content item %d structure (no artifacts field). Available fields: %s",
                        idx,
                        list(content.model_dump(exclude_none=True).keys()),
                    )
                    logging.debug("Full content: %s", json.dumps(content.model_dump(), indent=2, default=str))
                continue

            # Determine content type once per content item (cached via lru_cache)
            pulp_type = self._get_content_type_from_href(pulp_href)
            is_rpm = "rpm" in pulp_type.lower()

            # Process all artifacts in a single pass
            for relative_path, artifact_href in artifacts_dict.items():
                # Skip invalid artifact hrefs
                if not artifact_href or "/artifacts/" not in artifact_href:
                    continue

                # Get file info
                file_info = file_info_map.get(artifact_href)
                if not file_info:
                    missing_file_info += 1
                    if missing_file_info <= 3:  # Only log first few
                        logging.warning("No file info found for artifact href: %s", artifact_href)
                    continue

                # Construct artifact key based on content type (optimized logic)
                if is_rpm:
                    # RPM content - use just the filename as the key
                    artifact_key = relative_path
                else:
                    # File content (logs, SBOM, etc.) - use build_id/relative_path
                    artifact_key = f"{build_id}/{relative_path}" if build_id else relative_path
                    if not build_id and missing_file_info <= 1:
                        logging.warning(
                            "No build_id in labels for file content, using relative_path only: %s", relative_path
                        )

                # Construct distribution URL instead of using file_info.file (which is an API href)
                artifact_url = self._build_artifact_distribution_url(
                    relative_path,
                    is_rpm,
                    labels,
                    distribution_urls or {},
                    target_arch_repo=target_arch_repo,
                )

                if merge and artifact_key in results_model.artifacts:
                    existing = results_model.artifacts[artifact_key]
                    gi_sha = file_info.sha256 or ""
                    if existing.url != artifact_url or (existing.sha256 or "") != gi_sha:
                        logging.warning(
                            "Gathered artifact %s differs from incremental entry (keeping incremental)",
                            artifact_key,
                        )
                    continue

                # Add artifact to results model
                results_model.add_artifact(
                    key=artifact_key, url=artifact_url, sha256=file_info.sha256 or "", labels=labels
                )

        # Log summary statistics
        logging.info("Final results: %d artifacts processed", results_model.artifact_count)
        if missing_artifacts > 0:
            logging.warning("Content items without artifacts field: %d", missing_artifacts)
        if missing_file_info > 3:
            logging.warning("Missing file info for %d artifacts", missing_file_info)

        return results_model


__all__ = ["PulpClient"]
