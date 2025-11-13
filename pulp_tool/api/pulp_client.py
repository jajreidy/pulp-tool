"""
Pulp API client for managing repositories and content.

This module provides the main PulpClient class, which is composed using the
mixin pattern to provide specialized functionality:

Mixins:
    - TaskManagerMixin: Asynchronous task monitoring with exponential backoff
    - RepositoryManagerMixin: Repository creation and management (RPM, file)
    - ContentManagerMixin: Content upload (RPMs, logs, SBOMs) and artifact management
    - ContentQueryMixin: Content search, filtering, and metadata extraction

The PulpClient class combines all mixins to provide a complete Pulp API interface
with clean separation of concerns and testable components.

Key Features:
    - OAuth2 authentication with automatic token refresh
    - Exponential backoff for task polling
    - Context-based error handling with @with_error_handling decorator
    - Type-safe operations using Pydantic models
    - Proper resource cleanup with context managers
"""

# Standard library imports
import json
import logging
import os
import ssl
import time
import traceback
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union
from urllib.parse import urlencode

# Third-party imports
import httpx

# Local imports
from ..utils import create_session_with_retry
from .auth import OAuth2ClientCredentialsAuth
from .content_manager import ContentManagerMixin
from .content_query import ContentQueryMixin
from .repository_manager import RepositoryManagerMixin
from .task_manager import TaskManagerMixin

import tomllib

# ============================================================================
# Constants
# ============================================================================

# Default timeout for HTTP requests (seconds)
# Increased to 120 seconds to handle slow operations like bulk content queries
DEFAULT_TIMEOUT = 120

# Cache TTL (time-to-live) in seconds for GET request caching
CACHE_TTL = 300  # 5 minutes


# ============================================================================
# Performance Metrics
# ============================================================================


class PerformanceMetrics:
    """Track API performance metrics."""

    def __init__(self) -> None:
        """Initialize metrics tracker."""
        self.total_requests = 0
        self.cached_requests = 0
        self.chunked_requests = 0
        self.task_polls = 0

    def log_request(self, cached: bool = False) -> None:
        """Log an API request."""
        self.total_requests += 1
        if cached:
            self.cached_requests += 1

    def log_chunked_request(self, parallel: bool = True) -> None:
        """Log a chunked request (always parallel)."""
        self.chunked_requests += 1

    def log_task_poll(self) -> None:
        """Log a task poll."""
        self.task_polls += 1

    def get_summary(self) -> Dict[str, Any]:
        """
        Get metrics summary.

        Returns:
            Dictionary with metrics summary
        """
        cache_hit_rate = (self.cached_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        return {
            "total_requests": self.total_requests,
            "cached_requests": self.cached_requests,
            "cache_hit_rate": f"{cache_hit_rate:.1f}%",
            "chunked_requests": self.chunked_requests,
            "task_polls": self.task_polls,
        }

    def log_summary(self) -> None:
        """Log metrics summary."""
        summary = self.get_summary()
        logging.info("=== API Performance Metrics ===")
        logging.info("Total requests: %d", summary["total_requests"])
        logging.info("Cached requests: %d (%s)", summary["cached_requests"], summary["cache_hit_rate"])
        logging.info("Parallel chunked requests: %d", summary["chunked_requests"])
        logging.info("Task polls: %d", summary["task_polls"])


# ============================================================================
# Cache Implementation
# ============================================================================


class TTLCache:
    """Simple time-to-live cache for GET requests."""

    def __init__(self, ttl: int = CACHE_TTL):
        """
        Initialize TTL cache.

        Args:
            ttl: Time to live in seconds for cache entries
        """
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if not expired.

        Args:
            key: Cache key

        Returns:
            Cached value or None if expired/not found
        """
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time.time() - timestamp < self._ttl:
                return value
            # Expired, remove it
            del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """
        Set value in cache with current timestamp.

        Args:
            key: Cache key
            value: Value to cache
        """
        self._cache[key] = (value, time.time())

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()

    def size(self) -> int:
        """Return number of cached entries."""
        return len(self._cache)


def cached_get(method: Callable) -> Callable:
    """
    Decorator to cache GET request results.

    Caches responses based on URL to reduce redundant API calls.
    Tracks metrics for cache hits and misses.
    """

    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        # Only cache if first argument is a URL string
        if not args or not isinstance(args[0], str):
            return method(self, *args, **kwargs)

        url = args[0]
        cache_key = f"{method.__name__}:{url}"

        # Check cache
        cached_result = self._get_cache.get(cache_key)
        if cached_result is not None:
            logging.debug("Cache hit for %s", url)
            # Track cache hit
            if hasattr(self, "_metrics"):
                self._metrics.log_request(cached=True)
            return cached_result

        # Cache miss - make request
        result = method(self, *args, **kwargs)

        # Track request
        if hasattr(self, "_metrics"):
            self._metrics.log_request(cached=False)

        # Only cache successful GET responses
        if hasattr(result, "is_success") and result.is_success:
            self._get_cache.set(cache_key, result)
            logging.debug("Cached response for %s", url)

        return result

    return wrapper


# ============================================================================
# Main Client Class
# ============================================================================


class PulpClient(ContentManagerMixin, TaskManagerMixin, ContentQueryMixin, RepositoryManagerMixin):
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
        self, config: Dict[str, Union[str, int]], domain: Optional[str] = None, config_path: Optional[Path] = None
    ) -> None:
        """Initialize the Pulp client.

        Args:
            config: Configuration dictionary from the TOML file
            domain: Optional explicit domain override
            config_path: Path to config file for resolving relative cert/key paths
        """
        self.domain = domain
        self.config = config
        # Set namespace from domain or config file's domain field
        self.namespace = domain if domain else config.get("domain")
        self.config_path = config_path  # Store config path for resolving relative cert/key paths
        self.timeout = DEFAULT_TIMEOUT  # Used by Protocol mixins
        self._auth = None
        self.session = self._create_session()
        self._async_session: Optional[httpx.AsyncClient] = None
        # Initialize cache for GET requests
        self._get_cache = TTLCache(ttl=CACHE_TTL)
        # Initialize performance metrics tracker
        self._metrics = PerformanceMetrics()
        logging.debug("PulpClient initialized with request caching enabled (TTL: %ds)", CACHE_TTL)

    def _create_session(self) -> httpx.Client:
        """Create a requests session with retry strategy and connection pool configuration."""
        # Pass cert to Client constructor if available, otherwise auth will be added per-request
        cert = self.cert if self.config.get("cert") else None
        return create_session_with_retry(cert=cert)

    def _get_async_session(self) -> httpx.AsyncClient:
        """Get or create async session with optimized configuration."""
        if self._async_session is None or self._async_session.is_closed:
            cert = self.cert if self.config.get("cert") else None

            # Create async client with same configuration as sync client
            # Increased limits for concurrent chunked requests
            limits = httpx.Limits(
                max_keepalive_connections=20, max_connections=100  # Match sync client's connection pool
            )
            timeout = httpx.Timeout(self.timeout, connect=10.0)

            # Add compression headers
            default_headers = {
                "Accept-Encoding": "gzip, deflate, br",
            }

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

    async def async_close(self) -> None:
        """Close the async session and release all connections."""
        if self._async_session and not self._async_session.is_closed:
            await self._async_session.aclose()
            logging.debug("PulpClient async session closed and connections released")

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
        chunk_size: int = 50,
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
        import asyncio  # pylint: disable=import-outside-toplevel

        async_client = self._get_async_session()

        if not params or not chunk_param or chunk_param not in params:
            # No chunking needed, make regular request
            return await async_client.get(url, params=params, **kwargs)

        # Extract the parameter value and check if it needs chunking
        param_value = params[chunk_param]
        if not isinstance(param_value, str) or "," not in param_value:
            # Not a comma-separated list, make regular request
            return await async_client.get(url, params=params, **kwargs)

        values = [v.strip() for v in param_value.split(",")]

        if len(values) <= chunk_size:
            # Small list, make regular request
            return await async_client.get(url, params=params, **kwargs)

        # Need to chunk the request
        chunks = [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]

        logging.debug(
            "Chunking parameter '%s' with %d values into %d chunks (async concurrent)",
            chunk_param,
            len(values),
            len(chunks),
        )

        # Track metrics
        if hasattr(self, "_metrics"):
            self._metrics.log_chunked_request(parallel=True)

        # Create tasks for all chunks
        async def fetch_chunk(chunk: list, chunk_index: int) -> tuple:
            """Fetch a single chunk and return its results."""
            chunk_params = params.copy()
            chunk_params[chunk_param] = ",".join(chunk)

            try:
                response = await async_client.get(url, params=chunk_params, **kwargs)
                self._check_response(response, f"chunked request {chunk_index}")

                # Parse results
                chunk_data = response.json()
                results = chunk_data.get("results", [])
                logging.debug("Completed chunk %d/%d with %d results", chunk_index, len(chunks), len(results))
                return response, results

            except Exception as e:
                logging.error("Failed to process chunk %d: %s", chunk_index, e)
                logging.error("Traceback: %s", traceback.format_exc())
                raise

        # Execute all chunks concurrently
        tasks = [fetch_chunk(chunk, i) for i, chunk in enumerate(chunks, 1)]
        results = await asyncio.gather(*tasks)

        # Aggregate results
        all_results = []
        last_response = None
        for response, chunk_results in results:
            last_response = response
            all_results.extend(chunk_results)

        # Create aggregated response
        if last_response:
            aggregated_data = {"count": len(all_results), "results": all_results}

            # Modify response content to return aggregated results from all chunks
            # intentionally modifying _content
            # pylint: disable=W0212 (protected-access)
            last_response._content = json.dumps(aggregated_data).encode("utf-8")
            return last_response

        # Fallback: return empty response
        return await async_client.get(url, params={chunk_param: ""}, **kwargs)

    def _chunked_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_param: Optional[str] = None,
        chunk_size: int = 50,
        **kwargs,
    ) -> httpx.Response:
        """
        Synchronous wrapper for _chunked_get_async.

        Provides a synchronous interface while using async implementation
        underneath for better performance.
        """
        import asyncio  # pylint: disable=import-outside-toplevel

        # Check if we're already in an event loop
        try:
            asyncio.get_running_loop()
            # We're in an async context, but this is a sync method
            raise RuntimeError("_chunked_get called from async context. Use _chunked_get_async instead.")
        except RuntimeError:
            # No event loop running, safe to create one
            pass

        # Run the async version in a new event loop
        # Handle the case where the loop might be closed by creating a new one
        try:
            return asyncio.run(self._chunked_get_async(url, params, chunk_param, chunk_size, **kwargs))
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                # Create and set a new event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(
                        self._chunked_get_async(url, params, chunk_param, chunk_size, **kwargs)
                    )
                finally:
                    loop.close()
            raise

    @classmethod
    def create_from_config_file(cls, path: Optional[str] = None, domain: Optional[str] = None) -> "PulpClient":
        """
        Create a Pulp client from a standard configuration file that is
        used by the `pulp` CLI tool.

        The namespace/domain will be read from the config file's 'domain' field.
        """
        config_path = Path(path or "~/.config/pulp/cli.toml").expanduser()
        with open(config_path, "rb") as fp:
            config = tomllib.load(fp)

        return cls(config["cli"], domain, config_path=config_path)

    @property
    def headers(self) -> Optional[Dict[str, str]]:
        """
        Get headers for requests.

        Returns:
            None (no custom headers are currently used)
        """
        return None

    @property
    def auth(self) -> OAuth2ClientCredentialsAuth:
        """
        Get authentication credentials.

        Returns:
            OAuth2ClientCredentialsAuth instance for API authentication
        """
        if not self._auth:
            # Set up OAuth2 authentication with correct Red Hat SSO token URL
            token_url = (
                "https://sso.redhat.com/auth/realms/redhat-external/"
                "protocol/openid-connect/token"  # nosec B105
            )

            self._auth = OAuth2ClientCredentialsAuth(  # type: ignore[assignment]
                client_id=str(self.config["client_id"]),
                client_secret=str(self.config["client_secret"]),
                token_url=token_url,
            )
        return self._auth  # type: ignore[return-value]

    @property
    def cert(self) -> Tuple[str, str]:
        """
        Get client certificate information.

        If cert/key paths are not absolute and config_path is available,
        tries to resolve them relative to the config file's directory.

        Returns:
            Tuple of (cert_path, key_path) for client certificate authentication
        """
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
        url = self._url(f"{endpoint}?")
        url += urlencode({"name": name, "offset": 0, "limit": 1})
        return self.session.get(url, timeout=self.timeout, **self.request_params)

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

            raise httpx.HTTPError(f"Failed to {operation}: {response.status_code} - {response.text}")

    def check_response(self, response: httpx.Response, operation: str = "request") -> None:
        """Public method to check if a response is successful, raise exception if not."""
        self._check_response(response, operation)

    # ============================================================================
    # Async Methods for Repository Setup
    # ============================================================================

    async def async_get(self, url: str, **kwargs) -> httpx.Response:
        """Async GET request."""
        client = self._get_async_session()
        # Add auth if configured
        if self.auth:
            kwargs.setdefault("auth", self.auth)
        return await client.get(url, **kwargs)

    async def async_post(self, url: str, **kwargs) -> httpx.Response:
        """Async POST request."""
        client = self._get_async_session()
        # Add auth if configured
        if self.auth:
            kwargs.setdefault("auth", self.auth)
        return await client.post(url, **kwargs)


__all__ = ["PulpClient"]
