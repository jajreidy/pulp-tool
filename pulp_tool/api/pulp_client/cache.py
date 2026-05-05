"""
GET caching and request metrics for :class:`pulp_tool.api.pulp_client.client.PulpClient`.

Extracted to keep the client module smaller; behavior is unchanged.
"""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple

# Cache TTL (time-to-live) in seconds for GET request caching
CACHE_TTL = 300  # 5 minutes


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
        if not args or not isinstance(args[0], str):
            return method(self, *args, **kwargs)

        kw_key = tuple(sorted(kwargs.items()))
        cache_key = f"{method.__name__}:{args!r}:{kw_key!r}"

        cached_result = self._get_cache.get(cache_key)
        if cached_result is not None:
            logging.debug("Cache hit for %s", cache_key)
            if hasattr(self, "_metrics"):
                self._metrics.log_request(cached=True)
            return cached_result

        result = method(self, *args, **kwargs)

        if hasattr(self, "_metrics"):
            self._metrics.log_request(cached=False)

        if hasattr(result, "is_success") and result.is_success:
            self._get_cache.set(cache_key, result)
            logging.debug("Cached response for %s", cache_key)

        return result

    return wrapper


__all__ = ["CACHE_TTL", "PerformanceMetrics", "TTLCache", "cached_get"]
