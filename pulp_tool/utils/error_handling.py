"""
Error handling utilities for standardized error logging and handling.

This module provides reusable error handling patterns to eliminate
code duplication across the codebase.
"""

import logging
import sys
import traceback
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import httpx

# Type variable for generic function decorators
F = TypeVar("F", bound=Callable[..., Any])


def is_upload_auth_related_failure(error: BaseException) -> bool:
    """
    Return True if an exception likely indicates an authentication/authorization failure.

    Used by upload CLI commands for a temporary workaround: log a warning and exit 0 so
    pipelines do not fail hard on expected auth issues.

    Args:
        error: Exception raised during upload.

    Returns:
        True if the failure should be treated as auth-related for the workaround.
    """
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code in (401, 403)

    if isinstance(error, httpx.HTTPError):
        message = str(error).lower()
        if "401" in message or "403" in message:
            return True
        if "unauthorized" in message or "forbidden" in message:
            return True

    if isinstance(error, RuntimeError):
        message = str(error).lower()
        if "failed to obtain access token" in message:
            return True

    return False


def try_upload_auth_graceful_exit(operation: str, error: BaseException) -> bool:
    """
    Temporary workaround for upload commands: treat auth failures as non-fatal.

    If ``error`` looks like an auth problem, log a warning and return True so the
    caller can ``sys.exit(0)``. Otherwise return False and the caller should handle
    the error normally.

    Args:
        operation: Description for logs (e.g. ``upload operation``).
        error: The caught exception.

    Returns:
        True if the caller should exit successfully without re-raising.
    """
    if not is_upload_auth_related_failure(error):
        return False
    logging.warning(
        "Temporary upload workaround: authentication failure during %s; exiting with success (code 0). Error: %s",
        operation,
        error,
    )
    return True


def handle_http_error(error: httpx.HTTPError, operation: str, *, log_traceback: bool = True) -> None:
    """
    Handle HTTP errors with standardized logging.

    Args:
        error: The HTTP error to handle
        operation: Description of the operation that failed
        log_traceback: Whether to log the full traceback
    """
    error_message = str(error)

    # Provide helpful messages based on status code
    if "403" in error_message:
        logging.error(
            "Authentication failed during %s: You don't have permission to access this resource. "
            "Please check your credentials in the configuration file.",
            operation,
        )
    elif "401" in error_message:
        logging.error(
            "Authentication failed during %s: Invalid credentials. "
            "Please check your OAuth2 settings in the configuration file.",
            operation,
        )
    elif "404" in error_message:
        logging.error("Resource not found during %s: %s", operation, error)
    elif "500" in error_message or "502" in error_message or "503" in error_message:
        logging.error("Server error during %s: %s", operation, error)
    else:
        logging.error("HTTP error during %s: %s", operation, error)

    if log_traceback:
        logging.debug("Traceback: %s", traceback.format_exc())


def handle_generic_error(error: Exception, operation: str, *, log_traceback: bool = True) -> None:
    """
    Handle generic errors with standardized logging.

    Args:
        error: The exception to handle
        operation: Description of the operation that failed
        log_traceback: Whether to log the full traceback
    """
    logging.error("Unexpected error during %s: %s", operation, error)

    if log_traceback:
        logging.error("Traceback: %s", traceback.format_exc())


def with_error_handling(
    operation: str, *, exit_on_error: bool = False, exit_code: int = 1, reraise: bool = True
) -> Callable[[F], F]:
    """
    Decorator to wrap functions with consistent error handling.

    Args:
        operation: Description of the operation for logging
        exit_on_error: If True, call sys.exit on error
        exit_code: Exit code to use if exit_on_error is True
        reraise: If True, reraise the exception after logging (unless exiting)

    Returns:
        Decorator function

    Example:
        @with_error_handling("upload files", exit_on_error=True)
        def upload_files():
            # Implementation
            pass
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except httpx.HTTPError as e:
                handle_http_error(e, operation)
                if exit_on_error:
                    sys.exit(exit_code)
                if reraise:
                    raise
            except Exception as e:
                handle_generic_error(e, operation)
                if exit_on_error:
                    sys.exit(exit_code)
                if reraise:
                    raise

        return wrapper  # type: ignore[return-value]

    return decorator


def log_and_exit(message: str, exit_code: int = 1) -> None:
    """
    Log an error message and exit the program.

    Args:
        message: Error message to log
        exit_code: Exit code (default: 1)
    """
    logging.error(message)
    sys.exit(exit_code)


def try_parse_json(content: str, operation: str, *, default: Optional[Any] = None, raise_on_error: bool = True) -> Any:
    """
    Attempt to parse JSON content with error handling.

    Args:
        content: JSON string to parse
        operation: Description of operation for error messages
        default: Default value to return on error (if raise_on_error is False)
        raise_on_error: If True, raise exception on parse error

    Returns:
        Parsed JSON data or default value

    Raises:
        ValueError: If parsing fails and raise_on_error is True
    """
    import json

    try:
        return json.loads(content)
    except (ValueError, json.JSONDecodeError) as e:
        logging.error("Failed to parse JSON during %s: %s", operation, e)
        logging.debug("Content preview: %s", content[:500] if len(content) > 500 else content)

        if raise_on_error:
            raise ValueError(f"Invalid JSON during {operation}: {e}") from e

        return default


__all__ = [
    "handle_http_error",
    "handle_generic_error",
    "is_upload_auth_related_failure",
    "try_upload_auth_graceful_exit",
    "with_error_handling",
    "log_and_exit",
    "try_parse_json",
]
