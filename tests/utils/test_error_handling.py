"""Tests for error handling utilities."""

import pytest
import httpx

from pulp_tool.utils.error_handling import (
    handle_generic_error,
    handle_http_error,
    is_upload_auth_related_failure,
    log_and_exit,
    try_parse_json,
    try_upload_auth_graceful_exit,
    with_error_handling,
)


class TestHandleHttpError:
    """Tests for handle_http_error function."""

    def test_handle_403_error(self, caplog):
        """Test handling 403 Forbidden error."""
        error = httpx.HTTPError("403 Forbidden")
        handle_http_error(error, "test operation", log_traceback=False)

        assert "Authentication failed" in caplog.text
        assert "permission" in caplog.text.lower()

    def test_handle_401_error(self, caplog):
        """Test handling 401 Unauthorized error."""
        error = httpx.HTTPError("401 Unauthorized")
        handle_http_error(error, "test operation", log_traceback=False)

        assert "Authentication failed" in caplog.text
        assert "Invalid credentials" in caplog.text

    def test_handle_404_error(self, caplog):
        """Test handling 404 Not Found error."""
        error = httpx.HTTPError("404 Not Found")
        handle_http_error(error, "test operation", log_traceback=False)

        assert "Resource not found" in caplog.text

    def test_handle_500_error(self, caplog):
        """Test handling 500 Server Error."""
        error = httpx.HTTPError("500 Internal Server Error")
        handle_http_error(error, "test operation", log_traceback=False)

        assert "Server error" in caplog.text

    def test_handle_generic_http_error(self, caplog):
        """Test handling generic HTTP error."""
        error = httpx.HTTPError("400 Bad Request")
        handle_http_error(error, "test operation", log_traceback=False)

        assert "HTTP error" in caplog.text


class TestHandleGenericError:
    """Tests for handle_generic_error function."""

    def test_handle_generic_error(self, caplog):
        """Test handling generic exception."""
        error = ValueError("Test error")
        handle_generic_error(error, "test operation", log_traceback=False)

        assert "Unexpected error" in caplog.text
        assert "test operation" in caplog.text


class TestWithErrorHandling:
    """Tests for with_error_handling decorator."""

    def test_successful_execution(self):
        """Test decorator with successful function execution."""

        @with_error_handling("test operation", reraise=False)
        def successful_func():
            return "success"

        result = successful_func()
        assert result == "success"

    def test_http_error_reraise(self):
        """Test decorator with HTTP error that should reraise."""

        @with_error_handling("test operation", reraise=True)
        def failing_func():
            raise httpx.HTTPError("Test error")

        with pytest.raises(httpx.HTTPError):
            failing_func()

    def test_generic_error_reraise(self):
        """Test decorator with generic error that should reraise."""

        @with_error_handling("test operation", reraise=True)
        def failing_func():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            failing_func()

    def test_error_no_reraise(self, caplog):
        """Test decorator with error that should not reraise."""

        @with_error_handling("test operation", reraise=False)
        def failing_func():
            raise ValueError("Test error")

        result = failing_func()
        assert result is None
        assert "Unexpected error" in caplog.text

    def test_exit_on_error(self):
        """Test decorator with exit_on_error=True."""

        @with_error_handling("test operation", exit_on_error=True, reraise=False)
        def failing_func():
            raise ValueError("Test error")

        with pytest.raises(SystemExit) as exc_info:
            failing_func()

        assert exc_info.value.code == 1


class TestUploadAuthGracefulWorkaround:
    """Tests for temporary upload auth graceful-exit helpers."""

    def test_http_status_error_401_is_auth_failure(self):
        """HTTPStatusError with 401 counts as auth-related."""
        request = httpx.Request("GET", "https://pulp.example.com/")
        response = httpx.Response(401, request=request)
        err = httpx.HTTPStatusError("Unauthorized", request=request, response=response)
        assert is_upload_auth_related_failure(err) is True

    def test_http_status_error_403_is_auth_failure(self):
        """HTTPStatusError with 403 counts as auth-related."""
        request = httpx.Request("GET", "https://pulp.example.com/")
        response = httpx.Response(403, request=request)
        err = httpx.HTTPStatusError("Forbidden", request=request, response=response)
        assert is_upload_auth_related_failure(err) is True

    def test_http_status_error_500_not_auth_failure(self):
        """HTTPStatusError with 500 is not treated as auth-only workaround."""
        request = httpx.Request("GET", "https://pulp.example.com/")
        response = httpx.Response(500, request=request)
        err = httpx.HTTPStatusError("Server error", request=request, response=response)
        assert is_upload_auth_related_failure(err) is False

    def test_http_error_message_with_status_code(self):
        """Plain HTTPError with 401/403 in message is auth-related."""
        assert is_upload_auth_related_failure(httpx.HTTPError("401 Unauthorized")) is True
        assert is_upload_auth_related_failure(httpx.HTTPError("403 Forbidden")) is True

    def test_http_error_unauthorized_word_without_code(self):
        """HTTPError message may only say Unauthorized."""
        assert is_upload_auth_related_failure(httpx.HTTPError("Unauthorized")) is True

    def test_http_error_connection_not_auth(self):
        """Connection-style HTTPError is not auth-related."""
        assert is_upload_auth_related_failure(httpx.HTTPError("Connection failed")) is False

    def test_runtime_error_no_token_is_auth_related(self):
        """RuntimeError from OAuth when token is missing matches."""
        assert is_upload_auth_related_failure(RuntimeError("Failed to obtain access token")) is True

    def test_runtime_error_other_not_auth(self):
        """Other RuntimeError is not auth-related."""
        assert is_upload_auth_related_failure(RuntimeError("something else")) is False

    def test_try_upload_auth_graceful_exit_logs_and_returns_true(self, caplog):
        """try_upload_auth_graceful_exit warns and returns True for auth errors."""
        import logging

        caplog.set_level(logging.WARNING)
        err = httpx.HTTPError("403 Forbidden")
        assert try_upload_auth_graceful_exit("upload operation", err) is True
        assert "Temporary upload workaround" in caplog.text
        assert "upload operation" in caplog.text

    def test_try_upload_auth_graceful_exit_returns_false_when_not_auth(self, caplog):
        """Non-auth errors return False without workaround log."""
        import logging

        caplog.set_level(logging.WARNING)
        err = httpx.HTTPError("500 boom")
        assert try_upload_auth_graceful_exit("upload operation", err) is False
        assert "Temporary upload workaround" not in caplog.text


class TestLogAndExit:
    """Tests for log_and_exit function."""

    def test_log_and_exit_default(self, caplog):
        """Test log_and_exit with default exit code."""
        with pytest.raises(SystemExit) as exc_info:
            log_and_exit("Test error message")

        assert exc_info.value.code == 1
        assert "Test error message" in caplog.text

    def test_log_and_exit_custom_code(self, caplog):
        """Test log_and_exit with custom exit code."""
        with pytest.raises(SystemExit) as exc_info:
            log_and_exit("Test error message", exit_code=42)

        assert exc_info.value.code == 42


class TestTryParseJson:
    """Tests for try_parse_json function."""

    def test_parse_valid_json(self):
        """Test parsing valid JSON."""
        result = try_parse_json('{"key": "value"}', "test operation")
        assert result == {"key": "value"}

    def test_parse_invalid_json_with_raise(self):
        """Test parsing invalid JSON with raise_on_error=True."""
        with pytest.raises(ValueError) as exc_info:
            try_parse_json("invalid json", "test operation", raise_on_error=True)

        assert "Invalid JSON" in str(exc_info.value)

    def test_parse_invalid_json_with_default(self):
        """Test parsing invalid JSON with default value."""
        result = try_parse_json("invalid json", "test operation", default={"default": "value"}, raise_on_error=False)
        assert result == {"default": "value"}

    def test_parse_invalid_json_no_default(self):
        """Test parsing invalid JSON with no default."""
        result = try_parse_json("invalid json", "test operation", raise_on_error=False)
        assert result is None
