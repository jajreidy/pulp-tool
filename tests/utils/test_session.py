"""
Tests for session utilities.

This module tests session creation and configuration.
"""

import os
import ssl
import tempfile
from unittest.mock import Mock, patch
import httpx
import pytest
from pulp_tool.utils import create_session_with_retry
from pulp_tool.utils.session import RETRY_STATUS_CODES, RetryingAsyncClient, RetryingHttpClient, _compute_retry_delay_s


class TestSessionUtilities:
    """Test session utility functions."""

    def test_create_session_with_retry(self) -> None:
        """Test create_session_with_retry function."""
        session = create_session_with_retry()
        assert isinstance(session, httpx.Client)
        assert session.timeout is not None
        assert session.timeout.connect == 10.0
        assert not session.is_closed

    def test_create_session_with_cert_files_exist(self) -> None:
        """Test create_session_with_retry with actual certificate files."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as cert_file:
            cert_file.write("-----BEGIN CERTIFICATE-----\nfake cert\n-----END CERTIFICATE-----\n")
            cert_path = cert_file.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as key_file:
            key_file.write("-----BEGIN PRIVATE KEY-----\nfake key\n-----END PRIVATE KEY-----\n")
            key_path = key_file.name
        try:
            with patch("ssl.create_default_context") as mock_ssl:
                mock_context = Mock(spec=ssl.SSLContext)
                mock_ssl.return_value = mock_context
                session = create_session_with_retry(cert=(cert_path, key_path))
                mock_ssl.assert_called_once()
                mock_context.load_cert_chain.assert_called_once_with(certfile=cert_path, keyfile=key_path)
                assert isinstance(session, httpx.Client)
        finally:
            os.unlink(cert_path)
            os.unlink(key_path)

    def test_create_session_with_cert_files_not_exist(self) -> None:
        """Test create_session_with_retry with non-existent certificate files."""
        session = create_session_with_retry(cert=("/nonexistent/cert.pem", "/nonexistent/key.pem"))
        assert isinstance(session, httpx.Client)
        assert not session.is_closed

    def test_create_session_custom_timeout(self) -> None:
        """Test create_session_with_retry with custom timeout."""
        session = create_session_with_retry(timeout=60.0)
        assert isinstance(session, httpx.Client)
        assert session.timeout.connect == 10.0

    def test_create_session_custom_max_connections(self) -> None:
        """Test create_session_with_retry with custom max_connections."""
        session = create_session_with_retry(max_connections=200)
        assert isinstance(session, httpx.Client)
        assert not session.is_closed

    def test_create_session_http2_not_available(self) -> None:
        """Test create_session_with_retry when HTTP/2 is not available."""
        with patch("importlib.util.find_spec", return_value=None):
            session = create_session_with_retry()
            assert isinstance(session, httpx.Client)
            assert not session.is_closed

    def test_create_session_with_auth_tuple(self) -> None:
        """Test create_session_with_retry with Basic Auth (username, password) tuple."""
        session = create_session_with_retry(auth=("user", "pass"))
        assert isinstance(session, httpx.Client)
        assert session.auth is not None
        assert isinstance(session.auth, httpx.BasicAuth)

    def test_create_session_with_auth_basic(self) -> None:
        """Test create_session_with_retry with httpx.BasicAuth."""
        auth = httpx.BasicAuth("myuser", "mypass")
        session = create_session_with_retry(auth=auth)
        assert isinstance(session, httpx.Client)
        assert session.auth is auth

    def test_create_session_with_extra_headers(self) -> None:
        """Optional extra default headers are merged for API requests."""
        session = create_session_with_retry(extra_headers={"X-Custom": "1"})
        try:
            assert session.headers["X-Custom"] == "1"
        finally:
            session.close()

    def test_create_session_find_spec_attributeerror_disables_http2(self) -> None:
        """If importlib.util.find_spec fails, HTTP/2 is disabled without raising."""
        with patch("importlib.util.find_spec", side_effect=AttributeError("no spec")):
            session = create_session_with_retry()
            try:
                assert isinstance(session, httpx.Client)
                assert not session.is_closed
            finally:
                session.close()

    def test_compute_retry_delay_respects_retry_after(self) -> None:
        """429 with Retry-After uses the header delay."""
        response = httpx.Response(
            429, headers={"Retry-After": "12"}, request=httpx.Request("GET", "https://example.com/")
        )
        assert _compute_retry_delay_s(response=response, attempt_index=0, base_backoff=0.5) == 12.0

    def test_compute_retry_delay_exponential_backoff_without_retry_after(self) -> None:
        """Without Retry-After, delay follows exponential backoff from the base."""
        response = httpx.Response(503, request=httpx.Request("GET", "https://example.com/"))
        assert _compute_retry_delay_s(response=response, attempt_index=0, base_backoff=0.5) == 0.5
        assert _compute_retry_delay_s(response=response, attempt_index=1, base_backoff=0.5) == 1.0

    def test_compute_retry_delay_invalid_retry_after_falls_back_to_backoff(self) -> None:
        """429 with non-numeric Retry-After uses exponential backoff."""
        response = httpx.Response(
            429, headers={"Retry-After": "not-a-number"}, request=httpx.Request("GET", "https://example.com/")
        )
        assert _compute_retry_delay_s(response=response, attempt_index=1, base_backoff=0.25) == 0.5

    def test_retrying_http_client_succeeds_after_transient_failures(self) -> None:
        """504 then 200 — client retries and returns the successful response."""
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 3:
                return httpx.Response(504, text="timeout", request=request)
            return httpx.Response(200, json={"ok": True}, request=request)

        transport = httpx.MockTransport(handler)
        with patch("pulp_tool.utils.session.time.sleep"):
            client = RetryingHttpClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=4,
            )
            try:
                r = client.get("https://example.com/api/")
            finally:
                client.close()
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        assert len(attempts) == 3

    def test_retrying_http_client_returns_last_status_when_exhausted(self) -> None:
        """All attempts return 504 — last response is returned."""
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(504, text="timeout", request=request)

        transport = httpx.MockTransport(handler)
        with patch("pulp_tool.utils.session.time.sleep"):
            client = RetryingHttpClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=3,
            )
            try:
                r = client.get("https://example.com/x/")
            finally:
                client.close()
        assert r.status_code == 504
        assert calls["n"] == 3

    def test_retrying_http_client_stream_not_retried(self) -> None:
        """Streaming responses are not retried (single transport invocation on 504)."""
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(504, text="timeout", request=request)

        transport = httpx.MockTransport(handler)
        with patch("pulp_tool.utils.session.time.sleep"):
            client = RetryingHttpClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=4,
            )
            try:
                with client.stream("GET", "https://example.com/stream.bin") as r:
                    assert r.status_code == 504
            finally:
                client.close()
        assert calls["n"] == 1

    def test_retrying_http_client_close_error_still_backoffs(self) -> None:
        """If response.close() fails, retry loop still sleeps and continues."""
        phase = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            phase["n"] += 1
            if phase["n"] < 2:
                r = httpx.Response(503, text="x", request=request)
                orig_close = r.close

                def broken_close() -> None:
                    orig_close()
                    raise RuntimeError("close failed")

                r.close = broken_close
                return r
            return httpx.Response(200, json={"ok": True}, request=request)

        transport = httpx.MockTransport(handler)
        with patch("pulp_tool.utils.session.time.sleep"):
            client = RetryingHttpClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=4,
            )
            try:
                r = client.get("https://example.com/y/")
            finally:
                client.close()
        assert r.status_code == 200
        assert phase["n"] == 2

    def test_retry_status_codes_include_gateway_timeouts(self) -> None:
        """Public tuple documents transient codes used for response retries."""
        assert 504 in RETRY_STATUS_CODES
        assert 502 in RETRY_STATUS_CODES

    @pytest.mark.asyncio
    async def test_retrying_async_client_succeeds_after_transient_failures(self) -> None:
        """Async client retries 503 then succeeds."""
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 2:
                return httpx.Response(503, text="unavailable", request=request)
            return httpx.Response(200, json={"status": "ok"}, request=request)

        transport = httpx.MockTransport(handler)

        async def fake_sleep(_seconds: float) -> None:
            return None

        with patch("pulp_tool.utils.session.asyncio.sleep", side_effect=fake_sleep):
            async with RetryingAsyncClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=4,
            ) as client:
                r = await client.get("https://example.com/a/")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}
        assert len(attempts) == 2

    @pytest.mark.asyncio
    async def test_retrying_async_client_stream_not_retried(self) -> None:
        """Async streaming responses are not retried."""
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(504, text="timeout", request=request)

        transport = httpx.MockTransport(handler)
        async with RetryingAsyncClient(
            transport=transport,
            base_url="https://example.com",
            timeout=httpx.Timeout(5.0),
            response_retry_total_attempts=4,
        ) as client:
            async with client.stream("GET", "https://example.com/stream.bin") as r:
                assert r.status_code == 504
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_retrying_async_client_aclose_error_still_backoffs(self) -> None:
        """If response.aclose() fails, async retry loop still continues."""
        phase = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            phase["n"] += 1
            if phase["n"] < 2:
                r = httpx.Response(503, text="x", request=request)
                orig_aclose = r.aclose

                async def broken_aclose() -> None:
                    await orig_aclose()
                    raise RuntimeError("aclose failed")

                r.aclose = broken_aclose
                return r
            return httpx.Response(200, json={"done": True}, request=request)

        transport = httpx.MockTransport(handler)

        async def fake_sleep(_seconds: float) -> None:
            return None

        with patch("pulp_tool.utils.session.asyncio.sleep", side_effect=fake_sleep):
            async with RetryingAsyncClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=4,
            ) as client:
                r = await client.get("https://example.com/z/")
        assert r.status_code == 200
        assert r.json() == {"done": True}
        assert phase["n"] == 2

    @pytest.mark.asyncio
    async def test_retrying_async_client_returns_last_when_exhausted(self) -> None:
        """Async retries exhausted — last transient status is returned."""
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(504, text="timeout", request=request)

        transport = httpx.MockTransport(handler)
        with patch("pulp_tool.utils.session.asyncio.sleep", side_effect=lambda _s: None):
            async with RetryingAsyncClient(
                transport=transport,
                base_url="https://example.com",
                timeout=httpx.Timeout(5.0),
                response_retry_total_attempts=2,
            ) as client:
                r = await client.get("https://example.com/e/")
        assert r.status_code == 504
        assert calls["n"] == 2
