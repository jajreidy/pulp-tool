"""
OAuth2 authentication for Pulp API.

This module provides OAuth2 Client Credentials authentication
for secure API access with optimized token refresh logic.
"""

# Standard library imports
import logging
import traceback
from datetime import datetime, timedelta
from typing import Generator, Optional

# Third-party imports
import httpx

# Token refresh buffer - refresh token this many seconds before expiration
TOKEN_REFRESH_BUFFER = 60  # 1 minute buffer


class OAuth2ClientCredentialsAuth(httpx.Auth):
    """
    OAuth2 Client Credentials Grant authentication flow implementation.
    Based on pulp-cli's authentication mechanism.

    This handles automatic token retrieval, refresh, and 401 retry logic.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
    ):
        """
        Initialize OAuth2 authentication.

        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            token_url: URL for token endpoint (e.g., "https://console.redhat.com/token")
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url

        self._access_token: Optional[str] = None
        self._expire_at: Optional[datetime] = None

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        """
        Execute the authentication flow for a request.

        This method implements httpx.Auth interface and handles:
        - Token retrieval if needed (with proactive refresh buffer)
        - Adding Authorization header
        - Automatic retry on 401 with token refresh

        Optimization: Refreshes token TOKEN_REFRESH_BUFFER seconds before expiration
        to avoid 401 errors and unnecessary retries.
        """
        # Check if we need to fetch/refresh token
        # Add buffer to refresh token before it expires to avoid 401 errors
        refresh_threshold = datetime.now() + timedelta(seconds=TOKEN_REFRESH_BUFFER)
        if self._expire_at is None or self._expire_at < refresh_threshold:
            if self._expire_at is not None:
                logging.debug("Proactively refreshing token (expires in < %ds)", TOKEN_REFRESH_BUFFER)
            self._retrieve_token()

        if self._access_token is None:
            raise RuntimeError("Failed to obtain access token")

        # Add Authorization header
        request.headers["Authorization"] = f"Bearer {self._access_token}"

        # Send the request and get response
        response = yield request

        # If we get 401, try to refresh token and retry once
        if response.status_code == 401:
            logging.debug("Received 401, attempting token refresh")
            self._retrieve_token()

            if self._access_token is None:
                logging.error("Failed to refresh access token")
                yield response
                return

            # Update request with new token and retry
            request.headers["Authorization"] = f"Bearer {self._access_token}"
            yield request

    def _retrieve_token(self) -> None:
        """Fetch a new OAuth2 access token."""
        data = {"grant_type": "client_credentials"}

        try:
            response = httpx.post(
                self._token_url,
                data=data,
                auth=(self._client_id, self._client_secret),
                timeout=30,
            )
            response.raise_for_status()

            token = response.json()
            if "access_token" not in token or "expires_in" not in token:
                raise ValueError("Invalid token response format")

            self._expire_at = datetime.now() + timedelta(seconds=token["expires_in"])
            self._access_token = token["access_token"]

        except httpx.HTTPError as e:
            logging.error("Failed to retrieve OAuth2 token: %s", e)
            logging.error("Traceback: %s", traceback.format_exc())
            raise

    @property
    def access_token(self) -> Optional[str]:
        """Get the current access token (for debugging/inspection)."""
        return self._access_token

    @property
    def expires_at(self) -> Optional[datetime]:
        """Get the token expiration time (for debugging/inspection)."""
        return self._expire_at


__all__ = ["OAuth2ClientCredentialsAuth"]
