"""
Distribution client for downloading artifacts from Pulp.

This module provides client for downloading artifacts from
distribution repositories.
"""

# Standard library imports
import logging
import traceback
from typing import Tuple

# Third-party imports
import httpx

# Local imports
from ..utils import create_session_with_retry


class DistributionClient:
    """Client for downloading artifacts from distribution repositories."""

    def __init__(self, cert: str, key: str) -> None:
        """Initialize the distribution client with SSL certificates.

        Args:
            cert: Path to the SSL certificate file
            key: Path to the SSL private key file
        """
        self.cert = cert
        self.key = key
        self.session = self._create_session()

    def _create_session(self) -> httpx.Client:
        """Create an httpx client with retry strategy and connection pooling.

        Uses a 5-minute timeout to allow for downloading large RPM files.
        """
        return create_session_with_retry(cert=(self.cert, self.key), timeout=300.0)

    def pull_artifact(self, file_url: str) -> httpx.Response:
        """Pull artifact metadata from the given URL.

        Args:
            file_url: URL to fetch artifact metadata from

        Returns:
            Response object containing artifact metadata as JSON
        """
        logging.info("Pulling files %s", file_url)
        return self.session.get(file_url)

    def pull_data(self, filename: str, file_url: str, arch: str, artifact_type: str = "rpm") -> str:
        """Download and save artifact data to local filesystem.

        Args:
            filename: Name of the file to save
            file_url: URL to download the file from
            arch: Architecture for organizing the file path
            artifact_type: Type of artifact (rpm, log, sbom) - determines save location

        Returns:
            Full path to the saved file
        """
        from ..utils.path_utils import get_artifact_save_path

        logging.info("Pulling file %s", file_url)

        # Use centralized path utility
        file_full_filename = get_artifact_save_path(filename, arch, artifact_type)

        with self.session.stream("GET", file_url) as response:
            response.raise_for_status()

            # Optimize chunk size based on content length
            content_length = response.headers.get("content-length")
            chunk_size = 8192  # Default chunk size
            if content_length:
                file_size = int(content_length)
                # Use larger chunks for bigger files, but cap at 64KB
                chunk_size = min(max(8192, file_size // 100), 65536)

            with open(file_full_filename, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=chunk_size):
                    f.write(chunk)
        return file_full_filename

    def pull_data_async(self, download_info: Tuple[str, str, str, str]) -> Tuple[str, str]:
        """Download artifact data asynchronously.

        Args:
            download_info: Tuple of (artifact_name, file_url, arch, artifact_type)

        Returns:
            Tuple of (artifact_name, file_path)
        """
        artifact_name, file_url, arch, artifact_type = download_info
        try:
            file_path = self.pull_data(artifact_name, file_url, arch, artifact_type)
            return artifact_name, file_path
        except httpx.HTTPError as e:
            logging.error("Failed to download %s: %s", artifact_name, e)
            logging.debug("Traceback: %s", traceback.format_exc())
            raise


__all__ = ["DistributionClient"]
