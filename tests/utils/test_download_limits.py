"""Tests for download byte limit helpers."""

from pulp_tool.utils.constants import DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES
from pulp_tool.utils.download_limits import max_artifact_download_bytes


class TestMaxArtifactDownloadBytes:
    """Tests for max_artifact_download_bytes."""

    def test_default_when_env_unset(self, monkeypatch) -> None:
        monkeypatch.delenv("PULP_TOOL_MAX_DOWNLOAD_BYTES", raising=False)
        assert max_artifact_download_bytes() == DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES

    def test_env_override(self, monkeypatch) -> None:
        monkeypatch.setenv("PULP_TOOL_MAX_DOWNLOAD_BYTES", "1048576")
        assert max_artifact_download_bytes() == 1048576

    def test_invalid_env_falls_back_to_default(self, monkeypatch) -> None:
        monkeypatch.setenv("PULP_TOOL_MAX_DOWNLOAD_BYTES", "not-a-number")
        assert max_artifact_download_bytes() == DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES

    def test_non_positive_env_falls_back_to_default(self, monkeypatch) -> None:
        monkeypatch.setenv("PULP_TOOL_MAX_DOWNLOAD_BYTES", "0")
        assert max_artifact_download_bytes() == DEFAULT_MAX_ARTIFACT_DOWNLOAD_BYTES
