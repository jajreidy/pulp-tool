"""Tests for logging utilities."""

from pulp_tool.utils.logging_utils import (
    format_count_with_unit,
    format_artifact_counts,
)


class TestCountFormatting:
    """Tests for count formatting functions."""

    def test_format_count_with_unit_singular(self) -> None:
        """Test formatting singular count."""
        assert format_count_with_unit(1, "RPM") == "1 RPM"

    def test_format_count_with_unit_plural(self) -> None:
        """Test formatting plural count."""
        assert format_count_with_unit(5, "RPM") == "5 RPMs"

    def test_format_count_with_unit_explicit_singular(self) -> None:
        """Test formatting with explicit singular form."""
        result = format_count_with_unit(1, "repositories", singular="repository")
        assert result == "1 repository"

    def test_format_count_with_unit_already_plural(self) -> None:
        """Test formatting with already plural unit."""
        assert format_count_with_unit(5, "repositories") == "5 repositories"

    def test_format_artifact_counts_empty(self) -> None:
        """Test formatting empty artifact counts."""
        assert format_artifact_counts({}) == "No artifacts"

    def test_format_artifact_counts_with_data(self) -> None:
        """Test formatting artifact counts with data."""
        result = format_artifact_counts({"rpms": 5, "logs": 3, "sboms": 1})
        assert "5 RPMs" in result
        assert "3 logs" in result
        assert "1 SBOM" in result

    def test_format_artifact_counts_zero_values(self) -> None:
        """Test formatting artifact counts with zero values."""
        result = format_artifact_counts({"rpms": 5, "logs": 0})
        assert "5 RPMs" in result
        assert "log" not in result
