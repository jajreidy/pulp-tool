"""
Tests for validation utility functions.

This module tests file validation, build ID validation, and sanitization functions.
"""

import os
import tempfile
import pytest

from pulp_tool.utils import (
    validate_file_path,
    validate_build_id,
    sanitize_build_id_for_repository,
    validate_repository_setup,
    extract_build_id_from_artifact_json,
    extract_build_id_from_artifacts,
)
from pulp_tool.utils.validation import (
    _extract_field_from_artifact,
    extract_metadata_from_artifact_json,
    strip_namespace_from_build_id,
)
from pulp_tool.models.artifacts import ArtifactJsonResponse, ArtifactMetadata, PulledArtifacts


class TestFileValidation:
    """Test file validation utility functions."""

    def test_validate_file_path_success(self, temp_file):
        """Test validate_file_path with valid file."""
        validate_file_path(temp_file, "Test")
        # Should not raise any exception

    def test_validate_file_path_not_found(self):
        """Test validate_file_path with non-existent file."""
        with pytest.raises(FileNotFoundError, match="Test file not found"):
            validate_file_path("/non/existent/file.txt", "Test")

    def test_validate_file_path_empty(self):
        """Test validate_file_path with empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Test file is empty"):
                validate_file_path(temp_path, "Test")
        finally:
            os.unlink(temp_path)

    def test_validate_file_path_permission_error(self):
        """Test validate_file_path with permission error."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
            f.write(b"test content")

        try:
            # Make the file unreadable
            os.chmod(temp_path, 0o000)

            with pytest.raises(PermissionError, match="Cannot read"):
                validate_file_path(temp_path, "Test")
        finally:
            os.chmod(temp_path, 0o644)
            os.unlink(temp_path)


class TestBuildIDValidation:
    """Test build ID validation utility functions."""

    def test_validate_build_id_valid(self):
        """Test validate_build_id with valid build ID."""
        assert validate_build_id("test-build-123") is True

    def test_validate_build_id_invalid(self):
        """Test validate_build_id with invalid build ID."""
        assert validate_build_id("") is False
        assert validate_build_id(None) is False  # type: ignore[arg-type]
        assert validate_build_id(123) is False  # type: ignore[arg-type]

    def test_sanitize_build_id_for_repository(self):
        """Test sanitize_build_id_for_repository function."""
        # Test normal build ID
        assert sanitize_build_id_for_repository("test-build-123") == "test-build-123"

        # Test build ID with invalid characters
        assert sanitize_build_id_for_repository("test/build:123") == "test-build-123"

        # Test empty build ID
        assert sanitize_build_id_for_repository("") == "default-build"
        assert sanitize_build_id_for_repository(None) == "default-build"  # type: ignore[arg-type]

        # Test build ID with multiple consecutive hyphens
        assert sanitize_build_id_for_repository("test--build---123") == "test-build-123"

        # Test build ID with leading/trailing hyphens
        assert sanitize_build_id_for_repository("-test-build-123-") == "test-build-123"

    def test_sanitize_build_id_all_invalid_chars(self):
        """Test sanitize_build_id_for_repository with all invalid characters."""
        result = sanitize_build_id_for_repository("///:::***")
        assert result == "default-build"


class TestRepositoryValidation:
    """Test repository validation utility functions."""

    def test_validate_repository_setup_valid(self, mock_repositories):
        """Test validate_repository_setup function with valid setup."""
        is_valid, errors = validate_repository_setup(mock_repositories)

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_repository_setup_missing_repo(self):
        """Test validate_repository_setup function with missing repository."""
        repositories = {"rpms_prn": "test-prn", "rpms_href": "test-href"}

        is_valid, errors = validate_repository_setup(repositories)

        assert is_valid is False
        assert len(errors) > 0
        assert any("Missing logs repository PRN" in error for error in errors)

    def test_validate_repository_setup_missing_rpm_href(self):
        """Test validate_repository_setup function with missing RPM href."""
        repositories = {
            "rpms_prn": "test-prn",
            "logs_prn": "test-prn",
            "sbom_prn": "test-prn",
            "artifacts_prn": "test-prn",
        }

        is_valid, errors = validate_repository_setup(repositories)

        assert is_valid is False
        assert any("Missing rpms repository href" in error for error in errors)

    def test_validate_repository_setup_invalid_reference(self):
        """Test validate_repository_setup with invalid repository reference."""
        repositories = {
            "rpms_prn": "test-prn",
            "rpms_href": "  ",  # Empty/whitespace string
            "logs_prn": "test-prn",
            "logs_href": "test-href",
            "sbom_prn": "test-prn",
            "sbom_href": "test-href",
            "artifacts_prn": "test-prn",
            "artifacts_href": "test-href",
        }

        is_valid, errors = validate_repository_setup(repositories)

        assert is_valid is False
        assert any("Invalid repository reference" in error for error in errors)


class TestBuildIDExtraction:
    """Test build ID extraction functions."""

    def test_extract_build_id_with_artifact_json_response(self):
        """Test extract_build_id_from_artifact_json with ArtifactJsonResponse object."""
        # Create an ArtifactJsonResponse object
        metadata = ArtifactMetadata(labels={"build_id": "test-build-123", "arch": "x86_64"})
        artifact_json = ArtifactJsonResponse(artifacts={"test.rpm": metadata})

        build_id = extract_build_id_from_artifact_json(artifact_json)
        assert build_id == "test-build-123"

    def test_extract_build_id_with_dict_fallback(self):
        """Test extract_build_id_from_artifact_json with dict fallback path."""
        # Pass a dict with non-ArtifactMetadata values to trigger fallback
        artifact_json = {"artifacts": {"test.rpm": {"labels": {"build_id": "dict-build-456", "arch": "x86_64"}}}}

        build_id = extract_build_id_from_artifact_json(artifact_json)
        assert build_id == "dict-build-456"

    def test_extract_build_id_from_pulled_artifacts_model(self):
        """Test extract_build_id_from_artifacts with PulledArtifacts Pydantic model."""
        # Create a PulledArtifacts model with some artifacts
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_rpm("test.rpm", "/tmp/test.rpm", {"build_id": "model-build-789", "arch": "x86_64"})

        build_id = extract_build_id_from_artifacts(pulled_artifacts)
        assert build_id == "model-build-789"

    def test_extract_build_id_from_pulled_artifacts_sbom(self):
        """Test extract_build_id_from_artifacts extracting from sbom artifacts."""
        # Create a PulledArtifacts model with sbom artifacts
        pulled_artifacts = PulledArtifacts()
        pulled_artifacts.add_sbom("test.sbom", "/tmp/test.sbom", {"build_id": "sbom-build-123", "arch": "noarch"})

        build_id = extract_build_id_from_artifacts(pulled_artifacts)
        assert build_id == "sbom-build-123"

    def test_extract_build_id_from_pulled_artifacts_fallback(self):
        """Test extract_build_id_from_artifacts fallback when no build_id found."""
        # Create empty PulledArtifacts model
        pulled_artifacts = PulledArtifacts()

        build_id = extract_build_id_from_artifacts(pulled_artifacts)
        assert build_id == "rok-storage"


class TestAdditionalValidation:
    """Additional validation tests for coverage."""

    def test_strip_namespace_from_build_id(self):
        """Test strip_namespace_from_build_id function."""
        # Test with namespace prefix
        result = strip_namespace_from_build_id("namespace/build-123")
        assert result == "build-123"

        # Test without namespace
        result = strip_namespace_from_build_id("build-456")
        assert result == "build-456"

        # Test empty string
        result = strip_namespace_from_build_id("")
        assert result == ""

    # create_labels test temporarily removed

    def test_extract_field_from_artifact(self):
        """Test _extract_field_from_artifact with both dict and ArtifactMetadata inputs."""
        # Test with dict input (covers line 202)
        artifact_dict = {"labels": {"build_id": "test-build-123", "arch": "x86_64"}}
        result = _extract_field_from_artifact(artifact_dict, "build_id")
        assert result == "test-build-123"

        # Test with dict input and missing field
        result = _extract_field_from_artifact(artifact_dict, "missing_field")
        assert result is None

        # Test with dict input and missing labels
        artifact_dict_no_labels = {"file": "test.rpm"}
        result = _extract_field_from_artifact(artifact_dict_no_labels, "build_id")
        assert result is None

        # Test with ArtifactMetadata input
        metadata = ArtifactMetadata(labels={"build_id": "test-build-456", "arch": "x86_64"})
        result = _extract_field_from_artifact(metadata, "build_id")
        assert result == "test-build-456"

        # Test with ArtifactMetadata and empty labels
        metadata_no_labels = ArtifactMetadata(labels={})
        result = _extract_field_from_artifact(metadata_no_labels, "build_id")
        assert result is None

    def test_extract_metadata_from_artifact_json_with_labels_none(self):
        """Test extract_metadata_from_artifact_json with dict metadata having labels=None (covers line 169)."""
        # Test with dict metadata that has labels=None
        artifact_json = {"artifacts": {"test.rpm": {"labels": None, "url": "https://example.com/test.rpm"}}}
        result = extract_metadata_from_artifact_json(artifact_json, "build_id", fallback="fallback-value")
        # Should return fallback since labels is None (converted to empty dict)
        assert result == "fallback-value"

    def test_extract_metadata_from_artifact_json_with_non_dict_metadata(self):
        """Test extract_metadata_from_artifact_json with non-dict metadata (covers line 172)."""
        # Test with metadata that is not a dict (ArtifactMetadata object)
        metadata = ArtifactMetadata(labels={"build_id": "test-build-789", "arch": "x86_64"})
        artifact_json = {"artifacts": {"test.rpm": metadata}}
        result = extract_metadata_from_artifact_json(artifact_json, "build_id", fallback="fallback-value")
        # Should extract build_id from the ArtifactMetadata object
        assert result == "test-build-789"
