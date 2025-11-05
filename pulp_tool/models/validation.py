"""Validation and check result models."""

from typing import List, Dict

from pydantic import Field

from .base import KonfluxBaseModel


class RpmCheckResult(KonfluxBaseModel):
    """
    Result of checking RPMs against Pulp.

    Attributes:
        missing_rpms: List of RPM files not found on Pulp that need to be uploaded
        found_artifacts: List of artifact information for RPMs already on Pulp
    """

    missing_rpms: List[str] = Field(default_factory=list)
    found_artifacts: List[Dict[str, str]] = Field(default_factory=list)

    @property
    def missing_count(self) -> int:
        """Number of RPMs that need to be uploaded."""
        return len(self.missing_rpms)

    @property
    def found_count(self) -> int:
        """Number of RPMs already on Pulp."""
        return len(self.found_artifacts)

    @property
    def total_count(self) -> int:
        """Total number of RPMs checked."""
        return self.missing_count + self.found_count


class ValidationResult(KonfluxBaseModel):
    """
    Result of validation operations.

    Attributes:
        is_valid: Whether validation passed
        errors: List of validation error messages
    """

    is_valid: bool
    errors: List[str] = Field(default_factory=list)

    @property
    def error_count(self) -> int:
        """Number of validation errors."""
        return len(self.errors)

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    def add_error(self, error: str) -> None:
        """Add a validation error."""
        self.errors = self.errors + [error]  # Create new list to avoid mutation issues
        self.is_valid = False


class ArtifactValidationResult(KonfluxBaseModel):
    """
    Result of loading and validating artifacts.

    Attributes:
        artifact_json: Complete artifact JSON metadata from source
        artifacts: Dictionary of individual artifacts to process
    """

    artifact_json: Dict
    artifacts: Dict


__all__ = [
    "RpmCheckResult",
    "ValidationResult",
    "ArtifactValidationResult",
]
