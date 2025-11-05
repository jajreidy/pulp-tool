"""Base models for Konflux Pulp."""

from pydantic import BaseModel, ConfigDict


class KonfluxBaseModel(BaseModel):
    """Base model for all Konflux Pulp models."""

    model_config = ConfigDict(
        extra="forbid",  # Don't allow extra fields
        frozen=False,  # Allow modification (can be changed per model)
        validate_assignment=True,  # Validate on attribute assignment
    )


__all__ = ["KonfluxBaseModel"]
