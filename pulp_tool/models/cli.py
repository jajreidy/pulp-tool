"""Pulp Tool CLI models for JSON and input validation"""

from typing import Optional, Literal

from pydantic import Field

from .base import KonfluxBaseModel


class RepositoryOptions(KonfluxBaseModel):
    compression_type: Optional[Literal["zstd", "gz"]] = None
    checksum_type: Optional[Literal["unknown", "md5", "sha1", "sha224", "sha256", "sha384", "sha512"]] = None
    autopublish: bool = True


class DistributionOptions(KonfluxBaseModel):
    name: str
    base_path: str
    generate_repo_config: Optional[bool] = None


class Package(KonfluxBaseModel):
    pulp_href: str


class CreateRepository(KonfluxBaseModel):
    name: str
    packages: list[Package] = Field(min_length=1)
    repository_options: RepositoryOptions = RepositoryOptions()
    distribution_options: DistributionOptions
    custom_files: dict[str, str] = {}


__all__ = ["CreateRepository"]
