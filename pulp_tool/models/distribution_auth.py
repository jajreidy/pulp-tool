"""Distribution download auth resolved from CLI flags and ``cli.toml``."""

from __future__ import annotations

import logging
import os
from typing import Self

from pydantic import ConfigDict, Field

from ..utils.config_manager import ConfigManager
from .base import KonfluxBaseModel


def _expand_config_path(raw: str) -> str | None:
    expanded = os.path.expanduser(raw.strip())
    return expanded if os.path.exists(expanded) else None


class DistributionAuthSettings(KonfluxBaseModel):
    """TLS or basic auth plus Pulp API base URL for distribution fetches."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    cert_path: str | None = None
    key_path: str | None = None
    username: str | None = None
    password: str | None = Field(default=None, repr=False)
    pulp_api_base_url: str | None = None

    @property
    def has_cert_auth(self) -> bool:
        return bool(self.cert_path and self.key_path)

    @property
    def has_basic_auth(self) -> bool:
        return self.username is not None and self.password is not None

    @property
    def has_any_auth(self) -> bool:
        return self.has_cert_auth or self.has_basic_auth

    @classmethod
    def from_config(
        cls,
        *,
        auth_config_path: str | None,
        cert_path: str | None = None,
        key_path: str | None = None,
        username: str | None = None,
        password: str | None = None,
        pulp_api_base_url: str | None = None,
    ) -> Self:
        """Load distribution auth fields from config when not already set on the CLI."""
        if not auth_config_path:
            return cls(
                cert_path=cert_path,
                key_path=key_path,
                username=username,
                password=password,
                pulp_api_base_url=pulp_api_base_url,
            )
        try:
            config_manager = ConfigManager(auth_config_path)
            config_manager.load()
            if not cert_path:
                loaded_cert = config_manager.get("cli.cert")
                if loaded_cert and isinstance(loaded_cert, str) and loaded_cert.strip():
                    cert_path = _expand_config_path(loaded_cert)
            if not key_path:
                loaded_key = config_manager.get("cli.key")
                if loaded_key and isinstance(loaded_key, str) and loaded_key.strip():
                    key_path = _expand_config_path(loaded_key)
            if username is None:
                raw_user = config_manager.get("cli.username")
                username = str(raw_user).strip() if raw_user else None
            if password is None:
                raw_pass = config_manager.get("cli.password")
                password = str(raw_pass) if raw_pass is not None else None
            if pulp_api_base_url is None:
                loaded_base = config_manager.get("cli.base_url")
                if loaded_base and isinstance(loaded_base, str) and loaded_base.strip():
                    pulp_api_base_url = loaded_base.strip()
        except Exception as exc:
            logging.debug("Could not load auth from config %s: %s", auth_config_path, exc)
        return cls(
            cert_path=cert_path,
            key_path=key_path,
            username=username,
            password=password,
            pulp_api_base_url=pulp_api_base_url,
        )

    def with_pulp_api_base_url_from_configs(
        self,
        *,
        artifact_location: str,
        fallback_config_paths: tuple[str | None, ...],
    ) -> Self:
        """Fill ``pulp_api_base_url`` from alternate config files when the artifact URL is remote."""
        if not artifact_location.startswith(("http://", "https://")) or self.pulp_api_base_url:
            return self
        for base_config_path in fallback_config_paths:
            if not base_config_path:
                continue
            try:
                base_manager = ConfigManager(base_config_path)
                base_manager.load()
                loaded_base = base_manager.get("cli.base_url")
                if loaded_base and isinstance(loaded_base, str) and loaded_base.strip():
                    return self.model_copy(update={"pulp_api_base_url": loaded_base.strip()})
            except Exception as exc:
                logging.debug("Could not load cli.base_url from %s: %s", base_config_path, exc)
        return self


__all__ = ["DistributionAuthSettings"]
