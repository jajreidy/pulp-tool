"""Tests for DistributionAuthSettings."""

from unittest.mock import MagicMock, patch

from pulp_tool.models.distribution_auth import DistributionAuthSettings


class TestDistributionAuthSettings:
    def test_has_auth_properties(self) -> None:
        assert not DistributionAuthSettings().has_any_auth
        cert = DistributionAuthSettings(cert_path="/c.pem", key_path="/k.pem")
        assert cert.has_cert_auth and cert.has_any_auth
        basic = DistributionAuthSettings(username="u", password="p")
        assert basic.has_basic_auth and basic.has_any_auth

    @patch("pulp_tool.models.distribution_auth.ConfigManager")
    def test_from_config_loads_base_url(self, mock_cfg: MagicMock) -> None:
        manager = mock_cfg.return_value
        manager.load.return_value = None
        manager.get.side_effect = lambda key: {"cli.base_url": "https://pulp.example/"}.get(key)
        auth = DistributionAuthSettings.from_config(auth_config_path="/cli.toml")
        assert auth.pulp_api_base_url == "https://pulp.example/"

    def test_with_pulp_api_base_url_from_configs_skips_local_path(self) -> None:
        auth = DistributionAuthSettings()
        updated = auth.with_pulp_api_base_url_from_configs(
            artifact_location="/tmp/pulp_results.json",
            fallback_config_paths=("/other.toml",),
        )
        assert updated is auth

    @patch("pulp_tool.models.distribution_auth.ConfigManager")
    def test_with_pulp_api_base_url_from_configs_remote(self, mock_cfg: MagicMock) -> None:
        manager = mock_cfg.return_value
        manager.load.return_value = None
        manager.get.return_value = "https://remote.example/"
        auth = DistributionAuthSettings(cert_path="/c", key_path="/k")
        updated = auth.with_pulp_api_base_url_from_configs(
            artifact_location="https://remote.example/content/x",
            fallback_config_paths=("/fallback.toml",),
        )
        assert updated.pulp_api_base_url == "https://remote.example/"
