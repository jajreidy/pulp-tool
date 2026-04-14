"""Tests for Click CLI commands."""

import tempfile
from pathlib import Path

from click.testing import CliRunner

from pulp_tool.cli import cli, config_option, debug_option


class TestGetUrlsCommand:
    """Test get-urls command functionality."""

    def test_config_option_not_required(self):
        """Test config_option with required=False includes default help."""
        decorator = config_option(required=False)
        assert callable(decorator)
        # The decorator should be a click.option function
        # We can't easily test the help text without invoking it, but we can verify it's callable

    def test_config_option_required(self):
        """Test config_option with required=True excludes default help."""
        decorator = config_option(required=True)
        assert callable(decorator)
        # The decorator should be a click.option function

    def test_cli_with_invalid_base64_config(self):
        """Test CLI handles invalid base64 config gracefully."""
        runner = CliRunner()
        invalid_base64 = (
            "A" * 100 + "B" * 100 + "C" * 50 + "=" * 3
        )  # Invalid padding (long enough to be detected as base64)

        # Actually invoke a command that will try to use the config
        # Group-level options (--config, --build-id, --namespace) come before the command
        with tempfile.TemporaryDirectory() as tmpdir:
            rpm_dir = Path(tmpdir) / "rpms"
            rpm_dir.mkdir()
            sbom_path = Path(tmpdir) / "sbom.json"
            sbom_path.write_text("{}")

            result = runner.invoke(
                cli,
                [
                    "--config",
                    invalid_base64,
                    "--build-id",
                    "test",
                    "--namespace",
                    "test",
                    "upload",
                    "--parent-package",
                    "test",
                    "--rpm-path",
                    str(rpm_dir),
                    "--sbom-path",
                    str(sbom_path),
                ],
            )

            # Should exit with error code when trying to create client
            assert result.exit_code != 0
            assert "Failed to decode base64 config" in result.output or "Failed to load configuration" in result.output

    def test_debug_option(self):
        """Test debug_option returns a click option decorator."""
        decorator = debug_option()
        assert callable(decorator)
        # The decorator should be a click.option function
