"""Tests for Click CLI commands."""

from unittest.mock import Mock, patch

from click.testing import CliRunner

from pulp_tool.cli import cli


class TestCreateRepositoryCommand:

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_success(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "/api/pulp/konflux-test/api/v3/content/rpm/packages/019b1338-f265-7ad6-a278-8bead86e5c1d/",
            ],
        )
        assert result.exit_code == 0

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_no_packages_json(self, mock_helper_class, mock_client_class):
        """Test missing packages."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--json-data",
                """{
                    "name": "test-repo-name",
                    "distribution_options": {
                        "name": "test-distro-name",
                        "base_path": "test-base-path"
                    },
                    "packages":[]
                }""",
            ],
        )
        assert result.exit_code == 1
        assert "List should have at least 1 item" in result.output

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_no_packages_cli(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(created_resources=["test-href"])
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "",
            ],
        )
        assert "Unable to validate CLI options" in result.output

    @patch("pulp_tool.cli.create_repository.PulpClient")
    @patch("pulp_tool.cli.create_repository.PulpHelper")
    def test_create_repository_unexpected_error(self, mock_helper_class, mock_client_class):
        """Test successful create-repository flow."""
        runner = CliRunner()

        # Setup mocks

        mock_client = Mock()
        mock_client.close = Mock()
        mock_client.add_content.return_value = Mock(pulp_href="test-href")
        mock_client.wait_for_finished_task.return_value = Mock(side_effect=Exception())
        mock_client_class.create_from_config_file.return_value = mock_client

        mock_helper = Mock()
        mock_helper.create_or_get_repository.return_value = (
            "test-prn",
            "test-href",
        )
        mock_helper_class.return_value = mock_helper

        result = runner.invoke(
            cli,
            [
                "create-repository",
                "--repository-name",
                "test-repo-name",
                "--base-path",
                "test-base-path",
                "--packages",
                "/api/pulp/konflux-test/api/v3/content/file/packages/019b1338-f265-7ad6-a278-8bead86e5c1d/",
            ],
        )
        assert "Unexpected error during create-repository operation" in result.output
