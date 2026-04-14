#!/usr/bin/env python3
"""
Tests for pulp_tool.pull module.
"""

import json
from typing import Tuple
from unittest.mock import MagicMock, Mock, patch

import httpx
import pytest

from pulp_tool.models.artifacts import ArtifactFile
from pulp_tool.pull import (
    download_artifacts_concurrently,
    load_and_validate_artifacts,
    setup_repositories_if_needed,
)
from pulp_tool.pull.reporting import _extract_artifact_info


class TestSetupRepositories:
    """Test setup_repositories_if_needed function."""

    def test_setup_repositories_adds_konflux_prefix(self, tmp_path, mock_config):
        """Test setup_repositories_if_needed adds konflux- prefix to domain."""
        from pulp_tool.models.context import PullContext

        # Create config file with domain that doesn't have konflux- prefix
        config_file = tmp_path / "config.toml"
        config_file.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "test-domain"')

        args = PullContext(
            config=str(config_file),
            transfer_dest=str(config_file),
            build_id="test-build",
            namespace="test-namespace",
        )

        with (
            patch("pulp_tool.pull.download.PulpClient") as mock_client_class,
            patch("pulp_tool.pull.download.PulpHelper") as mock_helper_class,
            patch("pulp_tool.pull.download.logging") as mock_logging,
        ):
            mock_client = Mock()
            mock_client_class.create_from_config_file.return_value = mock_client

            # Mock PulpHelper to avoid real repository setup
            mock_helper = Mock()
            mock_helper.setup_repositories.return_value = Mock()
            mock_helper_class.return_value = mock_helper

            setup_repositories_if_needed(args)

            # Verify domain was passed with konflux- prefix
            mock_client_class.create_from_config_file.assert_called_once()
            # Get the call arguments - call_args[1] is kwargs
            call_args = mock_client_class.create_from_config_file.call_args
            kwargs = call_args[1] if isinstance(call_args, tuple) else call_args.kwargs
            assert kwargs["domain"] == "konflux-test-domain"
            mock_logging.debug.assert_called()

    def test_setup_repositories_preserves_existing_konflux_prefix(self, tmp_path, mock_config):
        """Test setup_repositories_if_needed preserves existing konflux- prefix."""
        from pulp_tool.models.context import PullContext

        # Create config file with domain that already has konflux- prefix
        config_file = tmp_path / "config.toml"
        config_file.write_text('[cli]\nbase_url = "https://pulp.example.com"\ndomain = "konflux-test-domain"')

        args = PullContext(
            config=str(config_file),
            transfer_dest=str(config_file),
            build_id="test-build",
            namespace="test-namespace",
        )

        with (
            patch("pulp_tool.pull.download.PulpClient") as mock_client_class,
            patch("pulp_tool.pull.download.PulpHelper") as mock_helper_class,
            patch("pulp_tool.pull.download.logging") as mock_logging,
        ):
            mock_client = Mock()
            mock_client_class.create_from_config_file.return_value = mock_client

            # Mock PulpHelper to avoid real repository setup
            mock_helper = Mock()
            mock_helper.setup_repositories.return_value = Mock()
            mock_helper_class.return_value = mock_helper

            setup_repositories_if_needed(args)

            # Verify domain was preserved
            call_args = mock_client_class.create_from_config_file.call_args
            kwargs = call_args[1] if isinstance(call_args, tuple) else call_args.kwargs
            assert kwargs["domain"] == "konflux-test-domain"
            mock_logging.debug.assert_called()

    def test_setup_repositories_with_artifact_json_parent_package(self, mock_config, temp_config_file):
        """Test setup_repositories_if_needed extracts parent_package from artifact_json (lines 113-114)."""
        args = Mock()
        args.config = temp_config_file
        args.transfer_dest = temp_config_file
        args.build_id = "test-build"

        artifact_json = {"parent_package": "test-package"}

        with (
            patch("pulp_tool.pull.download.PulpClient.create_from_config_file") as mock_create,
            patch("pulp_tool.pull.download.determine_build_id", return_value="test-build"),
            patch("pulp_tool.pull.download.extract_metadata_from_artifact_json") as mock_extract,
            patch("pulp_tool.pull.download.PulpHelper") as mock_helper,
        ):
            mock_client = Mock()
            mock_create.return_value = mock_client
            mock_helper_instance = Mock()
            mock_helper.return_value = mock_helper_instance
            mock_extract.return_value = "test-package"

            from pulp_tool.models.repository import RepositoryRefs

            mock_repos = RepositoryRefs(
                rpms_href="/test/",
                rpms_prn="",
                logs_href="",
                logs_prn="",
                sbom_href="",
                sbom_prn="",
                artifacts_href="",
                artifacts_prn="",
            )
            mock_helper_instance.setup_repositories.return_value = mock_repos

            result = setup_repositories_if_needed(args, artifact_json=artifact_json)

            assert result == mock_client
            mock_extract.assert_called_once_with(artifact_json, "parent_package")
            mock_helper.assert_called_once_with(mock_client, parent_package="test-package")


class TestLoadAndValidateArtifacts:
    """Test load_and_validate_artifacts function."""

    def test_load_and_validate_artifacts_no_location(self):
        """Test load_and_validate_artifacts exits when no artifact_location (lines 151-152)."""
        import sys

        args = Mock()
        args.artifact_location = None

        mock_client = Mock()

        # sys.exit is imported inside the function, so we patch sys.exit directly
        # Since sys.exit raises SystemExit, we catch that exception
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
            mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_no_artifacts(self, temp_file):
        """Test load_and_validate_artifacts exits when no artifacts found (lines 157-161)."""
        import sys

        args = Mock()
        args.artifact_location = temp_file

        # Write JSON without artifacts
        with open(temp_file, "w") as f:
            json.dump({"distributions": {}}, f)

        mock_client = Mock()

        # sys.exit is imported inside the function, so we patch sys.exit directly
        # Since sys.exit raises SystemExit, we catch that exception
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
            mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_validation_fails_bad_url(self, temp_file):
        """Invalid artifact url exits with validation error."""
        import sys

        args = Mock()
        args.artifact_location = temp_file

        with open(temp_file, "w") as f:
            json.dump(
                {
                    "artifacts": {
                        "a.rpm": {"labels": {}, "url": "ftp://example.com/a.rpm"},
                    },
                },
                f,
            )

        mock_client = Mock()
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
        mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_validation_fails_extra_top_level_key(self, temp_file):
        """Unknown top-level keys are rejected (strict document shape)."""
        import sys

        args = Mock()
        args.artifact_location = temp_file

        with open(temp_file, "w") as f:
            json.dump(
                {
                    "artifacts": {
                        "a.rpm": {"labels": {}, "url": "https://example.com/a.rpm"},
                    },
                    "build_id": "should-not-be-here",
                },
                f,
            )

        mock_client = Mock()
        with patch.object(sys, "exit", side_effect=SystemExit(1)) as mock_exit:
            with pytest.raises(SystemExit):
                load_and_validate_artifacts(args, mock_client)
        mock_exit.assert_called_once_with(1)

    def test_load_and_validate_artifacts_converts_to_typed_models(self, temp_file):
        """Test load_and_validate_artifacts converts artifacts to typed models (lines 164, 166, 170)."""
        args = Mock()
        args.artifact_location = temp_file

        # Write JSON with artifacts
        artifact_data = {
            "artifacts": {
                "test.rpm": {
                    "labels": {"build_id": "test-build", "arch": "x86_64"},
                    "url": "https://example.com/api/pulp-content/ns/build/rpms/Packages/t/test.rpm",
                    "sha256": "a" * 64,
                },
                "test.sbom": {
                    "labels": {"build_id": "test-build", "arch": "noarch"},
                    "url": "https://example.com/api/pulp-content/ns/build/sbom/test.sbom",
                },
            },
            "distributions": {
                "rpms": "https://example.com/rpms/",
                "sbom": "https://example.com/sbom/",
            },
        }

        with open(temp_file, "w") as f:
            json.dump(artifact_data, f)

        mock_client = Mock()

        result = load_and_validate_artifacts(args, mock_client)

        assert result.artifacts is not None
        assert len(result.artifacts) == 2
        assert "test.rpm" in result.artifacts
        assert "test.sbom" in result.artifacts
        # Verify artifacts are ArtifactMetadata instances
        from pulp_tool.models.artifacts import ArtifactMetadata

        assert isinstance(result.artifacts["test.rpm"], ArtifactMetadata)
        assert isinstance(result.artifacts["test.sbom"], ArtifactMetadata)
        # Verify artifact_json is ArtifactJsonResponse
        from pulp_tool.models.artifacts import ArtifactJsonResponse

        assert isinstance(result.artifact_json, ArtifactJsonResponse)


class TestDownloadArtifactsConcurrently:
    """Test download_artifacts_concurrently function."""

    def test_download_artifacts_concurrently_success(self, tmp_path):
        """Test successful concurrent downloads (lines 219-220, 222, 228-232, 235, 238-239, 241, 244, 246-252, 254)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {
                "labels": {"build_id": "test-build", "arch": "x86_64"},
                "url": "https://example.com/rpms/Packages/t/test.rpm",
            },
            "test.sbom": {
                "labels": {"build_id": "test-build", "arch": "noarch"},
                "url": "https://example.com/sbom/test.sbom",
            },
            "test.log": {
                "labels": {"build_id": "test-build", "arch": "noarch"},
                "url": "https://example.com/logs/test.log",
            },
        }
        distros = {
            "rpms": "https://example.com/rpms/",
            "sbom": "https://example.com/sbom/",
            "logs": "https://example.com/logs/",
        }

        mock_client = Mock()

        # Create mock futures
        future1: Future[Tuple[str, str]] = Future()
        future2: Future[Tuple[str, str]] = Future()
        future3: Future[Tuple[str, str]] = Future()

        # Set results for futures
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        future2.set_result(("test.sbom", str(tmp_path / "test.sbom")))
        future3.set_result(("test.log", str(tmp_path / "test.log")))

        # Create files
        (tmp_path / "test.rpm").write_text("rpm content")
        (tmp_path / "test.sbom").write_text("sbom content")
        (tmp_path / "test.log").write_text("log content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None

            # Mock submit to return futures
            mock_executor.submit.side_effect = [future1, future2, future3]

            # Mock as_completed to return futures in order
            mock_as_completed.return_value = [future1, future2, future3]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 3
            assert result.failed == 0
            assert len(result.pulled_artifacts.rpms) == 1
            assert len(result.pulled_artifacts.sboms) == 1
            assert len(result.pulled_artifacts.logs) == 1

    def test_download_artifacts_concurrently_with_dict_labels(self, tmp_path):
        """Test download with dict-based artifact labels (lines 241)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {
                "labels": {"build_id": "test-build"},
                "url": "https://example.com/rpms/Packages/t/test.rpm",
            },  # dict format
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.return_value = future1
            mock_as_completed.return_value = [future1]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert "test.rpm" in result.pulled_artifacts.rpms

    def test_download_artifacts_concurrently_with_artifact_metadata(self, tmp_path):
        """Test download with ArtifactMetadata instances (lines 238-239)."""
        import concurrent.futures
        from concurrent.futures import Future

        from pulp_tool.models.artifacts import ArtifactMetadata

        artifacts = {
            "test.rpm": ArtifactMetadata(
                labels={"build_id": "test-build", "arch": "x86_64"},
                url="https://example.com/rpms/Packages/t/test.rpm",
            ),
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.return_value = future1
            mock_as_completed.return_value = [future1]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert "test.rpm" in result.pulled_artifacts.rpms

    def test_download_artifacts_concurrently_with_httpx_error(self, tmp_path):
        """Test download handles httpx.HTTPError exceptions (lines 256-260)."""
        import concurrent.futures
        from concurrent.futures import Future

        artifacts = {
            "test.rpm": {
                "labels": {"build_id": "test-build"},
                "url": "https://example.com/rpms/Packages/t/test.rpm",
            },
            "test2.rpm": {
                "labels": {"build_id": "test-build"},
                "url": "https://example.com/rpms/Packages/t/test2.rpm",
            },
        }
        distros = {
            "rpms": "https://example.com/rpms/",
        }

        mock_client = Mock()
        future1: Future[Tuple[str, str]] = Future()
        future2: Future[Tuple[str, str]] = Future()
        future1.set_result(("test.rpm", str(tmp_path / "test.rpm")))
        future2.set_exception(httpx.HTTPError("Network error"))
        (tmp_path / "test.rpm").write_text("rpm content")

        with (
            patch.object(concurrent.futures, "ThreadPoolExecutor") as mock_executor_class,
            patch.object(concurrent.futures, "as_completed") as mock_as_completed,
        ):
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor
            mock_executor_class.return_value.__exit__.return_value = None
            mock_executor.submit.side_effect = [future1, future2]
            mock_as_completed.return_value = [future1, future2]

            result = download_artifacts_concurrently(artifacts, distros, mock_client, max_workers=4)

            assert result.completed == 1
            assert result.failed == 1


class TestExtractArtifactInfo:
    """Test _extract_artifact_info function."""

    def test_extract_artifact_info_with_dict(self):
        """Test _extract_artifact_info with dict input."""
        artifact_data = {
            "file": "/path/to/file.rpm",
            "labels": {"build_id": "test-build", "arch": "x86_64"},
        }

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {"build_id": "test-build", "arch": "x86_64"}

    def test_extract_artifact_info_with_dict_no_labels(self):
        """Test _extract_artifact_info with dict input without labels."""
        artifact_data = {"file": "/path/to/file.rpm"}

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {}

    def test_extract_artifact_info_with_model(self):
        """Test _extract_artifact_info with model object."""
        artifact_data = ArtifactFile(
            file="/path/to/file.rpm",
            labels={"build_id": "test-build", "arch": "x86_64"},
        )

        file_path, labels = _extract_artifact_info(artifact_data)

        assert file_path == "/path/to/file.rpm"
        assert labels == {"build_id": "test-build", "arch": "x86_64"}

    def test_extract_artifact_info_with_model_no_labels(self):
        """Test _extract_artifact_info with model object without labels."""

        # Create a mock object that has file but no labels
        class MockArtifact:
            file = "/path/to/file.rpm"

        artifact_data = MockArtifact()

        file_path, labels = _extract_artifact_info(artifact_data)  # type: ignore[arg-type]

        assert file_path == "/path/to/file.rpm"
        assert labels == {}

    def test_extract_artifact_info_unexpected_type(self):
        """Test _extract_artifact_info raises ValueError for unexpected type."""
        # Create an object that doesn't have file attribute and isn't a dict
        artifact_data = 123  # int type

        with pytest.raises(ValueError, match="Unexpected artifact_data type"):
            _extract_artifact_info(artifact_data)  # type: ignore[arg-type]
