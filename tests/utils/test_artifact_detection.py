"""Tests for artifact detection utilities."""

from types import SimpleNamespace
from unittest.mock import patch

from pulp_tool.models.artifacts import ArtifactMetadata
from pulp_tool.utils.artifact_detection import (
    _embedded_artifact_url,
    build_artifact_url,
    categorize_artifacts_by_type,
    detect_arch_from_filepath,
    detect_arch_from_rpm_filename,
    detect_artifact_type,
    extract_architecture_from_metadata,
    group_rpm_paths_by_arch,
    rpm_packages_letter_and_basename,
)


class TestDetectArtifactType:
    """Tests for detect_artifact_type function."""

    def test_detect_rpm(self):
        """Test detecting RPM artifacts."""
        assert detect_artifact_type("package.rpm") == "rpm"
        assert detect_artifact_type("package.RPM") == "rpm"
        assert detect_artifact_type("PACKAGE.RPM") == "rpm"

    def test_detect_log(self):
        """Test detecting log artifacts."""
        assert detect_artifact_type("build.log") == "log"
        assert detect_artifact_type("build.LOG") == "log"

    def test_detect_sbom(self):
        """Test detecting SBOM artifacts."""
        assert detect_artifact_type("sbom.json") == "sbom"
        assert detect_artifact_type("package.SBOM") == "sbom"

    def test_detect_artifact_type_unknown(self):
        """Test detect_artifact_type returns None for unknown types (line 41)."""
        assert detect_artifact_type("unknown.txt") is None
        assert detect_artifact_type("file.tar.gz") is None


class TestRpmPackagesLetterAndBasename:
    """Tests for rpm_packages_letter_and_basename."""

    def test_plain_filename(self):
        assert rpm_packages_letter_and_basename("whale.rpm") == ("whale.rpm", "w")
        assert rpm_packages_letter_and_basename("Whale.rpm") == ("Whale.rpm", "w")

    def test_nested_path_uses_basename_letter(self):
        assert rpm_packages_letter_and_basename("Packages/W/whale.rpm") == ("whale.rpm", "w")
        assert rpm_packages_letter_and_basename("Packages/l/libecpg-debuginfo.rpm") == (
            "libecpg-debuginfo.rpm",
            "l",
        )

    def test_arch_prefix_path(self):
        assert rpm_packages_letter_and_basename("s390x/libecpg-debuginfo-16.1.el10.s390x.rpm") == (
            "libecpg-debuginfo-16.1.el10.s390x.rpm",
            "l",
        )

    def test_empty_or_no_basename(self):
        assert rpm_packages_letter_and_basename("") == ("", "a")
        assert rpm_packages_letter_and_basename("   ") == ("", "a")
        assert rpm_packages_letter_and_basename("///") == ("", "a")


class TestBuildArtifactUrl:
    """Tests for build_artifact_url function."""

    def test_build_rpm_url(self):
        """Test building RPM URL."""
        distros = {"rpms": "https://example.com/rpms/"}
        url = build_artifact_url("package.rpm", "rpm", distros)
        assert url == "https://example.com/rpms/Packages/p/package.rpm"

    def test_build_rpm_url_from_packages_path(self):
        """RPM URL letter comes from basename, not Packages/ or arch prefix."""
        distros = {"rpms": "https://example.com/rpms/"}
        url = build_artifact_url("Packages/W/Whale.rpm", "rpm", distros)
        assert url == "https://example.com/rpms/Packages/w/Whale.rpm"

    def test_build_log_url(self):
        """Test building log URL."""
        distros = {"logs": "https://example.com/logs/"}
        url = build_artifact_url("build.log", "log", distros)
        assert url == "https://example.com/logs/build.log"

    def test_build_sbom_url(self):
        """Test building SBOM URL."""
        distros = {"sbom": "https://example.com/sbom/"}
        url = build_artifact_url("sbom.json", "sbom", distros)
        assert url == "https://example.com/sbom/sbom.json"

    def test_build_artifact_url_invalid_type(self):
        """Test build_artifact_url returns None for invalid type (line 68)."""
        distros = {"rpms": "https://example.com/rpms/"}
        url = build_artifact_url("package.rpm", "invalid", distros)
        assert url is None

    def test_build_rpm_url_empty_name_returns_none(self):
        distros = {"rpms": "https://example.com/rpms/"}
        assert build_artifact_url("", "rpm", distros) is None
        assert build_artifact_url("///", "rpm", distros) is None


class TestExtractArchitectureFromMetadata:
    """Tests for extract_architecture_from_metadata function."""

    def test_extract_from_artifact_metadata(self):
        """Test extracting architecture from ArtifactMetadata (line 88)."""
        metadata = ArtifactMetadata(labels={"arch": "x86_64"})
        assert extract_architecture_from_metadata(metadata) == "x86_64"

    def test_extract_from_artifact_metadata_no_arch(self):
        """Test extracting architecture from ArtifactMetadata without arch (line 88)."""
        metadata = ArtifactMetadata(labels={})
        assert extract_architecture_from_metadata(metadata) == "noarch"

    def test_extract_from_dict(self):
        """Test extracting architecture from dict."""
        metadata = {"labels": {"arch": "aarch64"}}
        assert extract_architecture_from_metadata(metadata) == "aarch64"

    def test_extract_from_dict_no_arch(self):
        """Test extracting architecture from dict without arch."""
        metadata: dict[str, dict[str, str]] = {"labels": {}}
        assert extract_architecture_from_metadata(metadata) == "noarch"


class TestEmbeddedArtifactUrl:
    """Tests for _embedded_artifact_url generic-object branch (getattr url)."""

    def test_non_model_non_dict_with_url_attribute(self):
        """Objects with a url attribute use getattr path (line 139)."""
        assert (
            _embedded_artifact_url(SimpleNamespace(url="  https://pull.example/x.rpm  "))
            == "https://pull.example/x.rpm"
        )

    def test_non_model_non_dict_without_url_returns_none(self):
        """Generic object without url yields None from getattr."""
        assert _embedded_artifact_url(SimpleNamespace()) is None


class TestCategorizeArtifactsByType:
    """Tests for categorize_artifacts_by_type function."""

    def test_categorize_prefers_embedded_url(self):
        """Use url from results JSON when present instead of building from distros."""
        embedded = "https://mtls.example.com/api/pulp-content/ns/build/logs/s390x/build.log"
        artifacts = {
            "ns/build/s390x/build.log": ArtifactMetadata(
                labels={"arch": "s390x"},
                url=embedded,
                sha256="65cc68fa",
            ),
        }
        wrong_distros = {"logs": "https://wrong.example.com/logs/"}

        with patch("pulp_tool.utils.artifact_detection.build_artifact_url") as mock_build:
            result = categorize_artifacts_by_type(artifacts, wrong_distros)

        assert len(result) == 1
        assert result[0][0] == "ns/build/s390x/build.log"
        assert result[0][1] == embedded
        mock_build.assert_not_called()

    def test_categorize_embedded_url_dict_metadata(self):
        """Dict artifact entries with url skip build_artifact_url."""
        embedded = "https://source.example/rpms/Packages/p/pkg.rpm"
        artifacts = {
            "pkg.rpm": {"labels": {"arch": "x86_64"}, "url": embedded, "sha256": "a" * 64},
        }
        with patch("pulp_tool.utils.artifact_detection.build_artifact_url") as mock_build:
            result = categorize_artifacts_by_type(artifacts, {"rpms": "https://other/"})
        assert len(result) == 1
        assert result[0][1] == embedded
        mock_build.assert_not_called()

    def test_categorize_basic(self):
        """Test basic artifact categorization."""
        artifacts = {
            "package.rpm": ArtifactMetadata(labels={"arch": "x86_64"}),
            "build.log": ArtifactMetadata(labels={"arch": "noarch"}),
            "sbom.json": ArtifactMetadata(labels={"arch": "noarch"}),
        }
        distros = {
            "rpms": "https://example.com/rpms/",
            "logs": "https://example.com/logs/",
            "sbom": "https://example.com/sbom/",
        }

        result = categorize_artifacts_by_type(artifacts, distros)

        assert len(result) == 3
        assert ("package.rpm", "https://example.com/rpms/Packages/p/package.rpm", "x86_64", "rpm") in result
        assert ("build.log", "https://example.com/logs/build.log", "noarch", "log") in result
        assert ("sbom.json", "https://example.com/sbom/sbom.json", "noarch", "sbom") in result

    def test_categorize_unknown_type(self):
        """Test categorization skips unknown artifact types (lines 120-121)."""
        artifacts = {
            "unknown.txt": ArtifactMetadata(labels={"arch": "noarch"}),
        }
        distros = {"rpms": "https://example.com/rpms/"}

        with patch("pulp_tool.utils.artifact_detection.logging") as mock_logging:
            result = categorize_artifacts_by_type(artifacts, distros)

            assert len(result) == 0
            mock_logging.debug.assert_called_once_with("Skipping %s: could not determine artifact type", "unknown.txt")

    def test_categorize_no_url(self):
        """Test categorization skips artifacts when URL cannot be built (lines 126-127)."""
        artifacts = {
            "package.rpm": ArtifactMetadata(labels={"arch": "x86_64"}),
        }
        # Use invalid artifact type to trigger None URL return
        with (
            patch("pulp_tool.utils.artifact_detection.detect_artifact_type", return_value="rpm"),
            patch("pulp_tool.utils.artifact_detection.build_artifact_url", return_value=None),
            patch("pulp_tool.utils.artifact_detection.logging") as mock_logging,
        ):
            result = categorize_artifacts_by_type(artifacts, {})

            assert len(result) == 0
            mock_logging.debug.assert_called_once_with("Skipping %s: could not build download URL", "package.rpm")

    def test_categorize_content_type_filter(self):
        """Test categorization with content type filter (lines 131-132)."""
        artifacts = {
            "package.rpm": ArtifactMetadata(labels={"arch": "x86_64"}),
            "build.log": ArtifactMetadata(labels={"arch": "noarch"}),
        }
        distros = {
            "rpms": "https://example.com/rpms/",
            "logs": "https://example.com/logs/",
        }

        with patch("pulp_tool.utils.artifact_detection.logging") as mock_logging:
            result = categorize_artifacts_by_type(artifacts, distros, content_types=["rpm"])

            assert len(result) == 1
            assert ("package.rpm", "https://example.com/rpms/Packages/p/package.rpm", "x86_64", "rpm") in result
            mock_logging.debug.assert_called_once_with(
                "Skipping %s: content type %s not in filter %s", "build.log", "log", ["rpm"]
            )

    def test_categorize_architecture_filter(self):
        """Test categorization with architecture filter (lines 136-137)."""
        artifacts = {
            "package1.rpm": ArtifactMetadata(labels={"arch": "x86_64"}),
            "package2.rpm": ArtifactMetadata(labels={"arch": "aarch64"}),
        }
        distros = {"rpms": "https://example.com/rpms/"}

        with patch("pulp_tool.utils.artifact_detection.logging") as mock_logging:
            result = categorize_artifacts_by_type(artifacts, distros, archs=["x86_64"])

            assert len(result) == 1
            assert ("package1.rpm", "https://example.com/rpms/Packages/p/package1.rpm", "x86_64", "rpm") in result
            mock_logging.debug.assert_called_once_with(
                "Skipping %s: architecture %s not in filter %s", "package2.rpm", "aarch64", ["x86_64"]
            )

    def test_categorize_with_both_filters(self):
        """Test categorization with both content type and architecture filters."""
        artifacts = {
            "package.rpm": ArtifactMetadata(labels={"arch": "x86_64"}),
            "build.log": ArtifactMetadata(labels={"arch": "noarch"}),
            "sbom.json": ArtifactMetadata(labels={"arch": "noarch"}),
        }
        distros = {
            "rpms": "https://example.com/rpms/",
            "logs": "https://example.com/logs/",
            "sbom": "https://example.com/sbom/",
        }

        result = categorize_artifacts_by_type(
            artifacts, distros, content_types=["rpm", "log"], archs=["x86_64", "noarch"]
        )

        assert len(result) == 2
        assert ("package.rpm", "https://example.com/rpms/Packages/p/package.rpm", "x86_64", "rpm") in result
        assert ("build.log", "https://example.com/logs/build.log", "noarch", "log") in result


class TestDetectArchFromFilepath:
    """Tests for detect_arch_from_filepath function."""

    def test_detect_arch_from_path(self):
        """Test detecting x86_64 architecture from file path."""
        assert detect_arch_from_filepath("/path/to/x86_64/package.rpm") == "x86_64"
        assert detect_arch_from_filepath("/build/x86_64/package.rpm") == "x86_64"

    def test_detect_arch_from_path_case_insensitive(self):
        """Test that architecture detection is case insensitive."""
        assert detect_arch_from_filepath("/path/to/X86_64/package.rpm") == "x86_64"
        assert detect_arch_from_filepath("/path/to/AARCH64/package.rpm") == "aarch64"

    def test_detect_arch_from_path_no_match(self):
        """Test that None is returned when no architecture is found in path."""
        assert detect_arch_from_filepath("/path/to/package.rpm") is None
        assert detect_arch_from_filepath("/path/package.rpm") is None
        assert detect_arch_from_filepath("package.rpm") is None

    def test_detect_arch_from_path_not_at_start_or_end(self):
        """Test that architecture must be in the middle of the path."""
        # Should not match if arch is at the start
        assert detect_arch_from_filepath("/x86_64/package.rpm") is None
        assert detect_arch_from_filepath("x86_64/package.rpm") is None
        # Should not match if arch is at the end
        assert detect_arch_from_filepath("/path/to/package.x86_64") is None


class TestDetectArchFromRpmFilename:
    """Tests for detect_arch_from_rpm_filename function."""

    def test_detect_arch_from_filename_x86_64(self):
        """Test detecting x86_64 architecture from RPM filename."""
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.x86_64.rpm") == "x86_64"
        assert detect_arch_from_rpm_filename("package-1.0.0-1.x86_64.rpm") == "x86_64"

    def test_detect_arch_from_filename_noarch(self):
        """Test detecting noarch architecture from RPM filename."""
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.noarch.rpm") == "noarch"

    def test_detect_arch_from_filename_src(self):
        """Test detecting src 'architecture' from source RPM filename."""
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.src.rpm") == "src"

    def test_detect_arch_from_filename_no_match(self):
        """Test that None is returned when no architecture is found in filename."""
        assert detect_arch_from_rpm_filename("/path/to/package.rpm") is None
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0.rpm") is None
        assert detect_arch_from_rpm_filename("package.rpm") is None

    def test_detect_arch_from_filename_unsupported_arch(self):
        """Test that unsupported architectures return None."""
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.i386.rpm") is None
        assert detect_arch_from_rpm_filename("/path/to/package-1.0.0-1.armv7hl.rpm") is None

    def test_detect_arch_from_filename_with_underscores(self):
        """Test that architectures with underscores work correctly."""
        assert detect_arch_from_rpm_filename("/path/to/pack_age-1.0.0-1.x86_64.rpm") == "x86_64"


class TestGroupRpmPathsByArch:
    """Tests for group_rpm_paths_by_arch function."""

    def test_groups_by_detected_arch(self):
        """Test grouping RPMs by detected architecture from path/filename."""
        paths = [
            "/path/to/x86_64/package.rpm",
            "/path/to/package-1.0.0-1.aarch64.rpm",
            "/path/to/noarch/foo.rpm",
        ]
        result = group_rpm_paths_by_arch(paths)
        assert set(result.keys()) == {"x86_64", "aarch64", "noarch"}
        assert result["x86_64"] == ["/path/to/x86_64/package.rpm"]
        assert result["aarch64"] == ["/path/to/package-1.0.0-1.aarch64.rpm"]
        assert result["noarch"] == ["/path/to/noarch/foo.rpm"]

    def test_explicit_arch_applies_to_all(self):
        """Test that explicit_arch is used for all paths."""
        paths = ["/path/package1.rpm", "/path/package2.rpm"]
        result = group_rpm_paths_by_arch(paths, explicit_arch="noarch")
        assert result == {"noarch": ["/path/package1.rpm", "/path/package2.rpm"]}

    def test_skips_undetected_and_logs_warning(self):
        """Test that paths with undetectable arch are skipped and warning is logged."""
        paths = ["/path/to/package.rpm"]  # No arch in path or filename
        with patch("pulp_tool.utils.artifact_detection.logging") as mock_logging:
            result = group_rpm_paths_by_arch(paths)
        assert result == {}
        mock_logging.warning.assert_called_once()

    def test_empty_list_returns_empty_dict(self):
        """Test that empty input returns empty dict."""
        assert group_rpm_paths_by_arch([]) == {}
        assert group_rpm_paths_by_arch([], explicit_arch="x86_64") == {}
