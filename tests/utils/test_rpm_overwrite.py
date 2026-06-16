"""Tests for RPM upload overwrite helpers."""

import logging
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pulp_tool.models.pulp_api import RpmPackageResponse, TaskResponse
from pulp_tool.utils.rpm_overwrite import (
    filter_rpm_hrefs_in_repository_version,
    remove_rpms_matching_local_files_from_repository,
)


def _rpm_pkg(href: str, sha: str = "a" * 64) -> RpmPackageResponse:
    return RpmPackageResponse(pulp_href=href, name="pkg", version="1", release="1", arch="x86_64", sha256=sha)


def _touch_rpm(path: str, content: bytes = b"x") -> None:
    with open(path, "wb") as fp:
        fp.write(content)


def _valid_rpm_path(name: str = "overwrite-test-pkg-1.0-1.x86_64.rpm", content: bytes = b"x") -> str:
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, name)
    _touch_rpm(path, content)
    return path


class TestFilterRpmHrefsInRepositoryVersion:

    def test_filters_to_packages_returned_by_list(self) -> None:
        client = MagicMock()
        client.list_rpm_packages.return_value = ([_rpm_pkg("/p/a/"), _rpm_pkg("/p/b/")], None, None, 2)
        rv = "/versions/1/"
        candidates = ["/p/a/", "/p/b/", "/p/c/"]
        out = filter_rpm_hrefs_in_repository_version(client, rv, candidates)
        assert set(out) == {"/p/a/", "/p/b/"}
        client.list_rpm_packages.assert_called_once()
        call_kw = client.list_rpm_packages.call_args.kwargs
        assert call_kw["repository_version"] == rv
        assert "/p/a/" in call_kw["pulp_href__in"]
        assert "/p/b/" in call_kw["pulp_href__in"]
        assert "/p/c/" in call_kw["pulp_href__in"]

    def test_empty_candidates(self) -> None:
        client = MagicMock()
        assert filter_rpm_hrefs_in_repository_version(client, "/v/", []) == []
        client.list_rpm_packages.assert_not_called()


class TestRemoveRpmsMatchingLocalFilesFromRepository:

    def test_no_paths(self) -> None:
        client = MagicMock()
        assert remove_rpms_matching_local_files_from_repository(client, [], "/repo/", None) == 0
        client.fetch_rpm_repository_by_href.assert_not_called()

    def test_no_latest_version_skips_modify(self) -> None:
        path = _valid_rpm_path()
        try:
            client = MagicMock()
            pkg = _rpm_pkg("/pulp/api/v3/content/rpm/packages/nv/")
            with patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[pkg]):
                repo = MagicMock()
                repo.latest_version_href = None
                client.fetch_rpm_repository_by_href.return_value = repo
                n = remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
            assert n == 0
            client.modify_repository_content.assert_not_called()
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))

    def test_filter_empty_nothing_to_remove_skips_modify(self) -> None:
        path = _valid_rpm_path()
        try:
            client = MagicMock()
            pkg = _rpm_pkg("/pulp/api/v3/content/rpm/packages/out/")
            with (
                patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[pkg]),
                patch("pulp_tool.utils.rpm_overwrite.filter_rpm_hrefs_in_repository_version", return_value=[]),
            ):
                repo = MagicMock()
                repo.latest_version_href = "/v/0/"
                client.fetch_rpm_repository_by_href.return_value = repo
                n = remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
            assert n == 0
            client.modify_repository_content.assert_not_called()
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))

    def test_no_matching_packages_in_pulp(self) -> None:
        path = _valid_rpm_path()
        try:
            client = MagicMock()
            with patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[]):
                n = remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
            assert n == 0
            client.fetch_rpm_repository_by_href.assert_not_called()
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))

    def test_deduplicates_same_nvra_before_search(self) -> None:
        tmp = tempfile.mkdtemp()
        p1 = os.path.join(tmp, "dedup-pkg-1.0-1.fc40.x86_64.rpm")
        p2 = os.path.join(tmp, "nested", "dedup-pkg-1.0-1.fc40.x86_64.rpm")
        try:
            os.makedirs(os.path.dirname(p2), exist_ok=True)
            _touch_rpm(p1)
            _touch_rpm(p2)
            client = MagicMock()
            captured: list[list[str]] = []

            def _capture(_c: MagicMock, filenames: list[str], _sb: str | None) -> list[RpmPackageResponse]:
                captured.append(list(filenames))
                return []

            with patch(
                "pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite",
                side_effect=_capture,
            ):
                n = remove_rpms_matching_local_files_from_repository(client, [p1, p2], "/repo/", None)
            assert n == 0
            assert captured == [["dedup-pkg-1.0-1.fc40.x86_64.rpm"]]
        finally:
            os.unlink(p1)
            os.unlink(p2)
            os.rmdir(os.path.join(tmp, "nested"))
            os.rmdir(tmp)

    def test_unparseable_paths_skipped_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        tmp = tempfile.mkdtemp()
        bad = os.path.join(tmp, "foo.rpm")
        try:
            _touch_rpm(bad)
            client = MagicMock()
            with patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[]):
                with caplog.at_level(logging.WARNING):
                    n = remove_rpms_matching_local_files_from_repository(client, [bad], "/repo/", None)
            assert n == 0
            assert any("unparseable RPM path" in r.message for r in caplog.records)
        finally:
            os.unlink(bad)
            os.rmdir(tmp)

    def test_removes_when_confirmed_in_repo(self) -> None:
        path = _valid_rpm_path(content=b"unique-bytes-for-content")
        try:
            client = MagicMock()
            pkg = _rpm_pkg("/pulp/api/v3/content/rpm/packages/xyz/")
            with (
                patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[pkg]),
                patch(
                    "pulp_tool.utils.rpm_overwrite.filter_rpm_hrefs_in_repository_version", return_value=[pkg.pulp_href]
                ),
            ):
                repo = MagicMock()
                repo.latest_version_href = "/pulp/api/v3/repositories/rpm/rpm/1/versions/0/"
                client.fetch_rpm_repository_by_href.return_value = repo
                client.modify_repository_content.return_value = TaskResponse(
                    pulp_href="/tasks/t1/", state="pending", created_resources=[]
                )
                client.wait_for_finished_task.return_value = TaskResponse(
                    pulp_href="/tasks/t1/", state="completed", created_resources=[]
                )
                n = remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
            assert n == 1
            client.modify_repository_content.assert_called_once()
            assert client.modify_repository_content.call_args[1]["remove_content_units"] == [pkg.pulp_href]
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))

    def test_raises_when_task_failed(self) -> None:
        path = _valid_rpm_path()
        try:
            client = MagicMock()
            pkg = _rpm_pkg("/pulp/api/v3/content/rpm/packages/fail/")
            with (
                patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[pkg]),
                patch(
                    "pulp_tool.utils.rpm_overwrite.filter_rpm_hrefs_in_repository_version", return_value=[pkg.pulp_href]
                ),
            ):
                repo = MagicMock()
                repo.latest_version_href = "/v/0/"
                client.fetch_rpm_repository_by_href.return_value = repo
                client.modify_repository_content.return_value = TaskResponse(
                    pulp_href="/tasks/t2/", state="pending", created_resources=[]
                )
                client.wait_for_finished_task.return_value = TaskResponse(
                    pulp_href="/tasks/t2/", state="failed", created_resources=[], error={"reason": "x"}
                )
                with pytest.raises(RuntimeError, match="Overwrite remove_content_units task failed"):
                    remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))

    def test_continues_when_task_incomplete_after_wait(self, caplog: pytest.LogCaptureFixture) -> None:
        path = _valid_rpm_path()
        try:
            client = MagicMock()
            pkg = _rpm_pkg("/pulp/api/v3/content/rpm/packages/timeout/")
            with (
                patch("pulp_tool.utils.rpm_overwrite.search_rpms_by_filenames_for_overwrite", return_value=[pkg]),
                patch(
                    "pulp_tool.utils.rpm_overwrite.filter_rpm_hrefs_in_repository_version", return_value=[pkg.pulp_href]
                ),
            ):
                repo = MagicMock()
                repo.latest_version_href = "/v/0/"
                client.fetch_rpm_repository_by_href.return_value = repo
                client.modify_repository_content.return_value = TaskResponse(
                    pulp_href="/tasks/t3/", state="pending", created_resources=[]
                )
                client.wait_for_finished_task.return_value = TaskResponse(
                    pulp_href="/tasks/t3/", state="running", created_resources=[]
                )
                with caplog.at_level(logging.WARNING):
                    n = remove_rpms_matching_local_files_from_repository(client, [path], "/repo/", None)
            assert n == 0
            assert any("did not complete" in r.message for r in caplog.records)
        finally:
            os.unlink(path)
            os.rmdir(os.path.dirname(path))
