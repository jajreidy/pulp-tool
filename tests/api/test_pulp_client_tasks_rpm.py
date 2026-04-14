"""PulpClient tests (split module)."""

import re
from unittest.mock import patch

import httpx
import pytest


class TestPulpClient:
    def test_get_task(self, mock_pulp_client, httpx_mock):
        """Test _get_task method."""
        # Mock the task endpoint
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(
                200,
                json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed", "result": {"status": "success"}},
            )
        )

        result = mock_pulp_client.get_task("/pulp/api/v3/tasks/12345/")

        # Now returns a TaskResponse model
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "completed"

    def test_wait_for_finished_task_success(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task method with successful completion."""
        # Mock the task endpoint
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "completed"})
        )

        with patch("time.sleep"):
            result = mock_pulp_client.wait_for_finished_task("/pulp/api/v3/tasks/12345/")

        # Now returns a TaskResponse model
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "completed"

    def test_wait_for_finished_task_timeout(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task method with timeout."""
        # Mock the task endpoint to return running state
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/pulp/api/v3/tasks/12345/", "state": "running"})
        )

        # The method now raises TimeoutError instead of returning the last response
        with patch("time.sleep"), patch("time.time", side_effect=[0, 0.5, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]):
            with patch("pulp_tool.api.tasks.operations.logging"):
                result = mock_pulp_client.wait_for_finished_task("/pulp/api/v3/tasks/12345/", timeout=1)

        # Now returns a TaskResponse model even on timeout (last state)
        from pulp_tool.models.pulp_api import TaskResponse

        assert isinstance(result, TaskResponse)
        assert result.state == "running"

    def test_find_content_by_build_id(self, mock_pulp_client, httpx_mock):
        """Test find_content method by build_id."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~test-build-123"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/pulp/api/v3/content/rpm/packages/12345/"}]}
            )
        )

        result = mock_pulp_client.find_content("build_id", "test-build-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_find_content_by_href(self, mock_pulp_client, httpx_mock):
        """Test find_content method by href."""
        # Mock the content search endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_href__in=/pulp/api/v3/content/12345/"
        ).mock(return_value=httpx.Response(200, json={"results": [{"pulp_href": "/pulp/api/v3/content/12345/"}]}))

        result = mock_pulp_client.find_content("href", "/pulp/api/v3/content/12345/")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_find_content_invalid_type(self, mock_pulp_client):
        """Test find_content method with invalid search type."""
        with pytest.raises(ValueError, match="Unknown search type"):
            mock_pulp_client.find_content("invalid", "test-value")

    def test_find_content_raises_on_http_error(self, mock_pulp_client, httpx_mock):
        """Non-success responses from content search are checked before JSON parsing."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~bad"
        ).mock(return_value=httpx.Response(502, text="Bad Gateway"))

        with pytest.raises(httpx.HTTPError, match="Failed to find content"):
            mock_pulp_client.find_content("build_id", "bad")

    def test_gather_content_data_empty_body_after_success_status(self, mock_pulp_client, httpx_mock):
        """Malformed empty200 from Pulp produces a clear error (regression for JSONDecodeError)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/?pulp_label_select=build_id~empty-body"
        ).mock(return_value=httpx.Response(200, content=b""))

        with pytest.raises(ValueError, match="Empty response body"):
            mock_pulp_client.gather_content_data("empty-body")

    def test_get_file_locations(self, mock_pulp_client, httpx_mock):
        """Test get_file_locations method."""
        # Mock the artifacts endpoint
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/artifacts/"
            "?pulp_href__in=/pulp/api/v3/artifacts/12345/"
        ).mock(return_value=httpx.Response(200, json={"results": [{"pulp_href": "/pulp/api/v3/artifacts/12345/"}]}))

        artifacts = [{"file": "/pulp/api/v3/artifacts/12345/"}]

        result = mock_pulp_client.get_file_locations(artifacts)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_pkgIDs(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_pkgIDs method."""
        # Mock the RPM search endpoint - URL encoding uses %2C for comma
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/content/rpm/packages/"
            "?pkgId__in=abcd1234%2Cefgh5678"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/pulp/api/v3/content/rpm/packages/12345/"}]}
            )
        )

        pkg_ids = ["abcd1234", "efgh5678"]

        result = mock_pulp_client.get_rpm_by_pkgIDs(pkg_ids)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_filenames(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames parses filename to NVR and searches by name+version+release."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "location_href": "pkg-1.0-1.x86_64.rpm",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames(["pkg-1.0-1.x86_64.rpm"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["location_href"] == "pkg-1.0-1.x86_64.rpm"

    def test_get_rpm_by_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_signed_by method."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pulp_label_select.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "pulp_labels": {"signed_by": "key-id-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_signed_by(["key-id-123"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-id-123"

    def test_get_rpm_by_checksums_and_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_checksums_and_signed_by with empty checksums returns empty (line 1436)."""
        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by([], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_checksums_and_signed_by_multi_chunk(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_checksums_and_signed_by with 4+ checksums uses multi-chunk path (lines 1454-1471)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64, "b" * 64, "c" * 64, "d" * 64]  # 4 checksums = 2 chunks (chunk_size=3)
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pkgId.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": f"/pkg/{i}/", "pkgId": c, "pulp_labels": {"signed_by": "key-123"}}
                        for i, c in enumerate(checksums)
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by(checksums, "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 4

    def test_get_rpm_by_checksums_and_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_checksums_and_signed_by combines filters in single query."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksum = "a" * 64
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pkgId.*signed_by")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "pkgId": checksum,
                            "pulp_labels": {"signed_by": "key-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_checksums_and_signed_by([checksum], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-123"

    def test_get_rpm_by_filenames_and_signed_by_combined_multi_nvr(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames_and_signed_by combined path with 2 NVRs uses multi-chunk (lines 1535-1544)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 2 NVRs -> 2 chunks in _fetch_rpm_by_nvr_and_signed_by_combined
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pkg/1/",
                            "name": "pkg1",
                            "version": "1.0",
                            "release": "1",
                            "pulp_labels": {"signed_by": "key-123"},
                        },
                        {
                            "pulp_href": "/pkg/2/",
                            "name": "pkg2",
                            "version": "1.0",
                            "release": "1",
                            "pulp_labels": {"signed_by": "key-123"},
                        },
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(
            ["pkg1-1.0-1.x86_64.rpm", "pkg2-1.0-1.x86_64.rpm"], "key-123"
        )

        assert result.status_code == 200
        assert len(result.json()["results"]) == 2

    def test_get_rpm_by_filenames_and_signed_by(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames_and_signed_by parses to NVR and combines with signed_by."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pulp/api/v3/content/rpm/packages/12345/",
                            "location_href": "pkg-1.0-1.x86_64.rpm",
                            "pulp_labels": {"signed_by": "key-123"},
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["pkg-1.0-1.x86_64.rpm"], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_labels"]["signed_by"] == "key-123"

    def test_get_rpm_by_filenames_and_signed_by_fallback_on_400(self, mock_pulp_client, httpx_mock):
        """Test fallback when combined query returns 400 (line 1487)."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        pkg1 = {"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.rpm", "pulp_labels": {"signed_by": "key-123"}}
        err_400 = httpx.Response(400, content=b"Bad Request")
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                err_400,
                httpx.Response(200, json={"results": [pkg1]}),
                httpx.Response(200, json={"results": [pkg1]}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["pkg1-1.0-1.x86_64.rpm"], "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1

    def test_get_rpm_by_filenames_and_signed_by_fallback_on_500(self, mock_pulp_client, httpx_mock):
        """Test fallback to two calls + intersect when combined query returns 500."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        pkg1 = {"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.rpm", "pulp_labels": {"signed_by": "key-123"}}
        pkg2 = {"pulp_href": "/pkg/2/", "location_href": "pkg2-1.0-1.rpm", "pulp_labels": {"signed_by": "other"}}
        pkg3 = {"pulp_href": "/pkg/3/", "location_href": "pkg3.rpm", "pulp_labels": {"signed_by": "key-123"}}
        err_500 = httpx.Response(500, content=b"Server Error")
        # Order: 2 combined chunk requests (both 500), 2 by_nvr chunks, 1 by_signed
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                err_500,
                err_500,
                httpx.Response(200, json={"results": [pkg1]}),
                httpx.Response(200, json={"results": [pkg2]}),
                httpx.Response(200, json={"results": [pkg1, pkg3]}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(
            ["pkg1-1.0-1.x86_64.rpm", "pkg2-1.0-1.x86_64.rpm"], "key-123"
        )

        assert result.status_code == 200
        # Intersection: only pkg1 (in both by_hrefs and by_signed)
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_href"] == "/pkg/1/"

    def test_get_rpm_by_filenames_and_signed_by_fallback_signed_by_first_when_many_nvrs(
        self, mock_pulp_client, httpx_mock
    ):
        """Test fallback uses signed_by-first (1 call) when NVRs >= 5 instead of N+1."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # Combined returns 500 (5 parallel chunk requests); with 5 NVRs we use signed_by-first - 1 call
        pkgs = [
            {
                "pulp_href": f"/pkg/{i}/",
                "name": f"pkg{i}",
                "version": "1.0",
                "release": "1",
                "pulp_labels": {"signed_by": "key-123"},
            }
            for i in range(5)
        ]
        err_500 = httpx.Response(500, content=b"Server Error")
        ok_signed = httpx.Response(200, json={"results": pkgs, "next": None})
        # 5 combined chunk requests (all 500) + 1 signed_by request (200)
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(side_effect=[err_500] * 5 + [ok_signed])

        filenames = [f"pkg{i}-1.0-1.x86_64.rpm" for i in range(5)]
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(filenames, "key-123")

        assert result.status_code == 200
        assert len(result.json()["results"]) == 5
        # Combined makes 5 chunk requests (all 500); fallback uses signed_by-first = 1 call (not 5 NVR + 1)
        package_calls = [c for c in httpx_mock.calls if "packages" in str(getattr(c.request, "url", ""))]
        assert len(package_calls) == 6  # 5 combined + 1 signed_by

    def test_get_rpm_by_filenames_and_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_filenames_and_signed_by with empty filenames returns empty (line 1487)."""
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by([], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_and_signed_by_all_unparseable(self, mock_pulp_client):
        """Test get_rpm_by_filenames_and_signed_by with all unparseable filenames returns empty (line 1487)."""
        result = mock_pulp_client.get_rpm_by_filenames_and_signed_by(["bad.rpm", "nover.rpm"], "key-123")
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_empty(self, mock_pulp_client):
        """Test get_rpm_by_filenames with empty list returns empty."""
        result = mock_pulp_client.get_rpm_by_filenames([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_filenames_skips_unparseable(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames skips unparseable filenames and searches parseable ones."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*name=good-pkg")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/pkg/1/",
                            "location_href": "good-pkg-1.0-1.x86_64.rpm",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_by_filenames(["bad.rpm", "good-pkg-1.0-1.x86_64.rpm", "malformed"])

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["location_href"] == "good-pkg-1.0-1.x86_64.rpm"

    def test_get_rpm_by_signed_by_empty(self, mock_pulp_client):
        """Test get_rpm_by_signed_by with empty list returns empty results."""
        result = mock_pulp_client.get_rpm_by_signed_by([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_unsigned_checksums_empty(self, mock_pulp_client):
        """Test get_rpm_by_unsigned_checksums with empty list returns empty results."""
        result = mock_pulp_client.get_rpm_by_unsigned_checksums([])
        assert result.status_code == 200
        assert result.json()["results"] == []
        assert result.json()["count"] == 0

    def test_get_rpm_by_unsigned_checksums_single_chunk(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_unsigned_checksums with 1-20 items uses single request path."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64 for _ in range(5)]
        results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}} for i, c in enumerate(checksums)
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*unsigned_checksum")).mock(
            return_value=httpx.Response(200, json={"results": results})
        )

        result = mock_pulp_client.get_rpm_by_unsigned_checksums(checksums)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 5

    def test_get_rpm_by_filenames_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames with 3 NVRs chunks and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 3 NVRs = 3 chunks (chunk_size=1 for packages.redhat.com complexity limit)
        chunk_results = [
            [{"pulp_href": "/pkg/0/", "location_href": "pkg0-1.0-1.x86_64.rpm"}],
            [{"pulp_href": "/pkg/1/", "location_href": "pkg1-1.0-1.x86_64.rpm"}],
            [{"pulp_href": "/pkg/2/", "location_href": "pkg2-1.0-1.x86_64.rpm"}],
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[httpx.Response(200, json={"results": r}) for r in chunk_results]
        )

        filenames = [f"pkg{i}-1.0-1.x86_64.rpm" for i in range(3)]
        result = mock_pulp_client.get_rpm_by_filenames(filenames)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 3

    def test_get_rpm_by_filenames_deduplicates_by_pulp_href(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_filenames deduplicates results by pulp_href when chunks return same package."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # Two chunks both return same package (duplicate pulp_href) - should dedupe to 1
        pkg = {"pulp_href": "/pkg/0/", "location_href": "pkg0-1.0-1.x86_64.rpm"}
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*")).mock(
            side_effect=[
                httpx.Response(200, json={"results": [pkg]}),
                httpx.Response(200, json={"results": [pkg]}),
            ]
        )

        filenames = ["pkg0-1.0-1.x86_64.rpm", "pkg0-1.0-1.src.rpm"]  # same NVR, different arch
        result = mock_pulp_client.get_rpm_by_filenames(filenames)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 1
        assert result.json()["results"][0]["pulp_href"] == "/pkg/0/"

    def test_get_rpm_by_signed_by_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_signed_by with 9 items triggers chunking and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        # 9 items = 3 chunks (4 + 4 + 1) with chunk_size=4
        chunk_results = [
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(0, 4)],
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(4, 8)],
            [{"pulp_href": f"/pkg/{i}/", "pulp_labels": {"signed_by": f"key-{i}"}} for i in range(8, 9)],
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*pulp_label_select.*signed_by")).mock(
            side_effect=[httpx.Response(200, json={"results": r}) for r in chunk_results]
        )

        keys = [f"key-{i}" for i in range(9)]
        result = mock_pulp_client.get_rpm_by_signed_by(keys)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 9

    def test_get_rpm_by_unsigned_checksums_chunking(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_by_unsigned_checksums with 25+ items triggers chunking and merges results."""
        httpx_mock.post("https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token").mock(
            return_value=httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        )
        checksums = ["a" * 64 for _ in range(25)]
        chunk1_results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}} for i, c in enumerate(checksums[:20])
        ]
        chunk2_results = [
            {"pulp_href": f"/pkg/{i}/", "pulp_labels": {"unsigned_checksum": c}}
            for i, c in enumerate(checksums[20:], start=20)
        ]
        httpx_mock.get(re.compile(r".*content/rpm/packages/.*q=.*unsigned_checksum")).mock(
            side_effect=[
                httpx.Response(200, json={"results": chunk1_results}),
                httpx.Response(200, json={"results": chunk2_results}),
            ]
        )

        result = mock_pulp_client.get_rpm_by_unsigned_checksums(checksums)

        assert result.status_code == 200
        assert len(result.json()["results"]) == 25
