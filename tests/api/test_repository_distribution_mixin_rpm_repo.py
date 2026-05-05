"""
Tests for BaseRepositoryMixin and BaseDistributionMixin.

This module tests repository and distribution mixin methods that need coverage.
"""

import httpx
from pulp_tool.models.pulp_api import (
    RepositoryRequest,
    DistributionRequest,
    RepositoryResponse,
    DistributionResponse,
    RpmRepositoryRequest,
    RpmRepositoryResponse,
)


class TestBaseRepositoryMixin:
    """Test BaseRepositoryMixin methods."""

    def test_create_repository_with_task(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_repository when response contains a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )
        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)
        assert response.status_code == 202
        assert task_href == "/api/v3/tasks/12345/"

    def test_create_repository_without_task(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_repository when response doesn't contain a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/repositories/12345/", "name": "test-repo"})
        )
        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)
        assert response.status_code == 201
        assert task_href is None

    def test_create_repository_invalid_json(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_repository when response has invalid JSON."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(201, text="not json")
        )
        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)
        assert response.status_code == 201
        assert task_href is None

    def test_get_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test get_repository method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/"
            "?name=test-repo&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/api/v3/repositories/12345/", "name": "test-repo"}]}
            )
        )
        result = mock_pulp_client.get_file_repository("test-repo")
        assert isinstance(result, RepositoryResponse)
        assert result.name == "test-repo"

    def test_list_repositories(self, mock_pulp_client, httpx_mock) -> None:
        """Test list_repositories method."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [{"pulp_href": "/api/v3/repositories/12345/", "name": "test-repo"}],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )
        results, next_url, prev_url, count = mock_pulp_client.list_file_repositories()
        assert len(results) == 1
        assert isinstance(results[0], RepositoryResponse)
        assert count == 1

    def test_update_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test update_repository method."""
        httpx_mock.patch("https://pulp.example.com/api/v3/repositories/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/api/v3/repositories/12345/", "name": "updated-repo"})
        )
        request = RepositoryRequest(name="updated-repo")
        result = mock_pulp_client.update_file_repository("/api/v3/repositories/12345/", request)
        assert isinstance(result, RepositoryResponse)
        assert result.name == "updated-repo"

    def test_delete_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test delete_repository method."""
        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/12345/").mock(return_value=httpx.Response(204))
        mock_pulp_client.delete_file_repository("/api/v3/repositories/12345/")
        assert True


class TestBaseDistributionMixin:
    """Test BaseDistributionMixin methods."""

    def test_create_distribution_with_task(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_distribution when response contains a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )
        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)
        assert response.status_code == 202
        assert task_href == "/api/v3/tasks/12345/"

    def test_create_distribution_without_task(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_distribution when response doesn't contain a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/distributions/12345/", "name": "test-distro"})
        )
        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)
        assert response.status_code == 201
        assert task_href is None

    def test_create_distribution_invalid_json(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_distribution when response has invalid JSON."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(201, text="not json")
        )
        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)
        assert response.status_code == 201
        assert task_href is None

    def test_get_distribution(self, mock_pulp_client, httpx_mock) -> None:
        """Test get_distribution method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/"
            "?name=test-distro&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": "/api/v3/distributions/12345/", "name": "test-distro", "base_path": "test-distro"}
                    ]
                },
            )
        )
        result = mock_pulp_client.get_file_distribution("test-distro")
        assert isinstance(result, DistributionResponse)
        assert result.name == "test-distro"

    def test_list_distributions(self, mock_pulp_client, httpx_mock) -> None:
        """Test list_distributions method."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/?").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {"pulp_href": "/api/v3/distributions/12345/", "name": "test-distro", "base_path": "test-distro"}
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )
        results, next_url, prev_url, count = mock_pulp_client.list_file_distributions()
        assert len(results) == 1
        assert isinstance(results[0], DistributionResponse)
        assert count == 1

    def test_update_distribution(self, mock_pulp_client, httpx_mock) -> None:
        """Test update_distribution method."""
        httpx_mock.patch("https://pulp.example.com/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": "/api/v3/distributions/12345/",
                    "name": "updated-distro",
                    "base_path": "updated-distro",
                },
            )
        )
        request = DistributionRequest(name="updated-distro", base_path="updated-distro")
        result = mock_pulp_client.update_file_distribution("/api/v3/distributions/12345/", request)
        assert isinstance(result, DistributionResponse)
        assert result.name == "updated-distro"

    def test_delete_distribution(self, mock_pulp_client, httpx_mock) -> None:
        """Test delete_distribution method."""
        httpx_mock.delete("https://pulp.example.com/api/v3/distributions/12345/").mock(return_value=httpx.Response(204))
        mock_pulp_client.delete_file_distribution("/api/v3/distributions/12345/")
        assert True


class TestRpmRepositoryMixin:
    """Test RpmRepositoryMixin methods."""

    def test_create_rpm_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test create_rpm_repository method (lines 34-35)."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/repositories/12345/", "name": "test-rpm-repo"})
        )
        request = RpmRepositoryRequest(name="test-rpm-repo")
        response, task_href = mock_pulp_client.create_rpm_repository(request)
        assert response.status_code == 201
        assert task_href is None

    def test_get_rpm_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test get_rpm_repository method (lines 52-54)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/"
            "?name=test-rpm-repo&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/api/v3/repositories/12345/", "name": "test-rpm-repo"}]}
            )
        )
        result = mock_pulp_client.get_rpm_repository("test-rpm-repo")
        assert isinstance(result, RpmRepositoryResponse)
        assert result.name == "test-rpm-repo"

    def test_fetch_rpm_repository_by_href(self, mock_pulp_client, httpx_mock) -> None:
        """Test fetch_rpm_repository_by_href GETs repository detail by pulp_href."""
        href = "/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/abc/"
        httpx_mock.get(f"https://pulp.example.com{href}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": href,
                    "name": "my-repo",
                    "latest_version_href": "/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/abc/versions/1/",
                },
            )
        )
        result = mock_pulp_client.fetch_rpm_repository_by_href(href)
        assert isinstance(result, RpmRepositoryResponse)
        assert result.name == "my-repo"
        assert result.latest_version_href is not None
        assert result.latest_version_href.endswith("/versions/1/")

    def test_list_rpm_repositories(self, mock_pulp_client, httpx_mock) -> None:
        """Test list_rpm_repositories method (lines 73-74)."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [{"pulp_href": "/api/v3/repositories/12345/", "name": "test-rpm-repo"}],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )
        results, next_url, prev_url, count = mock_pulp_client.list_rpm_repositories()
        assert len(results) == 1
        assert isinstance(results[0], RpmRepositoryResponse)
        assert count == 1

    def test_update_rpm_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test update_rpm_repository method (line 92)."""
        httpx_mock.patch("https://pulp.example.com/api/v3/repositories/12345/").mock(
            return_value=httpx.Response(
                200, json={"pulp_href": "/api/v3/repositories/12345/", "name": "updated-rpm-repo"}
            )
        )
        request = RpmRepositoryRequest(name="updated-rpm-repo")
        result = mock_pulp_client.update_rpm_repository("/api/v3/repositories/12345/", request)
        assert isinstance(result, RpmRepositoryResponse)
        assert result.name == "updated-rpm-repo"

    def test_delete_rpm_repository(self, mock_pulp_client, httpx_mock) -> None:
        """Test delete_rpm_repository method (line 106)."""
        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/12345/").mock(return_value=httpx.Response(204))
        mock_pulp_client.delete_rpm_repository("/api/v3/repositories/12345/")
        assert True
