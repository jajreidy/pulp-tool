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
    RpmDistributionRequest,
    RpmRepositoryResponse,
    RpmDistributionResponse,
)


class TestBaseRepositoryMixin:
    """Test BaseRepositoryMixin methods."""

    def test_create_repository_with_task(self, mock_pulp_client, httpx_mock):
        """Test create_repository when response contains a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )

        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)

        assert response.status_code == 202
        assert task_href == "/api/v3/tasks/12345/"

    def test_create_repository_without_task(self, mock_pulp_client, httpx_mock):
        """Test create_repository when response doesn't contain a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/repositories/12345/", "name": "test-repo"})
        )

        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)

        assert response.status_code == 201
        assert task_href is None

    def test_create_repository_invalid_json(self, mock_pulp_client, httpx_mock):
        """Test create_repository when response has invalid JSON."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/").mock(
            return_value=httpx.Response(201, text="not json")
        )

        request = RepositoryRequest(name="test-repo")
        response, task_href = mock_pulp_client.create_file_repository(request)

        assert response.status_code == 201
        assert task_href is None  # Should handle ValueError gracefully

    def test_get_repository(self, mock_pulp_client, httpx_mock):
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

    def test_list_repositories(self, mock_pulp_client, httpx_mock):
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

    def test_update_repository(self, mock_pulp_client, httpx_mock):
        """Test update_repository method."""
        httpx_mock.patch("https://pulp.example.com/api/v3/repositories/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/api/v3/repositories/12345/", "name": "updated-repo"})
        )

        request = RepositoryRequest(name="updated-repo")
        result = mock_pulp_client.update_file_repository("/api/v3/repositories/12345/", request)

        assert isinstance(result, RepositoryResponse)
        assert result.name == "updated-repo"

    def test_delete_repository(self, mock_pulp_client, httpx_mock):
        """Test delete_repository method."""
        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/12345/").mock(return_value=httpx.Response(204))

        mock_pulp_client.delete_file_repository("/api/v3/repositories/12345/")

        # Should not raise an exception
        assert True


class TestBaseDistributionMixin:
    """Test BaseDistributionMixin methods."""

    def test_create_distribution_with_task(self, mock_pulp_client, httpx_mock):
        """Test create_distribution when response contains a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(202, json={"task": "/api/v3/tasks/12345/"})
        )

        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)

        assert response.status_code == 202
        assert task_href == "/api/v3/tasks/12345/"

    def test_create_distribution_without_task(self, mock_pulp_client, httpx_mock):
        """Test create_distribution when response doesn't contain a task."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/distributions/12345/", "name": "test-distro"})
        )

        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)

        assert response.status_code == 201
        assert task_href is None

    def test_create_distribution_invalid_json(self, mock_pulp_client, httpx_mock):
        """Test create_distribution when response has invalid JSON."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(201, text="not json")
        )

        request = DistributionRequest(name="test-distro", base_path="test-distro")
        response, task_href = mock_pulp_client.create_file_distribution(request)

        assert response.status_code == 201
        assert task_href is None  # Should handle ValueError gracefully

    def test_get_distribution(self, mock_pulp_client, httpx_mock):
        """Test get_distribution method."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/"
            "?name=test-distro&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-distro",
                            "base_path": "test-distro",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_file_distribution("test-distro")

        assert isinstance(result, DistributionResponse)
        assert result.name == "test-distro"

    def test_list_distributions(self, mock_pulp_client, httpx_mock):
        """Test list_distributions method."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/?").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-distro",
                            "base_path": "test-distro",
                        }
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

    def test_update_distribution(self, mock_pulp_client, httpx_mock):
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

    def test_delete_distribution(self, mock_pulp_client, httpx_mock):
        """Test delete_distribution method."""
        httpx_mock.delete("https://pulp.example.com/api/v3/distributions/12345/").mock(return_value=httpx.Response(204))

        mock_pulp_client.delete_file_distribution("/api/v3/distributions/12345/")

        # Should not raise an exception
        assert True


class TestRpmRepositoryMixin:
    """Test RpmRepositoryMixin methods."""

    def test_create_rpm_repository(self, mock_pulp_client, httpx_mock):
        """Test create_rpm_repository method (lines 34-35)."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/rpm/rpm/").mock(
            return_value=httpx.Response(201, json={"pulp_href": "/api/v3/repositories/12345/", "name": "test-rpm-repo"})
        )

        request = RpmRepositoryRequest(name="test-rpm-repo")
        response, task_href = mock_pulp_client.create_rpm_repository(request)

        assert response.status_code == 201
        assert task_href is None

    def test_get_rpm_repository(self, mock_pulp_client, httpx_mock):
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

    def test_list_rpm_repositories(self, mock_pulp_client, httpx_mock):
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

    def test_update_rpm_repository(self, mock_pulp_client, httpx_mock):
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

    def test_delete_rpm_repository(self, mock_pulp_client, httpx_mock):
        """Test delete_rpm_repository method (line 106)."""
        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/12345/").mock(return_value=httpx.Response(204))

        mock_pulp_client.delete_rpm_repository("/api/v3/repositories/12345/")

        # Should not raise an exception
        assert True


class TestRpmDistributionMixin:
    """Test RpmDistributionMixin methods."""

    def test_create_rpm_distribution(self, mock_pulp_client, httpx_mock):
        """Test create_rpm_distribution method (lines 34-35)."""
        httpx_mock.post("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/rpm/rpm/").mock(
            return_value=httpx.Response(
                201, json={"pulp_href": "/api/v3/distributions/12345/", "name": "test-rpm-distro", "base_path": "test"}
            )
        )

        request = RpmDistributionRequest(name="test-rpm-distro", base_path="test")
        response, task_href = mock_pulp_client.create_rpm_distribution(request)

        assert response.status_code == 201
        assert task_href is None

    def test_get_rpm_distribution(self, mock_pulp_client, httpx_mock):
        """Test get_rpm_distribution method (lines 52-54)."""
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/rpm/rpm/"
            "?name=test-rpm-distro&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-rpm-distro",
                            "base_path": "test",
                        }
                    ]
                },
            )
        )

        result = mock_pulp_client.get_rpm_distribution("test-rpm-distro")

        assert isinstance(result, RpmDistributionResponse)
        assert result.name == "test-rpm-distro"

    def test_list_rpm_distributions(self, mock_pulp_client, httpx_mock):
        """Test list_rpm_distributions method (lines 73-74)."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/rpm/rpm/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-rpm-distro",
                            "base_path": "test",
                        }
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mock_pulp_client.list_rpm_distributions()

        assert len(results) == 1
        assert isinstance(results[0], RpmDistributionResponse)
        assert count == 1

    def test_update_rpm_distribution(self, mock_pulp_client, httpx_mock):
        """Test update_rpm_distribution method (line 92)."""
        httpx_mock.patch("https://pulp.example.com/api/v3/distributions/12345/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": "/api/v3/distributions/12345/",
                    "name": "updated-rpm-distro",
                    "base_path": "updated",
                },
            )
        )

        request = DistributionRequest(name="updated-rpm-distro", base_path="updated")
        result = mock_pulp_client.update_rpm_distribution("/api/v3/distributions/12345/", request)

        assert isinstance(result, RpmDistributionResponse)
        assert result.name == "updated-rpm-distro"

    def test_delete_rpm_distribution(self, mock_pulp_client, httpx_mock):
        """Test delete_rpm_distribution method (line 106)."""
        httpx_mock.delete("https://pulp.example.com/api/v3/distributions/12345/").mock(return_value=httpx.Response(204))

        mock_pulp_client.delete_rpm_distribution("/api/v3/distributions/12345/")

        # Should not raise an exception
        assert True


class TestBaseRepositoryMixinDirect:
    """Test BaseRepositoryMixin methods directly (not through concrete implementations)."""

    def test_get_repository_direct(self, mock_pulp_client, httpx_mock):
        """Test get_repository method directly (line 57)."""
        from pulp_tool.api.repositories.base import BaseRepositoryMixin

        # Create a test client that uses BaseRepositoryMixin methods directly
        class TestClient(BaseRepositoryMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._url = client._url
                self._get_resource = client._get_resource

        test_client = TestClient(mock_pulp_client)
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/repositories/file/file/"
            "?name=test-repo&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200, json={"results": [{"pulp_href": "/api/v3/repositories/12345/", "name": "test-repo"}]}
            )
        )

        result = test_client.get_repository("api/v3/repositories/file/file/", "test-repo")

        assert isinstance(result, RepositoryResponse)
        assert result.name == "test-repo"

    def test_list_repositories_direct(self, mock_pulp_client, httpx_mock):
        """Test list_repositories method directly (line 72)."""
        from pulp_tool.api.repositories.base import BaseRepositoryMixin

        class TestClient(BaseRepositoryMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._url = client._url
                self._list_resources = client._list_resources

        test_client = TestClient(mock_pulp_client)
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

        results, next_url, prev_url, count = test_client.list_repositories("api/v3/repositories/file/file/")

        assert len(results) == 1
        assert isinstance(results[0], RepositoryResponse)
        assert count == 1

    def test_update_repository_direct(self, mock_pulp_client, httpx_mock):
        """Test update_repository method directly (line 85)."""
        from pulp_tool.api.repositories.base import BaseRepositoryMixin

        class TestClient(BaseRepositoryMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._update_resource = client._update_resource

        test_client = TestClient(mock_pulp_client)
        httpx_mock.patch("https://pulp.example.com/api/v3/repositories/12345/").mock(
            return_value=httpx.Response(200, json={"pulp_href": "/api/v3/repositories/12345/", "name": "updated-repo"})
        )

        request = RepositoryRequest(name="updated-repo")
        result = test_client.update_repository("/api/v3/repositories/12345/", request)

        assert isinstance(result, RepositoryResponse)
        assert result.name == "updated-repo"

    def test_delete_repository_direct(self, mock_pulp_client, httpx_mock):
        """Test delete_repository method directly (line 94)."""
        from pulp_tool.api.repositories.base import BaseRepositoryMixin

        class TestClient(BaseRepositoryMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._delete_resource = client._delete_resource

        test_client = TestClient(mock_pulp_client)
        httpx_mock.delete("https://pulp.example.com/api/v3/repositories/12345/").mock(return_value=httpx.Response(204))

        test_client.delete_repository("/api/v3/repositories/12345/")

        # Should not raise an exception
        assert True


class TestBaseDistributionMixinDirect:
    """Test BaseDistributionMixin methods directly (not through concrete implementations)."""

    def test_get_distribution_direct(self, mock_pulp_client, httpx_mock):
        """Test get_distribution method directly (line 57)."""
        from pulp_tool.api.distributions.base import BaseDistributionMixin

        class TestClient(BaseDistributionMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._url = client._url
                self._get_resource = client._get_resource

        test_client = TestClient(mock_pulp_client)
        httpx_mock.get(
            "https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/"
            "?name=test-distro&offset=0&limit=1"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-distro",
                            "base_path": "test-distro",
                        }
                    ]
                },
            )
        )

        result = test_client.get_distribution("api/v3/distributions/file/file/", "test-distro")

        assert isinstance(result, DistributionResponse)
        assert result.name == "test-distro"

    def test_list_distributions_direct(self, mock_pulp_client, httpx_mock):
        """Test list_distributions method directly (line 72)."""
        from pulp_tool.api.distributions.base import BaseDistributionMixin

        class TestClient(BaseDistributionMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._url = client._url
                self._list_resources = client._list_resources

        test_client = TestClient(mock_pulp_client)
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/distributions/file/file/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/distributions/12345/",
                            "name": "test-distro",
                            "base_path": "test-distro",
                        }
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = test_client.list_distributions("api/v3/distributions/file/file/")

        assert len(results) == 1
        assert isinstance(results[0], DistributionResponse)
        assert count == 1

    def test_update_distribution_direct(self, mock_pulp_client, httpx_mock):
        """Test update_distribution method directly (line 85)."""
        from pulp_tool.api.distributions.base import BaseDistributionMixin

        class TestClient(BaseDistributionMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._update_resource = client._update_resource

        test_client = TestClient(mock_pulp_client)
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
        result = test_client.update_distribution("/api/v3/distributions/12345/", request)

        assert isinstance(result, DistributionResponse)
        assert result.name == "updated-distro"

    def test_delete_distribution_direct(self, mock_pulp_client, httpx_mock):
        """Test delete_distribution method directly (line 94)."""
        from pulp_tool.api.distributions.base import BaseDistributionMixin

        class TestClient(BaseDistributionMixin):
            def __init__(self, client):
                self.config = client.config
                self.session = client.session
                self.timeout = client.timeout
                self.request_params = client.request_params
                self._delete_resource = client._delete_resource

        test_client = TestClient(mock_pulp_client)
        httpx_mock.delete("https://pulp.example.com/api/v3/distributions/12345/").mock(return_value=httpx.Response(204))

        test_client.delete_distribution("/api/v3/distributions/12345/")

        # Should not raise an exception
        assert True
