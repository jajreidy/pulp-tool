"""Tests for response utilities module."""

import pytest
import httpx

from pulp_tool.utils.response_utils import (
    parse_json_response,
    extract_task_href,
    extract_created_resources,
    check_task_success,
    extract_results_list,
    content_find_results_from_json,
    content_find_results_from_response,
    extract_single_result,
    get_response_field,
)
from pulp_tool.models.pulp_api import TaskResponse


class TestParseJsonResponse:
    """Test parse_json_response utility."""

    def test_parse_json_success(self):
        """Test parsing successful JSON response."""
        response = httpx.Response(200, json={"key": "value", "number": 42})
        result = parse_json_response(response, "test operation")
        assert result == {"key": "value", "number": 42}

    def test_parse_json_skip_success_check(self):
        """Test parsing JSON with success check disabled."""
        response = httpx.Response(400, json={"error": "test"})
        result = parse_json_response(response, "test operation", check_success=False)
        assert result == {"error": "test"}

    def test_parse_json_non_success_status(self):
        """Test parsing JSON with non-success status raises error."""
        response = httpx.Response(400, json={"error": "test"})
        with pytest.raises(ValueError, match="Response not successful"):
            parse_json_response(response, "test operation")

    def test_parse_json_invalid_json(self):
        """Test parsing invalid JSON raises error."""
        response = httpx.Response(200, content=b"not json")
        with pytest.raises(ValueError, match="Invalid JSON response"):
            parse_json_response(response, "test operation")


class TestExtractTaskHref:
    """Test extract_task_href utility."""

    def test_extract_task_href_success(self):
        """Test successful task href extraction."""
        response = httpx.Response(202, json={"task": "/pulp/api/v3/tasks/12345/"})
        result = extract_task_href(response, "create repository")
        assert result == "/pulp/api/v3/tasks/12345/"

    def test_extract_task_href_missing_key(self):
        """Test extraction fails when task key is missing."""
        response = httpx.Response(200, json={"other": "data"})
        with pytest.raises(KeyError, match="does not contain task href"):
            extract_task_href(response, "create repository")

    def test_extract_task_href_invalid_json(self):
        """Test extraction fails with invalid JSON."""
        response = httpx.Response(200, content=b"not json")
        with pytest.raises(ValueError, match="Invalid JSON response"):
            extract_task_href(response, "create repository")


class TestExtractCreatedResources:
    """Test extract_created_resources utility."""

    def test_extract_created_resources_success(self):
        """Test extracting created resources from task response."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="completed",
            created_resources=["/pulp/api/v3/repo/1/", "/pulp/api/v3/repo/2/"],
        )
        result = extract_created_resources(task_response, "test operation")
        assert len(result) == 2
        assert "/pulp/api/v3/repo/1/" in result

    def test_extract_created_resources_empty(self):
        """Test extracting when no resources were created."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="completed",
            created_resources=[],
        )
        result = extract_created_resources(task_response, "test operation")
        assert result == []

    def test_extract_created_resources_none(self):
        """Test extracting when created_resources is None."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="completed",
        )
        result = extract_created_resources(task_response, "test operation")
        assert result == []


class TestCheckTaskSuccess:
    """Test check_task_success utility."""

    def test_check_task_success_completed(self):
        """Test successful task check."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="completed",
        )
        result = check_task_success(task_response, "test operation")
        assert result is True

    def test_check_task_success_failed_with_error(self):
        """Test failed task with error description."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="failed",
            error={"description": "Database connection failed"},
        )
        with pytest.raises(ValueError, match="Task failed.*Database connection failed"):
            check_task_success(task_response, "test operation")

    def test_check_task_success_failed_no_error(self):
        """Test failed task without error description."""
        task_response = TaskResponse(
            pulp_href="/pulp/api/v3/tasks/123/",
            state="failed",
        )
        with pytest.raises(ValueError, match="Task failed.*Unknown error"):
            check_task_success(task_response, "test operation")


class TestExtractResultsList:
    """Test extract_results_list utility."""

    def test_extract_results_list_success(self):
        """Test extracting results list from response."""
        response = httpx.Response(
            200,
            json={
                "results": [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
                "count": 2,
            },
        )
        result = extract_results_list(response, "search operation")
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_extract_results_list_empty_allowed(self):
        """Test extracting empty results when allowed."""
        response = httpx.Response(200, json={"results": [], "count": 0})
        result = extract_results_list(response, "search operation", allow_empty=True)
        assert result == []

    def test_extract_results_list_empty_not_allowed(self):
        """Test extracting empty results when not allowed raises error."""
        response = httpx.Response(200, json={"results": [], "count": 0})
        with pytest.raises(ValueError, match="Empty results"):
            extract_results_list(response, "search operation", allow_empty=False)

    def test_extract_results_list_missing_results_key(self):
        """Test extraction when results key is missing."""
        response = httpx.Response(200, json={"count": 0})
        result = extract_results_list(response, "search operation", allow_empty=True)
        assert result == []


class TestContentFindResultsFromResponse:
    """Test content_find_results_from_response utility."""

    def test_success_paginated(self):
        """Parses standard paginated body."""
        response = httpx.Response(
            200,
            json={"results": [{"pulp_href": "/c/1/"}], "count": 1},
            request=httpx.Request("GET", "https://pulp.example.com/content/"),
        )
        assert content_find_results_from_response(response, "test") == [{"pulp_href": "/c/1/"}]

    def test_empty_body(self):
        """Empty body yields clear ValueError (not JSONDecodeError)."""
        response = httpx.Response(
            200,
            content=b"",
            request=httpx.Request("GET", "https://pulp.example.com/content/"),
        )
        with pytest.raises(ValueError, match="Empty response body"):
            content_find_results_from_response(response, "test")

    def test_non_success(self):
        response = httpx.Response(
            502,
            content=b"bad gateway",
            request=httpx.Request("GET", "https://pulp.example.com/content/"),
        )
        with pytest.raises(ValueError, match="Response not successful"):
            content_find_results_from_response(response, "test")

    def test_invalid_json(self):
        response = httpx.Response(
            200,
            content=b"not json",
            request=httpx.Request("GET", "https://pulp.example.com/content/"),
        )
        with pytest.raises(ValueError, match="Invalid JSON"):
            content_find_results_from_response(response, "test")


class TestContentFindResultsFromJson:
    """Test content_find_results_from_json utility."""

    def test_paginated_dict(self):
        """Standard Pulp paginated list body."""
        data = {"results": [{"pulp_href": "/c/1/"}], "count": 1}
        assert content_find_results_from_json(data) == [{"pulp_href": "/c/1/"}]

    def test_bare_list(self):
        """Bare JSON array as returned by some content list responses."""
        data = [{"pulp_href": "/c/1/"}]
        assert content_find_results_from_json(data) == [{"pulp_href": "/c/1/"}]

    def test_filters_non_dict_entries_in_list(self):
        """Non-dict list entries are skipped."""
        assert content_find_results_from_json([{"a": 1}, "skip", None]) == [{"a": 1}]

    def test_dict_missing_results(self):
        assert content_find_results_from_json({"count": 0}) == []

    def test_dict_results_not_list(self):
        assert content_find_results_from_json({"results": None}) == []

    def test_non_collection_returns_empty(self):
        assert content_find_results_from_json(123) == []


class TestExtractSingleResult:
    """Test extract_single_result utility."""

    def test_extract_single_result_success(self):
        """Test extracting single result from response."""
        response = httpx.Response(
            200,
            json={"results": [{"id": 1, "name": "test"}], "count": 1},
        )
        result = extract_single_result(response, "get operation")
        assert result == {"id": 1, "name": "test"}

    def test_extract_single_result_empty(self):
        """Test extracting single result from empty response fails."""
        response = httpx.Response(200, json={"results": [], "count": 0})
        with pytest.raises(ValueError, match="Empty results"):
            extract_single_result(response, "get operation")


class TestGetResponseField:
    """Test get_response_field utility."""

    def test_get_response_field_exists(self):
        """Test getting existing field from response."""
        response = httpx.Response(200, json={"field": "value", "other": 123})
        result = get_response_field(response, "field", "test operation")
        assert result == "value"

    def test_get_response_field_missing_with_default(self):
        """Test getting missing field returns default."""
        response = httpx.Response(200, json={"other": "value"})
        result = get_response_field(response, "missing", "test operation", default="default_value")
        assert result == "default_value"

    def test_get_response_field_missing_no_default(self):
        """Test getting missing field with no default returns None."""
        response = httpx.Response(200, json={"other": "value"})
        result = get_response_field(response, "missing", "test operation")
        assert result is None

    def test_get_response_field_none_value(self):
        """Test getting field with None value."""
        response = httpx.Response(200, json={"field": None})
        result = get_response_field(response, "field", "test operation", default="default")
        assert result is None
