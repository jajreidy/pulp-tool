"""
Response utilities for parsing and validating HTTP responses.

This module provides reusable utilities for working with HTTP responses
from the Pulp API, eliminating code duplication.
"""

import logging
import traceback
from typing import Any, Dict, Optional

import httpx

from ..models.pulp_api import TaskResponse


def parse_json_response(response: httpx.Response, operation: str, *, check_success: bool = True) -> Dict[str, Any]:
    """
    Parse JSON response with error handling.

    Args:
        response: HTTP response to parse
        operation: Description of operation for error messages
        check_success: If True, check response status before parsing

    Returns:
        Parsed JSON dictionary

    Raises:
        ValueError: If JSON parsing fails or response is not successful
    """
    if check_success and not response.is_success:
        raise ValueError(f"Response not successful for {operation}: " f"{response.status_code} - {response.text}")

    try:
        return response.json()
    except ValueError as e:
        logging.error("Failed to parse JSON response for %s: %s", operation, e)
        logging.error("Response content: %s", response.text[:500])
        logging.error("Traceback: %s", traceback.format_exc())
        raise ValueError(f"Invalid JSON response from Pulp API during {operation}: {e}") from e


def extract_task_href(response: httpx.Response, operation: str) -> str:
    """
    Extract task href from response.

    Args:
        response: HTTP response containing task href
        operation: Description of operation for error messages

    Returns:
        Task href string

    Raises:
        KeyError: If task href is not found in response
    """
    try:
        data = parse_json_response(response, operation)
        return data["task"]
    except KeyError as e:
        logging.error("No task href found in response for %s", operation)
        logging.error("Response data: %s", data if "data" in locals() else "N/A")
        raise KeyError(f"Response does not contain task href for {operation}") from e


def extract_created_resources(task_response: TaskResponse, operation: str) -> list:
    """
    Extract created resources from task response.

    Args:
        task_response: TaskResponse model
        operation: Description of operation for logging

    Returns:
        List of created resource hrefs
    """
    if not task_response.created_resources:
        logging.debug("No created resources in task response for %s", operation)
        return []

    logging.debug("Extracted %d created resources from %s", len(task_response.created_resources), operation)
    return task_response.created_resources


def check_task_success(task_response: TaskResponse, operation: str) -> bool:
    """
    Check if a task completed successfully.

    Args:
        task_response: TaskResponse model to check
        operation: Description of operation for error messages

    Returns:
        True if task was successful

    Raises:
        ValueError: If task failed
    """
    if not task_response.is_successful:
        error_msg = task_response.error.get("description", "Unknown error") if task_response.error else "Unknown error"
        logging.error("Task failed for %s: %s", operation, error_msg)
        raise ValueError(f"Task failed for {operation}: {error_msg}")

    return True


def extract_results_list(response: httpx.Response, operation: str, *, allow_empty: bool = False) -> list:
    """
    Extract 'results' list from response.

    Args:
        response: HTTP response to parse
        operation: Description of operation for error messages
        allow_empty: If False, raise error on empty results

    Returns:
        List of results from response

    Raises:
        ValueError: If results are empty and allow_empty is False
    """
    data = parse_json_response(response, operation)
    results = data.get("results", [])

    if not results and not allow_empty:
        logging.error("No results found in response for %s", operation)
        raise ValueError(f"Empty results for {operation}")

    return results


def extract_single_result(response: httpx.Response, operation: str) -> Dict[str, Any]:
    """
    Extract single result from response results list.

    Args:
        response: HTTP response to parse
        operation: Description of operation for error messages

    Returns:
        First result dictionary

    Raises:
        ValueError: If no results found
    """
    results = extract_results_list(response, operation, allow_empty=False)
    return results[0]


def get_response_field(
    response: httpx.Response, field_name: str, operation: str, *, default: Optional[Any] = None
) -> Any:
    """
    Get a specific field from response JSON.

    Args:
        response: HTTP response to parse
        field_name: Name of field to extract
        operation: Description of operation for error messages
        default: Default value if field not found

    Returns:
        Field value or default
    """
    data = parse_json_response(response, operation)
    value = data.get(field_name, default)

    if value is None and default is None:
        logging.debug("Field '%s' not found in response for %s", field_name, operation)

    return value


__all__ = [
    "parse_json_response",
    "extract_task_href",
    "extract_created_resources",
    "check_task_success",
    "extract_results_list",
    "extract_single_result",
    "get_response_field",
]
