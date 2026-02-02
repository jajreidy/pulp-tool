"""
Base API mixin providing common HTTP methods and response parsing.

This module provides the foundation for all Pulp API resource operations,
including standardized HTTP methods, response parsing with model validation,
and error handling patterns.
"""

import logging
import traceback
from typing import Any, Callable, Optional, Protocol, Type, TypeVar, runtime_checkable

import httpx
from pydantic import BaseModel, ValidationError

from ..models.pulp_api import PulpBaseModel

# Type variable for response models
T = TypeVar("T", bound=PulpBaseModel)


@runtime_checkable
class BaseApiMixin(Protocol):
    """Protocol that provides base API operations for Pulp."""

    # Required attributes
    _url: Callable[[str], str]  # Method that constructs URLs
    session: Any  # httpx.Client
    timeout: int
    request_params: dict
    config: dict

    def _check_response(self, response: httpx.Response, operation: str) -> None:
        """Check response for errors."""
        ...  # pragma: no cover - defined in implementation


class BaseResourceMixin:
    """Base mixin providing common HTTP methods and response parsing for Pulp API resources."""

    def _parse_response(
        self, response: httpx.Response, model_class: Type[T], operation: str, *, check_success: bool = True
    ) -> T:
        """
        Parse HTTP response into a Pydantic model.

        Args:
            response: HTTP response to parse
            model_class: Pydantic model class to parse into
            operation: Description of operation for error messages
            check_success: If True, check response status before parsing

        Returns:
            Parsed model instance

        Raises:
            ValueError: If response is not successful or parsing fails
            ValidationError: If response doesn't match model schema
        """
        if check_success:
            self._check_response(response, operation)

        try:
            json_data = response.json()
            return model_class(**json_data)
        except ValidationError as e:
            logging.error("Failed to validate %s response: %s", operation, e)
            logging.error("Response content: %s", response.text[:500])
            logging.error("Traceback: %s", traceback.format_exc())
            raise ValueError(f"Invalid response format for {operation}: {e}") from e
        except ValueError as e:
            logging.error("Failed to parse JSON response for %s: %s", operation, e)
            logging.error("Response content: %s", response.text[:500])
            logging.error("Traceback: %s", traceback.format_exc())
            raise ValueError(f"Invalid JSON response from Pulp API during {operation}: {e}") from e

    def _parse_list_response(
        self,
        response: httpx.Response,
        model_class: Type[T],
        operation: str,
        *,
        check_success: bool = True,
    ) -> list[T]:
        """
        Parse paginated HTTP response into a list of Pydantic models.

        Args:
            response: HTTP response to parse
            model_class: Pydantic model class to parse each result into
            operation: Description of operation for error messages
            check_success: If True, check response status before parsing

        Returns:
            List of parsed model instances
        """
        if check_success:
            self._check_response(response, operation)  # type: ignore[attr-defined]

        try:
            json_data = response.json()
            results = json_data.get("results", [])
            return [model_class(**item) for item in results]
        except (ValidationError, KeyError) as e:
            logging.error("Failed to validate %s list response: %s", operation, e)
            logging.error("Response content: %s", response.text[:500])
            logging.error("Traceback: %s", traceback.format_exc())
            raise ValueError(f"Invalid response format for {operation}: {e}") from e
        except ValueError as e:
            logging.error("Failed to parse JSON response for %s: %s", operation, e)
            logging.error("Response content: %s", response.text[:500])
            logging.error("Traceback: %s", traceback.format_exc())
            raise ValueError(f"Invalid JSON response from Pulp API during {operation}: {e}") from e

    def _get_resource(self, endpoint: str, model_class: Type[T], name: Optional[str] = None, **query_params: Any) -> T:
        """
        Get a single resource by name or href.

        Args:
            endpoint: API endpoint path
            model_class: Pydantic model class for the response
            name: Resource name (for query-based lookup)
            **query_params: Additional query parameters

        Returns:
            Parsed resource model

        Raises:
            ValueError: If resource not found or parsing fails
        """
        if name:
            query_params["name"] = name
            query_params.setdefault("offset", 0)
            query_params.setdefault("limit", 1)

        url = self._url(f"{endpoint}?")  # type: ignore[attr-defined]
        if query_params:
            from urllib.parse import urlencode

            url += urlencode(query_params)

        response = self.session.get(url, timeout=self.timeout, **self.request_params)  # type: ignore[attr-defined]
        self._check_response(response, f"get {endpoint}")  # type: ignore[attr-defined]

        json_data = response.json()
        results = json_data.get("results", [])

        if not results:
            raise ValueError(f"Resource not found: {name or endpoint}")

        if len(results) > 1:
            logging.warning("Multiple resources found for name '%s', using first result", name)

        return model_class(**results[0])

    def _list_resources(
        self, endpoint: str, model_class: Type[T], **query_params: Any
    ) -> tuple[list[T], Optional[str], Optional[str], int]:
        """
        List resources with pagination support.

        Args:
            endpoint: API endpoint path
            model_class: Pydantic model class for each result
            **query_params: Query parameters (offset, limit, filters, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)
        """
        url = self._url(f"{endpoint}?")  # type: ignore[attr-defined]
        if query_params:
            from urllib.parse import urlencode

            url += urlencode(query_params)

        response = self.session.get(url, timeout=self.timeout, **self.request_params)  # type: ignore[attr-defined]
        self._check_response(response, f"list {endpoint}")  # type: ignore[attr-defined]

        json_data = response.json()
        results = [model_class(**item) for item in json_data.get("results", [])]

        return (
            results,
            json_data.get("next"),
            json_data.get("previous"),
            json_data.get("count", len(results)),
        )

    def _create_resource(
        self, endpoint: str, request_model: BaseModel, response_model_class: Type[T], operation: str
    ) -> T:
        """
        Create a resource.

        Args:
            endpoint: API endpoint path
            request_model: Pydantic model with request data
            response_model_class: Pydantic model class for the response
            operation: Description of operation for error messages

        Returns:
            Parsed response model
        """
        url = self._url(endpoint)  # type: ignore[attr-defined]
        data = request_model.model_dump(exclude_none=True)

        response = self.session.post(  # type: ignore[attr-defined]
            url, json=data, timeout=self.timeout, **self.request_params
        )
        return self._parse_response(response, response_model_class, operation)

    def _update_resource(self, href: str, request_model: BaseModel, response_model_class: Type[T], operation: str) -> T:
        """
        Update a resource by href.

        Args:
            href: Full resource href (e.g., "/pulp/api/v3/repositories/rpm/rpm/{uuid}/")
            request_model: Pydantic model with update data
            response_model_class: Pydantic model class for the response
            operation: Description of operation for error messages

        Returns:
            Parsed response model
        """
        url = str(self.config["base_url"]) + href
        data = request_model.model_dump(exclude_none=True)

        response = self.session.patch(url, json=data, timeout=self.timeout, **self.request_params)
        return self._parse_response(response, response_model_class, operation)

    def _delete_resource(self, href: str, operation: str) -> None:
        """
        Delete a resource by href.

        Args:
            href: Full resource href
            operation: Description of operation for error messages
        """
        url = str(self.config["base_url"]) + href  # type: ignore[attr-defined]
        response = self.session.delete(url, timeout=self.timeout, **self.request_params)  # type: ignore[attr-defined]
        self._check_response(response, operation)  # type: ignore[attr-defined]


__all__ = ["BaseResourceMixin", "BaseApiMixin"]
