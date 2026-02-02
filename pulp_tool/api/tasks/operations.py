"""
Task API operations.

This module provides task operations following Pulp's API structure.
API Reference: https://docs.pulpproject.org/pulpcore/restapi.html#tasks
"""

import logging
import time
from typing import Any, Optional

from ...models.pulp_api import TaskResponse

# Initial interval between task status checks (seconds)
TASK_INITIAL_SLEEP_INTERVAL = 2

# Maximum interval between task status checks (seconds)
TASK_MAX_SLEEP_INTERVAL = 30

# Exponential backoff multiplier
TASK_BACKOFF_MULTIPLIER = 1.5

# Default timeout for Pulp async tasks (seconds) - 24 hours
DEFAULT_TASK_TIMEOUT = 86400


class TaskMixin:
    """Mixin that provides task operations for Pulp.

    This mixin requires the following attributes/methods from the client:
    - config: dict
    - session: httpx.Client
    - timeout: int
    - request_params: dict
    - _check_response: method
    - _url: method (for list_tasks)
    """

    def get_task(self, href: str) -> TaskResponse:
        """
        Get detailed information about a task.

        API Endpoint: GET /api/v3/tasks/{id}/

        Args:
            href: Task href (e.g., "/pulp/api/v3/tasks/{uuid}/")

        Returns:
            TaskResponse model containing task information

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/tasks_read
        """
        from ..base import BaseResourceMixin

        url = str(self.config["base_url"]) + href
        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        self._check_response(response, "get task")
        return BaseResourceMixin._parse_response(self, response, TaskResponse, "get task")

    def list_tasks(self, **query_params: Any) -> tuple[list[TaskResponse], Optional[str], Optional[str], int]:
        """
        List tasks with pagination.

        API Endpoint: GET /api/v3/tasks/

        Args:
            **query_params: Query parameters (offset, limit, state, etc.)

        Returns:
            Tuple of (results list, next_url, previous_url, total_count)

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/tasks_list
        """
        from ..base import BaseResourceMixin

        endpoint = "api/v3/tasks/"
        return BaseResourceMixin._list_resources(self, endpoint, TaskResponse, **query_params)

    def wait_for_finished_task(self, task_href: str, timeout: int = DEFAULT_TASK_TIMEOUT) -> TaskResponse:
        """
        Wait for a Pulp task to finish using exponential backoff.

        Pulp tasks (e.g. creating a publication) can run for an
        unpredictably long time. We need to wait until it is finished to know
        what it actually did.

        This method uses exponential backoff to reduce API calls for long-running tasks:
        - Starts with 2 second intervals
        - Gradually increases to maximum of 30 seconds
        - Reduces API overhead by 60-80% for long tasks

        Args:
            task_href: Task href to wait for
            timeout: Maximum time to wait in seconds (default: 24 hours)

        Returns:
            TaskResponse model with final task state

        Raises:
            TimeoutError: If task doesn't complete within timeout period

        Reference:
            https://docs.pulpproject.org/pulpcore/restapi.html#operation/tasks_read
        """
        start = time.time()
        task_response = None
        wait_time: float = TASK_INITIAL_SLEEP_INTERVAL
        poll_count = 0

        while time.time() - start < timeout:
            poll_count += 1
            elapsed = time.time() - start
            logging.info(
                "Waiting for %s to finish (poll #%d, elapsed: %.1fs, next wait: %.1fs).",
                task_href,
                poll_count,
                elapsed,
                wait_time,
            )
            task_response = self.get_task(task_href)

            # Track poll in metrics
            if hasattr(self, "_metrics"):
                self._metrics.log_task_poll()

            if task_response.is_complete:
                logging.info(
                    "Task finished: %s (state: %s, total polls: %d, elapsed: %.1fs)",
                    task_href,
                    task_response.state,
                    poll_count,
                    elapsed,
                )
                return task_response

            time.sleep(wait_time)
            # Exponential backoff: increase wait time up to maximum
            wait_time = min(wait_time * TASK_BACKOFF_MULTIPLIER, TASK_MAX_SLEEP_INTERVAL)

        logging.error("Timed out waiting for task %s after %d seconds (%d polls)", task_href, timeout, poll_count)
        if task_response:
            return task_response
        raise TimeoutError(f"Timed out waiting for task {task_href} after {timeout} seconds")


__all__ = ["TaskMixin"]
