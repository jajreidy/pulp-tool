"""
Task management operations for Pulp API.

This module handles waiting for and monitoring Pulp async tasks.
"""

import logging
import time
from typing import Any, Protocol, runtime_checkable

from ..models.pulp_api import TaskResponse

# Initial interval between task status checks (seconds)
TASK_INITIAL_SLEEP_INTERVAL = 2

# Maximum interval between task status checks (seconds)
TASK_MAX_SLEEP_INTERVAL = 30

# Exponential backoff multiplier
TASK_BACKOFF_MULTIPLIER = 1.5

# Default timeout for Pulp async tasks (seconds) - 24 hours
DEFAULT_TASK_TIMEOUT = 86400


@runtime_checkable
class TaskManagerMixin(Protocol):
    """Protocol that provides task polling and waiting for Pulp async operations."""

    # Required attributes
    config: dict
    session: Any  # httpx.Client
    timeout: int
    request_params: dict
    _metrics: Any  # Optional PerformanceMetrics

    def _get_task(self, task: str) -> TaskResponse:
        """
        Get detailed information about a task.

        Args:
            task: Task href to get information for

        Returns:
            TaskResponse model containing task information
        """
        url = str(self.config["base_url"]) + task
        response = self.session.get(url, timeout=self.timeout, **self.request_params)
        response.raise_for_status()
        return TaskResponse(**response.json())

    def wait_for_finished_task(self, task: str, timeout: int = DEFAULT_TASK_TIMEOUT) -> TaskResponse:
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
            task: Task href to wait for
            timeout: Maximum time to wait in seconds (default: 24 hours)

        Returns:
            TaskResponse model with final task state

        Raises:
            TimeoutError: If task doesn't complete within timeout period
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
                task,
                poll_count,
                elapsed,
                wait_time,
            )
            task_response = self._get_task(task)

            # Track poll in metrics
            if hasattr(self, "_metrics"):
                self._metrics.log_task_poll()

            if task_response.is_complete:
                logging.info(
                    "Task finished: %s (state: %s, total polls: %d, elapsed: %.1fs)",
                    task,
                    task_response.state,
                    poll_count,
                    elapsed,
                )
                return task_response

            time.sleep(wait_time)
            # Exponential backoff: increase wait time up to maximum
            wait_time = min(wait_time * TASK_BACKOFF_MULTIPLIER, TASK_MAX_SLEEP_INTERVAL)

        logging.error("Timed out waiting for task %s after %d seconds (%d polls)", task, timeout, poll_count)
        if task_response:
            return task_response
        raise TimeoutError(f"Timed out waiting for task {task} after {timeout} seconds")


__all__ = ["TaskManagerMixin"]
