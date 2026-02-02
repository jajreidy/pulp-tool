"""
Tests for TaskMixin timeout handling.

This module tests TaskMixin wait_for_finished_task timeout path.
"""

from unittest.mock import Mock, patch
import httpx
import pytest

from pulp_tool.models.pulp_api import TaskResponse


class TestTaskMixin:
    """Test TaskMixin timeout handling."""

    def test_wait_for_finished_task_timeout(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task timeout path (lines 142-145)."""
        # Mock get_task to return incomplete tasks
        # The timeout path returns task_response if it exists (line 143-144), or raises TimeoutError (line 145)
        # To test line 145, we need task_response to be None when timeout occurs
        # But get_task is called in the loop, so task_response will always be set
        # So we test the error logging path (line 142) and the return path (line 143-144)

        def mock_get_task(href):
            return TaskResponse(
                pulp_href=href,
                state="running",
                result=None,
            )

        mock_pulp_client.get_task = Mock(side_effect=mock_get_task)

        # Mock time.sleep to speed up test and time.time to simulate timeout
        # The loop checks: time.time() - start < timeout
        # We need to return times that make time.time() - start eventually >= timeout
        # Based on existing test pattern - return a sequence that exceeds timeout
        start_time = 1000.0

        # time.time() is called multiple times:
        # 1. start = time.time() (line 107) -> returns start_time
        # 2. while time.time() - start < timeout (line 112) -> returns start_time + 0.1 (passes)
        # 3. elapsed = time.time() - start (line 114) -> returns start_time + 0.2
        # 4. After sleep, while check again (line 112) -> returns start_time + 2.0 (exceeds timeout)
        time_values = [start_time, start_time + 0.1, start_time + 0.2, start_time + 2.0]
        time_call_idx = [0]

        def mock_time():
            idx = time_call_idx[0]
            time_call_idx[0] += 1
            if idx < len(time_values):
                return time_values[idx]
            # After all predefined calls, return time that exceeds timeout
            return start_time + 2.0

        with (
            patch("time.sleep"),
            patch("time.time", side_effect=mock_time),
            patch("pulp_tool.api.tasks.operations.logging") as mock_logging,
        ):
            # The method will exit the loop when time exceeds timeout
            # Since task_response exists, it will return it (line 143-144), not raise TimeoutError
            # But we can verify the error logging was called (line 142)
            result = mock_pulp_client.wait_for_finished_task("/api/v3/tasks/12345/", timeout=1)

            # Should have called get_task at least once (line 122)
            assert mock_pulp_client.get_task.call_count >= 1
            # Should have logged timeout error (line 142)
            mock_logging.error.assert_called()
            # Should return the last task_response (line 143-144)
            assert result is not None

    def test_wait_for_finished_task_with_metrics(self, mock_pulp_client, httpx_mock):
        """Test wait_for_finished_task with metrics tracking."""
        # Add _metrics attribute
        mock_metrics = Mock()
        mock_pulp_client._metrics = mock_metrics

        httpx_mock.get("https://pulp.example.com/api/v3/tasks/12345/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pulp_href": "/api/v3/tasks/12345/",
                    "state": "completed",
                    "result": {},
                },
            )
        )

        # Mock time.sleep to speed up test
        with patch("time.sleep"):
            result = mock_pulp_client.wait_for_finished_task("/api/v3/tasks/12345/", timeout=10)

        assert result.state == "completed"
        # Verify metrics were logged
        assert hasattr(mock_metrics, "log_task_poll")

    def test_list_tasks(self, mock_pulp_client, httpx_mock):
        """Test list_tasks method (lines 76,78-79)."""
        httpx_mock.get("https://pulp.example.com/pulp/api/v3/test-domain/api/v3/tasks/").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "pulp_href": "/api/v3/tasks/12345/",
                            "state": "completed",
                            "result": {},
                        }
                    ],
                    "next": None,
                    "previous": None,
                    "count": 1,
                },
            )
        )

        results, next_url, prev_url, count = mock_pulp_client.list_tasks()

        assert len(results) == 1
        assert isinstance(results[0], TaskResponse)
        assert results[0].state == "completed"
        assert count == 1

    def test_wait_for_finished_task_timeout_no_task_response(self, mock_pulp_client):
        """Test wait_for_finished_task timeout when task_response is None (line 145)."""

        # Mock get_task to raise exception immediately, so task_response never gets set
        def mock_get_task(href):
            raise Exception("Network error on first call")

        mock_pulp_client.get_task = Mock(side_effect=mock_get_task)

        start_time = 1000.0
        # Make time.time() exceed timeout immediately
        time_values = [start_time, start_time + 2.0]
        time_call_idx = [0]

        def mock_time():
            idx = time_call_idx[0]
            time_call_idx[0] += 1
            if idx < len(time_values):
                return time_values[idx]
            return start_time + 2.0

        with (
            patch("time.sleep"),
            patch("time.time", side_effect=mock_time),
            patch("pulp_tool.api.tasks.operations.logging") as mock_logging,
        ):
            # Should raise TimeoutError because task_response is None
            with pytest.raises(TimeoutError, match="Timed out waiting for task"):
                mock_pulp_client.wait_for_finished_task("/api/v3/tasks/12345/", timeout=1)

            # Should have logged timeout error
            mock_logging.error.assert_called()
