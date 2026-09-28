"""Unit tests for e2e deferred distribution verification queue."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

E2E_DIR = Path(__file__).resolve().parents[2] / "e2e"
sys.path.insert(0, str(E2E_DIR))

from distribution_verify_queue import DistributionVerifyQueue, PendingDistributionCheck  # noqa: E402


def test_distribution_verify_queue_runs_in_order() -> None:
    queue = DistributionVerifyQueue()
    order: list[str] = []

    queue.defer_callable("first", lambda _suite: order.append("first"))
    queue.defer_callable("second", lambda _suite: order.append("second"), build_id="b1")

    suite = Mock()
    queue.run_all(suite)
    assert order == ["first", "second"]
    assert queue.pending()[1].build_id == "b1"


def test_pending_distribution_check_dataclass() -> None:
    ran = False

    def _run(_suite: object) -> None:
        nonlocal ran
        ran = True

    check = PendingDistributionCheck(label="x", run=_run, build_id="build-a")
    check.run(Mock())
    assert ran
