"""Deferred pulp-content HTTP verification for e2e (run after all uploads complete)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

DistributionCheckRunner = Callable[[Any], None]


@dataclass(frozen=True, slots=True)
class PendingDistributionCheck:
    """One deferred distribution / pulp_results HTTP verification."""

    label: str
    run: DistributionCheckRunner
    build_id: str | None = None


class DistributionVerifyQueue:
    """FIFO queue of pulp-content checks to run in a batch after uploads."""

    def __init__(self) -> None:
        self._pending: list[PendingDistributionCheck] = []

    def defer(self, check: PendingDistributionCheck) -> None:
        self._pending.append(check)

    def defer_callable(
        self,
        label: str,
        run: DistributionCheckRunner,
        *,
        build_id: str | None = None,
    ) -> None:
        self.defer(PendingDistributionCheck(label=label, run=run, build_id=build_id))

    def pending(self) -> list[PendingDistributionCheck]:
        return list(self._pending)

    def clear(self) -> None:
        self._pending.clear()

    def run_all(self, suite: Any) -> None:
        """Invoke each registered check in registration order."""
        for check in self._pending:
            check.run(suite)


__all__ = ["DistributionVerifyQueue", "PendingDistributionCheck"]
