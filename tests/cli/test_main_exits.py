"""Tests for CLI main() exit codes (typed exceptions)."""

from unittest.mock import patch

import pytest

from pulp_tool.cli import main
from pulp_tool.exceptions import PulpToolConfigError, PulpToolHTTPError


def test_main_pulp_tool_config_error_exits_2() -> None:
    with patch("pulp_tool.cli.cli", side_effect=PulpToolConfigError("bad config")):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 2


def test_main_pulp_tool_http_error_exits_3() -> None:
    with patch("pulp_tool.cli.cli", side_effect=PulpToolHTTPError("boom", response=None)):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 3


def test_main_keyboard_interrupt_exits_130() -> None:
    with patch("pulp_tool.cli.cli", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 130
