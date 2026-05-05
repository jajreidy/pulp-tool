"""
Tests for logging utilities.

This module tests logging setup and formatter functionality.
"""

import logging
import os
import unittest
from unittest.mock import Mock, patch

try:
    from pythonjsonlogger.json import JsonFormatter as JsonFormatterCls
except ImportError:
    from pythonjsonlogger.jsonlogger import JsonFormatter as JsonFormatterCls
from pulp_tool.utils import setup_logging, WrappingFormatter


class TestLoggingUtilities(unittest.TestCase):
    """Test logging utility functions."""

    def setUp(self) -> None:
        self._prev_json_env = os.environ.get("PULP_TOOL_JSON_LOG")
        os.environ.pop("PULP_TOOL_JSON_LOG", None)

    def tearDown(self) -> None:
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)
        if self._prev_json_env is not None:
            os.environ["PULP_TOOL_JSON_LOG"] = self._prev_json_env
        else:
            os.environ.pop("PULP_TOOL_JSON_LOG", None)

    def test_setup_logging_debug(self) -> None:
        """Test setup_logging with debug level."""
        with patch("logging.basicConfig") as mock_basic_config:
            setup_logging(verbosity=2)
            mock_basic_config.assert_called_once()

    def test_setup_logging_info(self) -> None:
        """Test setup_logging with info level."""
        with patch("logging.basicConfig") as mock_basic_config:
            setup_logging(verbosity=1)
            mock_basic_config.assert_called_once()

    def test_setup_logging_with_wrapping(self) -> None:
        """Test setup_logging with wrapping enabled."""
        setup_logging(verbosity=2, use_wrapping=True)
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG
        assert len(root_logger.handlers) > 0

    def test_wrapping_formatter(self) -> None:
        """Test WrappingFormatter class."""
        formatter = WrappingFormatter(width=50)
        record = Mock()
        record.getMessage.return_value = "Short message"
        record.levelname = "INFO"
        record.name = "test"
        record.pathname = "/test/path"
        record.lineno = 1
        record.funcName = "test_func"
        record.exc_text = None
        record.exc_info = None
        record.stack_info = None
        formatted = formatter.format(record)
        assert len(formatted) <= 50
        record.getMessage.return_value = (
            "This is a very long message that should be wrapped because it exceeds the specified width limit"
        )
        formatted = formatter.format(record)
        assert "\n" in formatted

    def test_setup_logging_json_mode(self) -> None:
        """PULP_TOOL_JSON_LOG enables python-json-logger JsonFormatter on the root handler."""
        with patch.dict(os.environ, {"PULP_TOOL_JSON_LOG": "1"}):
            setup_logging(verbosity=1, use_wrapping=False)
        root = logging.getLogger()
        self.assertTrue(root.handlers)
        self.assertIsInstance(root.handlers[0].formatter, JsonFormatterCls)
        root.handlers.clear()

    def test_setup_logging_json_mode_boolean_words(self) -> None:
        """Accept true/yes for PULP_TOOL_JSON_LOG."""
        with patch.dict(os.environ, {"PULP_TOOL_JSON_LOG": "yes"}):
            setup_logging(verbosity=0, use_wrapping=False)
        root = logging.getLogger()
        self.assertTrue(root.handlers)
        self.assertIsInstance(root.handlers[0].formatter, JsonFormatterCls)
        root.handlers.clear()

    def test_setup_logging_json_debug_and_http_verbosity(self) -> None:
        """JSON mode: verbosity>=2 uses DEBUG; verbosity>=3 turns httpx/httpcore to DEBUG."""
        with patch.dict(os.environ, {"PULP_TOOL_JSON_LOG": "1"}):
            setup_logging(verbosity=2, use_wrapping=False)
        self.assertEqual(logging.getLogger().level, logging.DEBUG)
        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        with patch.dict(os.environ, {"PULP_TOOL_JSON_LOG": "1"}):
            setup_logging(verbosity=3, use_wrapping=False)
        self.assertEqual(logging.getLogger("httpx").level, logging.DEBUG)
        self.assertEqual(logging.getLogger("httpcore").level, logging.DEBUG)
        logging.getLogger().handlers.clear()
