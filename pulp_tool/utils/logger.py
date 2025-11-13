"""
Logging configuration and utilities for the Konflux Pulp package.

This module provides logging setup, custom formatters, and logging utilities
to ensure consistent and readable logging across the package.
"""

import logging
from typing import Optional

# ============================================================================
# Logging Configuration Constants
# ============================================================================

# Default width for log message wrapping
DEFAULT_LOG_WIDTH = 120

# ============================================================================
# Custom Formatters
# ============================================================================


class WrappingFormatter(logging.Formatter):
    """
    Custom formatter that wraps long log messages for better readability.

    This formatter extends the standard logging formatter to handle
    long messages by wrapping them at a specified width.
    """

    def __init__(
        self, fmt: Optional[str] = None, datefmt: Optional[str] = None, width: int = DEFAULT_LOG_WIDTH
    ) -> None:
        """
        Initialize the wrapping formatter.

        Args:
            fmt: Format string for log messages
            datefmt: Date format string
            width: Maximum width for log message wrapping
        """
        super().__init__(fmt, datefmt)
        self.width = width

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with line wrapping.

        Args:
            record: Log record to format

        Returns:
            Formatted log message with wrapping
        """
        formatted = super().format(record)

        # Only wrap if the message is longer than the specified width
        if len(formatted) > self.width:
            lines = []
            current_line = ""

            for word in formatted.split():
                if len(current_line + " " + word) <= self.width:
                    current_line += (" " + word) if current_line else word
                else:
                    if current_line:
                        lines.append(current_line)
                    current_line = word

            if current_line:
                lines.append(current_line)

            formatted = "\n".join(lines)

        return formatted


# ============================================================================
# Logging Setup Functions
# ============================================================================


def setup_logging(verbosity: int = 0, use_wrapping: bool = False) -> None:
    """
    Setup logging configuration with multi-level verbosity.

    This function configures the logging system with appropriate level and
    formatter. Optionally uses a custom wrapping formatter for better readability.

    Args:
        verbosity: Verbosity level (0=WARNING, 1=INFO, 2=DEBUG, 3+=DEBUG with HTTP logs)
        use_wrapping: If True, use wrapping formatter for long messages

    Verbosity Levels:
        0 (default): WARNING - Only warnings and errors, progress bars only
        1 (-d):      INFO - Normal output with summary messages
        2 (-dd):     DEBUG - Verbose output with detailed information
        3+ (-ddd):   DEBUG - Maximum verbosity including HTTP request logs

    Example:
        >>> from pulp_tool.logger import setup_logging
        >>> setup_logging(0)  # WARNING level (default)
        >>> setup_logging(1)  # INFO level
        >>> setup_logging(2)  # DEBUG level
        >>> setup_logging(3)  # DEBUG level with HTTP logs
    """
    # Map verbosity count to logging level
    if verbosity == 0:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    else:  # 2 or higher
        level = logging.DEBUG

    if use_wrapping:
        # Setup with wrapping formatter
        formatter = WrappingFormatter(fmt="%(asctime)s - %(levelname)s - %(message)s", width=120)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        # Clear any existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        root_logger.setLevel(level)
    else:
        # Basic logging setup
        logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

    # Configure HTTP client logging based on verbosity
    # httpx logs every HTTP request at INFO level which clutters the output
    if verbosity < 3:
        # Silence HTTP logs unless maximum verbosity is requested
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    else:
        # At maximum verbosity (-ddd), show HTTP request logs
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)


# ============================================================================
# Convenience Functions
# ============================================================================


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.

    Args:
        name: Name for the logger (typically __name__)

    Returns:
        Logger instance

    Example:
        >>> from pulp_tool.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Application started")
    """
    return logging.getLogger(name)


__all__ = [
    "WrappingFormatter",
    "setup_logging",
    "get_logger",
]
