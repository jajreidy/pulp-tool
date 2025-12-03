"""
Utility modules for Konflux Pulp operations.
"""

from .logger import setup_logging, WrappingFormatter, get_logger
from .pulp_helper import PulpHelper
from .session import create_session_with_retry
from .validation.build_id import (
    determine_build_id,
    extract_build_id_from_artifact_json,
    extract_build_id_from_artifacts,
    extract_metadata_from_artifact_json,
    extract_metadata_from_artifacts,
    sanitize_build_id_for_repository,
    strip_namespace_from_build_id,
    validate_build_id,
)
from .validation.file import validate_file_path
from .validation.repository import validate_repository_setup
from .uploads import create_labels, upload_artifacts_to_repository, upload_rpms_logs, upload_log
from .url import get_pulp_content_base_url
from ..models.repository import RepositoryRefs

# New utility modules for clean code refactoring
from . import error_handling
from . import response_utils
from . import logging_utils
from . import iteration_utils
from . import constants
from . import predicates
from . import artifact_detection
from . import path_utils
from . import config_manager

__all__ = [
    "setup_logging",
    "WrappingFormatter",
    "get_logger",
    "PulpHelper",
    "create_session_with_retry",
    "determine_build_id",
    "extract_build_id_from_artifact_json",
    "extract_build_id_from_artifacts",
    "extract_metadata_from_artifact_json",
    "extract_metadata_from_artifacts",
    "sanitize_build_id_for_repository",
    "strip_namespace_from_build_id",
    "validate_build_id",
    "validate_file_path",
    "validate_repository_setup",
    "create_labels",
    "upload_artifacts_to_repository",
    "upload_rpms_logs",
    "upload_log",
    "get_pulp_content_base_url",
    "RepositoryRefs",
    # New utility modules
    "error_handling",
    "response_utils",
    "logging_utils",
    "iteration_utils",
    "constants",
    "predicates",
    "artifact_detection",
    "path_utils",
    "config_manager",
]
