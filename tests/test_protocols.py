"""Tests for protocol modules."""

# Import to trigger __init__.py lines 8, 10
from pulp_tool.protocols import RepositoryProtocol

# Import to trigger repository_protocol.py lines 8, 10, 13, 21, 33, 45, 59
from pulp_tool.protocols.repository_protocol import RepositoryProtocol as RepoProtocol


def test_repository_protocol_import():
    """Test that RepositoryProtocol can be imported from protocols package."""
    # This tests __init__.py lines 8, 10
    assert RepositoryProtocol is not None
    assert RepositoryProtocol == RepoProtocol


def test_repository_protocol_is_protocol():
    """Test that RepositoryProtocol is a Protocol type."""
    # Protocols are abstract, so we can't instantiate them
    # But we can verify the type exists
    # This tests repository_protocol.py line 13
    from typing import Protocol

    assert issubclass(RepoProtocol, Protocol)  # type: ignore[arg-type]


def test_repository_protocol_interface():
    """Test that RepositoryProtocol defines the expected interface."""
    # Check that the protocol has the expected methods
    # This tests repository_protocol.py lines 21, 33, 45
    assert hasattr(RepoProtocol, "setup_repositories")
    assert hasattr(RepoProtocol, "get_distribution_urls")
    assert hasattr(RepoProtocol, "create_or_get_repository")

    # Verify method signatures exist (they're ellipsis in the protocol)
    import inspect

    sig_setup = inspect.signature(RepoProtocol.setup_repositories)
    sig_dist = inspect.signature(RepoProtocol.get_distribution_urls)
    sig_create = inspect.signature(RepoProtocol.create_or_get_repository)

    assert "build_id" in sig_setup.parameters
    assert "build_id" in sig_dist.parameters
    assert "build_id" in sig_create.parameters
    assert "repo_type" in sig_create.parameters
