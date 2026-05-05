"""Tests for protocol modules."""

from pulp_tool.protocols import RepositoryProtocol
from pulp_tool.protocols.repository_protocol import RepositoryProtocol as RepoProtocol


def test_repository_protocol_import() -> None:
    """Test that RepositoryProtocol can be imported from protocols package."""
    assert RepositoryProtocol is not None
    assert RepositoryProtocol == RepoProtocol


def test_repository_protocol_is_protocol() -> None:
    """Test that RepositoryProtocol is a Protocol type."""
    from typing import Protocol

    assert issubclass(RepoProtocol, Protocol)


def test_repository_protocol_interface() -> None:
    """Test that RepositoryProtocol defines the expected interface."""
    assert hasattr(RepoProtocol, "setup_repositories")
    assert hasattr(RepoProtocol, "get_distribution_urls")
    assert hasattr(RepoProtocol, "create_or_get_repository")
    import inspect

    sig_setup = inspect.signature(RepoProtocol.setup_repositories)
    sig_dist = inspect.signature(RepoProtocol.get_distribution_urls)
    sig_create = inspect.signature(RepoProtocol.create_or_get_repository)
    assert "build_id" in sig_setup.parameters
    assert "build_id" in sig_dist.parameters
    assert "build_id" in sig_create.parameters
    assert "repo_type" in sig_create.parameters
