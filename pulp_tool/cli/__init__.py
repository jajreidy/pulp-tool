"""
Unified CLI entry point for Pulp Tool operations using Click.

This module provides the main CLI group and shared options.
"""

import sys
from typing import Any, Callable, Optional, TypeVar

import click

from . import get_repo_md, transfer, upload
from .._version import __version__

F = TypeVar("F", bound=Callable[..., Any])


# ============================================================================
# Common Click Options - Reusable decorators for shared options
# ============================================================================


def config_option(required: bool = False) -> Callable[[F], F]:
    """Shared --config option for commands."""
    default_help = " (default: ~/.config/pulp/cli.toml)" if not required else ""
    return click.option(
        "--config",
        required=required,
        type=click.Path(exists=True),
        help=f"Path to Pulp CLI config file{default_help}",
    )


def debug_option() -> Callable[[F], F]:
    """Shared --debug option for verbosity control."""
    return click.option(
        "-d",
        "--debug",
        count=True,
        help="Increase verbosity (use -d for INFO, -dd for DEBUG, -ddd for DEBUG with HTTP logs)",
    )


# ============================================================================
# CLI Group
# ============================================================================


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__, prog_name="pulp-tool")
@click.option(
    "--config",
    type=click.Path(exists=True),
    help="Path to Pulp CLI config file (default: ~/.config/pulp/cli.toml)",
)
@click.option(
    "--build-id",
    help="Build identifier (required for some commands)",
)
@click.option(
    "--namespace",
    help="Namespace for the build (required for some commands)",
)
@click.option(
    "-d",
    "--debug",
    count=True,
    help="Increase verbosity (use -d for INFO, -dd for DEBUG, -ddd for DEBUG with HTTP logs)",
)
@click.option(
    "--max-workers",
    type=int,
    default=4,
    help="Maximum number of concurrent workers (default: 4)",
)
@click.pass_context
def cli(
    ctx: click.Context,
    config: Optional[str],
    build_id: Optional[str],
    namespace: Optional[str],
    debug: int,
    max_workers: int,
) -> None:
    """Pulp Tool - Upload and transfer artifacts to/from Pulp repositories."""
    # Store shared options in context for subcommands to access
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["build_id"] = build_id
    ctx.obj["namespace"] = namespace
    ctx.obj["debug"] = debug
    ctx.obj["max_workers"] = max_workers


# Register subcommands
cli.add_command(upload.upload)
cli.add_command(transfer.transfer)
cli.add_command(get_repo_md.get_repo_md)


# ============================================================================
# Main Entry Point
# ============================================================================


def main() -> None:
    """Main entry point for the CLI."""
    try:
        cli()  # pylint: disable=no-value-for-parameter  # Click handles parameters
    except KeyboardInterrupt:
        click.echo("\n\nOperation cancelled by user", err=True)
        sys.exit(130)


__all__ = ["cli", "main", "config_option", "debug_option"]
