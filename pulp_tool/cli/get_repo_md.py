"""
Get repo MD command for Pulp Tool CLI.

This module provides the get-repo-md command for downloading .repo configuration files.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import click
import httpx

from ..api import DistributionClient
from ..utils import setup_logging
from ..utils.config_manager import ConfigManager
from ..utils.error_handling import handle_generic_error, handle_http_error


@click.command()
@click.option(
    "--base-url",
    help="Pulp base URL (e.g., https://pulp.example.com). Alternative to --config.",
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output directory for .repo files (default: current directory). Files named {build_id}.repo",
)
@click.option(
    "--cert-path",
    type=click.Path(exists=True),
    help="Path to SSL certificate file for authentication (optional, can come from config)",
)
@click.option(
    "--key-path",
    type=click.Path(exists=True),
    help="Path to SSL private key file for authentication (optional, can come from config)",
)
@click.pass_context
def get_repo_md(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ctx: click.Context,
    base_url: Optional[str],
    output: Optional[str],
    cert_path: Optional[str],
    key_path: Optional[str],
) -> None:
    """Download .repo configuration file(s) from Pulp RPM distributions.

    **WORK IN PROGRESS**: This command is currently under development and may have
    incomplete functionality or behavior changes.

    Supports downloading multiple .repo files by providing comma-separated
    build IDs with --build_id.

    Either provide --config OR both --base_url and --namespace.
    """
    # Get shared options from context
    config = ctx.obj["config"]
    namespace = ctx.obj["namespace"]
    build_id = ctx.obj["build_id"]
    debug = ctx.obj["debug"]

    # Validate required option
    if not build_id:
        click.echo("Error: --build-id is required for get-repo-md command", err=True)
        ctx.exit(1)

    setup_logging(debug)

    # WORK IN PROGRESS: This command is under development
    logging.warning("get-repo-md command is a work in progress and may have incomplete functionality")

    distribution_client = None
    successful_downloads = []
    failed_downloads = []

    try:
        # Get certificate paths from config if not provided via CLI
        if config and (not cert_path or not key_path):
            try:
                config_manager = ConfigManager(config)
                config_manager.load()
                if not cert_path:
                    cert_path = config_manager.get("cli.cert")
                if not key_path:
                    key_path = config_manager.get("cli.key")
            except Exception as e:
                logging.debug("Could not load cert/key from config: %s", e)

        # Validate certificate/key pair
        if (cert_path and not key_path) or (key_path and not cert_path):
            logging.error(
                "Both certificate and key paths must be provided together (via --config, --cert-path, or --key-path)"
            )
            sys.exit(1)

        # Validate that either config OR (base-url AND namespace) are provided
        has_config = config is not None
        has_direct_params = base_url is not None and namespace is not None

        if not has_config and not has_direct_params:
            # Try default config path as fallback
            default_config = Path("~/.config/pulp/cli.toml").expanduser()
            if default_config.exists():
                config = str(default_config)
                has_config = True
                logging.debug("Using default config path: %s", config)
            else:
                logging.error("Must provide either --config OR both --base_url and --namespace")
                sys.exit(1)

        if has_config and has_direct_params:
            logging.error("Cannot use both --config and --base_url/--namespace. Choose one approach.")
            sys.exit(1)

        # Parse comma-delimited build IDs
        build_ids = [bid.strip() for bid in build_id.split(",") if bid.strip()]

        if not build_ids:
            logging.error("No valid build IDs provided")
            sys.exit(1)

        # Only RPM repositories support .repo files
        repo_type = "rpms"

        logging.info("Build IDs: %s", ", ".join(build_ids))
        logging.info("Repository type: %s", repo_type)

        # Get base_url and namespace from config or command line
        if has_config:
            # Load from config file
            config_manager = ConfigManager(config)
            config_manager.load()
            base_url = config_manager.get("cli.base_url")
            namespace = config_manager.get("cli.domain", "")

            if not namespace:
                logging.error("No domain/namespace found in config file")
                sys.exit(1)

            logging.info("Loaded from config: %s", config)
        else:
            # Use command line parameters (already validated above)
            logging.info("Using command line parameters")

        logging.info("Using namespace: %s", namespace)
        logging.info("Using base URL: %s", base_url)

        # Determine output directory
        output_dir = Path(output) if output else Path.cwd()
        if output and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            logging.info("Created output directory: %s", output_dir)

        # Initialize distribution client if using certificates
        if cert_path and key_path:
            logging.debug("Using authenticated requests with certificates")
            distribution_client = DistributionClient(cert_path, key_path)

        # Download .repo files for all build IDs
        total_downloads = len(build_ids)
        logging.info("Downloading %d .repo file(s)", total_downloads)

        for bid in build_ids:
            # Define filename early for error handling
            filename = f"{bid}.repo"

            try:
                # Construct distribution URL for RPM repository
                # Format: {base_url}/api/pulp-content/{build_id}/rpms/
                distribution_url = f"{base_url}/api/pulp-content/{namespace}/{bid}/{repo_type}/"
                repo_file_url = f"{distribution_url}config.repo"

                logging.debug("Fetching .repo file from: %s", repo_file_url)

                # Download the .repo file
                if distribution_client:
                    response = distribution_client.pull_artifact(repo_file_url)
                else:
                    # Use basic httpx for unauthenticated requests
                    logging.debug("Using unauthenticated request")
                    response = httpx.get(repo_file_url, timeout=30.0)

                response.raise_for_status()

                # Determine output path
                output_path = output_dir / filename

                # Save the file
                with open(output_path, "wb") as f:
                    f.write(response.content)

                successful_downloads.append((bid, output_path))
                logging.info("Downloaded: %s", filename)

            except httpx.HTTPStatusError as e:
                error_msg = f"HTTP {e.response.status_code}"
                if e.response.status_code == 404:
                    error_msg += " (not found)"
                elif e.response.status_code == 403:
                    error_msg += " (access denied)"
                elif e.response.status_code == 401:
                    error_msg += " (unauthorized - authentication required)"
                failed_downloads.append((bid, error_msg))
                logging.warning("Failed to download %s: %s", filename, error_msg)

            except Exception as e:
                error_msg = str(e)
                failed_downloads.append((bid, error_msg))
                logging.warning("Failed to download %s: %s", filename, error_msg)

        # Print summary
        click.echo(f"\n{'=' * 80}")
        click.echo(f"Download Summary: {len(successful_downloads)} succeeded, {len(failed_downloads)} failed")
        click.echo(f"{'=' * 80}")

        if successful_downloads:
            click.echo("\nSuccessfully downloaded:")
            for bid, path in successful_downloads:
                click.echo(f"  ✓ {path.name} -> {path.absolute()}")

            click.echo("\nTo use these repositories, copy the files to /etc/yum.repos.d/:")
            if len(successful_downloads) == 1:
                click.echo(f"  sudo cp {successful_downloads[0][1]} /etc/yum.repos.d/")
            else:
                click.echo(f"  sudo cp {output_dir}/*.repo /etc/yum.repos.d/")

        if failed_downloads:
            click.echo("\nFailed downloads:")
            for bid, error in failed_downloads:
                click.echo(f"  ✗ {bid}.repo: {error}")

        # Exit with appropriate code
        if failed_downloads and not successful_downloads:
            sys.exit(1)  # All downloads failed
        elif failed_downloads:
            sys.exit(2)  # Partial success
        else:
            sys.exit(0)  # All succeeded

    except FileNotFoundError as e:
        logging.error("Config file not found: %s", e)
        sys.exit(1)
    except KeyError as e:
        logging.error("Missing required configuration in config file: %s", e)
        sys.exit(1)
    except IOError as e:
        logging.error("Failed to write .repo file(s): %s", e)
        sys.exit(1)
    except httpx.HTTPError as e:
        handle_http_error(e, "get-repo-md operation")
        sys.exit(1)
    except Exception as e:
        handle_generic_error(e, "get-repo-md operation")
        sys.exit(1)
    finally:
        # Clean up distribution client if it was created
        if distribution_client and hasattr(distribution_client, "session"):
            distribution_client.session.close()
            logging.debug("Distribution client session closed")


__all__ = ["get_repo_md"]
