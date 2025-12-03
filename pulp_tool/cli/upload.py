"""
Upload command for Pulp Tool CLI.

This module provides the upload command for uploading RPMs, logs, and SBOM files.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Optional

import click
import httpx

from ..api import PulpClient
from ..models.context import UploadContext
from ..utils import PulpHelper, setup_logging
from ..utils.error_handling import handle_http_error, handle_generic_error


@click.command()
@click.option("--parent-package", required=True, help="Parent package name")
@click.option("--rpm-path", required=True, type=click.Path(exists=True), help="Path to directory containing RPM files")
@click.option("--sbom-path", required=True, type=click.Path(exists=True), help="Path to SBOM file")
@click.option(
    "--artifact-results",
    help="Comma-separated paths for Konflux artifact results location (url_path,digest_path)",
)
@click.option("--sbom-results", type=click.Path(), help="Path to write SBOM results")
@click.pass_context
def upload(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ctx: click.Context,
    parent_package: str,
    rpm_path: str,
    sbom_path: str,
    artifact_results: Optional[str],
    sbom_results: Optional[str],
) -> None:
    """Upload RPMs, logs, and SBOM files to Pulp repositories."""
    # Get shared options from context
    build_id = ctx.obj["build_id"]
    namespace = ctx.obj["namespace"]
    config = ctx.obj["config"]
    debug = ctx.obj["debug"]

    # Validate required options
    if not build_id:
        click.echo("Error: --build-id is required for upload command", err=True)
        ctx.exit(1)
    if not namespace:
        click.echo("Error: --namespace is required for upload command", err=True)
        ctx.exit(1)

    setup_logging(debug, use_wrapping=True)

    client = None
    try:
        # Initialize client and timestamp
        # The namespace/domain will be read from the config file
        client = PulpClient.create_from_config_file(path=config)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Create context object with generated date_str
        args = UploadContext(
            build_id=build_id,
            date_str=date_str,
            namespace=namespace,
            parent_package=parent_package,
            rpm_path=rpm_path,
            sbom_path=sbom_path,
            config=config,
            artifact_results=artifact_results,
            sbom_results=sbom_results,
            debug=debug,
        )

        # Setup repositories using helper
        # Namespace is automatically read from config file via client
        repository_helper = PulpHelper(client, parent_package=parent_package)
        repositories = repository_helper.setup_repositories(build_id)
        logging.info("Repository setup completed")

        # Process uploads
        logging.info("Starting upload process")
        results_json_url = repository_helper.process_uploads(client, args, repositories)

        # Check if results JSON URL was generated successfully
        if not results_json_url:
            logging.error("Upload completed but results JSON was not created")
            sys.exit(1)

        logging.info("All operations completed successfully")

        # Report the results JSON URL
        click.echo("\n" + "=" * 80)
        click.echo(f"RESULTS JSON URL: {results_json_url}")
        if not artifact_results:
            click.echo("NOTE: Results JSON created but not written to Konflux artifact files")
            click.echo("      Use --artifact_results to specify file paths for Konflux integration")
        click.echo("=" * 80)

        sys.exit(0)

    except httpx.HTTPError as e:
        handle_http_error(e, "upload operation")
        sys.exit(1)
    except Exception as e:
        handle_generic_error(e, "upload operation")
        sys.exit(1)
    finally:
        # Ensure client session is properly closed
        if client:
            client.close()
            logging.debug("Client session closed")


__all__ = ["upload"]
