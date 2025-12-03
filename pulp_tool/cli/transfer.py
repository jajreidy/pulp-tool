"""
Transfer command for Pulp Tool CLI.

This module provides the transfer command for downloading artifacts and optionally re-uploading to Pulp.
"""

import logging
import os
import sys
from typing import Optional

import click
import httpx

from ..api import DistributionClient
from ..models.context import TransferContext
from ..transfer import (
    download_artifacts_concurrently,
    generate_transfer_report,
    load_and_validate_artifacts,
    setup_repositories_if_needed,
    upload_downloaded_files_to_pulp,
)
from ..utils import setup_logging
from ..utils.config_manager import ConfigManager
from ..utils.error_handling import handle_generic_error, handle_http_error


@click.command()
@click.option(
    "--artifact-location",
    help="Path to local artifact metadata JSON file or HTTP URL. Mutually exclusive with --build-id + --namespace.",
)
@click.option(
    "--content-types",
    help=(
        "Comma-separated list of content types to transfer (rpm,log,sbom). "
        "If not specified, all types are transferred."
    ),
)
@click.option(
    "--archs",
    help=(
        "Comma-separated list of architectures to transfer (e.g., x86_64,aarch64,noarch). "
        "If not specified, all architectures are transferred."
    ),
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
def transfer(  # pylint: disable=too-many-positional-arguments
    ctx: click.Context,
    artifact_location: Optional[str],
    content_types: Optional[str],
    archs: Optional[str],
    cert_path: Optional[str],
    key_path: Optional[str],
) -> None:
    """Download artifacts and optionally re-upload to Pulp repositories."""
    # Get shared options from context
    namespace = ctx.obj["namespace"]
    config = ctx.obj["config"]
    build_id = ctx.obj["build_id"]
    debug = ctx.obj["debug"]
    max_workers = ctx.obj["max_workers"]

    setup_logging(debug)

    # Validate mutually exclusive options
    if artifact_location and (namespace or build_id):
        click.echo("Error: Cannot use --artifact-location with --build-id and --namespace", err=True)
        sys.exit(1)

    # Generate artifact_location from build_id + namespace if needed
    if namespace or build_id:
        # Both must be provided together
        if not (namespace and build_id):
            click.echo("Error: Both --build-id and --namespace must be provided together", err=True)
            sys.exit(1)

        if not config:
            click.echo("Error: --config is required when using --build-id and --namespace", err=True)
            sys.exit(1)

        # Load config to get base_url
        config_manager = ConfigManager(config)
        config_manager.load()
        base_url = config_manager.get("cli.base_url")

        # Construct artifact_location URL
        artifact_location = f"{base_url}/api/pulp-content/{namespace}/{build_id}/artifacts/pulp_results.json"
        logging.info("Auto-generated artifact location: %s", artifact_location)

    elif not artifact_location:
        click.echo("Error: Either --artifact-location OR (--build-id AND --namespace) must be provided", err=True)
        sys.exit(1)

    # Parse comma-separated filters
    content_types_list = [ct.strip() for ct in content_types.split(",")] if content_types else None
    archs_list = [arch.strip() for arch in archs.split(",")] if archs else None

    # Get certificate paths from config if not provided via CLI
    if config and (not cert_path or not key_path):
        try:
            config_manager = ConfigManager(config)
            config_manager.load()
            if not cert_path:
                loaded_cert = config_manager.get("cli.cert")
                # Only assign if we got a non-empty value that exists
                if loaded_cert and isinstance(loaded_cert, str) and loaded_cert.strip():
                    loaded_cert = loaded_cert.strip()
                    # Expand user path and resolve relative paths
                    expanded_cert = os.path.expanduser(loaded_cert)
                    if os.path.exists(expanded_cert):
                        cert_path = expanded_cert
            if not key_path:
                loaded_key = config_manager.get("cli.key")
                # Only assign if we got a non-empty value that exists
                if loaded_key and isinstance(loaded_key, str) and loaded_key.strip():
                    loaded_key = loaded_key.strip()
                    # Expand user path and resolve relative paths
                    expanded_key = os.path.expanduser(loaded_key)
                    if os.path.exists(expanded_key):
                        key_path = expanded_key
        except Exception as e:
            logging.debug("Could not load cert/key from config: %s", e)
            # If config loading fails, cert_path/key_path remain None and will be caught by validation below

    # Check if artifact_location is a remote URL BEFORE creating context
    is_remote = artifact_location.startswith(("http://", "https://")) if artifact_location else False

    # Validate that cert_path and key_path are provided for remote URLs
    if is_remote and (not cert_path or not key_path):
        logging.error(
            "Certificate and key paths are required when artifact_location is a remote URL. "
            "Provide them via --config, --cert-path, or --key-path."
        )
        sys.exit(1)

    # Create context object
    args = TransferContext(
        artifact_location=artifact_location,
        namespace=namespace,
        key_path=key_path,
        config=config,
        build_id=build_id,
        debug=debug,
        max_workers=max_workers,
        content_types=content_types_list,
        archs=archs_list,
    )

    try:
        # Initialize distribution client only if needed
        distribution_client = None
        if cert_path and key_path:
            logging.info("Initializing distribution client...")
            distribution_client = DistributionClient(cert_path, key_path)

        # Load artifact metadata and validate
        artifact_data = load_and_validate_artifacts(args, distribution_client)

        # Set up repositories if configuration is provided
        pulp_client = setup_repositories_if_needed(args, artifact_data.artifact_json)  # type: ignore[arg-type]

        # Process artifacts by type
        distros = artifact_data.artifact_json.distributions  # pylint: disable=no-member

        # Download artifacts concurrently
        download_result = download_artifacts_concurrently(
            artifact_data.artifacts,
            distros,
            distribution_client,
            max_workers,
            args.content_types,
            args.archs,
        )

        # Upload downloaded files to Pulp repositories if client is available
        upload_info = None
        if pulp_client:
            logging.info("Uploading downloaded files to Pulp repositories...")
            upload_info = upload_downloaded_files_to_pulp(pulp_client, download_result.pulled_artifacts, args)
        else:
            logging.info("No Pulp client available, skipping upload to repositories")

        # Generate and display transfer report
        generate_transfer_report(
            download_result.pulled_artifacts, download_result.completed, download_result.failed, args, upload_info
        )

        # Check for any errors and exit with error code if found
        has_errors = False
        error_messages = []

        # Check for download failures
        if download_result.failed > 0:
            has_errors = True
            error_messages.append(f"{download_result.failed} artifact download(s) failed")

        # Check for upload errors
        if upload_info and upload_info.has_errors:
            has_errors = True
            error_count = len(upload_info.upload_errors)
            error_messages.append(f"{error_count} upload error(s) occurred")

        if has_errors:
            logging.error("Transfer completed with errors:")
            for msg in error_messages:
                logging.error("  - %s", msg)
            sys.exit(1)

        logging.info("All operations completed successfully")

    except httpx.HTTPError as e:
        handle_http_error(e, "transfer operation")
        sys.exit(1)
    except Exception as e:
        handle_generic_error(e, "transfer operation")
        sys.exit(1)
    finally:
        # Ensure pulp client session is properly closed if it was created
        if "pulp_client" in locals() and pulp_client:
            pulp_client.close()
            logging.debug("PulpClient session closed")
        if distribution_client and hasattr(distribution_client, "session"):
            distribution_client.session.close()
            logging.debug("Distribution client session closed")


__all__ = ["transfer"]
