"""
Pull command for Pulp Tool CLI.

Downloads artifacts from Pulp (or OCI-backed ``pulp_results.json``) without promoting to another domain.
Use ``transfer`` to download and re-upload to a destination Pulp instance.
"""

import click

from ..models.artifact_pull_run import ArtifactPullRunOptions
from ..utils import setup_logging
from .artifact_pull_run import run_artifact_pull_flow


@click.command()
@click.option(
    "--artifact-location",
    help=(
        "Path to pulp_results.json, Pulp content HTTPS URL, or OCI manifest ref (repo@sha256:… or oci:…). "
        "Mutually exclusive with --build-id + --namespace."
    ),
)
@click.option(
    "--content-types",
    help=("Comma-separated list of content types to pull (rpm,log,sbom). If not specified, all types are pulled."),
)
@click.option(
    "--archs",
    help=(
        "Comma-separated list of architectures to pull (e.g., x86_64,aarch64,noarch). "
        "If not specified, all architectures are pulled."
    ),
)
@click.option(
    "--cert-path",
    type=click.Path(exists=True),
    help="Path to SSL certificate file for authentication (optional, can come from --config)",
)
@click.option(
    "--key-path",
    type=click.Path(exists=True),
    help="Path to SSL private key file for authentication (optional, can come from --config)",
)
@click.option(
    "--distribution-config",
    type=click.Path(exists=True),
    help=(
        "Path to config file for distribution auth (cert/key or username/password). "
        "If not set, auth is loaded from --config."
    ),
)
@click.pass_context
def pull(  # pylint: disable=too-many-positional-arguments
    ctx: click.Context,
    artifact_location: str | None,
    content_types: str | None,
    archs: str | None,
    cert_path: str | None,
    key_path: str | None,
    distribution_config: str | None,
) -> None:
    """Download artifacts from Pulp repositories (no destination promotion)."""
    config = ctx.obj["config"]
    namespace = ctx.obj["namespace"]
    build_id = ctx.obj["build_id"]
    debug = ctx.obj["debug"]
    max_workers = ctx.obj["max_workers"]

    setup_logging(debug)

    run_artifact_pull_flow(
        ArtifactPullRunOptions(
            artifact_location=artifact_location,
            content_types=content_types,
            archs=archs,
            cert_path=cert_path,
            key_path=key_path,
            source_config_path=(config or "").strip() or None,
            dest_config_path=None,
            distribution_config=distribution_config,
            side_tag=None,
            artifact_results=None,
            snapshot_path=None,
            oci_storage=None,
            verify_rpm_gpg=False,
            gpg_keyring=None,
            namespace=namespace,
            build_id=build_id,
            debug=debug,
            max_workers=max_workers,
            operation_label="pull",
        )
    )


__all__ = ["pull"]
