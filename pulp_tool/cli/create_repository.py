import logging
import sys
from typing import Optional

import click
from pydantic import ValidationError


from ..api import PulpClient
from ..models.cli import CreateRepository, DistributionOptions, Package, RepositoryOptions
from ..models.pulp_api import (
    DistributionRequest,
    RepositoryRequest,
    RpmRepositoryRequest,
    RpmDistributionRequest,
)
from ..utils import PulpHelper, setup_logging
from ..utils.error_handling import handle_generic_error


@click.command()
@click.option(
    "--repository-name",
    help="A unique name for this repository. (required when not using json)",
)
@click.option(
    "--packages",
    help="Comma-separated list of packages to be added to the newly created repository. (required when not using json)",
)
@click.option(
    "--compression-type",
    type=click.Choice(["zstd", "gz"]),
    help="The compression type to use for metadata files.",
)
@click.option(
    "--checksum-type",
    type=click.Choice(["unknown", "md5", "sha1", "sha224", "sha256", "sha384", "sha512"]),
    help="The preferred checksum type during repo publish.",
)
@click.option(
    "--skip-publish",
    is_flag=True,
    help="Disables autopublish for a repository.",
)
@click.option(
    "--base-path",
    help="The base (relative) path component of the published url. (required when not using json)",
)
@click.option(
    "--generate-repo-config",
    is_flag=True,
    help="An option specifying whether Pulp should generate *.repo files. (Ignored for non-rpm distributions)",
)
@click.option(
    "-j",
    "--json-data",
    help="JSON string input. CLI options are ignored when JSON data is provided",
)
@click.pass_context
def create_repository(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    ctx: click.Context,
    repository_name: str,
    packages: str,
    compression_type: str,
    checksum_type: str,
    skip_publish: bool,
    base_path: str,
    generate_repo_config: bool,
    json_data: Optional[str],
):
    """
    Create a custom defined repository.
    """

    # Get shared options from context
    config = ctx.obj["config"]
    debug = ctx.obj["debug"]

    setup_logging(debug, use_wrapping=True)

    repo_data = None
    if json_data:
        try:
            repo_data = CreateRepository.model_validate_json(json_data)
        except ValidationError as e:
            logging.error("Unable to validate json data:")
            for error in e.errors():
                if error["type"] == "json_invalid":
                    logging.error(f"Invalid JSON: {error['msg']}")
                else:
                    logging.error(f"{e.title}-{error['loc'][0]}: {error['msg']}")
            sys.exit(1)
    else:
        missing_option = False
        if repository_name is None:
            logging.error("Missing option --repository-name: Input should be a valid string")
            missing_option = True
        if packages is None:
            logging.error("Missing option --packages: Input should be comma-separated list of packages")
            missing_option = True
        if base_path is None:
            logging.error("Missing option --base-path: Input should be a valid string")
            missing_option = True
        if missing_option:
            sys.exit(1)
        try:
            repo_data = CreateRepository(
                name=repository_name,
                packages=[Package(pulp_href=package) for package in packages.split(",") if package],
                repository_options=RepositoryOptions(
                    compression_type=compression_type,  # type: ignore
                    checksum_type=checksum_type,  # type: ignore
                    autopublish=not skip_publish,
                ),
                distribution_options=DistributionOptions(
                    name=repository_name, base_path=base_path, generate_repo_config=generate_repo_config
                ),
            )
        except ValidationError as e:
            logging.error("Unable to validate CLI options:")
            for error in e.errors():
                logging.error(f"{e.title}-{error['loc'][0]}: {error['msg']}")
            sys.exit(1)

    if repo_data is not None:
        package_list = []
        for package in repo_data.packages:
            package_list.append(package.pulp_href)

    if "file" in package_list[0]:
        repo_api = "file"
        new_repo = RepositoryRequest(
            name=repo_data.name,
            **repo_data.repository_options.model_dump(exclude_none=True),
        )
        new_distro = DistributionRequest(**repo_data.distribution_options.model_dump(exclude_none=True))
    else:
        repo_api = "rpm"
        new_repo = RpmRepositoryRequest(
            name=repo_data.name, **repo_data.repository_options.model_dump(exclude_none=True)
        )
        new_distro = RpmDistributionRequest(**repo_data.distribution_options.model_dump(exclude_none=True))

    client = None
    try:

        client = PulpClient.create_from_config_file(path=config)
        repository_helper = PulpHelper(client)
        if repo_data is not None:
            logging.info(
                "Creating repository %s, with distribution %s",
                repo_data.name,
                repo_data.distribution_options.name,
            )
            repo_prn, repo_href = repository_helper.create_or_get_repository(None, repo_api, new_repo, new_distro)

            logging.info(f"Repository created: -{repo_prn} -{repo_href}")

        if repo_href:
            logging.info("Updating repository with packages")
            logging.debug(f"Packages: {package_list}")
            repo_task = client.add_content(repo_href, package_list)
            finished_task = client.wait_for_finished_task(repo_task.pulp_href)
            if finished_task.created_resources:
                logging.debug(
                    "Captured %d created resources from RPM add_content", len(finished_task.created_resources)
                )
            logging.info("Repository updated")

    except Exception as e:
        handle_generic_error(e, "create-repository operation")
        sys.exit(1)
    finally:
        # Ensure pulp client session is properly closed if it was created
        if client:
            client.close()
            logging.debug("PulpClient session closed")


__all__ = ["create_repository"]
