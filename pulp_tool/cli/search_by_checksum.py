"""
Search by checksum command for Pulp Tool CLI.

This module provides the search-by-checksum command for finding RPM packages
in Pulp by their SHA256 checksum.

Results.json format (--results-json input and --output-results output):
    {
      "artifacts": {
        "<artifact_key>": {
          "labels": {"arch": "...", "build_id": "...", ...},
          "url": "https://...",
          "sha256": "<64-char hex>"
        },
        ...
      },
      "distributions": {
        "rpms": "https://...",
        "logs": "https://...",
        "sbom": "https://...",
        "artifacts": "https://..."
      }
    }

Artifact keys may be simple filenames (e.g. "pkg.rpm") or paths (e.g.
"namespace/build-id/sbom-merged.json"). Only entries whose key ends with
".rpm" are treated as RPMs and searched/removed. Output preserves the same
structure with found RPMs removed.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click
import httpx

from ..api import PulpClient
from ..models.pulp_api import RpmPackageResponse
from ..utils import setup_logging
from ..utils.error_handling import handle_http_error, handle_generic_error

SHA256_HEX_LENGTH = 64


# -----------------------------------------------------------------------------
# Results.json helpers
# -----------------------------------------------------------------------------


def _is_rpm_artifact(key: str) -> bool:
    """Return True if artifact key represents an RPM (ends with .rpm)."""
    return key.lower().endswith(".rpm")


def _extract_rpm_checksums_from_results(results: Dict[str, Any]) -> List[str]:
    """Extract SHA256 checksums from RPM artifacts in results.json structure."""
    checksums: List[str] = []
    seen: Set[str] = set()
    artifacts = results.get("artifacts", {})
    for key, info in artifacts.items():
        if not _is_rpm_artifact(key):
            continue
        if not isinstance(info, dict):
            continue
        sha256 = info.get("sha256", "").strip().lower()
        if sha256 and len(sha256) == SHA256_HEX_LENGTH and sha256 not in seen:
            checksums.append(sha256)
            seen.add(sha256)
    return checksums


def _remove_found_artifacts(results: Dict[str, Any], found_checksums: Set[str]) -> Dict[str, Any]:
    """Remove RPM artifacts whose sha256 is in found_checksums. Returns a copy."""
    out = json.loads(json.dumps(results))  # Deep copy
    artifacts = out.get("artifacts", {})
    to_remove = [
        key
        for key, info in artifacts.items()
        if _is_rpm_artifact(key)
        and isinstance(info, dict)
        and info.get("sha256", "").strip().lower() in found_checksums
    ]
    for key in to_remove:
        del artifacts[key]
    return out


# -----------------------------------------------------------------------------
# Checksum helpers
# -----------------------------------------------------------------------------


def _validate_checksum(checksum: str) -> bool:
    """Validate that a string is a valid SHA256 checksum (64 hex chars)."""
    return len(checksum) == SHA256_HEX_LENGTH and all(c in "0123456789abcdef" for c in checksum.lower())


def _collect_checksums(checksum: tuple[str, ...], checksums: Optional[str]) -> List[str]:
    """Collect and deduplicate checksums from --checksum and --checksums options."""
    result: List[str] = []
    seen: set[str] = set()
    for c in checksum:
        c_stripped = c.strip().lower()
        if c_stripped and c_stripped not in seen:
            result.append(c_stripped)
            seen.add(c_stripped)
    if checksums:
        for c in checksums.split(","):
            c_stripped = c.strip().lower()
            if c_stripped and c_stripped not in seen:
                result.append(c_stripped)
                seen.add(c_stripped)
    return result


# -----------------------------------------------------------------------------
# Pulp API
# -----------------------------------------------------------------------------


def _search_pulp_for_rpms(client: PulpClient, checksums: List[str]) -> List[RpmPackageResponse]:
    """Query Pulp for RPM packages matching the given checksums. Returns parsed packages."""
    response = client.get_rpm_by_pkgIDs(checksums)
    response.raise_for_status()
    results_raw = response.json().get("results", [])

    packages: List[RpmPackageResponse] = []
    for item in results_raw:
        try:
            packages.append(RpmPackageResponse(**item))
        except Exception:
            pass
    return packages


# -----------------------------------------------------------------------------
# Output formatting
# -----------------------------------------------------------------------------


def _format_table(packages: List[RpmPackageResponse]) -> str:
    """Format packages as a human-readable table."""
    if not packages:
        return ""

    widths = {"pkgId": 66, "name": 30, "version": 12, "release": 12, "arch": 10, "pulp_href": 50}
    header = " ".join(f"{k:<{widths[k]}}" for k in widths)
    separator = "-" * len(header)
    lines = [header, separator]

    for pkg in packages:
        line = (
            f"{pkg.pkgId:<{widths['pkgId']}} "
            f"{pkg.name[: widths['name'] - 1]:<{widths['name']}} "
            f"{pkg.version[: widths['version'] - 1]:<{widths['version']}} "
            f"{pkg.release[: widths['release'] - 1]:<{widths['release']}} "
            f"{pkg.arch:<{widths['arch']}} "
            f"{pkg.pulp_href[: widths['pulp_href'] - 1]:<{widths['pulp_href']}}"
        )
        lines.append(line)
    return "\n".join(lines)


def _packages_to_json(packages: List[RpmPackageResponse]) -> str:
    """Convert packages to JSON output."""
    data = [
        {
            "pkgId": pkg.pkgId,
            "pulp_href": pkg.pulp_href,
            "name": pkg.name,
            "epoch": pkg.epoch,
            "version": pkg.version,
            "release": pkg.release,
            "arch": pkg.arch,
            "pulp_labels": pkg.pulp_labels,
        }
        for pkg in packages
    ]
    return json.dumps(data, indent=2)


# -----------------------------------------------------------------------------
# Mode handlers
# -----------------------------------------------------------------------------


def _run_direct_search(
    config: str,
    checksum_list: List[str],
    output_format: str,
    checksums_only: bool,
) -> None:
    """Search by checksums from CLI options and print results."""
    client = PulpClient.create_from_config_file(path=config)
    packages = _search_pulp_for_rpms(client, checksum_list)

    if checksums_only:
        for pkg in packages:
            click.echo(pkg.pkgId)
    elif output_format == "json":
        click.echo(_packages_to_json(packages))
    else:
        table = _format_table(packages)
        if table:
            click.echo(table)


def _run_results_json_mode(config: str, results_json: Path, output_results: Path) -> None:
    """Load results.json, remove RPMs found in Pulp, write filtered output."""
    try:
        with open(results_json, encoding="utf-8") as f:
            results_data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        click.echo(f"Error: Failed to read results.json: {e}", err=True)
        sys.exit(1)

    checksum_list = _extract_rpm_checksums_from_results(results_data)
    if not checksum_list:
        output_results.parent.mkdir(parents=True, exist_ok=True)
        with open(output_results, "w", encoding="utf-8") as f:
            json.dump(results_data, f, indent=2)
        click.echo(f"Wrote results to {output_results} (no RPM artifacts to filter)")
        return

    invalid = [c for c in checksum_list if not _validate_checksum(c)]
    if invalid:
        click.echo(
            f"Error: Invalid checksum in results.json (expected 64 hex chars): {invalid[0]}",
            err=True,
        )
        sys.exit(1)

    try:
        client = PulpClient.create_from_config_file(path=config)
        packages = _search_pulp_for_rpms(client, checksum_list)
        found_checksums = {pkg.pkgId.lower() for pkg in packages}

        filtered_results = _remove_found_artifacts(results_data, found_checksums)
        output_results.parent.mkdir(parents=True, exist_ok=True)
        with open(output_results, "w", encoding="utf-8") as f:
            json.dump(filtered_results, f, indent=2)

        removed = len(results_data.get("artifacts", {})) - len(filtered_results.get("artifacts", {}))
        click.echo(f"Wrote results to {output_results} (removed {removed} found RPM(s))")
    except httpx.HTTPError as e:
        handle_http_error(e, "search by checksum")
        sys.exit(1)
    except Exception as e:
        handle_generic_error(e, "search by checksum")
        sys.exit(1)


# -----------------------------------------------------------------------------
# CLI command
# -----------------------------------------------------------------------------


@click.command("search-by-checksum")
@click.option(
    "-c",
    "--checksum",
    "checksum",
    multiple=True,
    help="SHA256 checksum of an RPM package (can be repeated)",
)
@click.option(
    "--checksums",
    help="Comma-separated list of SHA256 checksums",
)
@click.option(
    "--results-json",
    type=click.Path(exists=True, path_type=Path),
    help="Path to results.json (pulp_results.json) to filter; extracts RPM checksums and removes found content",
)
@click.option(
    "--output-results",
    type=click.Path(path_type=Path),
    help="Path to write filtered results.json (required when --results-json is used)",
)
@click.option(
    "-o",
    "--output",
    "output_format",
    type=click.Choice(["json", "table"]),
    default="json",
    help="Output format for direct checksum search (default: json)",
)
@click.option(
    "--checksums-only",
    is_flag=True,
    help="Output only matched checksums, one per line (for scripting)",
)
@click.pass_context
def search_by_checksum(
    ctx: click.Context,
    checksum: tuple[str, ...],
    checksums: Optional[str],
    results_json: Optional[Path],
    output_results: Optional[Path],
    output_format: str,
    checksums_only: bool,
) -> None:
    """Search for RPM packages in Pulp by SHA256 checksum."""
    config = ctx.obj["config"]
    debug = ctx.obj["debug"]
    setup_logging(debug, use_wrapping=True)

    if not config:
        click.echo("Error: --config is required for search-by-checksum", err=True)
        sys.exit(1)

    if results_json is not None:
        if output_results is None:
            click.echo(
                "Error: --output-results is required when --results-json is used",
                err=True,
            )
            sys.exit(1)
        _run_results_json_mode(config, results_json, output_results)
        return

    checksum_list = _collect_checksums(checksum, checksums)
    if not checksum_list:
        click.echo(
            "Error: At least one checksum must be provided (--checksum, --checksums, or --results-json)",
            err=True,
        )
        sys.exit(1)

    invalid = [c for c in checksum_list if not _validate_checksum(c)]
    if invalid:
        click.echo(
            f"Error: Invalid checksum format (expected 64 hex chars): {invalid[0]}",
            err=True,
        )
        sys.exit(1)

    try:
        _run_direct_search(config, checksum_list, output_format, checksums_only)
    except httpx.HTTPError as e:
        handle_http_error(e, "search by checksum")
        sys.exit(1)
    except Exception as e:
        handle_generic_error(e, "search by checksum")
        sys.exit(1)
