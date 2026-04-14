"""Temporary TOML config files for CLI tests."""

from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def tempfile_config():
    """Yield path to a minimal valid ``cli.toml`` in a temporary directory."""
    tmpdir = tempfile.mkdtemp()
    path = Path(tmpdir) / "config.toml"
    try:
        path.write_text(
            '[cli]\nbase_url = "https://pulp.example.com"\n' 'api_root = "/pulp/api/v3"\n' 'domain = "test-domain"'
        )
        yield str(path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
