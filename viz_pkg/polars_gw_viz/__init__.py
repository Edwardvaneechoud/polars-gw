"""Bundled Graphic Walker + React assets for polars-gw[viz]."""

from importlib import resources


def assets_dir() -> str:
    """Return the filesystem path to the bundled viz assets."""
    return str(resources.files(__name__))
