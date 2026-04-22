"""Project version helpers."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib


def _version_from_pyproject() -> str:
    """Read the package version from pyproject.toml for local source runs."""
    project_root = Path(__file__).resolve().parent.parent
    pyproject_path = project_root / "pyproject.toml"
    with pyproject_path.open("rb") as handle:
        pyproject = tomllib.load(handle)
    return pyproject["project"]["version"]


def get_version() -> str:
    """Return the installed package version, falling back to pyproject in source checkouts."""
    try:
        return version("opx")
    except PackageNotFoundError:
        return _version_from_pyproject()


__version__ = get_version()
