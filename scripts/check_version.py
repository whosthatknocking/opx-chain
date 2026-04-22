"""Validate project version consistency and optional Git tag alignment."""

from __future__ import annotations

import argparse
import os
import re
import sys

from opx_chain import __version__
from opx_chain.config import SCRIPT_VERSION
from opx_chain.version import get_version

SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for explicit tag validation."""
    parser = argparse.ArgumentParser(
        description="Validate opx version consistency and optional tag alignment."
    )
    parser.add_argument(
        "--tag",
        help="Optional tag name to validate against the package version, for example v0.1.0.",
    )
    return parser.parse_args()


def _resolve_tag_name(cli_tag: str | None) -> str | None:
    """Resolve the tag to validate from CLI args or GitHub Actions environment."""
    if cli_tag:
        return cli_tag
    if os.getenv("GITHUB_REF_TYPE") == "tag":
        return os.getenv("GITHUB_REF_NAME")
    return None


def main() -> int:
    """Validate that all code paths agree on the project version."""
    args = parse_args()
    package_version = get_version()

    if not SEMVER_PATTERN.match(package_version):
        print(
            (
                "Version validation failed: "
                f"pyproject/package version '{package_version}' is not plain semver X.Y.Z."
            ),
            file=sys.stderr,
        )
        return 1

    if __version__ != package_version:
        print(
            (
                "Version validation failed: "
                f"opx.__version__='{__version__}' does not match '{package_version}'."
            ),
            file=sys.stderr,
        )
        return 1

    if SCRIPT_VERSION != package_version:
        print(
            (
                "Version validation failed: "
                f"SCRIPT_VERSION='{SCRIPT_VERSION}' does not match '{package_version}'."
            ),
            file=sys.stderr,
        )
        return 1

    tag_name = _resolve_tag_name(args.tag)
    if tag_name is not None and tag_name != f"v{package_version}":
        print(
            (
                "Version validation failed: "
                f"tag '{tag_name}' does not match expected 'v{package_version}'."
            ),
            file=sys.stderr,
        )
        return 1

    print(f"Version validation passed: {package_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
