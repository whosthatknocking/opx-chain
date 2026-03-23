#!/usr/bin/env python3
"""Capture a documentation screenshot of the local CSV viewer."""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

from playwright.sync_api import sync_playwright


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "images" / "viewer-option-chain.png"


def wait_for_server(url: str, timeout_seconds: float = 15.0) -> None:
    """Poll the local viewer until it responds or the timeout expires."""
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.5) as response:
                if 200 <= response.status < 500:
                    return
        except Exception as exc:  # pylint: disable=broad-exception-caught  # pragma: no cover
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"Viewer did not become ready at {url}: {last_error}")


def pick_free_port() -> int:
    """Pick an ephemeral localhost port for a temporary viewer process."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def capture_screenshot(url: str, output_path: Path, theme: str) -> None:
    """Open the viewer in Playwright and save a full-page screenshot."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1100}, color_scheme=theme)
        page.goto(url, wait_until="domcontentloaded")
        # Set the app theme explicitly, then reload so the app initializes in the requested mode.
        page.evaluate(
            """([selectedTheme]) => {
                window.localStorage.setItem('options-fetcher-theme', selectedTheme);
                document.documentElement.dataset.theme = selectedTheme;
                document.body.setAttribute('data-theme', selectedTheme);
            }""",
            [theme],
        )
        page.goto(url, wait_until="networkidle")
        page.screenshot(path=str(output_path), full_page=True)
        browser.close()


def parse_args() -> argparse.Namespace:
    """Parse screenshot script CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Capture a screenshot of the local Options Fetcher viewer."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Where to save the screenshot (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--theme",
        choices=("light", "dark"),
        default="dark",
        help="Preferred browser color scheme for the screenshot.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Viewer port to use. Defaults to a free ephemeral port.",
    )
    return parser.parse_args()


def main() -> int:
    """Launch the local viewer, capture a screenshot, and shut it down."""
    args = parse_args()
    port = args.port or pick_free_port()
    url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env["OPX_VIEWER_HOST"] = "127.0.0.1"
    env["OPX_VIEWER_PORT"] = str(port)
    env["OPX_VIEWER_QUIET"] = "1"

    with subprocess.Popen(
        [sys.executable, "viewer.py"],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) as process:
        try:
            wait_for_server(url)
            capture_screenshot(url, args.output, args.theme)
            print(f"Saved screenshot: {args.output}")
            return 0
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    raise SystemExit(main())
