"""Compatibility entrypoint for running the fetcher from the repo root."""

from opx.fetcher import main


if __name__ == "__main__":
    raise SystemExit(main())
