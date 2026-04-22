"""Config-driven storage backend factory."""

from __future__ import annotations

import os
from pathlib import Path

from opx_chain.storage.filesystem import FilesystemBackend

_APP_NAME = "opx-chain"


def _default_data_dir() -> Path:
    xdg = os.environ.get("XDG_DATA_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "share"
    return base / _APP_NAME


def get_storage_backend(config=None):
    """Return a configured StorageBackend, or None when storage is disabled.

    When config is None, the process runtime config is loaded automatically.
    Returns None when storage.enable = false (the default).
    Returns a FilesystemBackend or SqliteIndexedBackend when enabled.
    """
    if config is None:
        from opx_chain.config import get_runtime_config  # pylint: disable=import-outside-toplevel
        config = get_runtime_config()

    if not config.storage_enabled:
        return None

    base = config.storage_dir if config.storage_dir else _default_data_dir()
    kwargs = {
        "output_dir": base / "output",
        "logs_dir": base / "logs",
        "debug_dir": config.debug_dump_dir,
        "max_runs_retained": config.storage_max_runs_retained,
        "dataset_format": config.storage_dataset_format,
    }

    if config.storage_backend == "sqlite":
        from opx_chain.storage.sqlite_indexed import SqliteIndexedBackend  # pylint: disable=import-outside-toplevel,no-name-in-module
        return SqliteIndexedBackend(db_path=base / "opx-chain.db", **kwargs)

    return FilesystemBackend(**kwargs)
