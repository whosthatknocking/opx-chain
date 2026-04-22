"""Config-driven storage backend factory."""

from __future__ import annotations

from pathlib import Path

from opx.storage.filesystem import FilesystemBackend


def get_storage_backend(config=None):
    """Return a configured StorageBackend, or None when storage is disabled.

    When config is None, the process runtime config is loaded automatically.
    Returns None when storage.enable = false (the default).
    Returns a FilesystemBackend when storage.enable = true and
    storage.backend = 'filesystem'.
    """
    if config is None:
        from opx.config import get_runtime_config  # pylint: disable=import-outside-toplevel
        config = get_runtime_config()

    if not config.storage_enabled:
        return None

    if config.storage_backend == "filesystem":
        return FilesystemBackend(
            output_dir=Path("output"),
            logs_dir=Path("logs"),
            debug_dir=config.debug_dump_dir,
            max_runs_retained=config.storage_max_runs_retained,
        )

    return None
