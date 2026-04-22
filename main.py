"""Compatibility entrypoint for the packaged fetcher CLI."""

from opx_chain import fetcher as _fetcher

RUNS_DIR = _fetcher.RUNS_DIR
LOCKS_DIR = _fetcher.LOCKS_DIR
FETCHER_LOCK_PATH = _fetcher.FETCHER_LOCK_PATH
format_file_size = _fetcher.format_file_size
get_runtime_config = _fetcher.get_runtime_config
describe_runtime_config = _fetcher.describe_runtime_config
create_run_logger = _fetcher.create_run_logger
fetch_ticker_option_chain = _fetcher.fetch_ticker_option_chain
validate_export_frame = _fetcher.validate_export_frame
emit_validation_report = _fetcher.emit_validation_report
write_options_csv = _fetcher.write_options_csv
get_storage_backend = _fetcher.get_storage_backend
pd = _fetcher.pd
datetime = _fetcher.datetime  # pylint: disable=invalid-name


def acquire_fetcher_lock():
    """Acquire the fetcher lock using the current legacy shim globals."""
    _fetcher.LOCKS_DIR = LOCKS_DIR
    _fetcher.FETCHER_LOCK_PATH = FETCHER_LOCK_PATH
    return _fetcher.acquire_fetcher_lock()


def release_fetcher_lock(lock_handle):
    """Release the fetcher lock using the current legacy shim globals."""
    _fetcher.FETCHER_LOCK_PATH = FETCHER_LOCK_PATH
    return _fetcher.release_fetcher_lock(lock_handle)


_DELEGATED = (
    "RUNS_DIR", "LOCKS_DIR", "FETCHER_LOCK_PATH",
    "format_file_size", "get_runtime_config", "describe_runtime_config",
    "create_run_logger", "fetch_ticker_option_chain", "validate_export_frame",
    "emit_validation_report", "write_options_csv", "get_storage_backend",
    "pd", "datetime",
)


def main(argv=None):
    """Delegate to the packaged fetcher while honoring patched legacy globals."""
    saved = {attr: getattr(_fetcher, attr) for attr in _DELEGATED}
    try:
        for attr in _DELEGATED:
            setattr(_fetcher, attr, globals()[attr])
        return _fetcher.main(argv)
    finally:
        for attr, val in saved.items():
            setattr(_fetcher, attr, val)


if __name__ == "__main__":
    raise SystemExit(main())
