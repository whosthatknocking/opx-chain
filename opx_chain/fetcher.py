"""CLI entrypoint for fetching option chains and writing the export CSV."""
# pylint: disable=too-many-lines

import argparse
from collections.abc import Callable
from dataclasses import dataclass, fields as dataclass_fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
from pathlib import Path
import shutil
import signal

import pandas as pd

from opx_chain import SCHEMA_VERSION
from opx_chain.config import (
    SUPPORTED_PROVIDERS,
    describe_runtime_config,
    get_runtime_config,
    get_runtime_config_override,
    set_runtime_config_override,
)
from opx_chain.config_coercion import ConfigError, coerce_path
from opx_chain.error_summary import compact_exception_summary
from opx_chain.export import (  # pylint: disable=unused-import
    prepare_export_frame,
    write_options_csv,
)
from opx_chain.fetch import fetch_ticker_option_chain, fetch_ticker_price_context
from opx_chain.integrity import (
    OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
    OptionChainDataIntegrityError,
    OptionChainIntegrityCode,
    OptionChainIntegritySummary,
    OptionChainRowScope,
    OptionChainSchemaCompatibilityError,
)
from opx_chain.iv_history_backfill import run_iv_history_backfill
from opx_chain.json_utils import dumps_strict_json
from opx_chain.locks import acquire_nonblocking_file_lock, release_file_lock
from opx_chain.paths import get_runs_dir
from opx_chain.price_context import PRICE_CONTEXT_RECORD_FIELDS, PRICE_CONTEXT_SCHEMA_VERSION
from opx_chain.price_history import get_price_history_store
from opx_chain.positions import (
    DEFAULT_POSITIONS_PATH,
    PositionSet,
    load_positions,
    positions_fingerprint,
    resolve_positions_path,
)
from opx_chain.providers import get_data_provider
from opx_chain.runlog import create_run_logger, get_logger, log_run_started
from opx_chain.storage.atomic import atomic_file_write, atomic_write_bytes
from opx_chain.storage.factory import get_data_dir, get_storage_backend
from opx_chain.storage.integrity import prepare_option_chain_dataset
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetHandle,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
    ValidationRecord,
)
from opx_chain.tickers import is_valid_ticker
from opx_chain.timestamps import format_utc_compact, format_utc_z_seconds
from opx_chain.validate import ValidationFinding, emit_validation_report, validate_export_frame
from opx_chain.valuation_evidence import build_option_valuation_evidence

_DATA_DIR = get_data_dir()
RUNS_DIR = _DATA_DIR / "runs"
FETCHER_LOCK_PATH = _DATA_DIR / "fetcher.lock"
STALE_RUNNING_RUN_SECONDS = 30
UNCLEAN_SHUTDOWN_ERROR = "process_terminated_uncleanly"
_CONFIG_FINGERPRINT_EXCLUDED_FIELDS = frozenset(
    {
        "config_path",
        "config_warnings",
        "debug_dump_dir",
        "marketdata_api_token",
        "massive_api_key",
        "storage_dir",
        "today",
        "viewer_host",
        "viewer_port",
    }
)


@dataclass(frozen=True)
class TickerFetchProgress:
    """Structured progress emitted around each ticker in a normal fetch run."""

    ticker: str
    current: int
    total: int
    status: str


TickerFetchProgressCallback = Callable[[TickerFetchProgress], None]


def _emit_ticker_fetch_progress(
    callback: TickerFetchProgressCallback | None,
    progress: TickerFetchProgress,
    logger,
) -> None:
    if callback is None:
        return
    try:
        callback(progress)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if logger is not None:
            logger.warning(
                "ticker_progress_callback_failed ticker=%s current=%s total=%s status=%s error=%s",
                progress.ticker,
                progress.current,
                progress.total,
                progress.status,
                compact_exception_summary(exc),
            )


def parse_args(argv=None):
    """Parse fetcher CLI arguments."""
    if argv is None and "PYTEST_CURRENT_TEST" in os.environ:
        argv = []
    parser = argparse.ArgumentParser(
        prog="opx-fetch",
        description="Fetch option chains and write a consolidated CSV export.",
    )
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        "--enable-filters",
        action="store_true",
        help="Force shared post-download filters on for this run.",
    )
    filter_group.add_argument(
        "--disable-filters",
        action="store_true",
        help="Force shared post-download filters off for this run.",
    )
    parser.add_argument(
        "--positions",
        type=Path,
        default=None,
        help="Path to positions CSV. Defaults to the XDG data-dir positions file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate config, resolve positions, and verify the storage backend "
            "without making any API calls or writing any output."
        ),
    )
    price_context_group = parser.add_mutually_exclusive_group()
    price_context_group.add_argument(
        "--price-context-only",
        action="store_true",
        help="Fetch only optional daily-OHLCV price context and skip option-chain export.",
    )
    price_context_group.add_argument(
        "--enable-price-context",
        action="store_true",
        help="Force optional price-context enrichment on for this run.",
    )
    price_context_group.add_argument(
        "--disable-price-context",
        action="store_true",
        help="Force optional price-context enrichment off for this run.",
    )
    return parser.parse_args(argv)


def apply_cli_overrides(config, args):
    """Apply one-off CLI overrides on top of the resolved runtime config."""
    override_labels = []
    if args.enable_filters:
        config = replace(config, enable_filters=True)
        override_labels.append("filters_enable=true")
    elif args.disable_filters:
        config = replace(config, enable_filters=False)
        override_labels.append("filters_enable=false")
    if args.enable_price_context or args.price_context_only:
        config = replace(config, price_context_enable=True)
        override_labels.append("price_context_enable=true")
    elif args.disable_price_context:
        config = replace(config, price_context_enable=False)
        override_labels.append("price_context_enable=false")
    if args.price_context_only:
        override_labels.append("price_context_only=true")
    return config, ", ".join(override_labels) if override_labels else None


def _coerce_run_fetch_bool(value, *, field_name: str) -> bool:
    """Validate boolean run_fetch overrides at the public API boundary."""
    if not isinstance(value, bool):
        raise ConfigError(f"Config field '{field_name}' must be true or false.")
    return value


def _coerce_run_fetch_non_negative_int(value, *, field_name: str) -> int:
    """Validate nonnegative integer run_fetch overrides."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Config field '{field_name}' must be an integer.")
    if value < 0:
        raise ConfigError(f"Config field '{field_name}' must be >= 0.")
    return value


def _coerce_run_fetch_tickers(tickers) -> tuple[str, ...]:
    """Validate and normalize programmatic ticker overrides."""
    if isinstance(tickers, (str, bytes)) or not isinstance(tickers, (list, tuple)):
        raise ConfigError("Config field 'run_fetch.tickers' must be a sequence of strings.")
    normalized = []
    for ticker in tickers:
        if not isinstance(ticker, str):
            raise ConfigError("Config field 'run_fetch.tickers' must be a sequence of strings.")
        symbol = ticker.strip().upper()
        if not symbol:
            raise ConfigError("Config field 'run_fetch.tickers' must not contain blank values.")
        if not is_valid_ticker(symbol):
            raise ConfigError(
                "Config field 'run_fetch.tickers' must contain valid stock ticker symbols."
            )
        normalized.append(symbol)
    if not normalized:
        raise ConfigError("Config field 'run_fetch.tickers' must not be empty.")
    return tuple(normalized)


def _with_max_expiration_weeks(config, max_expiration_weeks: int):
    """Return config with the expiration-window source and derived date in sync."""
    max_expiration_weeks = _coerce_run_fetch_non_negative_int(
        max_expiration_weeks,
        field_name="run_fetch.max_expiration_weeks",
    )
    max_expiration = (
        None
        if max_expiration_weeks == 0
        else (config.today + timedelta(weeks=max_expiration_weeks)).isoformat()
    )
    return replace(
        config,
        max_expiration_weeks=max_expiration_weeks,
        max_expiration=max_expiration,
    )


def _with_data_provider(config, data_provider: str):
    """Return config with a validated provider override for this run only."""
    if not isinstance(data_provider, str):
        raise ConfigError("Config field 'run_fetch.data_provider' must be a string.")
    provider = str(data_provider or "").strip().lower()
    if provider not in SUPPORTED_PROVIDERS:
        supported = ", ".join(sorted(SUPPORTED_PROVIDERS))
        raise ConfigError(
            f"unsupported data provider {data_provider!r}; expected one of: {supported}"
        )
    if provider == "marketdata" and not config.marketdata_api_token:
        raise ConfigError(
            "Config field 'providers.marketdata.api_token' is required when "
            "run_fetch.data_provider='marketdata'."
        )
    if provider == "massive" and not config.massive_api_key:
        raise ConfigError(
            "Config field 'providers.massive.api_key' is required when "
            "run_fetch.data_provider='massive'."
        )
    return replace(config, data_provider=provider)


def format_file_size(byte_count):
    """Format byte counts into a small human-readable string."""
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.1f} KB"
    return f"{byte_count / (1024 * 1024):.1f} MB"


def _fingerprint_value(value):
    """Return a JSON-stable representation for config fingerprinting."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, list):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _fingerprint_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _config_fingerprint_payload(config) -> dict[str, object]:
    """Return config fields that participate in fetch output fingerprinting."""
    return {
        field.name: _fingerprint_value(getattr(config, field.name))
        for field in dataclass_fields(config)
        if field.name not in _CONFIG_FINGERPRINT_EXCLUDED_FIELDS
    }


def _config_fingerprint(config) -> str:
    """Return a SHA-256 hex digest of the config fields that affect fetch output."""
    return _canonical_json_fingerprint(_config_fingerprint_payload(config))


def _canonical_json_fingerprint(payload: dict[str, object]) -> str:
    """Return a SHA-256 hex digest for a strict canonical JSON payload."""
    serialized = dumps_strict_json(payload, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _positions_fingerprint(
    positions_path: Path,
    position_set: PositionSet | None = None,
) -> str:
    """Return SHA-256 of canonical parsed positions, or empty string if absent."""
    return positions_fingerprint(positions_path, position_set)


def _runtime_data_dir(config) -> Path:
    """Return the run-data base directory for the resolved runtime config."""
    return Path(config.storage_dir) if config.storage_dir else get_data_dir()


def _runs_dir(config) -> Path:
    """Return the directory for CSV side writes and latest pointers."""
    return get_runs_dir(config.storage_dir, default_runs_dir=RUNS_DIR)


def _write_price_context_artifact(output_path: Path, payload: dict[str, object]) -> None:
    """Atomically write a standalone price-context JSON artifact."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_file_write(
        output_path,
        lambda tmp_path: tmp_path.write_text(
            dumps_strict_json(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        ),
    )


def _run_price_context_fetch(config, effective_tickers, logger) -> Path:
    # pylint: disable=too-many-locals
    """Fetch optional price context and write the independent JSON artifact."""
    provider = get_data_provider()
    records = []
    store = get_price_history_store(config)
    try:
        for ticker in effective_tickers:
            prepare_ticker_fetch = getattr(provider, "prepare_ticker_fetch", None)
            if callable(prepare_ticker_fetch):
                prepare_ticker_fetch(ticker)
            context = fetch_ticker_price_context(
                ticker,
                provider=provider,
                logger=logger,
                config=config,
                store=store,
            )
            records.append({
                "ticker": ticker,
                **{field: context.get(field) for field in PRICE_CONTEXT_RECORD_FIELDS},
            })
            status = context.get("price_context_staleness_status")
            as_of = context.get("price_context_as_of") or "none"
            print(f"{ticker}: price_context  status={status}  as_of={as_of}")
    finally:
        store.close()

    fetched_at = datetime.now(tz=timezone.utc)
    timestamp = format_utc_compact(fetched_at)
    runs_dir = _runs_dir(config)
    output_path = runs_dir / f"price_context_{timestamp}.json"
    payload = {
        "artifact_type": "price_context",
        "schema_version": PRICE_CONTEXT_SCHEMA_VERSION,
        "provider": config.data_provider,
        "fetched_at": format_utc_z_seconds(fetched_at),
        "tickers": list(effective_tickers),
        "records": records,
    }
    _write_price_context_artifact(output_path, payload)
    latest_path = runs_dir / "price_context_latest.json"
    atomic_file_write(latest_path, lambda tmp_path: shutil.copy2(output_path, tmp_path))
    if logger:
        logger.info(
            "price_context_finished provider=%s tickers=%s output=%s",
            config.data_provider,
            len(effective_tickers),
            output_path,
        )
    return output_path


def _interrupt_stale_running_runs(storage) -> int:
    """Mark stale running run records interrupted before opening a new run."""
    cutoff = datetime.now(tz=timezone.utc) - timedelta(seconds=STALE_RUNNING_RUN_SECONDS)
    return storage.interrupt_stale_runs(cutoff, UNCLEAN_SHUTDOWN_ERROR)


def _fetcher_lock_path(config) -> Path:
    """Return the lock path for the resolved runtime config."""
    return _runtime_data_dir(config) / "fetcher.lock" if config.storage_dir else FETCHER_LOCK_PATH


def acquire_fetcher_lock(lock_path: Path | None = None):
    """Acquire an exclusive non-blocking lock for the fetcher process."""
    resolved_lock_path = lock_path or FETCHER_LOCK_PATH
    handle = acquire_nonblocking_file_lock(resolved_lock_path)
    if handle is None:
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(f"{resolved_lock_path}\n")
    handle.flush()
    return handle


def release_fetcher_lock(lock_handle, lock_path: Path | None = None):
    """Close the lock handle while keeping the lock file path stable."""
    del lock_path
    release_file_lock(lock_handle)


def _dry_run_logger() -> logging.Logger:
    """Return a stdlib logger that discards dry-run log records."""
    logger = get_logger("fetcher.dry_run")
    if not any(isinstance(handler, logging.NullHandler) for handler in logger.handlers):
        logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return logger


def _validation_sample(finding: ValidationFinding) -> str:
    """Return a compact JSON sample for one validation finding."""
    sample = {
        "message": finding.message,
        "row_index": finding.row_index,
        "contract_symbol": finding.contract_symbol,
        "field": finding.field,
    }
    return dumps_strict_json(
        {key: value for key, value in sample.items() if value is not None},
        sort_keys=True,
    )


def _record_validation_findings(storage, run_id: str, findings: list[ValidationFinding]) -> None:
    """Persist grouped validation findings to the storage backend."""
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for finding in findings:
        key = (finding.severity, finding.code)
        if key not in grouped:
            grouped[key] = {"count": 0, "sample": _validation_sample(finding)}
        grouped[key]["count"] = int(grouped[key]["count"]) + 1

    for (severity, code), payload in sorted(grouped.items()):
        storage.record_validation(ValidationRecord(
            run_id=run_id,
            severity=severity,
            code=code,
            count=int(payload["count"]),
            sample=str(payload["sample"]),
        ))


def _record_integrity_summary(
    storage,
    run_id: str,
    summary: OptionChainIntegritySummary,
) -> None:
    """Persist the canonical bounded integrity projection as grouped diagnostics."""
    samples_by_code = {}
    for sample in summary.samples:
        samples_by_code.setdefault(sample.code, sample)
    for code, count in sorted(summary.counts_by_code.items(), key=lambda item: item[0].value):
        sample = samples_by_code.get(code)
        storage.record_validation(
            ValidationRecord(
                run_id=run_id,
                severity=(
                    "warning"
                    if code is OptionChainIntegrityCode.OPTIONAL_FIELD_DEGRADED
                    else "error"
                ),
                code=code.value,
                count=count,
                sample=(
                    dumps_strict_json(sample.to_dict(), sort_keys=True)
                    if sample is not None
                    else None
                ),
            )
        )


def _dataset_tickers(frame: pd.DataFrame) -> tuple[str, ...]:
    """Return normalized ticker symbols present in a just-written dataset."""
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return ()
    ticker_column = next(
        (
            column
            for column in ("underlying_symbol", "ticker", "symbol")
            if column in frame.columns
        ),
        None,
    )
    if ticker_column is None:
        return ()
    values = []
    for raw_value in frame[ticker_column].dropna():
        value = str(raw_value).strip().upper()
        if value:
            values.append(value)
    return tuple(dict.fromkeys(values))


def _format_iv_history_ingest_summary(result) -> str:
    """Return a compact one-line summary for automatic IV-history ingestion."""
    rows = tuple(getattr(result, "rows", ()) or ())
    status_counts: dict[str, int] = {}
    source_rows = 0
    stored_rows = 0
    for row in rows:
        status = str(getattr(row, "status", None) or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        source_rows += int(getattr(row, "source_rows", 0) or 0)
        stored_rows += int(getattr(row, "stored_rows", 0) or 0)
    statuses = ",".join(
        f"{status}:{count}" for status, count in sorted(status_counts.items())
    ) or "none"
    return (
        f"datasets={len(rows)}  status={statuses}  "
        f"source_rows={source_rows}  stored_rows={stored_rows}"
    )


def _ingest_iv_history_for_dataset(dataset_record, frame, config, storage, logger) -> str | None:
    """Ingest the just-published dataset into durable IV history without failing fetch."""
    if dataset_record is None or storage is None:
        return None
    tickers = _dataset_tickers(frame)
    try:
        result = run_iv_history_backfill(
            providers=(dataset_record.provider,),
            tickers=tickers or None,
            dataset_ids=(dataset_record.dataset_id,),
            config=config,
            storage=storage,
        )
        summary = _format_iv_history_ingest_summary(result)
        if logger:
            logger.info(
                "iv_history_ingest dataset_id=%s %s",
                dataset_record.dataset_id,
                summary,
            )
        return summary
    except Exception as exc:  # pylint: disable=broad-exception-caught
        error_summary = compact_exception_summary(exc)
        if logger:
            logger.warning(
                "iv_history_ingest_failed dataset_id=%s error=%s",
                dataset_record.dataset_id,
                error_summary,
                exc_info=True,
            )
        return f"skipped  dataset={dataset_record.dataset_id[:8]}  error={error_summary}"


def _delete_prepublication_artifacts(storage, run_id: str, dataset_record) -> None:
    """Remove run artifacts when fetch exits before dataset publication."""
    if dataset_record is not None:
        return
    try:
        storage.delete_run_artifacts(run_id)
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def _raise_keyboard_interrupt_on_sigterm(_signum, _frame) -> None:
    """Route SIGTERM through the same cleanup path as Ctrl-C."""
    raise KeyboardInterrupt


class _SigtermAsKeyboardInterrupt:
    """Temporarily translate SIGTERM into KeyboardInterrupt when supported."""

    def __init__(self) -> None:
        self._previous_handler = None
        self._installed = False

    def __enter__(self):
        try:
            self._previous_handler = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, _raise_keyboard_interrupt_on_sigterm)
            self._installed = True
        except (AttributeError, ValueError):
            self._installed = False
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        if self._installed:
            signal.signal(signal.SIGTERM, self._previous_handler)
        return False


def _run_log_reference(run_id: str, log_path: Path) -> bytes:
    """Return a storage-managed reference to the shared append-only run log."""
    payload = {
        "artifact_type": "run_log",
        "run_id": run_id,
        "log_path": str(log_path.resolve()),
        "log_scope": "shared_append_only",
        "lookup_hint": "Search the shared log for this storage run_id.",
    }
    return dumps_strict_json(payload, sort_keys=True, indent=2).encode()


def _write_validated_csv_artifacts(  # pylint: disable=too-many-arguments
    export_df: pd.DataFrame,
    *,
    provider: str,
    output_path: Path,
    latest_path: Path,
    publication_id: str,
    source_frame: pd.DataFrame | None = None,
    positions_content_hash: str | None = None,
    positions_content: bytes | None = None,
) -> int:
    """Validate exact CSV bytes before atomically replacing either artifact."""
    prepared = prepare_option_chain_dataset(
        DatasetWrite(
            data=export_df,
            provider=provider,
            schema_version=SCHEMA_VERSION,
            format="csv",
        ),
        dataset_id=publication_id,
    )
    evidence = build_option_valuation_evidence(
        source_frame if source_frame is not None else export_df, prepared.frame,
        dataset_id=publication_id, dataset_content_hash=prepared.content_hash,
        provider=provider, assessed_at=datetime.now(tz=timezone.utc).isoformat(),
        positions_content_hash=positions_content_hash,
    )
    if positions_content is not None:
        if hashlib.sha256(positions_content).hexdigest() != positions_content_hash:
            raise ValueError("positions snapshot does not match valuation receipt")
        atomic_write_bytes(output_path.with_suffix(".positions.csv"), positions_content)
    atomic_write_bytes(output_path.with_suffix(".valuation.json"), evidence)
    atomic_write_bytes(output_path, prepared.content)
    if positions_content is not None:
        atomic_write_bytes(latest_path.with_suffix(".positions.csv"), positions_content)
    atomic_write_bytes(latest_path.with_suffix(".valuation.json"), evidence)
    atomic_write_bytes(latest_path, prepared.content)
    return len(prepared.content)


def _do_fetch_with_lock_held(  # pylint: disable=too-many-arguments,too-many-branches,too-many-locals,too-many-statements
    config,
    positions_path: Path | None,
    cli_override: str | None,
    *,
    dry_run: bool = False,
    price_context_only: bool = False,
    skip_events: bool = False,
    progress_callback: TickerFetchProgressCallback | None = None,
) -> DatasetHandle | None:
    """Execute the fetch pipeline. Lock must already be held by caller. Raises on failure."""
    logger = None
    log_path = None
    storage = None
    run_id = None
    dataset_record = None
    dataset_handle = None
    iv_history_summary = None
    try:
        storage = get_storage_backend(config)
        if dry_run:
            logger = _dry_run_logger()
            print(f"Today: {config.today}  [DRY RUN — no API calls or writes]")
        else:
            logger, log_path = create_run_logger()
            print(f"Today: {config.today}  Log: {log_path}")
        if cli_override:
            print(f"CLI override: {cli_override}")
        recovered_runs = 0
        if storage is not None and not dry_run and not price_context_only:
            recovered_runs = _interrupt_stale_running_runs(storage)
            if recovered_runs:
                logger.warning(
                    "recovered_stale_running_runs count=%s status=interrupted",
                    recovered_runs,
                )
        runs_today = storage.count_runs_today(config.data_provider) if storage else 0
        print("Config:")
        for line in describe_runtime_config(config):
            print(f"  {line}")
        if recovered_runs:
            print(f"  Recovered stale running runs: {recovered_runs} marked interrupted")
        if runs_today > 0:
            print(
                f"  Completed runs today ({config.data_provider}): {runs_today}"
                f" (this will be run {runs_today + 1})"
            )
        if config.config_warnings:
            print("Config fallbacks:")
            for warning in config.config_warnings:
                print(f"  {warning}")
        resolved_positions_path = resolve_positions_path(
            positions_path,
            default=DEFAULT_POSITIONS_PATH,
        )
        positions_content = (resolved_positions_path.read_bytes()
                             if resolved_positions_path.exists() else None)
        position_set = load_positions(resolved_positions_path)
        if positions_content != (resolved_positions_path.read_bytes()
                                  if resolved_positions_path.exists() else None):
            raise ValueError("positions changed while loading the fetch input")
        extra_tickers = tuple(
            t for t in sorted(position_set.tickers) if t not in set(config.tickers)
        )
        effective_tickers = config.tickers + extra_tickers
        if resolved_positions_path.exists():
            print(
                f"Positions ({resolved_positions_path}): "
                f"{len(position_set.stock_tickers)} stocks, "
                f"{len(position_set.option_keys)} options"
            )
        else:
            print(f"Positions ({resolved_positions_path}): file not found, skipping")
        if extra_tickers:
            print(f"  Added from positions: {', '.join(extra_tickers)}")
        if dry_run:
            print()
            print(f"Would fetch {len(effective_tickers)} ticker(s): {', '.join(effective_tickers)}")
            if price_context_only:
                print("Mode: price-context-only")
            if storage is not None:
                print(f"Storage: {type(storage).__name__} (reachable)")
            print()
            print("Dry-run complete.")
            return None

        if price_context_only:
            output_path = _run_price_context_fetch(config, effective_tickers, logger)
            print()
            print(f"Saved price context: {output_path}")
            return None

        if storage is not None:
            run_id = storage.create_run(RunContext(
                provider=config.data_provider,
                tickers=effective_tickers,
                config_fingerprint=_config_fingerprint(config),
                positions_fingerprint=_positions_fingerprint(
                    resolved_positions_path,
                    position_set,
                ),
            ))
        log_run_started(logger, run_id=run_id, config=config)
        logger.info(
            "run_context today=%s max_expiration=%s provider=%s config_path=%s",
            config.today,
            config.max_expiration,
            config.data_provider,
            config.config_path,
        )
        if cli_override:
            logger.info("cli_override %s", cli_override)
        for line in describe_runtime_config(config):
            logger.info("config_applied %s", line)
        for warning in config.config_warnings:
            logger.warning("config_fallback %s", warning)
        logger.info("positions path: %s", resolved_positions_path)
        logger.info(
            "positions stocks=%s options=%s extra_tickers=%s",
            len(position_set.stock_tickers),
            len(position_set.option_keys),
            len(extra_tickers),
        )

        price_context_path = None
        if config.price_context_enable:
            price_context_path = _run_price_context_fetch(config, effective_tickers, logger)

        ticker_frames = []
        ticker_results: list[TickerFetchResult] = []
        validation_findings = []
        filtered_row_counts = []
        total_tickers = len(effective_tickers)
        for ticker_index, ticker in enumerate(effective_tickers, start=1):
            _emit_ticker_fetch_progress(
                progress_callback,
                TickerFetchProgress(ticker, ticker_index - 1, total_tickers, "starting"),
                logger,
            )
            counts_before = len(filtered_row_counts)
            ticker_df = fetch_ticker_option_chain(
                ticker,
                logger=logger,
                validation_findings=validation_findings,
                filtered_row_counts=filtered_row_counts,
                position_set=position_set,
                skip_events=skip_events,
            )
            if not ticker_df.empty:
                ticker_frames.append(ticker_df)
            kept = len(ticker_df)
            attrs = getattr(ticker_df, "attrs", {})
            filtered_this = int(
                attrs.get(
                    "filtered_row_count",
                    sum(filtered_row_counts[counts_before:]),
                )
            )
            normalized_count = int(attrs.get("normalized_row_count", kept + filtered_this))
            raw_count = int(attrs.get("raw_row_count", normalized_count))
            exp_count = (
                int(ticker_df["expiration_date"].nunique())
                if kept and "expiration_date" in ticker_df.columns else 0
            )
            fetch_status = attrs.get(
                "fetch_status",
                "ok" if not ticker_df.empty else "skipped",
            )
            ticker_result = TickerFetchResult(
                ticker=ticker,
                raw_row_count=raw_count,
                normalized_row_count=normalized_count,
                kept_row_count=kept,
                filtered_row_count=filtered_this,
                expiration_count=exp_count,
                status=str(fetch_status),
                error_summary=attrs.get("fetch_error_summary"),
            )
            ticker_results.append(ticker_result)
            if storage is not None and run_id is not None:
                storage.record_ticker_result(run_id, ticker_result)
            _emit_ticker_fetch_progress(
                progress_callback,
                TickerFetchProgress(ticker, ticker_index, total_tickers, "completed"),
                logger,
            )

        filtered_out_rows = sum(filtered_row_counts)
        if logger:
            logger.info("filter_summary filtered_out_rows=%s", filtered_out_rows)

        if not ticker_frames:
            print("No data fetched.")
            logger.warning("run_finished no_data_fetched=true")
            if storage is not None and run_id is not None:
                storage.fail_run(run_id, "no data fetched")
                run_id = None
            raise RuntimeError("No data fetched.")

        combined = pd.concat(ticker_frames, ignore_index=True)
        if config.enable_validation:
            validation_findings.extend(validate_export_frame(combined))
            emit_validation_report(validation_findings, logger=logger)
            if storage is not None and run_id is not None:
                _record_validation_findings(storage, run_id, validation_findings)
        row_count = len(combined)

        runs_dir = _runs_dir(config)
        write_csv = storage is None or config.storage_also_write_csv
        csv_written = False
        timestamp = format_utc_compact(datetime.now(tz=timezone.utc))
        if storage is not None and run_id is not None:
            csv_output_dir = runs_dir / run_id / "output"
        else:
            csv_output_dir = runs_dir
        output_path = csv_output_dir / f"options_engine_output_{timestamp}.csv"
        latest_path = runs_dir / "options_engine_output_latest.csv"
        export_df = prepare_export_frame([combined])
        positions_content_hash = (hashlib.sha256(positions_content).hexdigest()
                                  if positions_content is not None else None)
        file_size_bytes = 0
        if storage is None:
            file_size_bytes = _write_validated_csv_artifacts(
                export_df,
                provider=config.data_provider,
                output_path=output_path,
                latest_path=latest_path,
                publication_id=f"csv-only-{timestamp}",
                source_frame=combined,
                positions_content_hash=positions_content_hash,
                positions_content=positions_content,
            )
            csv_written = True

        if storage is not None and run_id is not None:
            resolved_expiration_weeks = config.max_expiration_weeks
            if (
                isinstance(resolved_expiration_weeks, bool)
                or not isinstance(resolved_expiration_weeks, int)
                or resolved_expiration_weeks < 0
            ):
                raise ConfigError(
                    "Config field 'settings.max_expiration_weeks' must be a nonnegative "
                    "integer for durable option-chain publication."
                )
            row_scope = OptionChainRowScope(
                schema_version=OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION,
                post_download_filters_enabled=config.enable_filters,
                max_expiration_weeks=resolved_expiration_weeks,
                normalized_row_count=sum(
                    item.normalized_row_count for item in ticker_results
                ),
                kept_row_count=sum(item.kept_row_count for item in ticker_results),
                filtered_row_count=sum(item.filtered_row_count for item in ticker_results),
                ticker_count=len(ticker_results),
            )
            if positions_content is not None:
                storage.write_artifact(run_id, ArtifactWrite(
                    artifact_type="sidecar",
                    content=positions_content,
                    filename="positions.csv",
                ))
            if log_path is not None:
                storage.write_artifact(run_id, ArtifactWrite(
                    artifact_type="run_log",
                    content=_run_log_reference(run_id, log_path),
                    filename="run_log_reference.json",
                ))
            dataset_record = storage.write_dataset(run_id, DatasetWrite(
                data=export_df,
                provider=config.data_provider,
                schema_version=SCHEMA_VERSION,
                format=config.storage_dataset_format,
                row_scope=row_scope,
            ))
            storage.write_artifact(run_id, ArtifactWrite(
                artifact_type="sidecar",
                filename=f"{dataset_record.dataset_id}.valuation.json",
                content=build_option_valuation_evidence(
                    combined, export_df, dataset_id=dataset_record.dataset_id,
                    dataset_content_hash=dataset_record.content_hash, provider=config.data_provider,
                    assessed_at=datetime.now(tz=timezone.utc).isoformat(),
                    positions_content_hash=positions_content_hash,
                ),
            ))
            storage.finalize_run(run_id, RunSummary(status="complete"))
            dataset_handle = storage.get_dataset(dataset_record.dataset_id)
            if write_csv:
                try:
                    file_size_bytes = _write_validated_csv_artifacts(
                        export_df,
                        provider=config.data_provider,
                        output_path=output_path,
                        latest_path=latest_path,
                        publication_id=f"compatibility-{dataset_record.dataset_id}",
                        source_frame=combined,
                        positions_content_hash=positions_content_hash,
                        positions_content=positions_content,
                    )
                    csv_written = True
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "compatibility_csv_write_failed dataset_id=%s error=%s",
                        dataset_record.dataset_id,
                        compact_exception_summary(exc),
                        exc_info=True,
                    )
            iv_history_summary = _ingest_iv_history_for_dataset(
                dataset_record,
                export_df,
                config,
                storage,
                logger,
            )

        print()
        if csv_written:
            print(f"Saved: {output_path}")
        if price_context_path is not None:
            print(f"Price context: {price_context_path}")
        if dataset_record is not None:
            artifact_path = Path(dataset_record.location)
            artifact_size = (
                format_file_size(artifact_path.stat().st_size)
                if artifact_path.exists() else "unknown size"
            )
            run_short = run_id[:8] if run_id else "?"
            print(
                f"Storage: run={run_short}  "
                f"artifact={artifact_path}  {artifact_size}"
            )
        if iv_history_summary is not None:
            print(f"IV history: {iv_history_summary}")

        if csv_written:
            file_size = format_file_size(file_size_bytes)
            summary = f"rows={row_count}  size={file_size}  dropped={filtered_out_rows}"
        else:
            summary = f"rows={row_count}  dropped={filtered_out_rows}"
        print(summary)

        logger.info(
            "run_finished ticker_frames=%s rows_written=%s file_size_bytes=%s"
            " also_csv=%s run_id=%s",
            len(ticker_frames),
            row_count,
            file_size_bytes,
            csv_written,
            run_id,
        )
        return dataset_handle
    except (OptionChainDataIntegrityError, OptionChainSchemaCompatibilityError) as exc:
        print(f"\nFatal error: {exc}")
        if logger:
            logger.exception("run_finished integrity_failure error: %s", exc)
        if storage is not None and run_id is not None:
            if isinstance(exc, OptionChainDataIntegrityError):
                try:
                    _record_integrity_summary(storage, run_id, exc.summary)
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            _delete_prepublication_artifacts(storage, run_id, dataset_record)
            try:
                storage.fail_run(run_id, str(exc))
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        raise
    except KeyboardInterrupt:
        print("\nInterrupted.")
        if logger:
            logger.warning("run_finished interrupted=true")
        if storage is not None and run_id is not None:
            _delete_prepublication_artifacts(storage, run_id, dataset_record)
            storage.finalize_run(
                run_id, RunSummary(status="interrupted", error_summary="interrupted")
            )
        raise
    except Exception as exc:
        print(f"\nFatal error: {exc}")
        if logger:
            logger.exception("run_finished fatal error: %s", exc)
        if storage is not None and run_id is not None:
            _delete_prepublication_artifacts(storage, run_id, dataset_record)
            try:
                storage.fail_run(run_id, str(exc))
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        raise


def run_fetch(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches
    positions_path: Path | str | None = None,
    tickers: tuple[str, ...] | None = None,
    max_expiration_weeks: int | None = None,
    enable_filters: bool | None = None,
    stale_quote_seconds: int | None = None,
    data_provider: str | None = None,
    dry_run: bool = False,
    price_context_only: bool = False,
    skip_events: bool = False,
    progress_callback: TickerFetchProgressCallback | None = None,
) -> DatasetHandle | None:
    """Trigger a fresh option-chain fetch and write the result to storage.

    This is the programmatic entry point for downstream consumers (e.g.
    opx-strategy stage 3) that import opx_chain directly rather than
    invoking opx-fetch as a subprocess.

    positions_path: override the default positions.csv location.
    tickers: override the ticker list from config for this run only.
    max_expiration_weeks: override the expiration window from config for this run only.
    enable_filters: override generic post-download filtering for this run only.
    stale_quote_seconds: override the staleness threshold from config for this run only.
    data_provider: override settings.data_provider for this run only.
    dry_run: validate config, positions, and storage without API calls or writes.
    price_context_only: fetch/cache daily-OHLCV context without option-chain export.
    skip_events: skip provider corporate-event fetches during option-chain export.
    progress_callback: receive non-blocking structured per-ticker progress events.

    Raises RuntimeError if another fetch run is already active.
    Raises RuntimeError if the fetch produces no data.
    Propagates any provider-level exception on fatal failure.
    """
    config = get_runtime_config()
    dry_run = _coerce_run_fetch_bool(dry_run, field_name="run_fetch.dry_run")
    price_context_only = _coerce_run_fetch_bool(
        price_context_only,
        field_name="run_fetch.price_context_only",
    )
    skip_events = _coerce_run_fetch_bool(skip_events, field_name="run_fetch.skip_events")
    if progress_callback is not None and not callable(progress_callback):
        raise ConfigError("Config field 'run_fetch.progress_callback' must be callable.")
    if positions_path is not None:
        if isinstance(positions_path, str):
            positions_path = coerce_path(positions_path, field_name="run_fetch.positions_path")
        elif not isinstance(positions_path, Path):
            raise ConfigError("Config field 'run_fetch.positions_path' must be a path or string.")
    if tickers is not None:
        config = replace(config, tickers=_coerce_run_fetch_tickers(tickers))
    if max_expiration_weeks is not None:
        config = _with_max_expiration_weeks(config, max_expiration_weeks)
    if enable_filters is not None:
        config = replace(
            config,
            enable_filters=_coerce_run_fetch_bool(
                enable_filters,
                field_name="run_fetch.enable_filters",
            ),
        )
    if stale_quote_seconds is not None:
        config = replace(
            config,
            stale_quote_seconds=_coerce_run_fetch_non_negative_int(
                stale_quote_seconds,
                field_name="run_fetch.stale_quote_seconds",
            ),
        )
    if data_provider is not None:
        config = _with_data_provider(config, data_provider)
    if price_context_only:
        config = replace(config, price_context_enable=True)
    previous_override = get_runtime_config_override()
    if dry_run:
        try:
            set_runtime_config_override(config)
            with _SigtermAsKeyboardInterrupt():
                return _do_fetch_with_lock_held(
                    config,
                    positions_path,
                    cli_override=None,
                    dry_run=True,
                    price_context_only=price_context_only,
                    skip_events=skip_events,
                    progress_callback=progress_callback,
                )
        finally:
            set_runtime_config_override(previous_override)

    lock_path = _fetcher_lock_path(config)
    lock_handle = acquire_fetcher_lock(lock_path)
    if lock_handle is None:
        raise RuntimeError(f"Another fetcher run is already active: {lock_path}")
    try:
        set_runtime_config_override(config)
        with _SigtermAsKeyboardInterrupt():
            return _do_fetch_with_lock_held(
                config,
                positions_path,
                cli_override=None,
                dry_run=dry_run,
                price_context_only=price_context_only,
                skip_events=skip_events,
                progress_callback=progress_callback,
            )
    finally:
        set_runtime_config_override(previous_override)
        release_fetcher_lock(lock_handle, lock_path)


def _run_dry_run(
    config,
    positions_path: Path | None,
    cli_override,
    *,
    price_context_only: bool = False,
) -> int:
    """Run dry-run validation without acquiring the cross-process fetcher lock."""
    previous_override = get_runtime_config_override()
    try:
        set_runtime_config_override(config)
        with _SigtermAsKeyboardInterrupt():
            _do_fetch_with_lock_held(
                config,
                positions_path,
                cli_override=cli_override,
                dry_run=True,
                price_context_only=price_context_only,
            )
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:  # pylint: disable=broad-exception-caught
        return 1
    finally:
        set_runtime_config_override(previous_override)


def main(argv=None):
    """Fetch configured tickers and write the consolidated CSV output."""
    args = parse_args(argv)
    config, cli_override = apply_cli_overrides(get_runtime_config(), args)
    if args.dry_run:
        return _run_dry_run(
            config,
            args.positions,
            cli_override,
            price_context_only=args.price_context_only,
        )

    lock_path = _fetcher_lock_path(config)
    lock_handle = acquire_fetcher_lock(lock_path)
    if lock_handle is None:
        print(f"Another fetcher run is already active: {lock_path}")
        return 1
    previous_override = get_runtime_config_override()
    try:
        set_runtime_config_override(config)
        with _SigtermAsKeyboardInterrupt():
            _do_fetch_with_lock_held(
                config,
                args.positions,
                cli_override=cli_override,
                dry_run=args.dry_run,
                price_context_only=args.price_context_only,
            )
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception:  # pylint: disable=broad-exception-caught
        return 1
    finally:
        set_runtime_config_override(previous_override)
        release_fetcher_lock(lock_handle, lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
