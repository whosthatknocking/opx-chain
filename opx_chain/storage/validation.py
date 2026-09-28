"""Input-boundary helpers shared by storage backend implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Integral
from typing import Any

import pandas as pd

from opx_chain.integrity import (
    OptionChainDatasetFactsStatus,
    OptionChainIntegrityStatus,
    OptionChainRowScope,
)
from opx_chain.storage._disk import validate_path_component
from opx_chain.storage.models import (
    ArtifactWrite,
    DatasetWrite,
    RunContext,
    RunSummary,
    TickerFetchResult,
    ValidationRecord,
)
from opx_chain.tickers import is_valid_ticker


INVALID_TICKER_FILTER = ""
ARTIFACT_TYPES = frozenset({"debug_payload", "run_log", "sidecar"})
RUN_SUMMARY_STATUSES = frozenset({"complete", "failed", "interrupted"})
TICKER_FETCH_STATUSES = frozenset({"ok", "ok_with_warnings", "skipped", "error"})
VALIDATION_SEVERITIES = frozenset({"error", "warning", "info"})


@dataclass(frozen=True)
class DatasetListFilters:
    """Validated filters for listing retained datasets."""

    limit: int
    provider: str | None
    since: datetime | None
    until: datetime | None
    ticker: str | None
    integrity_status: OptionChainIntegrityStatus | None
    dataset_facts_status: OptionChainDatasetFactsStatus | None


def validate_list_limit(value: Any) -> int:
    """Return a stable nonnegative integer dataset-list limit."""
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError("list_datasets limit must be a nonnegative integer")
    limit = int(value)
    if limit < 0:
        raise ValueError("list_datasets limit must be a nonnegative integer")
    return limit


def validate_optional_text_filter(value: Any, *, name: str) -> str | None:
    """Validate an optional nonblank string filter."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value.strip()


def validate_required_text(value: Any, *, name: str) -> str:
    """Validate a required nonblank string boundary value."""
    text = validate_optional_text_filter(value, name=name)
    if text is None:
        raise ValueError(f"{name} must be a nonblank string")
    return text


def validate_optional_text(value: Any, *, name: str) -> str | None:
    """Validate optional text stored in durable metadata."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def validate_storage_id(value: Any, *, name: str) -> str:
    """Validate a storage identifier that may also be used as a path component."""
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return validate_path_component(value)


def validate_run_id(value: Any) -> str:
    """Validate a storage run id."""
    return validate_storage_id(value, name="run_id")


def validate_dataset_id(value: Any) -> str:
    """Validate a storage dataset id."""
    return validate_storage_id(value, name="dataset_id")


def validate_nonnegative_int(value: Any, *, name: str) -> int:
    """Validate durable nonnegative integer metadata."""
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a nonnegative integer")
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return normalized


def validate_positive_int(value: Any, *, name: str) -> int:
    """Validate durable positive integer metadata."""
    normalized = validate_nonnegative_int(value, name=name)
    if normalized <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return normalized


def validate_optional_datetime_filter(value: Any, *, name: str) -> datetime | None:
    """Validate and normalize an optional datetime filter."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, datetime):
        raise ValueError(f"{name} must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def validate_stale_run_inputs(
    cutoff: Any,
    error_summary: Any,
) -> tuple[datetime, str]:
    """Validate interrupt_stale_runs inputs."""
    cutoff = validate_optional_datetime_filter(cutoff, name="cutoff")
    if cutoff is None:
        raise ValueError("cutoff must be a datetime")
    return cutoff, validate_required_text(error_summary, name="error_summary")


def validate_ticker_filter(value: Any) -> str | None:
    """Validate a dataset ticker filter and return normalized text.

    String values that are not valid ticker symbols are a valid no-match filter:
    they should return no datasets instead of behaving like wildcards.
    """
    text = validate_optional_text_filter(value, name="ticker")
    if text is None:
        return None
    normalized = text.upper()
    if not is_valid_ticker(normalized):
        return INVALID_TICKER_FILTER
    return normalized


def validate_dataset_list_filters(  # pylint: disable=too-many-arguments
    *,
    limit: Any,
    provider: Any,
    since: Any,
    until: Any,
    ticker: Any,
    integrity_status: Any = None,
    dataset_facts_status: Any = None,
) -> DatasetListFilters:
    """Validate all list_datasets query inputs with one shared boundary path."""
    return DatasetListFilters(
        limit=validate_list_limit(limit),
        provider=validate_optional_text_filter(provider, name="provider"),
        since=validate_optional_datetime_filter(since, name="since"),
        until=validate_optional_datetime_filter(until, name="until"),
        ticker=validate_ticker_filter(ticker),
        integrity_status=(
            None
            if integrity_status is None
            else OptionChainIntegrityStatus(integrity_status)
        ),
        dataset_facts_status=(
            None
            if dataset_facts_status is None
            else OptionChainDatasetFactsStatus(dataset_facts_status)
        ),
    )


def validate_run_context(context: RunContext) -> RunContext:
    """Validate and normalize metadata used to create a storage run."""
    if not isinstance(context, RunContext):
        raise ValueError("context must be a RunContext")
    provider = validate_required_text(context.provider, name="provider")
    script_version = validate_required_text(context.script_version, name="script_version")
    if isinstance(context.tickers, str):
        raise ValueError("RunContext.tickers must be an iterable of ticker strings")
    try:
        ticker_values = tuple(context.tickers)
    except TypeError as exc:
        raise ValueError("RunContext.tickers must be an iterable of ticker strings") from exc
    tickers: list[str] = []
    for index, raw_ticker in enumerate(ticker_values):
        if not isinstance(raw_ticker, str):
            raise ValueError(f"RunContext.tickers[{index}] must be a ticker string")
        ticker = raw_ticker.strip().upper()
        if not is_valid_ticker(ticker):
            raise ValueError(f"RunContext.tickers[{index}] is not a valid ticker")
        tickers.append(ticker)
    return RunContext(
        provider=provider,
        tickers=tuple(tickers),
        config_fingerprint=validate_optional_text(
            context.config_fingerprint,
            name="config_fingerprint",
        ) or "",
        positions_fingerprint=validate_optional_text(
            context.positions_fingerprint,
            name="positions_fingerprint",
        ) or "",
        script_version=script_version,
    )


def sanitize_retained_run_tickers(value: Any) -> tuple[str, ...]:
    """Return valid retained run tickers, or an empty tuple for corrupt metadata."""
    if not isinstance(value, (list, tuple)):
        return ()
    tickers: list[str] = []
    for raw_ticker in value:
        if not isinstance(raw_ticker, str):
            return ()
        ticker = raw_ticker.strip().upper()
        if not is_valid_ticker(ticker):
            return ()
        tickers.append(ticker)
    return tuple(tickers)


def validate_ticker_fetch_result(result: TickerFetchResult) -> TickerFetchResult:
    """Validate per-ticker fetch result metadata before persistence."""
    if not isinstance(result, TickerFetchResult):
        raise ValueError("result must be a TickerFetchResult")
    ticker = result.ticker.strip().upper() if isinstance(result.ticker, str) else ""
    if not is_valid_ticker(ticker):
        raise ValueError("TickerFetchResult.ticker is not a valid ticker")
    status = validate_required_text(result.status, name="TickerFetchResult.status")
    if status not in TICKER_FETCH_STATUSES:
        raise ValueError(f"TickerFetchResult.status must be one of {sorted(TICKER_FETCH_STATUSES)}")
    raw_row_count = validate_nonnegative_int(
        result.raw_row_count,
        name="TickerFetchResult.raw_row_count",
    )
    normalized_row_count = validate_nonnegative_int(
        result.normalized_row_count,
        name="TickerFetchResult.normalized_row_count",
    )
    kept_row_count = validate_nonnegative_int(
        result.kept_row_count,
        name="TickerFetchResult.kept_row_count",
    )
    filtered_row_count = validate_nonnegative_int(
        result.filtered_row_count,
        name="TickerFetchResult.filtered_row_count",
    )
    if normalized_row_count > raw_row_count:
        raise ValueError(
            "TickerFetchResult.normalized_row_count must be <= raw_row_count"
        )
    if kept_row_count > normalized_row_count:
        raise ValueError(
            "TickerFetchResult.kept_row_count must be <= normalized_row_count"
        )
    if filtered_row_count != normalized_row_count - kept_row_count:
        raise ValueError(
            "TickerFetchResult.filtered_row_count must equal "
            "normalized_row_count - kept_row_count"
        )
    return TickerFetchResult(
        ticker=ticker,
        raw_row_count=raw_row_count,
        normalized_row_count=normalized_row_count,
        kept_row_count=kept_row_count,
        filtered_row_count=filtered_row_count,
        expiration_count=validate_nonnegative_int(
            result.expiration_count,
            name="TickerFetchResult.expiration_count",
        ),
        status=status,
        error_summary=validate_optional_text(
            result.error_summary,
            name="TickerFetchResult.error_summary",
        ),
    )


def validate_validation_record(record: ValidationRecord) -> ValidationRecord:
    """Validate retained validation summary metadata."""
    if not isinstance(record, ValidationRecord):
        raise ValueError("record must be a ValidationRecord")
    run_id = validate_run_id(record.run_id)
    severity = validate_required_text(record.severity, name="ValidationRecord.severity")
    if severity not in VALIDATION_SEVERITIES:
        raise ValueError(
            f"ValidationRecord.severity must be one of {sorted(VALIDATION_SEVERITIES)}"
        )
    code = validate_required_text(record.code, name="ValidationRecord.code")
    return ValidationRecord(
        run_id=run_id,
        severity=severity,
        code=code,
        count=validate_nonnegative_int(record.count, name="ValidationRecord.count"),
        sample=validate_optional_text(record.sample, name="ValidationRecord.sample"),
    )


def validate_dataset_write(dataset: DatasetWrite) -> DatasetWrite:
    """Validate canonical dataset write metadata before serialization."""
    if not isinstance(dataset, DatasetWrite):
        raise ValueError("dataset must be a DatasetWrite")
    if not isinstance(dataset.data, pd.DataFrame):
        raise ValueError("DatasetWrite.data must be a DataFrame")
    if dataset.row_scope is not None and not isinstance(dataset.row_scope, OptionChainRowScope):
        raise ValueError("DatasetWrite.row_scope must be an OptionChainRowScope or None")
    if dataset.row_scope is not None and dataset.row_scope.kept_row_count != len(dataset.data):
        raise ValueError("DatasetWrite.row_scope kept_row_count must equal data row count")
    return DatasetWrite(
        data=dataset.data,
        provider=validate_required_text(dataset.provider, name="DatasetWrite.provider"),
        schema_version=validate_positive_int(
            dataset.schema_version,
            name="DatasetWrite.schema_version",
        ),
        format=validate_required_text(dataset.format, name="DatasetWrite.format"),
        script_version=validate_required_text(
            dataset.script_version,
            name="DatasetWrite.script_version",
        ),
        row_scope=dataset.row_scope,
    )


def validate_artifact_write(artifact: ArtifactWrite) -> ArtifactWrite:
    """Validate artifact metadata before writing bytes."""
    if not isinstance(artifact, ArtifactWrite):
        raise ValueError("artifact must be an ArtifactWrite")
    artifact_type = validate_required_text(artifact.artifact_type, name="artifact_type")
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"artifact_type must be one of {sorted(ARTIFACT_TYPES)}")
    if not isinstance(artifact.content, bytes):
        raise ValueError("artifact.content must be bytes")
    filename = validate_path_component(
        validate_required_text(artifact.filename, name="filename")
    )
    return ArtifactWrite(
        artifact_type=artifact_type,
        content=artifact.content,
        filename=filename,
    )


def validate_run_summary(summary: RunSummary) -> RunSummary:
    """Validate lifecycle summary metadata before persistence."""
    if not isinstance(summary, RunSummary):
        raise ValueError("summary must be a RunSummary")
    status = validate_required_text(summary.status, name="RunSummary.status")
    if status not in RUN_SUMMARY_STATUSES:
        raise ValueError(f"RunSummary.status must be one of {sorted(RUN_SUMMARY_STATUSES)}")
    return RunSummary(
        status=status,
        error_summary=validate_optional_text(
            summary.error_summary,
            name="RunSummary.error_summary",
        ),
    )
