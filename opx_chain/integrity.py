"""Public option-chain dataset integrity contracts and neutral projections."""
# pylint: disable=too-many-instance-attributes

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping
import re
import unicodedata

import pandas as pd

from opx_chain.option_types import OPTION_TYPE_CALL, OPTION_TYPE_PUT

if TYPE_CHECKING:
    from opx_chain.storage.models import DatasetHandle, DatasetRecord


OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION = 1
OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION = 2
OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION = 1
OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION = 1
CONTRACT_STRIKE_TOLERANCE = Decimal("0.0005")

_CONTRACT_STRIKE_QUANTUM = Decimal("0.001")
_CONTRACT_KEY_ROOT_RE = re.compile(r"^[A-Z0-9]+$")
_CONTROL_FREE_LIMIT = 128


class _StringEnum(str, Enum):
    """Enum whose JSON and display value is its stable string value."""

    def __str__(self) -> str:
        return self.value


class OptionChainIntegrityStatus(_StringEnum):
    """Effective or declared integrity metadata state."""

    VALID = "valid"
    INVALID = "invalid"
    UNKNOWN = "unknown"


class OptionChainDatasetFactsStatus(_StringEnum):
    """Effective or declared neutral dataset-facts state."""

    AVAILABLE = "available"
    UNKNOWN = "unknown"


class OptionChainRowScopeStatus(_StringEnum):
    """Effective or declared acquisition row-scope metadata state."""

    AVAILABLE = "available"
    UNKNOWN = "unknown"


class OptionChainRowScopeIntegrityError(ValueError):
    """Raised when current row-scope metadata contradicts its dataset/run."""


class OptionChainIntegritySeverity(_StringEnum):
    """Finding fatality."""

    FATAL = "fatal"
    WARNING = "warning"


class OptionChainIntegrityBoundary(_StringEnum):
    """Boundary that observed an integrity finding."""

    PROVIDER_RESPONSE = "provider_response"
    PRE_FILTER = "pre_filter"
    EXPORT = "export"
    SERIALIZED_ARTIFACT = "serialized_artifact"
    STORED_ARTIFACT = "stored_artifact"


class OptionChainIntegrityCode(_StringEnum):
    """Stable provider-neutral option-chain integrity finding codes."""

    RESPONSE_SHAPE_INVALID = "RESPONSE_SHAPE_INVALID"
    REQUIRED_IDENTITY_FIELD_MISSING = "REQUIRED_IDENTITY_FIELD_MISSING"
    CONTRACT_SYMBOL_INVALID = "CONTRACT_SYMBOL_INVALID"
    CONTRACT_IDENTITY_MISMATCH = "CONTRACT_IDENTITY_MISMATCH"
    UNSUPPORTED_CONTRACT_IDENTITY = "UNSUPPORTED_CONTRACT_IDENTITY"
    REQUIRED_FIELD_INVALID = "REQUIRED_FIELD_INVALID"
    FIELD_VALUE_INVALID = "FIELD_VALUE_INVALID"
    DUPLICATE_CONTRACT_SYMBOL = "DUPLICATE_CONTRACT_SYMBOL"
    DUPLICATE_CONTRACT_KEY = "DUPLICATE_CONTRACT_KEY"
    DATASET_SCHEMA_INVALID = "DATASET_SCHEMA_INVALID"
    SERIALIZATION_INTEGRITY_FAILED = "SERIALIZATION_INTEGRITY_FAILED"
    DATASET_CONTENT_HASH_MISMATCH = "DATASET_CONTENT_HASH_MISMATCH"
    OPTIONAL_FIELD_DEGRADED = "OPTIONAL_FIELD_DEGRADED"


def _require_positive_version(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _bounded_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = "".join(
        character
        for character in str(value)
        if not unicodedata.category(character).startswith("C")
    ).strip()
    if not text:
        return None
    return text[:_CONTROL_FREE_LIMIT]


def _require_aware_datetime(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _datetime_to_wire(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _require_aware_datetime(value, field="datetime").isoformat().replace("+00:00", "Z")


def _datetime_from_wire(value: object | None, *, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 string or null")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _require_aware_datetime(parsed, field=field)


@dataclass(frozen=True)
class OptionChainIntegrityFinding:
    """One bounded, immutable option-chain integrity observation."""

    severity: OptionChainIntegritySeverity
    boundary: OptionChainIntegrityBoundary
    code: OptionChainIntegrityCode
    row_index: int | None
    ticker: str | None
    contract_symbol: str | None
    field: str | None
    expected: str | None
    actual: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "severity", OptionChainIntegritySeverity(self.severity))
        object.__setattr__(self, "boundary", OptionChainIntegrityBoundary(self.boundary))
        object.__setattr__(self, "code", OptionChainIntegrityCode(self.code))
        if self.row_index is not None and (
            isinstance(self.row_index, bool)
            or not isinstance(self.row_index, int)
            or self.row_index < 0
        ):
            raise ValueError("row_index must be a nonnegative integer or null")
        for field in ("ticker", "contract_symbol", "field", "expected", "actual"):
            object.__setattr__(self, field, _bounded_text(getattr(self, field)))

    def to_dict(self) -> dict[str, object]:
        """Return the documented JSON-safe finding shape."""
        return {
            "severity": self.severity.value,
            "boundary": self.boundary.value,
            "code": self.code.value,
            "row_index": self.row_index,
            "ticker": self.ticker,
            "contract_symbol": self.contract_symbol,
            "field": self.field,
            "expected": self.expected,
            "actual": self.actual,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "OptionChainIntegrityFinding":
        """Parse one finding from its documented serialized shape."""
        return cls(  # pylint: disable=too-many-function-args
            severity=OptionChainIntegritySeverity(value["severity"]),
            boundary=OptionChainIntegrityBoundary(value["boundary"]),
            code=OptionChainIntegrityCode(value["code"]),
            row_index=value.get("row_index"),
            ticker=value.get("ticker"),
            contract_symbol=value.get("contract_symbol"),
            field=value.get("field"),
            expected=value.get("expected"),
            actual=value.get("actual"),
        )


@dataclass(frozen=True)
class OptionChainIntegritySummary:
    """Canonical aggregate and bounded sample projection for one validation."""

    schema_version: int
    validator_version: int
    status: str
    checked_at: datetime
    dataset_id: str | None
    content_hash: str | None
    provider: str | None
    total_rows: int
    invalid_row_count: int
    fatal_finding_count: int
    warning_finding_count: int
    affected_ticker_count: int
    counts_by_code: Mapping[OptionChainIntegrityCode, int]
    samples: tuple[OptionChainIntegrityFinding, ...]

    def __post_init__(self) -> None:
        _require_positive_version(self.schema_version, field="schema_version")
        _require_positive_version(self.validator_version, field="validator_version")
        if self.status not in {"valid", "invalid"}:
            raise ValueError("status must be valid or invalid")
        object.__setattr__(
            self,
            "checked_at",
            _require_aware_datetime(self.checked_at, field="checked_at"),
        )
        for field in ("total_rows", "invalid_row_count", "fatal_finding_count",
                      "warning_finding_count", "affected_ticker_count"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")
        if (self.status == "invalid") != (self.fatal_finding_count > 0):
            raise ValueError("status must be invalid exactly when fatal findings exist")
        frozen_counts: dict[OptionChainIntegrityCode, int] = {}
        for raw_code, raw_count in dict(self.counts_by_code).items():
            code = OptionChainIntegrityCode(raw_code)
            if isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count <= 0:
                raise ValueError("counts_by_code values must be positive integers")
            frozen_counts[code] = raw_count
        object.__setattr__(self, "counts_by_code", MappingProxyType(frozen_counts))
        samples = tuple(self.samples)
        if len(samples) > 5:
            raise ValueError("samples must contain at most five findings")
        per_code: dict[OptionChainIntegrityCode, int] = {}
        for sample in samples:
            if not isinstance(sample, OptionChainIntegrityFinding):
                raise ValueError("samples must contain OptionChainIntegrityFinding values")
            per_code[sample.code] = per_code.get(sample.code, 0) + 1
            if per_code[sample.code] > 2:
                raise ValueError("samples must contain at most two findings per code")
        object.__setattr__(self, "samples", samples)
        for field in ("dataset_id", "content_hash", "provider"):
            object.__setattr__(self, field, _bounded_text(getattr(self, field)))

    def to_dict(self) -> dict[str, object]:
        """Return the documented JSON-safe summary shape."""
        return {
            "schema_version": self.schema_version,
            "validator_version": self.validator_version,
            "status": self.status,
            "checked_at": _datetime_to_wire(self.checked_at),
            "dataset_id": self.dataset_id,
            "content_hash": self.content_hash,
            "provider": self.provider,
            "total_rows": self.total_rows,
            "invalid_row_count": self.invalid_row_count,
            "fatal_finding_count": self.fatal_finding_count,
            "warning_finding_count": self.warning_finding_count,
            "affected_ticker_count": self.affected_ticker_count,
            "counts_by_code": {
                code.value: count
                for code, count in sorted(
                    self.counts_by_code.items(), key=lambda item: item[0].value
                )
            },
            "samples": [sample.to_dict() for sample in self.samples],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "OptionChainIntegritySummary":
        """Parse and validate one serialized summary."""
        counts = value.get("counts_by_code")
        samples = value.get("samples")
        if not isinstance(counts, Mapping) or not isinstance(samples, list):
            raise ValueError("integrity summary counts and samples are malformed")
        return cls(
            schema_version=value["schema_version"],
            validator_version=value["validator_version"],
            status=value["status"],
            checked_at=_datetime_from_wire(value["checked_at"], field="checked_at"),
            dataset_id=value.get("dataset_id"),
            content_hash=value.get("content_hash"),
            provider=value.get("provider"),
            total_rows=value["total_rows"],
            invalid_row_count=value["invalid_row_count"],
            fatal_finding_count=value["fatal_finding_count"],
            warning_finding_count=value["warning_finding_count"],
            affected_ticker_count=value["affected_ticker_count"],
            counts_by_code={
                OptionChainIntegrityCode(code): count
                for code, count in counts.items()
            },
            samples=tuple(OptionChainIntegrityFinding.from_dict(item) for item in samples),
        )


@dataclass(frozen=True)
class OptionChainTickerTimeBounds:
    """Per-ticker neutral timestamp bounds from a validated dataset."""

    ticker: str
    minimum: datetime | None
    maximum: datetime | None

    def __post_init__(self) -> None:
        ticker = _bounded_text(self.ticker)
        if ticker is None:
            raise ValueError("ticker must be nonempty")
        object.__setattr__(self, "ticker", ticker.upper())
        for field in ("minimum", "maximum"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, _require_aware_datetime(value, field=field))
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("minimum cannot be later than maximum")

    def to_dict(self) -> dict[str, object]:
        """Return the documented JSON-safe ticker-bounds shape."""
        return {
            "ticker": self.ticker,
            "minimum": _datetime_to_wire(self.minimum),
            "maximum": _datetime_to_wire(self.maximum),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "OptionChainTickerTimeBounds":
        """Parse and validate serialized ticker timestamp bounds."""
        return cls(
            ticker=value["ticker"],
            minimum=_datetime_from_wire(value.get("minimum"), field="minimum"),
            maximum=_datetime_from_wire(value.get("maximum"), field="maximum"),
        )


@dataclass(frozen=True)
class OptionChainDatasetFacts:
    """Neutral content-bound metadata derived from a validated frame."""

    schema_version: int
    content_hash: str
    tickers: tuple[str, ...]
    underlying_price_time_bounds: tuple[OptionChainTickerTimeBounds, ...]
    expiration_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        _require_positive_version(self.schema_version, field="schema_version")
        content_hash = _bounded_text(self.content_hash)
        if content_hash is None:
            raise ValueError("content_hash must be nonempty")
        object.__setattr__(self, "content_hash", content_hash)
        tickers = tuple(sorted({_normalize_underlying(value) for value in self.tickers}))
        object.__setattr__(self, "tickers", tickers)
        bounds = tuple(self.underlying_price_time_bounds)
        if any(not isinstance(item, OptionChainTickerTimeBounds) for item in bounds):
            raise ValueError("underlying_price_time_bounds are malformed")
        if tuple(item.ticker for item in bounds) != tuple(sorted(item.ticker for item in bounds)):
            raise ValueError("underlying_price_time_bounds must be sorted by ticker")
        if len({item.ticker for item in bounds}) != len(bounds):
            raise ValueError("underlying_price_time_bounds tickers must be unique")
        object.__setattr__(self, "underlying_price_time_bounds", bounds)
        expirations = tuple(self.expiration_dates)
        if any(not isinstance(item, date) or isinstance(item, datetime) for item in expirations):
            raise ValueError("expiration_dates must contain dates")
        if expirations != tuple(sorted(set(expirations))):
            raise ValueError("expiration_dates must be sorted and unique")
        object.__setattr__(self, "expiration_dates", expirations)

    def to_dict(self) -> dict[str, object]:
        """Return the documented JSON-safe facts shape."""
        return {
            "schema_version": self.schema_version,
            "content_hash": self.content_hash,
            "tickers": list(self.tickers),
            "underlying_price_time_bounds": [
                item.to_dict() for item in self.underlying_price_time_bounds
            ],
            "expiration_dates": [item.isoformat() for item in self.expiration_dates],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "OptionChainDatasetFacts":
        """Parse and validate one serialized facts projection."""
        bounds = value.get("underlying_price_time_bounds")
        tickers = value.get("tickers")
        expirations = value.get("expiration_dates")
        if (
            not isinstance(bounds, list)
            or not isinstance(tickers, list)
            or not isinstance(expirations, list)
        ):
            raise ValueError("dataset facts collections are malformed")
        return cls(
            schema_version=value["schema_version"],
            content_hash=value["content_hash"],
            tickers=tuple(tickers),
            underlying_price_time_bounds=tuple(
                OptionChainTickerTimeBounds.from_dict(item) for item in bounds
            ),
            expiration_dates=tuple(date.fromisoformat(item) for item in expirations),
        )


@dataclass(frozen=True)
class OptionChainRowScope:
    """Provider-neutral acquisition/filter scope for one published dataset."""

    schema_version: int
    post_download_filters_enabled: bool
    max_expiration_weeks: int
    normalized_row_count: int
    kept_row_count: int
    filtered_row_count: int
    ticker_count: int

    def __post_init__(self) -> None:
        _require_positive_version(self.schema_version, field="schema_version")
        if self.schema_version != OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported option-chain row-scope schema version: "
                f"{self.schema_version}"
            )
        if not isinstance(self.post_download_filters_enabled, bool):
            raise ValueError("post_download_filters_enabled must be a boolean")
        if (
            isinstance(self.max_expiration_weeks, bool)
            or not isinstance(self.max_expiration_weeks, int)
            or self.max_expiration_weeks < 0
        ):
            raise ValueError("max_expiration_weeks must be a nonnegative integer")
        for field in (
            "normalized_row_count",
            "kept_row_count",
            "filtered_row_count",
            "ticker_count",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")
        if self.normalized_row_count != self.kept_row_count + self.filtered_row_count:
            raise ValueError(
                "normalized_row_count must equal kept_row_count + filtered_row_count"
            )
        if not self.post_download_filters_enabled and self.filtered_row_count != 0:
            raise ValueError(
                "filtered_row_count must be zero when post-download filters are disabled"
            )

    def to_dict(self) -> dict[str, object]:
        """Return the strict documented JSON-safe row-scope shape."""
        return {
            "row_scope_schema_version": self.schema_version,
            "post_download_filters_enabled": self.post_download_filters_enabled,
            "max_expiration_weeks": self.max_expiration_weeks,
            "normalized_row_count": self.normalized_row_count,
            "kept_row_count": self.kept_row_count,
            "filtered_row_count": self.filtered_row_count,
            "ticker_count": self.ticker_count,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "OptionChainRowScope":
        """Parse one strict serialized row-scope object."""
        expected = {
            "row_scope_schema_version",
            "post_download_filters_enabled",
            "max_expiration_weeks",
            "normalized_row_count",
            "kept_row_count",
            "filtered_row_count",
            "ticker_count",
        }
        if set(value) != expected:
            raise ValueError("option-chain row-scope fields are malformed")
        return cls(
            schema_version=value["row_scope_schema_version"],
            post_download_filters_enabled=value["post_download_filters_enabled"],
            max_expiration_weeks=value["max_expiration_weeks"],
            normalized_row_count=value["normalized_row_count"],
            kept_row_count=value["kept_row_count"],
            filtered_row_count=value["filtered_row_count"],
            ticker_count=value["ticker_count"],
        )


@dataclass(frozen=True)
class ValidatedOptionChainDataset:
    """One exact checked dataset snapshot returned by storage."""

    handle: "DatasetHandle"
    frame: pd.DataFrame
    integrity: OptionChainIntegritySummary
    dataset_facts: OptionChainDatasetFacts

    def __post_init__(self) -> None:
        if not isinstance(self.frame, pd.DataFrame):
            raise ValueError("frame must be a pandas DataFrame")
        object.__setattr__(self, "frame", self.frame.copy(deep=True))


class OptionChainDataIntegrityError(RuntimeError):
    """Fatal provider-neutral option-chain integrity failure."""

    def __init__(self, summary: OptionChainIntegritySummary) -> None:
        if not isinstance(summary, OptionChainIntegritySummary):
            raise TypeError("summary must be an OptionChainIntegritySummary")
        if summary.status != "invalid":
            raise ValueError("integrity errors require an invalid summary")
        self.summary = summary
        super().__init__(
            "option-chain integrity validation failed: "
            f"fatal_findings={summary.fatal_finding_count} "
            f"invalid_rows={summary.invalid_row_count}"
        )


class OptionChainSchemaCompatibilityError(RuntimeError):
    """Stored dataset declares a row schema unsupported by this installation."""

    def __init__(
        self,
        dataset_id: str,
        declared_version: int,
        supported_versions: tuple[int, ...],
    ) -> None:
        dataset_id = _bounded_text(dataset_id)
        if dataset_id is None:
            raise ValueError("dataset_id must be nonempty")
        _require_positive_version(declared_version, field="declared_version")
        versions = tuple(supported_versions)
        if not versions:
            raise ValueError("supported_versions must not be empty")
        for version in versions:
            _require_positive_version(version, field="supported_versions")
        self.dataset_id = dataset_id
        self.declared_version = declared_version
        self.supported_versions = versions
        super().__init__(
            f"option-chain dataset {dataset_id} declares unsupported schema "
            f"{declared_version}; supported={versions}"
        )


def _normalize_underlying(value: object) -> str:
    text = _bounded_text(value)
    if text is None:
        raise ValueError("underlying_symbol must be nonempty")
    normalized = text.upper().replace(".", "").replace("-", "")
    if not normalized or not _CONTRACT_KEY_ROOT_RE.fullmatch(normalized):
        raise ValueError("underlying_symbol is not canonical")
    return normalized


def _normalize_option_side(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"c", OPTION_TYPE_CALL}:
        return OPTION_TYPE_CALL
    if text in {"p", OPTION_TYPE_PUT}:
        return OPTION_TYPE_PUT
    raise ValueError("option_type must be call or put")


def _normalize_expiration(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError("expiration_date must be a date or ISO date string")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError("expiration_date must be an ISO date") from exc


def _normalize_strike(value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("strike must be a finite positive number")
    try:
        strike = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("strike must be a finite positive number") from exc
    if not strike.is_finite() or strike <= 0:
        raise ValueError("strike must be a finite positive number")
    quantized = strike.quantize(_CONTRACT_STRIKE_QUANTUM, rounding=ROUND_HALF_UP)
    if abs(strike - quantized) >= CONTRACT_STRIKE_TOLERANCE:
        raise ValueError("strike is not representable to the nearest thousandth")
    return quantized


def canonical_option_contract_key(
    underlying_symbol: object,
    option_type: object,
    strike: object,
    expiration_date: object,
) -> tuple[str, str, Decimal, date]:
    """Return the canonical semantic identity for one standard option contract."""
    return (
        _normalize_underlying(underlying_symbol),
        _normalize_option_side(option_type),
        _normalize_strike(strike),
        _normalize_expiration(expiration_date),
    )


def compute_option_chain_dataset_facts(
    frame: pd.DataFrame,
    *,
    content_hash: str,
) -> OptionChainDatasetFacts:
    """Derive neutral content-bound facts from a validated canonical frame."""
    tickers = tuple(
        sorted({_normalize_underlying(value) for value in frame["underlying_symbol"]})
    )
    bounds: list[OptionChainTickerTimeBounds] = []
    for ticker in tickers:
        mask = frame["underlying_symbol"].map(_normalize_underlying) == ticker
        if "underlying_price_time" not in frame.columns:
            minimum = maximum = None
        else:
            times = pd.to_datetime(
                frame.loc[mask, "underlying_price_time"],
                utc=True,
                errors="coerce",
            ).dropna()
            minimum = times.min().to_pydatetime() if not times.empty else None
            maximum = times.max().to_pydatetime() if not times.empty else None
        bounds.append(OptionChainTickerTimeBounds(ticker, minimum, maximum))
    expirations = tuple(
        sorted(
            {
                _normalize_expiration(value)
                for value in frame["expiration_date"]
            }
        )
    )
    return OptionChainDatasetFacts(
        schema_version=OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION,
        content_hash=content_hash,
        tickers=tickers,
        underlying_price_time_bounds=tuple(bounds),
        expiration_dates=expirations,
    )


def evaluate_option_chain_integrity_status(record: "DatasetRecord") -> OptionChainIntegrityStatus:
    """Return effective integrity state using the one shared metadata truth table."""
    summary = getattr(record, "integrity_summary", None)
    declared = getattr(record, "integrity_status", OptionChainIntegrityStatus.UNKNOWN)
    try:
        declared = OptionChainIntegrityStatus(declared)
    except ValueError:
        return OptionChainIntegrityStatus.UNKNOWN
    if not isinstance(summary, OptionChainIntegritySummary):
        return OptionChainIntegrityStatus.UNKNOWN
    current = (
        getattr(record, "integrity_schema_version", None)
        == OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION
        and getattr(record, "integrity_validator_version", None)
        == OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION
        and summary.schema_version == OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION
        and summary.validator_version == OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION
        and summary.dataset_id == getattr(record, "dataset_id", None)
        and summary.provider == getattr(record, "provider", None)
        and summary.content_hash == getattr(record, "integrity_content_hash", None)
        and getattr(record, "integrity_checked_at", None) == summary.checked_at
    )
    if not current:
        return OptionChainIntegrityStatus.UNKNOWN
    if declared is OptionChainIntegrityStatus.INVALID and summary.status == "invalid":
        return OptionChainIntegrityStatus.INVALID
    if (
        declared is OptionChainIntegrityStatus.VALID
        and summary.status == "valid"
        and summary.content_hash == getattr(record, "content_hash", None)
    ):
        return OptionChainIntegrityStatus.VALID
    return OptionChainIntegrityStatus.UNKNOWN


def evaluate_option_chain_dataset_facts_status(
    record: "DatasetRecord",
) -> OptionChainDatasetFactsStatus:
    """Return effective facts state independently from integrity state."""
    declared = getattr(record, "dataset_facts_status", OptionChainDatasetFactsStatus.UNKNOWN)
    facts = getattr(record, "dataset_facts", None)
    try:
        declared = OptionChainDatasetFactsStatus(declared)
    except ValueError:
        return OptionChainDatasetFactsStatus.UNKNOWN
    if (
        declared is OptionChainDatasetFactsStatus.AVAILABLE
        and isinstance(facts, OptionChainDatasetFacts)
        and facts.schema_version == OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION
        and facts.content_hash == getattr(record, "content_hash", None)
    ):
        return OptionChainDatasetFactsStatus.AVAILABLE
    return OptionChainDatasetFactsStatus.UNKNOWN


def evaluate_option_chain_row_scope_status(
    record: "DatasetRecord",
) -> OptionChainRowScopeStatus:
    """Return effective row-scope state independently from dataset facts."""
    declared = getattr(record, "row_scope_status", OptionChainRowScopeStatus.UNKNOWN)
    row_scope = getattr(record, "row_scope", None)
    try:
        declared = OptionChainRowScopeStatus(declared)
    except ValueError:
        return OptionChainRowScopeStatus.UNKNOWN
    if (
        declared is OptionChainRowScopeStatus.AVAILABLE
        and isinstance(row_scope, OptionChainRowScope)
        and row_scope.schema_version == OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION
        and row_scope.kept_row_count == getattr(record, "row_count", None)
    ):
        return OptionChainRowScopeStatus.AVAILABLE
    return OptionChainRowScopeStatus.UNKNOWN


__all__ = [
    "OPTION_CHAIN_DATASET_FACTS_SCHEMA_VERSION",
    "OPTION_CHAIN_INTEGRITY_SUMMARY_SCHEMA_VERSION",
    "OPTION_CHAIN_INTEGRITY_VALIDATOR_VERSION",
    "OPTION_CHAIN_ROW_SCOPE_SCHEMA_VERSION",
    "OptionChainDataIntegrityError",
    "OptionChainDatasetFacts",
    "OptionChainDatasetFactsStatus",
    "OptionChainIntegrityBoundary",
    "OptionChainIntegrityCode",
    "OptionChainIntegrityFinding",
    "OptionChainIntegritySeverity",
    "OptionChainIntegrityStatus",
    "OptionChainIntegritySummary",
    "OptionChainRowScope",
    "OptionChainRowScopeIntegrityError",
    "OptionChainRowScopeStatus",
    "OptionChainSchemaCompatibilityError",
    "OptionChainTickerTimeBounds",
    "ValidatedOptionChainDataset",
    "canonical_option_contract_key",
]
