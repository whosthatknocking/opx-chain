"""Explicit read scope for portable retained history snapshots."""

from dataclasses import dataclass
from datetime import date, datetime, timezone
import re

from opx_chain.tickers import is_valid_ticker

MAX_SNAPSHOT_ROWS = 50_000


@dataclass(frozen=True)
class HistorySnapshotWindow:
    """One provider, explicit tickers and both market/availability cutoffs."""

    provider: str
    tickers: tuple[str, ...]
    start: date
    end: date
    observed_at: datetime

    def __post_init__(self) -> None:
        provider_valid = isinstance(self.provider, str) and re.fullmatch(
            r"[a-z0-9_]+", self.provider
        )
        tickers_valid = (
            isinstance(self.tickers, tuple)
            and 1 <= len(self.tickers) <= 256
            and not any(
                not isinstance(ticker, str)
                or ticker != ticker.upper()
                or not is_valid_ticker(ticker)
                for ticker in self.tickers
            )
            and len(set(self.tickers)) == len(self.tickers)
        )
        dates = (self.start, self.end)
        days_valid = all(  # pylint: disable=unidiomatic-typecheck
            type(day) is date for day in dates  # pylint: disable=unidiomatic-typecheck
        )
        observed = self.observed_at
        clock_valid = isinstance(observed, datetime) and observed.utcoffset() is not None
        if not (provider_valid and tickers_valid and days_valid and clock_valid):
            raise ValueError("invalid retained history window")
        if not self.start <= self.end <= observed.astimezone(timezone.utc).date():
            raise ValueError("invalid retained history window")

    def arguments(self) -> tuple[str, ...]:
        """Ordered parameters for the store-owned query."""
        return (
            self.provider,
            *self.tickers,
            self.start.isoformat(),
            self.end.isoformat(),
            self.observed_at.astimezone(timezone.utc).isoformat(),
        )

    def retain(self, rows: list) -> list[dict]:
        """Finish exact cutoff filtering; SQLite comparisons have millisecond resolution."""
        if len(rows) > MAX_SNAPSHOT_ROWS:
            raise ValueError("retained history exceeds its row budget")
        return [
            dict(row)
            for row in rows
            if datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00")) <= self.observed_at
        ]
