"""Load and parse portfolio positions for filter bypass and ticker inclusion."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

DEFAULT_POSITIONS_PATH = Path("filter/positions.csv")

_OPTION_RE = re.compile(r"^-?([A-Z.]+)(\d{2})(\d{2})(\d{2})([CP])(\d+\.?\d*)$")
_VALID_TICKER_RE = re.compile(r"^[A-Z.]{1,10}$")
_SKIP_SYMBOLS = {"SPAXX**"}
_SKIP_PREFIXES = ("Pending",)


@dataclass(frozen=True)
class OptionPositionKey:
    """Identifies a specific option contract held in the portfolio."""

    ticker: str
    expiration_date: str  # ISO format: YYYY-MM-DD
    option_type: str      # "call" or "put"
    strike: float


@dataclass(frozen=True)
class PositionSet:
    """Parsed portfolio positions used to guide fetch and filter behavior."""

    stock_tickers: frozenset[str]
    option_keys: frozenset[OptionPositionKey]

    @property
    def empty(self) -> bool:
        return not self.stock_tickers and not self.option_keys


EMPTY_POSITION_SET = PositionSet(frozenset(), frozenset())


def _parse_option_symbol(raw: str) -> OptionPositionKey | None:
    """Parse a Fidelity-style option symbol into a structured key, or return None."""
    clean = raw.strip().replace(" ", "")
    m = _OPTION_RE.match(clean)
    if not m:
        return None
    ticker, yy, mm, dd, cp, strike_str = m.groups()
    return OptionPositionKey(
        ticker=ticker,
        expiration_date=f"20{yy}-{mm}-{dd}",
        option_type="call" if cp == "C" else "put",
        strike=float(strike_str),
    )


def load_positions(path: Path | None = None) -> PositionSet:
    """Load positions.csv and return parsed stock tickers and option keys.

    Returns an empty PositionSet when the file does not exist or cannot be parsed.
    """
    resolved = (path or DEFAULT_POSITIONS_PATH).expanduser()
    if not resolved.exists():
        return EMPTY_POSITION_SET

    stock_tickers: set[str] = set()
    option_keys: set[OptionPositionKey] = set()

    try:
        with resolved.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or "Symbol" not in reader.fieldnames:
                return EMPTY_POSITION_SET
            for row in reader:
                symbol = (row.get("Symbol") or "").strip()
                if not symbol or symbol in _SKIP_SYMBOLS:
                    continue
                if any(symbol.startswith(p) for p in _SKIP_PREFIXES):
                    continue
                if symbol.startswith("-") or " -" in symbol:
                    key = _parse_option_symbol(symbol)
                    if key:
                        option_keys.add(key)
                elif _VALID_TICKER_RE.match(symbol):
                    stock_tickers.add(symbol)
    except Exception:  # pylint: disable=broad-except
        return EMPTY_POSITION_SET

    return PositionSet(frozenset(stock_tickers), frozenset(option_keys))
