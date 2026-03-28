"""Local HTTP viewer for browsing exported options CSV snapshots."""

from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
import webbrowser
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import parse_qs, urlparse

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype
from opx.config import get_runtime_config
from opx.export import UNWANTED_EXPORT_COLUMNS


REPO_ROOT = Path(__file__).resolve().parent.parent
STATIC_ROOT = Path(__file__).resolve().parent / "viewer_static"
USER_GUIDE_PATH = REPO_ROOT / "docs" / "USER_GUIDE.md"
FIELD_REFERENCE_PATH = REPO_ROOT / "docs" / "FIELD_REFERENCE.md"
OUTPUTS_DIR = REPO_ROOT / "output"
CSV_PATTERN = "options_engine_output_*.csv"
HIDDEN_COLUMNS = {
    "roll_from_days_to_expiration",
    *UNWANTED_EXPORT_COLUMNS,
}
DATASET_CARD_COLUMNS = (
    "premium_reference_method",
    "risk_free_rate_used",
    "data_source",
)
INTEGER_VIEWER_COLUMNS = frozenset({"days_to_expiration"})
REFERENCE_MISSING_DESCRIPTION = "No reference description available for this field."


class FreshnessSummary(TypedDict):
    """File-level freshness statistics exposed to the browser."""

    file_age_seconds: float
    file_modified_at: str
    option_quote_age_median_seconds: float | None
    option_quote_age_max_seconds: float | None
    underlying_quote_age_median_seconds: float | None
    underlying_quote_age_max_seconds: float | None


class DatasetCard(TypedDict):
    """Single dataset-wide card shown above the viewer table."""

    name: str
    value: str
    description: str


class ColumnDefinition(TypedDict):
    """Column metadata used by the frontend table configuration."""

    name: str
    description: str
    is_numeric: bool


class OpportunitySummary(TypedDict):
    """Compact summary of a single highlighted contract opportunity."""

    contract_symbol: str | None
    option_type: str | None
    expiration_date: str | None
    strike: float | None
    premium_reference_price: float | None
    return_on_margin_annualized_pct: float | None
    probability_itm_pct: float | None
    delta_abs: float | None
    strike_distance_pct: float | None
    risk_level: str | None
    spread_score: float | None
    dte_score: float | None
    theta_efficiency: float | None
    quote_quality_score: float | None
    option_score: float | None
    final_score: float | None
    bid_ask_spread_pct_of_mid: float | None
    summary: str | None


class TickerSummary(TypedDict):
    """Per-ticker summary record shown in the Summary tab."""

    ticker: str
    row_count: int
    call_count: int
    put_count: int
    expiration_count: int
    underlying_price: float | None
    underlying_day_change_pct: float | None
    median_implied_volatility_pct: float | None
    historical_volatility_pct: float | None
    iv_hv_ratio: float | None
    latest_status: str
    market_context: str
    profitable_opportunity: OpportunitySummary | None
    moderate_risk_opportunity: OpportunitySummary | None
    high_conviction_call: OpportunitySummary | None
    high_conviction_put: OpportunitySummary | None


class CsvPayload(TypedDict):
    """Serialized table payload returned by the CSV data endpoint."""

    selected_file: str
    row_count: int
    columns: list[ColumnDefinition]
    rows: list[dict[str, Any]]
    freshness_summary: FreshnessSummary
    dataset_cards: list[DatasetCard]


class SummaryHighlights(TypedDict):
    """Top highlighted ticker summaries for the Summary tab header."""

    most_profitable: TickerSummary | None
    moderate_risk: TickerSummary | None


class SummaryPayload(TypedDict):
    """Serialized summary-tab payload for a selected CSV export."""

    selected_file: str
    tickers: list[TickerSummary]
    highlights: SummaryHighlights


def discover_csv_files() -> list[Path]:
    """Return exported CSV files ordered by most recently modified first."""
    return sorted(
        OUTPUTS_DIR.glob(CSV_PATTERN),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def resolve_csv_path(csv_name: str | None = None) -> Path:
    """Resolve the requested CSV filename or fall back to the newest export."""
    files = discover_csv_files()
    if not files:
        raise FileNotFoundError("No CSV files were found in the output directory.")

    if not csv_name:
        return files[0]

    candidate = OUTPUTS_DIR / csv_name
    if (
        candidate.exists()
        and candidate.is_file()
        and candidate.name.startswith("options_engine_output_")
    ):
        return candidate

    raise FileNotFoundError(f"CSV file not found: {csv_name}")


def load_user_guide_text() -> str:
    """Load the user guide for field descriptions and the reference tab."""
    return USER_GUIDE_PATH.read_text(encoding="utf-8")


def load_field_reference_markdown() -> str:
    """Load the dedicated field-reference document used by the viewer."""
    return FIELD_REFERENCE_PATH.read_text(encoding="utf-8")


def extract_field_descriptions() -> dict[str, str]:
    """Parse user-guide bullet entries into per-field viewer descriptions."""
    descriptions: dict[str, str] = {}
    pattern = re.compile(r"^- `([^`]+)`: (.+)$")
    for line in load_field_reference_markdown().splitlines():
        match = pattern.match(line.strip())
        if match:
            descriptions[match.group(1)] = match.group(2)
    return descriptions


def normalize_value(value: Any) -> Any:
    """Convert pandas and NumPy scalar values into JSON-serializable values."""
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


def normalize_row_value(column: str, value: Any) -> Any:
    """Normalize row values, preserving integer semantics for whole-day fields."""
    normalized = normalize_value(value)
    if column in INTEGER_VIEWER_COLUMNS and normalized is not None:
        return int(normalized)
    return normalized


def is_truthy(value: Any) -> bool:
    """Interpret common string and numeric truthy values from CSV content."""
    return str(value).strip().lower() in {"true", "1", "yes"}


def coerce_number(series: Any) -> pd.Series:
    """Coerce an arbitrary series-like input into numeric pandas values."""
    return pd.to_numeric(series, errors="coerce")


def coerce_scalar_number(value: Any) -> float | None:
    """Coerce a single scalar into a float while preserving missing values."""
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


def build_freshness_summary(frame: pd.DataFrame, csv_path: Path) -> FreshnessSummary:
    """Build file-level freshness metadata for the current CSV snapshot."""
    option_quote_ages = pd.to_numeric(
        frame.get("quote_age_seconds"), errors="coerce"
    ).dropna()
    underlying_quote_ages = pd.to_numeric(
        frame.get("underlying_price_age_seconds"), errors="coerce"
    ).dropna()
    now = time.time()
    modified_at = csv_path.stat().st_mtime

    summary: FreshnessSummary = {
        "file_age_seconds": max(0.0, now - modified_at),
        "file_modified_at": datetime.fromtimestamp(modified_at).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "option_quote_age_median_seconds": None,
        "option_quote_age_max_seconds": None,
        "underlying_quote_age_median_seconds": None,
        "underlying_quote_age_max_seconds": None,
    }

    if not option_quote_ages.empty:
        summary["option_quote_age_median_seconds"] = float(option_quote_ages.median())
        summary["option_quote_age_max_seconds"] = float(option_quote_ages.max())

    if not underlying_quote_ages.empty:
        summary["underlying_quote_age_median_seconds"] = float(underlying_quote_ages.median())
        summary["underlying_quote_age_max_seconds"] = float(underlying_quote_ages.max())

    return summary


def get_single_value(frame: pd.DataFrame, column: str) -> str | None:
    """Return a dataset-wide constant value when exactly one non-null value exists."""
    if column not in frame.columns:
        return None
    values = frame[column].dropna().astype(str).unique().tolist()
    return values[0] if len(values) == 1 else None


def build_dataset_cards(frame: pd.DataFrame, descriptions: dict[str, str]) -> list[DatasetCard]:
    """Build header cards for fields that have one dataset-wide value."""
    cards: list[DatasetCard] = []
    for column in DATASET_CARD_COLUMNS:
        value = get_single_value(frame, column)
        if value is None:
            continue
        cards.append(
            {
                "name": column,
                "value": value,
                "description": descriptions.get(column, REFERENCE_MISSING_DESCRIPTION),
            }
        )
    return cards


def format_percent(value: float | None) -> float | None:
    """Convert a ratio into a percentage rounded for frontend display."""
    return None if value is None else round(value * 100, 1)


def normalize_opportunity(row: dict[str, Any] | None) -> OpportunitySummary | None:
    """Convert a row dict into the compact opportunity-summary schema."""
    if row is None:
        return None
    return {
        "contract_symbol": row.get("contract_symbol"),
        "option_type": row.get("option_type"),
        "expiration_date": row.get("expiration_date"),
        "strike": coerce_scalar_number(row.get("strike")),
        "premium_reference_price": coerce_scalar_number(row.get("premium_reference_price")),
        "return_on_margin_annualized_pct": format_percent(
            coerce_scalar_number(row.get("return_on_margin_annualized"))
        ),
        "probability_itm_pct": format_percent(coerce_scalar_number(row.get("probability_itm"))),
        "delta_abs": coerce_scalar_number(row.get("delta_abs")),
        "strike_distance_pct": format_percent(coerce_scalar_number(row.get("strike_distance_pct"))),
        "risk_level": row.get("risk_level"),
        "spread_score": coerce_scalar_number(row.get("spread_score")),
        "dte_score": coerce_scalar_number(row.get("dte_score")),
        "theta_efficiency": coerce_scalar_number(row.get("theta_efficiency")),
        "quote_quality_score": coerce_scalar_number(row.get("quote_quality_score")),
        "option_score": coerce_scalar_number(row.get("option_score")),
        "final_score": coerce_scalar_number(row.get("final_score")),
        "bid_ask_spread_pct_of_mid": format_percent(
            coerce_scalar_number(row.get("bid_ask_spread_pct_of_mid"))
        ),
        "summary": row.get("_summary"),
    }


def attach_opportunity_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach a one-line summary string used in summary highlight cards."""
    frame = frame.copy()
    empty_metric = pd.Series(index=frame.index, dtype="float64")
    rom = (
        coerce_number(frame.get("return_on_margin_annualized", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    itm = (
        coerce_number(frame.get("probability_itm", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    spread = (
        coerce_number(frame.get("bid_ask_spread_pct_of_mid", empty_metric))
        .mul(100)
        .round(1)
        .astype("string")
        .fillna("—")
    )
    frame["_summary"] = "ROM " + rom + "% · ITM " + itm + "% · spread " + spread + "%"
    return frame


def screen_primary_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    """Prefer rows passing the primary screen when that flag exists."""
    candidates = frame.copy()
    if "passes_primary_screen" not in candidates.columns:
        return candidates
    return candidates[candidates["passes_primary_screen"].map(is_truthy)]


def pick_profitable_opportunity(frame: pd.DataFrame) -> OpportunitySummary | None:
    """Select the highest-ROM opportunity after primary-screen filtering."""
    if frame.empty:
        return None
    candidates = screen_primary_candidates(frame)
    candidates = attach_opportunity_summary(candidates)
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized"))
    candidates["_score"] = coerce_number(candidates.get("option_score")).fillna(0)
    candidates["_final_score"] = coerce_number(candidates.get("final_score", candidates["_score"]))
    candidates["_final_score"] = candidates["_final_score"].fillna(candidates["_score"])
    candidates["_quality"] = coerce_number(candidates.get("quote_quality_score")).fillna(0)
    candidates = candidates.sort_values(
        by=["_rom", "_final_score", "_quality"],
        ascending=[False, False, False],
        na_position="last",
    )
    return normalize_opportunity(candidates.iloc[0].to_dict()) if not candidates.empty else None


def pick_moderate_risk_opportunity(frame: pd.DataFrame) -> OpportunitySummary | None:
    """Select a lower-delta, primary-screen candidate when possible."""
    if frame.empty:
        return None
    config = get_runtime_config()
    candidates = screen_primary_candidates(frame)
    candidates["_delta"] = coerce_number(candidates.get("delta_abs"))
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized"))
    candidates["_score"] = coerce_number(candidates.get("option_score")).fillna(0)
    candidates["_final_score"] = coerce_number(candidates.get("final_score", candidates["_score"]))
    candidates["_final_score"] = candidates["_final_score"].fillna(candidates["_score"])
    candidates["_spread"] = coerce_number(candidates.get("bid_ask_spread_pct_of_mid"))
    moderate = candidates[
        (candidates["_delta"].notna()) & (candidates["_delta"] <= 0.40)
        & (candidates["_spread"].notna())
        & (candidates["_spread"] <= config.max_spread_pct_of_mid)
    ]
    if moderate.empty:
        moderate = candidates[(candidates["_delta"].notna()) & (candidates["_delta"] <= 0.45)]
    moderate = attach_opportunity_summary(moderate)
    moderate = moderate.sort_values(
        by=["_final_score", "_rom", "_delta"],
        ascending=[False, False, True],
        na_position="last",
    )
    return normalize_opportunity(moderate.iloc[0].to_dict()) if not moderate.empty else None


def _compute_direction_alignment(day_change_pct: Any, option_type: str) -> pd.Series:
    """Return signed alignment so opposite-direction momentum is penalized."""
    changes = coerce_number(day_change_pct).fillna(0.0)
    if option_type == "call":
        return changes
    return -changes


def pick_high_conviction_opportunity(
    frame: pd.DataFrame,
    option_type: str,
) -> OpportunitySummary | None:
    """Select the strongest directional idea for one option side."""
    if frame.empty:
        return None
    candidates = screen_primary_candidates(frame)
    if "option_type" not in candidates.columns:
        return None
    candidates = candidates[candidates["option_type"].astype(str) == option_type].copy()
    if candidates.empty:
        return None

    candidates = attach_opportunity_summary(candidates)
    candidates["_rom"] = coerce_number(candidates.get("return_on_margin_annualized")).fillna(0.0)
    candidates["_final_score"] = coerce_number(
        candidates.get("final_score", candidates.get("option_score"))
    ).fillna(0.0)
    candidates["_quality"] = coerce_number(candidates.get("quote_quality_score")).fillna(0.0)
    candidates["_spread_score"] = coerce_number(candidates.get("spread_score")).fillna(0.0)
    candidates["_strike_distance_pct"] = coerce_number(candidates.get("strike_distance_pct"))
    candidates["_delta_abs"] = coerce_number(candidates.get("delta_abs"))
    candidates["_direction_alignment"] = _compute_direction_alignment(
        candidates.get("underlying_day_change_pct"),
        option_type,
    )
    direction_alignment_weight = 300.0
    delta_target = 0.40 if option_type == "call" else 0.35
    candidates["_distance_penalty"] = candidates["_strike_distance_pct"].fillna(1.0)
    candidates["_delta_penalty"] = (candidates["_delta_abs"] - delta_target).abs().fillna(1.0)
    candidates["_conviction_score"] = (
        candidates["_final_score"]
        + (candidates["_quality"] * 2.0)
        + (candidates["_spread_score"] * 0.5)
        + (candidates["_direction_alignment"] * direction_alignment_weight)
        - (candidates["_distance_penalty"] * 100.0)
        - (candidates["_delta_penalty"] * 40.0)
        + (candidates["_rom"] * 5.0)
    )
    candidates = candidates.sort_values(
        by=[
            "_conviction_score",
            "_final_score",
            "_quality",
            "_spread_score",
            "_strike_distance_pct",
        ],
        ascending=[False, False, False, False, True],
        na_position="last",
    )
    return normalize_opportunity(candidates.iloc[0].to_dict()) if not candidates.empty else None


def build_market_context(
    ticker: str,
    underlying_price: float | None,
    day_change_pct: float | None,
) -> str:
    """Summarize the latest underlying snapshot in plain language."""
    if underlying_price is None and day_change_pct is None:
        return f"{ticker} has no recent underlying snapshot in this file."
    if day_change_pct is None:
        return f"{ticker} last underlying price was {underlying_price:.2f}."
    direction = "up" if day_change_pct >= 0 else "down"
    return (
        f"{ticker} last underlying price was {underlying_price:.2f}, "
        f"{direction} {abs(day_change_pct) * 100:.1f}% versus previous close."
    )


def build_latest_status(
    day_change_pct: float | None,
    median_iv_pct: float | None,
    historical_volatility_pct: float | None,
) -> str:
    """Build a short status label summarizing move and volatility context."""
    if (
        day_change_pct is None
        and median_iv_pct is None
        and historical_volatility_pct is None
    ):
        return "Snapshot unavailable"

    status_parts = []
    if day_change_pct is not None:
        move_pct = day_change_pct * 100
        if move_pct > 0.2:
            status_parts.append(f"Up {move_pct:.1f}%")
        elif move_pct < -0.2:
            status_parts.append(f"Down {abs(move_pct):.1f}%")
        else:
            status_parts.append("Flat")

    if (
        median_iv_pct is not None
        and historical_volatility_pct is not None
        and historical_volatility_pct > 0
    ):
        iv_hv_ratio = median_iv_pct / historical_volatility_pct
        if iv_hv_ratio >= 1.15:
            status_parts.append("IV rich")
        elif iv_hv_ratio <= 0.9:
            status_parts.append("IV soft")
        else:
            status_parts.append("IV balanced")
    elif median_iv_pct is not None:
        status_parts.append("IV available")

    return " · ".join(status_parts) if status_parts else "Snapshot available"


def build_ticker_summary(ticker: str, frame: pd.DataFrame) -> TickerSummary:
    """Aggregate one ticker's rows into the Summary tab record shape."""
    underlying_price = coerce_number(frame.get("underlying_price")).dropna()
    day_change = coerce_number(frame.get("underlying_day_change_pct")).dropna()
    implied_volatility = coerce_number(frame.get("implied_volatility")).dropna()
    hv = coerce_number(frame.get("historical_volatility")).dropna()
    profitable = pick_profitable_opportunity(frame)
    moderate = pick_moderate_risk_opportunity(frame)
    high_conviction_call = pick_high_conviction_opportunity(frame, "call")
    high_conviction_put = pick_high_conviction_opportunity(frame, "put")
    underlying_price_value = None if underlying_price.empty else float(underlying_price.iloc[0])
    day_change_value = None if day_change.empty else float(day_change.iloc[0])
    median_iv_value = (
        None if implied_volatility.empty else round(float(implied_volatility.median()) * 100, 1)
    )
    hv_value = None if hv.empty else round(float(hv.iloc[0]) * 100, 1)
    return {
        "ticker": ticker,
        "row_count": int(len(frame.index)),
        "call_count": int((frame.get("option_type") == "call").sum()),
        "put_count": int((frame.get("option_type") == "put").sum()),
        "expiration_count": int(frame.get("expiration_date").nunique()),
        "underlying_price": underlying_price_value,
        "underlying_day_change_pct": format_percent(day_change_value),
        "median_implied_volatility_pct": median_iv_value,
        "historical_volatility_pct": hv_value,
        "iv_hv_ratio": (
            None
            if median_iv_value is None or hv_value in (None, 0)
            else round(median_iv_value / hv_value, 2)
        ),
        "latest_status": build_latest_status(day_change_value, median_iv_value, hv_value),
        "market_context": build_market_context(ticker, underlying_price_value, day_change_value),
        "profitable_opportunity": profitable,
        "moderate_risk_opportunity": moderate,
        "high_conviction_call": high_conviction_call,
        "high_conviction_put": high_conviction_put,
    }


def sort_ticker_candidates(
    items: list[TickerSummary],
    opportunity_key: str,
) -> list[TickerSummary]:
    """Sort ticker summaries by the chosen opportunity ROM value descending."""
    return sorted(
        items,
        key=lambda item: (
            (item[opportunity_key] or {}).get("return_on_margin_annualized_pct")
            or -10**9
        ),
        reverse=True,
    )


def build_summary_payload(csv_name: str | None = None) -> SummaryPayload:
    """Build the compact per-ticker summary payload used by the Summary tab."""
    csv_path = resolve_csv_path(csv_name)
    frame = pd.read_csv(csv_path)
    visible_columns = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
    frame = frame[visible_columns]
    tickers = sorted(frame["underlying_symbol"].dropna().astype(str).unique())

    ticker_summaries: list[TickerSummary] = []
    for ticker in tickers:
        ticker_frame = frame[frame["underlying_symbol"].astype(str) == ticker].copy()
        ticker_summaries.append(build_ticker_summary(ticker, ticker_frame))

    profitable_candidates = [
        item for item in ticker_summaries if item["profitable_opportunity"]
    ]
    moderate_candidates = [
        item for item in ticker_summaries if item["moderate_risk_opportunity"]
    ]
    profitable_candidates = sort_ticker_candidates(
        profitable_candidates, "profitable_opportunity"
    )
    moderate_candidates = sort_ticker_candidates(
        moderate_candidates, "moderate_risk_opportunity"
    )
    return {
        "selected_file": csv_path.name,
        "tickers": ticker_summaries,
        "highlights": {
            "most_profitable": profitable_candidates[0] if profitable_candidates else None,
            "moderate_risk": moderate_candidates[0] if moderate_candidates else None,
        },
    }


def build_column_definitions(
    frame: pd.DataFrame,
    descriptions: dict[str, str],
) -> list[ColumnDefinition]:
    """Build frontend column metadata including descriptions and numeric flags."""
    return [
        {
            "name": column,
            "description": descriptions.get(column, REFERENCE_MISSING_DESCRIPTION),
            "is_numeric": bool(
                is_numeric_dtype(frame[column]) and not is_bool_dtype(frame[column])
            ),
        }
        for column in frame.columns
    ]


def load_csv_payload(csv_name: str | None = None) -> CsvPayload:
    """Load the current CSV and serialize the table payload consumed by the browser."""
    csv_path = resolve_csv_path(csv_name)
    frame = pd.read_csv(csv_path)
    freshness_summary = build_freshness_summary(frame, csv_path)
    descriptions = extract_field_descriptions()
    dataset_cards = build_dataset_cards(frame, descriptions)
    visible_columns = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
    frame = frame[visible_columns]
    rows = [
        {column: normalize_row_value(column, value) for column, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    columns = build_column_definitions(frame, descriptions)
    return {
        "selected_file": csv_path.name,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
        "freshness_summary": freshness_summary,
        "dataset_cards": dataset_cards,
    }


def make_file_listing() -> list[dict[str, Any]]:
    """Return available export files with size and modified timestamps."""
    files = discover_csv_files()
    return [
        {
            "name": path.name,
            "size_bytes": path.stat().st_size,
            "modified_at": path.stat().st_mtime,
        }
        for path in files
    ]


class ViewerRequestHandler(SimpleHTTPRequestHandler):
    """Static-file and JSON API handler for the local CSV viewer."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def end_headers(self):
        """Disable caching so the viewer always serves fresh local data."""
        self.send_header(
            "Cache-Control",
            "no-store, no-cache, must-revalidate, max-age=0",
        )
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _respond_payload(self, payload_factory, csv_name: str | None = None) -> None:
        """Run a payload factory and translate missing files into 404 responses."""
        try:
            payload = payload_factory(csv_name)
        except FileNotFoundError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        self.respond_json(payload)

    def do_GET(self) -> None:
        """Serve viewer JSON endpoints or fall back to static assets."""
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            self.respond_json({"files": make_file_listing()})
            return
        if parsed.path == "/api/data":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            self._respond_payload(load_csv_payload, csv_name)
            return
        if parsed.path in {"/api/readme", "/api/reference"}:
            self.respond_json({"markdown": load_field_reference_markdown()})
            return
        if parsed.path == "/api/summary":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            self._respond_payload(build_summary_payload, csv_name)
            return
        if parsed.path == "/":
            self.path = "/index.html"  # pylint: disable=attribute-defined-outside-init
        super().do_GET()

    def respond_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        """Serialize and send a JSON response for one of the API endpoints."""
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
        """Optionally suppress request logs when running screenshot automation."""
        if os.environ.get("OPX_VIEWER_QUIET") == "1":
            return
        super().log_message(format, *args)


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the local viewer HTTP server."""
    server = ThreadingHTTPServer((host, port), ViewerRequestHandler)
    print(f"Options Screener running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def parse_args(argv=None):
    """Parse viewer CLI arguments."""
    if argv is None and "PYTEST_CURRENT_TEST" in os.environ:
        argv = []
    parser = argparse.ArgumentParser(
        prog="opx-viewer",
        description="Serve the local Options Screener UI.",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the viewer URL in the default browser after startup.",
    )
    return parser.parse_args(argv)


def open_viewer_in_browser(host: str, port: int) -> None:
    """Open the viewer URL in the default browser."""
    webbrowser.open(f"http://{host}:{port}", new=2)


def main(argv=None) -> None:
    """Start the local viewer using runtime config with optional env overrides."""
    args = parse_args(argv)
    config = get_runtime_config()
    host = os.environ.get("OPX_VIEWER_HOST", config.viewer_host)
    port = int(os.environ.get("OPX_VIEWER_PORT", str(config.viewer_port)))
    if args.open:
        threading.Timer(0.2, open_viewer_in_browser, args=(host, port)).start()
    serve(host=host, port=port)


if __name__ == "__main__":
    main()
