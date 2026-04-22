"""CLI tool to verify positions coverage and summarize saved-chain freshness."""
from pathlib import Path
from numbers import Real

import pandas as pd

from opx_chain.config import get_runtime_config
from opx_chain.positions import DEFAULT_POSITIONS_PATH, STRIKE_MATCH_TOLERANCE, load_positions
from opx_chain.storage.factory import get_storage_backend
from opx_chain.utils import read_dataset_file

OUTPUTS_DIR = Path("output")


def find_latest_output(outputs_dir: Path = OUTPUTS_DIR) -> Path | None:
    """Return the most recently modified CSV in the outputs directory."""
    csvs = sorted(outputs_dir.glob("options_engine_output_*.csv"), key=lambda p: p.stat().st_mtime)
    return csvs[-1] if csvs else None


def _utc_now() -> pd.Timestamp:
    """Return the current UTC timestamp."""
    return pd.Timestamp.now(tz="UTC")


def check_positions(positions_path: Path | None = None, output_path: Path | None = None):
    """Check every option position against the given (or latest) output CSV.

    Returns a tuple of (found, missing) lists where each element is an OptionPositionKey.
    """
    resolved_positions = (positions_path or DEFAULT_POSITIONS_PATH).expanduser()
    position_set = load_positions(resolved_positions)

    if position_set.empty:
        return [], []

    resolved_output = output_path or find_latest_output()
    if resolved_output is None or not resolved_output.exists():
        return [], list(position_set.option_keys)

    df = read_dataset_file(resolved_output)

    found, missing = [], []
    for key in sorted(
        position_set.option_keys,
        key=lambda k: (k.ticker, k.expiration_date, k.option_type),
    ):
        mask = (
            (df["underlying_symbol"] == key.ticker)
            & (df["expiration_date"] == key.expiration_date)
            & (df["option_type"] == key.option_type)
            & ((df["strike"] - key.strike).abs() < STRIKE_MATCH_TOLERANCE)
        )
        if df[mask].empty:
            missing.append(key)
        else:
            found.append((key, df[mask].iloc[0]))
    return found, missing


def _is_true_like(value) -> bool:
    """Interpret common boolean-like CSV values."""
    return str(value).strip().lower() in {"true", "1", "yes"}


def _format_filter_value(value) -> str:
    """Format row and threshold values for concise CLI output."""
    if value is None or pd.isna(value):
        return "missing"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Real):
        return f"{float(value):.4f}"
    return str(value)


def _format_quote_value(value) -> str:
    """Format bid/ask values consistently for CLI output."""
    if value is None or pd.isna(value):
        return "—"
    if isinstance(value, Real):
        return f"{float(value):.2f}"
    return str(value)


def _format_duration(seconds) -> str:
    """Render a duration in a terminal-friendly compact form."""
    if seconds is None or pd.isna(seconds):
        return "—"
    total_seconds = max(0, int(round(float(seconds))))
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days:
        return f"{days}d {hours:02}h {minutes:02}m"
    if hours:
        return f"{hours}h {minutes:02}m {secs:02}s"
    if minutes:
        return f"{minutes}m {secs:02}s"
    return f"{secs}s"


def _format_iso_timestamp(value) -> str:
    """Render timestamps consistently in UTC with a trailing Z."""
    if value is None or pd.isna(value):
        return "—"
    return pd.Timestamp(value).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_filter_failure(
    failures: list[str],
    *,
    filter_name: str,
    row_value,
    threshold_value,
    operator: str,
) -> None:
    """Append one failed filter note using canonical config naming."""
    failures.append(
        f"{filter_name}({_format_filter_value(row_value)}"
        f"{operator}{_format_filter_value(threshold_value)})"
    )


def _get_failed_primary_screen_filters(row: pd.Series) -> list[str]:
    """Return the configured primary-screen filters that the row fails."""
    config = get_runtime_config()
    failures: list[str] = []

    if config.min_bid is not None:
        bid = pd.to_numeric(row.get("bid"), errors="coerce")
        if pd.isna(bid) or bid < config.min_bid:
            _append_filter_failure(
                failures,
                filter_name="filters_min_bid",
                row_value=bid,
                threshold_value=config.min_bid,
                operator="<",
            )

    spread_pct = pd.to_numeric(row.get("bid_ask_spread_pct_of_mid"), errors="coerce")
    if pd.isna(spread_pct) or spread_pct > config.max_spread_pct_of_mid:
        _append_filter_failure(
            failures,
            filter_name="filters_max_spread_pct_of_mid",
            row_value=spread_pct,
            threshold_value=config.max_spread_pct_of_mid,
            operator=">",
        )

    open_interest = pd.to_numeric(row.get("open_interest"), errors="coerce")
    if pd.isna(open_interest) or open_interest < config.min_open_interest:
        _append_filter_failure(
            failures,
            filter_name="filters_min_open_interest",
            row_value=open_interest,
            threshold_value=config.min_open_interest,
            operator="<",
        )

    volume = pd.to_numeric(row.get("volume"), errors="coerce")
    if pd.isna(volume) or volume < config.min_volume:
        _append_filter_failure(
            failures,
            filter_name="filters_min_volume",
            row_value=volume,
            threshold_value=config.min_volume,
            operator="<",
        )

    return failures


def _summarize_quote_freshness(
    frame: pd.DataFrame,
    *,
    timestamp_column: str,
    stored_stale_column: str,
    stale_seconds: int,
    now: pd.Timestamp,
) -> dict[str, object]:
    """Summarize current freshness from saved timestamps and stored fetch-time flags."""
    timestamps = pd.to_datetime(frame.get(timestamp_column), errors="coerce", utc=True)
    if timestamps is None:
        timestamps = pd.Series(dtype="datetime64[ns, UTC]")
    valid_mask = timestamps.notna()
    rows_with_timestamp = int(valid_mask.sum())
    if rows_with_timestamp == 0:
        return {
            "rows_with_timestamp": 0,
            "stale_now_rows": 0,
            "stale_at_fetch_rows": 0,
            "newest_timestamp": None,
            "oldest_timestamp": None,
            "newest_age_seconds": None,
            "oldest_age_seconds": None,
        }

    valid_timestamps = timestamps[valid_mask]
    age_seconds = (now - valid_timestamps).dt.total_seconds()
    stored_flags = frame.get(stored_stale_column)
    stale_at_fetch_rows = 0
    if stored_flags is not None:
        stale_at_fetch_rows = int(sum(_is_true_like(value) for value in stored_flags[valid_mask]))
    return {
        "rows_with_timestamp": rows_with_timestamp,
        "stale_now_rows": int((age_seconds > stale_seconds).sum()),
        "stale_at_fetch_rows": stale_at_fetch_rows,
        "newest_timestamp": valid_timestamps.max(),
        "oldest_timestamp": valid_timestamps.min(),
        "newest_age_seconds": age_seconds.min(),
        "oldest_age_seconds": age_seconds.max(),
    }


def _summarize_underlying_freshness_now(
    frame: pd.DataFrame,
    *,
    stale_seconds: int,
    now: pd.Timestamp,
) -> list[dict[str, object]]:
    """Return per-underlying current freshness for stale saved stock snapshots."""
    if "underlying_symbol" not in frame or "underlying_price_time" not in frame:
        return []

    freshness = frame.loc[:, ["underlying_symbol", "underlying_price_time"]].copy()
    freshness["underlying_price_time"] = pd.to_datetime(
        freshness["underlying_price_time"], errors="coerce", utc=True
    )
    freshness = freshness.dropna(subset=["underlying_symbol", "underlying_price_time"])
    if freshness.empty:
        return []

    rows: list[dict[str, object]] = []
    for symbol, group in freshness.groupby("underlying_symbol", sort=True):
        newest_timestamp = group["underlying_price_time"].max()
        oldest_timestamp = group["underlying_price_time"].min()
        newest_age_seconds = (now - newest_timestamp).total_seconds()
        if newest_age_seconds <= stale_seconds:
            continue
        rows.append({
            "symbol": str(symbol),
            "row_count": int(len(group)),
            "distinct_timestamps": int(group["underlying_price_time"].nunique()),
            "newest_timestamp": newest_timestamp,
            "oldest_timestamp": oldest_timestamp,
            "newest_age_seconds": newest_age_seconds,
            "oldest_age_seconds": (now - oldest_timestamp).total_seconds(),
        })
    rows.sort(key=lambda row: (float(row["newest_age_seconds"]), row["symbol"]), reverse=True)
    return rows


def format_freshness_summary_lines(
    output_path: Path,
    *,
    frame: pd.DataFrame | None = None,
    now: pd.Timestamp | None = None,
) -> list[str]:
    """Build a read-time freshness summary for the selected output CSV."""
    resolved_frame = frame if frame is not None else pd.read_csv(output_path, low_memory=False)
    runtime_now = now or _utc_now()
    config = get_runtime_config()
    file_age_seconds = max(0.0, runtime_now.timestamp() - output_path.stat().st_mtime)
    option_summary = _summarize_quote_freshness(
        resolved_frame,
        timestamp_column="option_quote_time",
        stored_stale_column="is_stale_quote",
        stale_seconds=config.stale_quote_seconds,
        now=runtime_now,
    )
    underlying_summary = _summarize_quote_freshness(
        resolved_frame,
        timestamp_column="underlying_price_time",
        stored_stale_column="is_stale_underlying_price",
        stale_seconds=config.stale_quote_seconds,
        now=runtime_now,
    )
    stale_underlyings = _summarize_underlying_freshness_now(
        resolved_frame,
        stale_seconds=config.stale_quote_seconds,
        now=runtime_now,
    )

    lines = [
        "Freshness now:",
        (
            f"  file_age_now={_format_duration(file_age_seconds)}  "
            f"stale_quote_seconds={config.stale_quote_seconds}"
        ),
        (
            "  option_quotes_now: "
            f"rows_with_timestamp={option_summary['rows_with_timestamp']}  "
            f"stale_now_rows={option_summary['stale_now_rows']}  "
            f"stale_at_fetch_rows={option_summary['stale_at_fetch_rows']}  "
            f"newest_age={_format_duration(option_summary['newest_age_seconds'])}  "
            f"oldest_age={_format_duration(option_summary['oldest_age_seconds'])}"
        ),
        (
            "  underlying_quotes_now: "
            f"rows_with_timestamp={underlying_summary['rows_with_timestamp']}  "
            f"stale_now_rows={underlying_summary['stale_now_rows']}  "
            f"stale_at_fetch_rows={underlying_summary['stale_at_fetch_rows']}  "
            f"newest_age={_format_duration(underlying_summary['newest_age_seconds'])}  "
            f"oldest_age={_format_duration(underlying_summary['oldest_age_seconds'])}"
        ),
    ]
    if not stale_underlyings:
        lines.append("  stale_underlyings_now: none")
        return lines

    lines.append("  stale_underlyings_now:")
    for item in stale_underlyings:
        time_range = _format_iso_timestamp(item["newest_timestamp"])
        if item["distinct_timestamps"] > 1:
            time_range = (
                f"{_format_iso_timestamp(item['oldest_timestamp'])}.."
                f"{_format_iso_timestamp(item['newest_timestamp'])}"
            )
        lines.append(
            f"    - {item['symbol']:<6} rows={item['row_count']}  "
            f"distinct_times={item['distinct_timestamps']}  "
            f"time={time_range}  "
            f"newest_age={_format_duration(item['newest_age_seconds'])}"
        )
    return lines


def _format_found_position_lines(key, row: pd.Series) -> list[str]:
    """Build the CLI output lines for a found portfolio position."""
    passes = row.get("passes_primary_screen")
    screen_status = f"passes_primary_screen={'true' if _is_true_like(passes) else 'false'}"
    failed_filters = (
        _get_failed_primary_screen_filters(row) if not _is_true_like(passes) else []
    )
    lines = [(
        f"  FOUND    {key.ticker:<6} {key.expiration_date}  {key.option_type:<4}  "
        f"strike={key.strike:>7.1f}  bid={_format_quote_value(row['bid']):>6}  "
        f"ask={_format_quote_value(row['ask']):>6}  {screen_status}"
    )]
    if failed_filters:
        lines.append("           failed_filters:")
        lines.extend(
            f"             - {failure}"
            for failure in failed_filters
        )
    return lines


def main(argv=None):
    """Print a position coverage report for the latest output CSV."""
    import argparse  # pylint: disable=import-outside-toplevel

    parser = argparse.ArgumentParser(
        prog="opx-check",
        description=(
            "Check that every option position in the portfolio positions CSV "
            "appears in the latest output."
        ),
    )
    parser.add_argument("--positions", type=Path, default=None, help="Path to positions CSV.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to output CSV (default: latest).",
    )
    parser.add_argument(
        "--freshness",
        action="store_true",
        help=(
            "Recompute current quote freshness from saved timestamps and print a "
            "terminal-friendly summary."
        ),
    )
    args = parser.parse_args(argv)

    positions_path = (args.positions or DEFAULT_POSITIONS_PATH).expanduser()
    output_path = args.output

    if not positions_path.exists():
        print(f"Positions file not found: {positions_path}")
        return 1

    storage = get_storage_backend()
    if output_path is not None:
        resolved_output = output_path
    elif storage is not None:
        records = storage.list_datasets(limit=1)
        resolved_output = Path(records[0].location) if records else None
    else:
        resolved_output = find_latest_output()
    if resolved_output is None:
        print(f"No output CSV found in {OUTPUTS_DIR}/")
        return 1

    print(f"Positions: {positions_path}")
    print(f"Output: {resolved_output}")
    print()

    if args.freshness:
        for line in format_freshness_summary_lines(resolved_output):
            print(line)
        print()

    found, missing = check_positions(positions_path, resolved_output)
    total = len(found) + len(missing)

    if total == 0:
        print("No option positions found in positions file.")
        return 0

    for key, row in found:
        for line in _format_found_position_lines(key, row):
            print(line)

    for key in missing:
        print(
            f"  MISSING  {key.ticker:<6} {key.expiration_date}  {key.option_type:<4}  "
            f"strike={key.strike:>7.1f}"
        )

    print()
    print(
        f"Result: {len(found)}/{total} positions found"
        + (f"  ({len(missing)} missing)" if missing else "")
    )

    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
