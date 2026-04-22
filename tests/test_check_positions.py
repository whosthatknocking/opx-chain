"""Tests for opx.check_positions."""

import csv
import os
import time

import pandas as pd

from opx_chain.check_positions import (
    check_positions,
    find_latest_output,
    format_freshness_summary_lines,
    main,
)


def _write_positions(tmp_path, rows):
    path = tmp_path / "positions.csv"
    fieldnames = ["Account Number", "Account Name", "Symbol", "Description",
                  "Quantity", "Last Price", "Last Price Change", "Current Value",
                  "Today's Gain/Loss Dollar", "Today's Gain/Loss Percent",
                  "Total Gain/Loss Dollar", "Total Gain/Loss Percent",
                  "Percent Of Account", "Cost Basis Total", "Average Cost Basis", "Type"]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            full_row = {k: "" for k in fieldnames}
            full_row.update(row)
            writer.writerow(full_row)
    return path


def _write_output(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_find_latest_output_returns_none_when_empty(tmp_path):
    """Returns None when no output CSVs exist."""
    assert find_latest_output(tmp_path) is None


def test_find_latest_output_returns_most_recent(tmp_path):
    """Returns the most recently modified output CSV."""
    older = tmp_path / "options_engine_output_20260101_120000.csv"
    newer = tmp_path / "options_engine_output_20260102_120000.csv"
    older.write_text("x")
    time.sleep(0.01)
    newer.write_text("x")
    assert find_latest_output(tmp_path) == newer


def test_check_positions_found(tmp_path):
    """A position present in the output CSV appears in the found list."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "AAPL", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    found, missing = check_positions(pos_path, out_path)
    assert len(found) == 1
    assert not missing
    key, _row = found[0]
    assert key.ticker == "AAPL"
    assert key.strike == 200.0


def test_check_positions_missing(tmp_path):
    """A position absent from the output CSV appears in the missing list."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200", "Description": "AAPL JUN 20 2026 $200 CALL"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    found, missing = check_positions(pos_path, out_path)
    assert not found
    assert len(missing) == 1
    assert missing[0].ticker == "AAPL"


def test_check_positions_no_output_returns_all_missing(tmp_path):
    """All positions are reported missing when the output file does not exist."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    found, missing = check_positions(pos_path, tmp_path / "nonexistent.csv")
    assert not found
    assert len(missing) == 1


def test_check_positions_empty_positions_returns_empty(tmp_path):
    """Returns empty lists when the positions file has no option positions."""
    pos_path = _write_positions(tmp_path, [])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [])
    found, missing = check_positions(pos_path, out_path)
    assert not found
    assert not missing


def test_main_exits_0_all_found(tmp_path):
    """main() returns 0 when every position is present in the output."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "AAPL", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5,
         "passes_primary_screen": True},
    ])
    result = main(["--positions", str(pos_path), "--output", str(out_path)])
    assert result == 0


def test_main_exits_1_some_missing(tmp_path):
    """main() returns 1 when any position is missing from the output."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {"underlying_symbol": "MSFT", "expiration_date": "2026-06-20",
         "option_type": "call", "strike": 200.0, "bid": 5.0, "ask": 5.5},
    ])
    result = main(["--positions", str(pos_path), "--output", str(out_path)])
    assert result == 1


def test_main_prints_passes_primary_screen_true_for_passing_row(tmp_path, capsys):
    """Found rows should use the canonical passes_primary_screen naming."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.08,
            "open_interest": 500,
            "volume": 25,
            "passes_primary_screen": True,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert f"Positions: {pos_path}" in captured.out
    assert f"Output: {out_path}" in captured.out
    assert "passes_primary_screen=true" in captured.out
    assert "failed_filters:" not in captured.out


def test_main_prints_failed_primary_screen_filters_for_non_passing_row(tmp_path, capsys):
    """Found rows should show which configured primary-screen filters failed."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.30,
            "open_interest": 40,
            "volume": 5,
            "passes_primary_screen": False,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert "passes_primary_screen=false" in captured.out
    assert "failed_filters:" in captured.out
    assert "\n             - filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "\n             - filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "\n             - filters_min_volume(5.0000<10.0000)" in captured.out
    assert "filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "filters_min_volume(5.0000<10.0000)" in captured.out


def test_main_formats_quotes_to_two_decimals_and_wraps_failed_filters(tmp_path, capsys):
    """Found rows should render bid/ask consistently and wrap long filter summaries."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "bid_ask_spread_pct_of_mid": 0.30,
            "open_interest": 40,
            "volume": 5,
            "passes_primary_screen": False,
        },
    ])

    result = main(["--positions", str(pos_path), "--output", str(out_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert "bid=  5.00  ask=  5.50" in captured.out
    assert "\n           failed_filters:" in captured.out
    assert "\n             - filters_max_spread_pct_of_mid(0.3000>0.2500)" in captured.out
    assert "\n             - filters_min_open_interest(40.0000<100.0000)" in captured.out
    assert "\n             - filters_min_volume(5.0000<10.0000)" in captured.out


def test_format_freshness_summary_lines_recomputes_current_age_from_saved_timestamps(tmp_path):
    """Freshness summary should reflect read-time age, not just stored fetch-time flags."""
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "GOOGL",
            "option_quote_time": "2026-04-10T13:40:56Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
        {
            "underlying_symbol": "GOOGL",
            "option_quote_time": "2026-04-10T13:40:56Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
    ])
    file_time = pd.Timestamp("2026-04-21T12:50:56Z").timestamp()
    os.utime(out_path, (file_time, file_time))

    lines = format_freshness_summary_lines(
        out_path,
        now=pd.Timestamp("2026-04-21T13:50:56Z"),
    )
    rendered = "\n".join(lines)

    assert "Freshness now:" in rendered
    assert "file_age_now=1h 00m 00s" in rendered
    assert (
        "option_quotes_now: rows_with_timestamp=2  stale_now_rows=2  stale_at_fetch_rows=0"
        in rendered
    )
    assert (
        "underlying_quotes_now: rows_with_timestamp=2  stale_now_rows=2  "
        "stale_at_fetch_rows=0" in rendered
    )
    assert "stale_underlyings_now:" in rendered
    assert "GOOGL" in rendered
    assert "time=2026-04-10T13:50:56Z" in rendered
    assert "newest_age=11d 00h 00m" in rendered


def test_main_prints_freshness_summary_when_requested(tmp_path, capsys, monkeypatch):
    """--freshness should print a runtime freshness section alongside position coverage."""
    pos_path = _write_positions(tmp_path, [
        {"Symbol": " -AAPL260620C200"},
    ])
    out_path = _write_output(tmp_path, "options_engine_output_test.csv", [
        {
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-06-20",
            "option_type": "call",
            "strike": 200.0,
            "bid": 5.0,
            "ask": 5.5,
            "passes_primary_screen": True,
            "option_quote_time": "2026-04-21T16:40:00Z",
            "underlying_price_time": "2026-04-10T13:50:56Z",
            "is_stale_quote": False,
            "is_stale_underlying_price": False,
        },
    ])
    monkeypatch.setattr(
        "opx_chain.check_positions._utc_now",
        lambda: pd.Timestamp("2026-04-21T17:00:00Z"),
    )

    result = main([
        "--positions", str(pos_path), "--output", str(out_path), "--freshness",
    ])

    captured = capsys.readouterr()
    assert result == 0
    assert "Freshness now:" in captured.out
    assert (
        "underlying_quotes_now: rows_with_timestamp=1  stale_now_rows=1  "
        "stale_at_fetch_rows=0" in captured.out
    )
    assert "stale_underlyings_now:" in captured.out
    assert "AAPL" in captured.out
    assert "passes_primary_screen=true" in captured.out
