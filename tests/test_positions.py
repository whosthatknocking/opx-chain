"""Tests for portfolio positions loading and parsing."""

import textwrap
from pathlib import Path

import pytest

from opx.positions import (
    DEFAULT_POSITIONS_PATH,
    EMPTY_POSITION_SET,
    OptionPositionKey,
    load_positions,
)


def write_positions_csv(tmp_path: Path, content: str) -> Path:
    """Write a positions CSV file to a temp directory and return its path."""
    path = tmp_path / "positions.csv"
    path.write_text(textwrap.dedent(content))
    return path


def test_load_positions_returns_empty_when_file_missing(tmp_path):
    result = load_positions(tmp_path / "nonexistent.csv")
    assert result == EMPTY_POSITION_SET


def test_default_positions_path_points_to_repo_data_directory():
    assert DEFAULT_POSITIONS_PATH == Path("data/positions.csv")


def test_load_positions_parses_stock_tickers(tmp_path):
    path = write_positions_csv(tmp_path, """\
        Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,Percent Of Account,Cost Basis Total,Average Cost Basis,Type
        Z1,INDIVIDUAL,TSLA,TESLA INC,100,$391.00,-$1.00,$39100.00,,,,,10.00%,$39000.00,$390.00,Margin,
        Z1,INDIVIDUAL,NVDA,NVIDIA CORP,50,$200.00,$1.00,$10000.00,,,,,5.00%,$9000.00,$180.00,Margin,
    """)
    result = load_positions(path)
    assert result.stock_tickers == frozenset({"TSLA", "NVDA"})
    assert result.option_keys == frozenset()


def test_load_positions_parses_option_keys(tmp_path):
    path = write_positions_csv(tmp_path, """\
        Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,Percent Of Account,Cost Basis Total,Average Cost Basis,Type
        Z1,INDIVIDUAL, -TSLA260821P360,TSLA AUG 21 2026 $360 PUT,-2,$25.00,$2.22,-$5000.00,,,,,,,,,Margin,
        Z1,INDIVIDUAL, -UBER260618C82.5,UBER JUN 18 2026 $82.50 CALL,-52,$2.88,$0.01,-$14976.00,,,,,,,,,Margin,
    """)
    result = load_positions(path)
    assert result.stock_tickers == frozenset()
    assert OptionPositionKey(
        ticker="TSLA", expiration_date="2026-08-21", option_type="put", strike=360.0
    ) in result.option_keys
    assert OptionPositionKey(
        ticker="UBER", expiration_date="2026-06-18", option_type="call", strike=82.5
    ) in result.option_keys


def test_load_positions_excludes_cash_and_pending(tmp_path):
    path = write_positions_csv(tmp_path, """\
        Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,Percent Of Account,Cost Basis Total,Average Cost Basis,Type
        Z1,INDIVIDUAL,SPAXX**,HELD IN MONEY MARKET,,,,$60000.00,,,,,6.00%,,,Cash,
        Z1,INDIVIDUAL,TSLA,TESLA INC,100,$391.00,-$1.00,$39100.00,,,,,,,,,Margin,
        Z1,INDIVIDUAL,Pending activity,,,,,$164.84,,,,,,,,,
    """)
    result = load_positions(path)
    assert result.stock_tickers == frozenset({"TSLA"})
    assert result.option_keys == frozenset()


def test_load_positions_parses_mixed_stocks_and_options(tmp_path):
    path = write_positions_csv(tmp_path, """\
        Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,Percent Of Account,Cost Basis Total,Average Cost Basis,Type
        Z1,INDIVIDUAL,GOOGL,ALPHABET INC,100,$338.00,-$3.00,$33800.00,,,,,,,,,Margin,
        Z1,INDIVIDUAL, -GOOGL260618P310,GOOGL JUN 18 2026 $310 PUT,-3,$7.10,$0.55,-$2130.00,,,,,,,,,Margin,
        Z1,INDIVIDUAL, -GOOGL260918C350,GOOGL SEP 18 2026 $350 CALL,-1,$27.20,-$1.27,-$2720.00,,,,,,,,,Margin,
    """)
    result = load_positions(path)
    assert result.stock_tickers == frozenset({"GOOGL"})
    assert len(result.option_keys) == 2
    assert OptionPositionKey("GOOGL", "2026-06-18", "put", 310.0) in result.option_keys
    assert OptionPositionKey("GOOGL", "2026-09-18", "call", 350.0) in result.option_keys


def test_load_positions_returns_empty_on_missing_symbol_column(tmp_path):
    path = write_positions_csv(tmp_path, """\
        Account,Name
        Z1,INDIVIDUAL
    """)
    result = load_positions(path)
    assert result == EMPTY_POSITION_SET


def test_position_set_empty_property():
    assert EMPTY_POSITION_SET.empty
    non_empty = EMPTY_POSITION_SET.__class__(frozenset({"TSLA"}), frozenset())
    assert not non_empty.empty
