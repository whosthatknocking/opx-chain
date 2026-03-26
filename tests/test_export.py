"""Export-schema tests for canonical CSV stability."""

from pathlib import Path

import pandas as pd

from opx.export import CANONICAL_EXPORT_COLUMNS, reorder_export_columns, write_options_csv


def test_reorder_export_columns_drops_noncanonical_provider_fields():
    """Provider-specific extras should not silently expand the CSV schema."""
    frame = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "contract_symbol": "TSLA260417C00100000",
                "data_source": "stub",
                "risk_free_rate_used": 0.045,
                "provider_debug_field": "drop-me",
            }
        ]
    )

    result = reorder_export_columns(frame)

    assert result.columns.tolist() == [
        "underlying_symbol",
        "contract_symbol",
        "data_source",
        "risk_free_rate_used",
    ]
    assert "provider_debug_field" not in result.columns


def test_write_options_csv_persists_only_canonical_columns(tmp_path: Path):
    """CSV exports should preserve the canonical schema and discard extras."""
    output_path = tmp_path / "export.csv"
    frame = pd.DataFrame(
        [
            {
                "underlying_symbol": "TSLA",
                "contract_symbol": "TSLA260417C00100000",
                "option_type": "call",
                "expiration_date": "2026-04-17",
                "days_to_expiration": 28,
                "time_to_expiration_years": 28 / 365.0,
                "strike": 100.0,
                "underlying_price": 102.0,
                "bid": 1.0,
                "ask": 1.2,
                "volume": 10,
                "open_interest": 100,
                "implied_volatility": 0.3,
                "option_quote_time": pd.Timestamp("2026-03-20T13:40:00Z"),
                "option_score": 82.5,
                "data_source": "stub",
                "risk_free_rate_used": 0.045,
                "provider_debug_field": "drop-me",
            }
        ]
    )

    write_options_csv([frame], output_path)

    exported = pd.read_csv(output_path)

    assert "provider_debug_field" not in exported.columns
    assert set(exported.columns).issubset(set(CANONICAL_EXPORT_COLUMNS))
    assert "option_score" in exported.columns
    assert exported.loc[0, "option_score"] == 82.5
    assert exported.loc[0, "days_to_expiration"] == 28
