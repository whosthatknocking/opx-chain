"""Viewer helper tests for field descriptions, cards, and freshness metadata."""

from pathlib import Path

import pandas as pd

from options_fetcher import viewer


def test_extract_field_descriptions_reads_current_readme_entries():
    """README-backed field descriptions should stay discoverable for the viewer."""
    descriptions = viewer.extract_field_descriptions()

    assert "underlying_symbol" in descriptions
    assert "Use it to group rows by underlying." in descriptions["underlying_symbol"]


def test_build_dataset_cards_only_promotes_dataset_wide_constant_values():
    """Only dataset-wide constant values should be promoted into header cards."""
    frame = pd.DataFrame(
        [
            {
                "underlying_market_state": "POST",
                "vix_level": 18.5,
                "premium_reference_method": "mid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
            {
                "underlying_market_state": "POST",
                "vix_level": 18.5,
                "premium_reference_method": "bid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
        ]
    )

    cards = viewer.build_dataset_cards(frame, descriptions={"data_source": "Source label."})
    card_names = [card["name"] for card in cards]

    assert "underlying_market_state" in card_names
    assert "vix_level" in card_names
    assert "risk_free_rate_used" in card_names
    assert "data_source" in card_names
    assert "premium_reference_method" not in card_names


def test_build_column_definitions_marks_numeric_but_not_boolean_columns():
    """Boolean columns should not be classified as numeric in the viewer schema."""
    frame = pd.DataFrame(
        {
            "strike": [100.0, 105.0],
            "underlying_symbol": ["TSLA", "TSLA"],
            "passes_primary_screen": [True, False],
        }
    )

    definitions = viewer.build_column_definitions(frame, descriptions={})
    by_name = {column["name"]: column for column in definitions}

    assert by_name["strike"]["is_numeric"] is True
    assert by_name["underlying_symbol"]["is_numeric"] is False
    assert by_name["passes_primary_screen"]["is_numeric"] is False


def test_build_freshness_summary_reports_file_and_quote_ages(tmp_path: Path):
    """Freshness summary should report both file age and quote age statistics."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("placeholder", encoding="utf-8")
    frame = pd.DataFrame(
        {
            "quote_age_seconds": [10, 30, 50],
            "underlying_price_age_seconds": [5, 15, 25],
        }
    )

    summary = viewer.build_freshness_summary(frame, csv_path)

    assert summary["option_quote_age_median_seconds"] == 30.0
    assert summary["option_quote_age_max_seconds"] == 50.0
    assert summary["underlying_quote_age_median_seconds"] == 15.0
    assert summary["underlying_quote_age_max_seconds"] == 25.0
    assert summary["file_age_seconds"] >= 0
    assert len(summary["file_modified_at"]) == 19
