"""Viewer helper tests for field descriptions, cards, and freshness metadata."""
from pathlib import Path

import pandas as pd

from opx import viewer


def build_config(viewer_host: str, viewer_port: int):
    """Create a lightweight runtime-config stub for viewer tests."""
    return type(
        "Config",
        (),
        {"viewer_host": viewer_host, "viewer_port": viewer_port},
    )()


def test_extract_field_descriptions_reads_current_user_guide_entries():
    """User-guide field descriptions should stay discoverable for the viewer."""
    descriptions = viewer.extract_field_descriptions()

    assert "underlying_symbol" in descriptions
    assert "delta_safety_pct" in descriptions
    assert "Use it to group rows by underlying." in descriptions["underlying_symbol"]


def test_build_dataset_cards_only_promotes_dataset_wide_constant_values():
    """Only dataset-wide constant values should be promoted into header cards."""
    frame = pd.DataFrame(
        [
            {
                "premium_reference_method": "mid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
            {
                "premium_reference_method": "bid",
                "risk_free_rate_used": 0.045,
                "data_source": "yfinance",
            },
        ]
    )

    cards = viewer.build_dataset_cards(frame, descriptions={"data_source": "Source label."})
    card_names = [card["name"] for card in cards]

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


def test_normalize_row_value_keeps_days_to_expiration_as_integer():
    """Viewer payload serialization should keep days_to_expiration whole."""
    assert viewer.normalize_row_value("days_to_expiration", 14.0) == 14
    assert viewer.normalize_row_value("time_to_expiration_years", 14.0) == 14.0


def test_build_ticker_summary_marks_estimated_marketdata_earnings_dates():
    """Summary payload should preserve whether the next earnings date is estimated."""
    frame = pd.DataFrame(
        [
            {
                "underlying_price": 100.0,
                "underlying_day_change_pct": 0.01,
                "implied_volatility": 0.25,
                "historical_volatility": 0.20,
                "option_type": "call",
                "expiration_date": "2026-04-17",
                "next_earnings_date": "2026-04-30",
                "next_earnings_date_is_estimated": "True",
                "event_risk_score": 60.0,
            }
        ]
    )

    summary = viewer.build_ticker_summary("TSLA", frame)

    assert summary["next_earnings_date"] == "2026-04-30"
    assert summary["next_earnings_date_is_estimated"] is True
    assert summary["event_risk_score"] == 60.0


def test_pick_profitable_opportunity_prefers_higher_final_score_when_rom_matches():
    """Summary highlights should use final score as a tie-breaker ahead of quote quality."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "TSLA260417C00100000",
                "option_type": "call",
                "strike": 100.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.22,
                "risk_level": "LOW",
                "spread_score": 100.0,
                "dte_score": 100.0,
                "theta_efficiency": 10.0,
                "bid_ask_spread_pct_of_mid": 0.08,
                "return_on_margin_annualized": 1.5,
                "option_score": 90.0,
                "final_score": 80.0,
                "quote_quality_score": 7,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "TSLA260417C00105000",
                "option_type": "call",
                "strike": 105.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.24,
                "risk_level": "LOW",
                "spread_score": 85.0,
                "dte_score": 85.0,
                "theta_efficiency": 8.0,
                "bid_ask_spread_pct_of_mid": 0.09,
                "return_on_margin_annualized": 1.5,
                "option_score": 88.0,
                "final_score": 92.0,
                "quote_quality_score": 5,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_profitable_opportunity(frame)

    assert summary is not None
    assert summary["contract_symbol"] == "TSLA260417C00105000"
    assert summary["option_score"] == 88.0
    assert summary["final_score"] == 92.0


def test_pick_moderate_risk_opportunity_accepts_spread_at_config_cutoff(monkeypatch):
    """Moderate-risk selection should keep candidates whose spread equals the configured limit."""
    def make_config():
        return type("Config", (), {"max_spread_pct_of_mid": 0.25})()

    monkeypatch.setattr("opx.viewer.get_runtime_config", make_config)
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "EDGE",
                "option_type": "put",
                "strike": 95.0,
                "expiration_date": "2026-04-17",
                "probability_itm": 0.30,
                "delta_abs": 0.35,
                "strike_distance_pct": 0.04,
                "bid_ask_spread_pct_of_mid": 0.25,
                "return_on_margin_annualized": 1.2,
                "option_score": 82.0,
                "final_score": 87.0,
                "quote_quality_score": 7,
                "passes_primary_screen": True,
            }
        ]
    )

    summary = viewer.pick_moderate_risk_opportunity(frame)

    assert summary is not None
    assert summary["contract_symbol"] == "EDGE"


def test_pick_high_conviction_call_prefers_bullish_aligned_liquid_candidate():
    """Call conviction should prefer cleaner bullish alignment over raw ROM alone."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "CALL_ROM",
                "option_type": "call",
                "strike": 110.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": -0.03,
                "strike_distance_pct": 0.12,
                "delta_abs": 0.18,
                "spread_score": 70.0,
                "quote_quality_score": 4.0,
                "return_on_margin_annualized": 2.2,
                "option_score": 70.0,
                "final_score": 72.0,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "CALL_CONVICTION",
                "option_type": "call",
                "strike": 102.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": 0.025,
                "strike_distance_pct": 0.03,
                "delta_abs": 0.39,
                "spread_score": 92.0,
                "quote_quality_score": 8.0,
                "return_on_margin_annualized": 1.4,
                "option_score": 88.0,
                "final_score": 90.0,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_high_conviction_opportunity(frame, "call")

    assert summary is not None
    assert summary["contract_symbol"] == "CALL_CONVICTION"


def test_pick_high_conviction_put_prefers_bearish_aligned_candidate():
    """Put conviction should stay side-specific and prefer downside alignment."""
    frame = pd.DataFrame(
        [
            {
                "contract_symbol": "PUT_BULLISH",
                "option_type": "put",
                "strike": 95.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": 0.03,
                "strike_distance_pct": 0.02,
                "delta_abs": 0.34,
                "spread_score": 95.0,
                "quote_quality_score": 8.0,
                "return_on_margin_annualized": 1.3,
                "option_score": 89.0,
                "final_score": 91.0,
                "passes_primary_screen": True,
            },
            {
                "contract_symbol": "PUT_CONVICTION",
                "option_type": "put",
                "strike": 98.0,
                "expiration_date": "2026-04-17",
                "underlying_day_change_pct": -0.025,
                "strike_distance_pct": 0.04,
                "delta_abs": 0.36,
                "spread_score": 90.0,
                "quote_quality_score": 7.0,
                "return_on_margin_annualized": 1.2,
                "option_score": 86.0,
                "final_score": 89.0,
                "passes_primary_screen": True,
            },
        ]
    )

    summary = viewer.pick_high_conviction_opportunity(frame, "put")

    assert summary is not None
    assert summary["contract_symbol"] == "PUT_CONVICTION"


def test_viewer_main_uses_runtime_config_host_and_port(monkeypatch):
    """Viewer startup should default to the resolved runtime config values."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx.viewer.get_runtime_config",
        lambda: build_config("0.0.0.0", 9001),
    )
    monkeypatch.setattr("opx.viewer.serve", captured.update)

    monkeypatch.delenv("OPX_VIEWER_HOST", raising=False)
    monkeypatch.delenv("OPX_VIEWER_PORT", raising=False)

    viewer.main()

    assert captured == {"host": "0.0.0.0", "port": 9001}


def test_viewer_main_env_overrides_runtime_config(monkeypatch):
    """Explicit viewer environment variables should override file config values."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr("opx.viewer.serve", captured.update)
    monkeypatch.setenv("OPX_VIEWER_HOST", "0.0.0.0")
    monkeypatch.setenv("OPX_VIEWER_PORT", "9100")

    viewer.main()

    assert captured == {"host": "0.0.0.0", "port": 9100}


def test_viewer_main_can_open_browser(monkeypatch):
    """The --open flag should launch the resolved viewer URL in a browser."""
    captured: dict[str, object] = {}

    class ImmediateTimer:  # pylint: disable=too-few-public-methods
        """Run the scheduled browser open immediately during tests."""

        def __init__(self, _delay, callback, args=None, kwargs=None):
            self._callback = callback
            self._args = args or ()
            self._kwargs = kwargs or {}

        def start(self):
            """Execute the scheduled callback immediately."""
            self._callback(*self._args, **self._kwargs)

    monkeypatch.setattr(
        "opx.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr("opx.viewer.serve", lambda **kwargs: captured.update({"serve": kwargs}))
    monkeypatch.setattr(
        "opx.viewer.open_viewer_in_browser",
        lambda host, port: captured.update({"open": (host, port)}),
    )
    monkeypatch.setattr("opx.viewer.threading.Timer", ImmediateTimer)

    viewer.main(["--open"])

    assert captured == {
        "open": ("127.0.0.1", 8000),
        "serve": {"host": "127.0.0.1", "port": 8000},
    }


def test_viewer_main_does_not_open_browser_without_flag(monkeypatch):
    """Browser launch should remain opt-in."""
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "opx.viewer.get_runtime_config",
        lambda: build_config("127.0.0.1", 8000),
    )
    monkeypatch.setattr("opx.viewer.serve", lambda **kwargs: captured.update({"serve": kwargs}))
    monkeypatch.setattr(
        "opx.viewer.open_viewer_in_browser",
        lambda host, port: captured.update({"open": (host, port)}),
    )

    viewer.main([])

    assert captured == {"serve": {"host": "127.0.0.1", "port": 8000}}
