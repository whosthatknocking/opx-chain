"""Tests for shared scalar and timestamp utilities."""

from opx_chain.utils import normalize_timestamp


def test_normalize_timestamp_infers_numeric_epoch_units():
    """Numeric vendor timestamps should infer seconds, milliseconds, and nanoseconds."""
    assert str(normalize_timestamp(1710942000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000)) == "2024-03-20 13:40:00+00:00"
    assert str(normalize_timestamp(1710942000000000000)) == "2024-03-20 13:40:00+00:00"
