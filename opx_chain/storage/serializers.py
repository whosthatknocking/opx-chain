"""Dataset serializer protocol and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd


class DatasetSerializer(Protocol):  # pylint: disable=too-few-public-methods
    """Serialize a DataFrame to a file path. Returns bytes written."""

    format: str  # "csv" | "parquet"

    def serialize(self, df: pd.DataFrame, path: str) -> int: ...  # pylint: disable=missing-function-docstring


class CsvSerializer:  # pylint: disable=too-few-public-methods
    """CSV implementation of DatasetSerializer."""

    format = "csv"

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as CSV. Returns bytes written."""
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(dest, index=False)
        return dest.stat().st_size


class ParquetSerializer:  # pylint: disable=too-few-public-methods
    """Parquet implementation of DatasetSerializer. Requires pyarrow."""

    format = "parquet"

    def serialize(self, df: pd.DataFrame, path: str) -> int:
        """Write df to path as Parquet. Returns bytes written."""
        try:
            import pyarrow as _pyarrow  # pylint: disable=import-outside-toplevel
            del _pyarrow
        except ImportError as exc:
            raise RuntimeError(
                "Parquet serialization requires pyarrow. "
                "Install it with: pip install 'opx[parquet]'"
            ) from exc
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest, index=False, engine="pyarrow")
        return dest.stat().st_size


_SERIALIZERS: dict[str, DatasetSerializer] = {
    CsvSerializer.format: CsvSerializer(),
    ParquetSerializer.format: ParquetSerializer(),
}


def get_serializer(fmt: str) -> DatasetSerializer:
    """Return the serializer for the given format name."""
    try:
        return _SERIALIZERS[fmt]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset format: {fmt!r}") from exc
