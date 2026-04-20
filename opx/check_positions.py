"""CLI tool to verify that every option position appears in the latest output CSV."""
from pathlib import Path

import pandas as pd

from opx.positions import DEFAULT_POSITIONS_PATH, load_positions

OUTPUTS_DIR = Path("output")


def find_latest_output(outputs_dir: Path = OUTPUTS_DIR) -> Path | None:
    """Return the most recently modified CSV in the outputs directory."""
    csvs = sorted(outputs_dir.glob("options_engine_output_*.csv"), key=lambda p: p.stat().st_mtime)
    return csvs[-1] if csvs else None


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

    df = pd.read_csv(resolved_output, low_memory=False)

    found, missing = [], []
    for key in sorted(
        position_set.option_keys,
        key=lambda k: (k.ticker, k.expiration_date, k.option_type),
    ):
        mask = (
            (df["underlying_symbol"] == key.ticker)
            & (df["expiration_date"] == key.expiration_date)
            & (df["option_type"] == key.option_type)
            & ((df["strike"] - key.strike).abs() < 0.01)
        )
        if df[mask].empty:
            missing.append(key)
        else:
            found.append((key, df[mask].iloc[0]))
    return found, missing


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
    args = parser.parse_args(argv)

    positions_path = (args.positions or DEFAULT_POSITIONS_PATH).expanduser()
    output_path = args.output

    if not positions_path.exists():
        print(f"Positions file not found: {positions_path}")
        return 1

    resolved_output = output_path or find_latest_output()
    if resolved_output is None:
        print(f"No output CSV found in {OUTPUTS_DIR}/")
        return 1

    print(f"Positions : {positions_path}")
    print(f"Output    : {resolved_output}")
    print()

    found, missing = check_positions(positions_path, resolved_output)
    total = len(found) + len(missing)

    if total == 0:
        print("No option positions found in positions file.")
        return 0

    for key, row in found:
        passes = row.get("passes_primary_screen")
        screen_note = "" if passes is True else "  [fails screen]"
        print(
            f"  FOUND    {key.ticker:<6} {key.expiration_date}  {key.option_type:<4}  "
            f"strike={key.strike:>7.1f}  bid={row['bid']}  ask={row['ask']}{screen_note}"
        )

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
