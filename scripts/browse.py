"""Browse the Silver and Gold Delta tables interactively.

poetry run python scripts/browse.py                  # the data lake (EMAPS_DATA_DIR, default data/)
poetry run python scripts/browse.py sample_output    # the sample in the repository
"""

import sys

import polars as pl

from emaps_etl import gold, reader, silver
from emaps_etl.config import get_settings

TABLES = {
    "silver": [silver.MIX_TABLE, silver.FLOWS_TABLE],
    "gold": [gold.DAILY_MIX_TABLE, gold.IMPORTS_TABLE, gold.EXPORTS_TABLE],
}
TIME_COLUMNS = ["datetime_utc", "date_utc"]


def choose(prompt: str, options: list[str]) -> str | None:
    """Ask the user to pick an option by number. Returns None to quit."""
    print(f"\n{prompt}")
    for number, option in enumerate(options, start=1):
        print(f"  {number}. {option}")
    while True:
        answer = input("Choice (q to quit): ").strip().lower()
        if answer == "q":
            return None
        if answer.isdigit() and 1 <= int(answer) <= len(options):
            return options[int(answer) - 1]
        print(f"Enter a number from 1 to {len(options)}.")


def show(df: pl.DataFrame, layer: str, table: str) -> None:
    print(f"\n=== {layer}/{table}: {len(df):,} rows ===")
    print("\nColumns:")
    for name, dtype in df.schema.items():
        print(f"  {name:28} {dtype}")

    time_column = next((c for c in TIME_COLUMNS if c in df.columns), None)
    if time_column:
        print(f"\nTime range: {df[time_column].min()} .. {df[time_column].max()}")
        df = df.sort(time_column)
    print("\nLatest rows:")
    print(df.tail(15))


def main() -> None:
    storage = sys.argv[1] if len(sys.argv) > 1 else str(get_settings().data_dir)
    pl.Config.set_tbl_rows(20)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_width_chars(200)
    pl.Config.set_fmt_str_lengths(30)
    print(f"Browsing {storage}")

    while True:
        layer = choose("Layer:", list(TABLES))
        if layer is None:
            break
        table = choose(f"Table in {layer}:", TABLES[layer])
        if table is None:
            break
        print("Loading...")
        df = reader.read_table(reader.table_path(storage, layer, table))
        show(df, layer, table)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print()
