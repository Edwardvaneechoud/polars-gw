"""Minimal demo of gw_polars.walk() — requires ``gw-polars[viz]`` extras.

Run::

    uv run --extra viz python example/walk_demo.py
"""

import logging

import polars as pl

from gw_polars import walk

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s")


def main() -> None:
    df = pl.DataFrame({
        "city": ["Amsterdam", "Berlin", "Paris", "London", "Madrid"] * 200,
        "category": ["Electronics", "Clothing", "Food", "Books", "Sports"] * 200,
        "sales": pl.Series(range(1000)).cast(pl.Float64) * 0.5 + 50,
        "quantity": pl.Series(range(1000)) % 100,
    })
    handle = walk(df)
    try:
        input("Press Enter to stop…\n")
    finally:
        handle.stop()


if __name__ == "__main__":
    main()
