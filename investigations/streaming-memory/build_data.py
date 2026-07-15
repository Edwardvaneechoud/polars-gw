#!/usr/bin/env python3
"""Build the benchmark Parquet file (adapted verbatim from bench_polars_gw.build)."""
from __future__ import annotations

import os
import sys
import time

import numpy as np
import polars as pl
import pyarrow.parquet as pq

PATH = sys.argv[1] if len(sys.argv) > 1 else "bench_data.parquet"
N_ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000_000
CHUNK = 5_000_000


def build(path: str, n_rows: int, chunk: int = CHUNK) -> None:
    if os.path.exists(path):
        print(f"reusing existing {path} ({os.path.getsize(path)/1e9:.2f} GB)")
        return
    rng = np.random.default_rng(0)
    cities = np.array([f"city_{i}" for i in range(200)])
    t0 = time.perf_counter()
    writer = None
    for start in range(0, n_rows, chunk):
        n = min(chunk, n_rows - start)
        tbl = pl.DataFrame(
            {
                "ts": np.arange(start, start + n, dtype=np.int64),  # sorted
                "city": rng.choice(cities, n),
                "dept": rng.integers(0, 20, n).astype(np.int32),
                "salary": rng.normal(50_000, 12_000, n),
                "qty": rng.integers(1, 10, n).astype(np.int32),
                "pad1": rng.normal(0, 1, n),
                "pad2": rng.normal(0, 1, n),
            }
        ).to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(path, tbl.schema, compression="snappy")
        writer.write_table(tbl, row_group_size=200_000)
    writer.close()
    print(f"wrote {n_rows:,} rows in {time.perf_counter() - t0:.0f}s -> {os.path.getsize(path)/1e9:.2f} GB")


if __name__ == "__main__":
    build(PATH, N_ROWS)
