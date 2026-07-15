#!/usr/bin/env python3
"""Prove the reconstructed plan == the REAL executor path, and dump the plan verbatim."""
import hashlib
import json
import os
import resource
import threading
import time

import polars as pl

from polars_gw import execute_workflow

PATH = os.environ["BENCH_PATH"]
N = 100_000_000
PAGE = os.sysconf("SC_PAGE_SIZE")

FULL_SCAN = {
    "workflow": [{"type": "view", "query": [{"op": "aggregate", "groupBy": ["city"],
        "measures": [{"field": "salary", "agg": "mean", "asFieldKey": "a"},
                     {"field": "qty", "agg": "sum", "asFieldKey": "b"}]}]}],
    "limit": 200,
}


def rss_mb():
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE / 1e6


def fp(rows):
    def nv(x):
        return round(float(x), 4) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
    norm = sorted(tuple(sorted((str(k), nv(v)) for k, v in r.items())) for r in rows)
    return [hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12], len(norm)]


# ---- dump the exact plan the executor builds (mirror of executor logic) ----
lf = pl.scan_parquet(PATH).group_by(["city"], maintain_order=True).agg(
    [pl.col("salary").mean().alias("a"), pl.col("qty").sum().alias("b")]
).slice(0, 200).head(1_000_000)
print("========== explain(engine='streaming') on the executor's full-scan plan ==========")
print(lf.explain(engine="streaming"))
print("\n========== explain(optimized=True) [default/in-memory] ==========")
print(lf.explain(optimized=True))

# ---- run the REAL executor, measure RSS, compare fingerprint ----
from polars_gw.executor import clear_cache

clear_cache()
samples, stop, t0 = [], threading.Event(), time.perf_counter()
def s():
    while not stop.is_set():
        samples.append(rss_mb()); time.sleep(0.002)
th = threading.Thread(target=s, daemon=True); th.start()
base = rss_mb()
tc = time.perf_counter()
rows = execute_workflow(pl.scan_parquet(PATH), FULL_SCAN)  # THE REAL PATH
ms = (time.perf_counter() - tc) * 1000
stop.set(); th.join()
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000.0
print("\n========== REAL execute_workflow() full-scan ==========")
print(f"rows={len(rows)} fingerprint={fp(rows)} collect≈{ms:.0f}ms "
      f"peak_maxrss={peak:.0f}MB inflight_ws={max(samples)-base:.0f}MB")
print("matches reconstructed fingerprint 85381da9d6c1:", fp(rows)[0] == "85381da9d6c1")
