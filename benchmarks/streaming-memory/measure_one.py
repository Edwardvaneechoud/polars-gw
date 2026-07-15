#!/usr/bin/env python3
"""Standalone (polars-only) measurement of the streaming full-scan group_by.
Run under different polars versions via:
  uv run --no-project --with 'polars==X' python measure_one.py
"""
import hashlib
import json
import os
import resource
import threading
import time

import polars as pl

PATH = os.environ.get("BENCH_PATH")
PAGE = os.sysconf("SC_PAGE_SIZE")
MO = os.environ.get("MAINTAIN_ORDER", "1") == "1"


def rss_mb():
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE / 1e6


def fp(rows):
    def nv(x):
        return round(float(x), 4) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
    norm = sorted(tuple(sorted((str(k), nv(v)) for k, v in r.items())) for r in rows)
    return [hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12], len(norm)]


lf = pl.scan_parquet(PATH).group_by(["city"], maintain_order=MO).agg(
    [pl.col("salary").mean().alias("a"), pl.col("qty").sum().alias("b")]
).slice(0, 200).head(1_000_000)

samples, stop, t0 = [], threading.Event(), time.perf_counter()


def s():
    while not stop.is_set():
        samples.append(((time.perf_counter() - t0) * 1000, rss_mb()))
        time.sleep(0.002)


th = threading.Thread(target=s, daemon=True)
th.start()
base = rss_mb()
t_pre = (time.perf_counter() - t0) * 1000
tc = time.perf_counter()
rows = lf.collect(engine="streaming").to_dicts()
collect_ms = (time.perf_counter() - tc) * 1000
t_post = (time.perf_counter() - t0) * 1000
time.sleep(0.25)
stop.set()
th.join()

peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000.0
during = [(t, m) for t, m in samples if t_pre <= t <= t_post]
hi = max((m for _, m in during), default=0)
span = max(t_post - t_pre, 1)
q1 = max([m for t, m in during if t <= t_pre + 0.25 * span], default=base)
q3 = max([m for t, m in during if t >= t_pre + 0.75 * span], default=hi)
shape = "RAMP" if (q3 - q1) > 0.4 * (hi - base) else "PLATEAU"
print("@@JSON@@" + json.dumps({
    "polars": pl.__version__, "threads": pl.thread_pool_size(), "maintain_order": MO,
    "nrows": len(rows), "fingerprint": fp(rows), "collect_ms": round(collect_ms, 1),
    "baseline_mb": round(base, 1), "peak_maxrss_mb": round(peak, 1),
    "inflight_ws_mb": round(hi - base, 1), "q25_mb": round(q1, 1), "q75_mb": round(q3, 1),
    "shape": shape,
}))
