#!/usr/bin/env python3
"""Streaming/RSS investigation harness for polars-gw's group_by path.

Parent launches one FRESH SUBPROCESS per variant (clean per-process peak RSS),
all from a single invocation with a warm page cache. Each child:
  * builds the EXACT plan shape the executor builds (scan -> group_by -> slice
    -> head), parametrised by maintain_order / filter / slice / head / engine
  * samples /proc/self/statm every 2 ms across the collect (+ 300 ms after, to
    expose mimalloc page retention vs live working set)
  * reports peak ru_maxrss, the in-flight RSS curve, a result fingerprint, rows
  * (separately) dumps the POLARS_VERBOSE streaming node-graph
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import threading
import time

import polars as pl

PATH = os.environ.get("BENCH_PATH", "bench_data.parquet")
N_ROWS = int(os.environ.get("BENCH_ROWS", "100000000"))
PAGE = os.sysconf("SC_PAGE_SIZE")

AGGS = [pl.col("salary").mean().alias("a"), pl.col("qty").sum().alias("b")]


def build_lf(v: dict) -> pl.LazyFrame:
    lf = pl.scan_parquet(PATH)
    if v.get("filter"):
        hi = max(N_ROWS // 1000, 1)  # ~0.1% of rows, sorted ts -> row groups skip
        lf = lf.filter(pl.col("ts").is_between(0, hi))
    lf = lf.group_by(["city"], maintain_order=v["maintain_order"]).agg(AGGS)
    if v.get("slice"):
        lf = lf.slice(0, 200)
    if v.get("head"):
        lf = lf.head(1_000_000)
    return lf


VARIANTS: dict[str, dict] = {
    # exact executor plan, FULL SCAN, streaming (current behaviour)
    "full_current":      dict(maintain_order=True,  filter=False, slice=True,  head=True,  engine="streaming"),
    # same plan, maintain_order dropped
    "full_no_order":     dict(maintain_order=False, filter=False, slice=True,  head=True,  engine="streaming"),
    # same plan, forced in-memory engine (reference for "what full materialisation costs")
    "full_inmem":        dict(maintain_order=True,  filter=False, slice=True,  head=True,  engine="in-memory"),
    # SELECTIVE (0.1% filter), streaming, current + no_order
    "sel_current":       dict(maintain_order=True,  filter=True,  slice=True,  head=True,  engine="streaming"),
    "sel_no_order":      dict(maintain_order=False, filter=True,  slice=True,  head=True,  engine="streaming"),
    # bisection: bare scan -> group_by -> collect (no slice/head), true vs false
    "bare_true":         dict(maintain_order=True,  filter=False, slice=False, head=False, engine="streaming"),
    "bare_false":        dict(maintain_order=False, filter=False, slice=False, head=False, engine="streaming"),
}


def fingerprint(rows: list[dict]) -> list:
    def nv(x):
        return round(float(x), 4) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
    norm = sorted(tuple(sorted((str(k), nv(val)) for k, val in row.items())) for row in rows)
    return [hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12], len(norm)]


def rss_bytes() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE


def run_child(name: str) -> dict:
    v = VARIANTS[name]
    lf = build_lf(v)

    samples: list[tuple[float, float]] = []  # (t_ms, rss_mb)
    stop = threading.Event()
    t0 = time.perf_counter()

    def sampler():
        while not stop.is_set():
            samples.append(((time.perf_counter() - t0) * 1000, rss_bytes() / 1e6))
            time.sleep(0.002)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    baseline_mb = rss_bytes() / 1e6
    t_pre = (time.perf_counter() - t0) * 1000

    tc = time.perf_counter()
    res = lf.collect(engine=v["engine"])
    collect_ms = (time.perf_counter() - tc) * 1000
    t_post = (time.perf_counter() - t0) * 1000

    time.sleep(0.3)  # keep sampling to expose allocator retention after collect
    stop.set()
    th.join()

    rows = res.to_dicts()
    peak_maxrss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000.0  # KB->MB on Linux
    peak_sampled_mb = max((s[1] for s in samples), default=0.0)

    # in-flight peak = max RSS observed between t_pre and t_post (during the collect)
    during = [s[1] for s in samples if t_pre <= s[0] <= t_post]
    inflight_peak_mb = max(during, default=peak_sampled_mb)
    # post-collect plateau (allocator retention): median RSS after t_post
    after = [s[1] for s in samples if s[0] > t_post]
    post_plateau_mb = (sorted(after)[len(after) // 2] if after else peak_sampled_mb)

    return {
        "name": name,
        "engine": v["engine"],
        "nrows": len(rows),
        "fingerprint": fingerprint(rows),
        "collect_ms": round(collect_ms, 1),
        "baseline_mb": round(baseline_mb, 1),
        "peak_maxrss_mb": round(peak_maxrss_mb, 1),
        "peak_sampled_mb": round(peak_sampled_mb, 1),
        "inflight_peak_mb": round(inflight_peak_mb, 1),
        "inflight_working_set_mb": round(inflight_peak_mb - baseline_mb, 1),
        "post_plateau_mb": round(post_plateau_mb, 1),
        "t_pre_ms": round(t_pre, 1),
        "t_post_ms": round(t_post, 1),
        "curve": [[round(t, 1), round(m, 1)] for t, m in samples],
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--child")
    args = ap.parse_args()
    if args.child:
        out = run_child(args.child)
        print("@@JSON@@" + json.dumps(out))
