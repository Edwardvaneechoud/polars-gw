#!/usr/bin/env python3
"""Test memory knobs on the streaming full-scan group_by, + DuckDB reference.

Each config runs in a fresh subprocess (clean peak RSS) with the RSS sampler.
The parent sets the POLARS_* env knobs before spawning; the child reads
SCAN_LOW_MEMORY / ENGINE / MODE from env.
"""
from __future__ import annotations

import hashlib
import json
import os
import resource
import subprocess
import sys
import threading
import time

import polars as pl

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.environ.get("BENCH_PATH", os.path.join(HERE, "bench_data.parquet"))
os.environ["BENCH_PATH"] = PATH
PAGE = os.sysconf("SC_PAGE_SIZE")


def rss_bytes() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE


def fingerprint(rows):
    def nv(x):
        return round(float(x), 4) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
    norm = sorted(tuple(sorted((str(k), nv(v)) for k, v in r.items())) for r in rows)
    return [hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12], len(norm)]


def sampled(fn):
    samples = []
    stop = threading.Event()
    t0 = time.perf_counter()

    def sampler():
        while not stop.is_set():
            samples.append(((time.perf_counter() - t0) * 1000, rss_bytes() / 1e6))
            time.sleep(0.002)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    baseline = rss_bytes() / 1e6
    t_pre = (time.perf_counter() - t0) * 1000
    tc = time.perf_counter()
    rows = fn()
    collect_ms = (time.perf_counter() - tc) * 1000
    t_post = (time.perf_counter() - t0) * 1000
    time.sleep(0.3)
    stop.set()
    th.join()
    peak_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000.0
    during = [m for t, m in samples if t_pre <= t <= t_post]
    hi = max(during, default=0)
    # ramp check
    span = max(t_post - t_pre, 1)
    q1 = max([m for t, m in samples if t_pre <= t <= t_pre + 0.25 * span], default=baseline)
    q3 = max([m for t, m in samples if t_pre + 0.75 * span <= t <= t_post], default=hi)
    return {
        "nrows": len(rows), "fingerprint": fingerprint(rows), "collect_ms": round(collect_ms, 1),
        "baseline_mb": round(baseline, 1), "peak_maxrss_mb": round(peak_maxrss, 1),
        "inflight_peak_mb": round(hi, 1), "inflight_ws_mb": round(hi - baseline, 1),
        "q25_mb": round(q1, 1), "q75_mb": round(q3, 1),
        "shape": "RAMP" if (q3 - q1) > 0.4 * (hi - baseline) else "PLATEAU",
        "curve": [[round(t, 1), round(m, 1)] for t, m in samples],
    }


def child():
    mode = os.environ.get("MODE", "polars")
    if mode == "duckdb":
        import duckdb
        q = f"SELECT city, avg(salary) AS a, sum(qty) AS b FROM '{PATH}' GROUP BY city LIMIT 200"

        def fn():
            return duckdb.sql(q).fetchall()
        out = sampled(fn)
        # duckdb fetchall returns tuples; re-fingerprint via dict for comparability
        con = duckdb.connect()
        rows = [dict(zip(["city", "a", "b"], r)) for r in con.sql(q).fetchall()]
        out["fingerprint"] = fingerprint(rows)
    else:
        low = os.environ.get("SCAN_LOW_MEMORY") == "1"
        engine = os.environ.get("ENGINE", "streaming")
        lf = pl.scan_parquet(PATH, low_memory=low)
        lf = lf.group_by(["city"], maintain_order=True).agg(
            [pl.col("salary").mean().alias("a"), pl.col("qty").sum().alias("b")]
        ).slice(0, 200).head(1_000_000)

        def fn():
            return lf.collect(engine=engine).to_dicts()
        out = sampled(fn)
    print("@@JSON@@" + json.dumps(out))


CONFIGS = [
    # (label, env-overrides, description)
    ("baseline",          {},                                             "scan default, streaming"),
    ("low_memory",        {"SCAN_LOW_MEMORY": "1"},                       "scan_parquet(low_memory=True)"),
    ("prefetch1",         {"POLARS_PREFETCH_SIZE": "1"},                  "1 row-group prefetched at a time"),
    ("prefetch2",         {"POLARS_PREFETCH_SIZE": "2"},                  "2 row-groups prefetched"),
    ("lowmem+prefetch1",  {"SCAN_LOW_MEMORY": "1", "POLARS_PREFETCH_SIZE": "1"}, "both"),
    ("chunk_50k",         {"POLARS_STREAMING_CHUNK_SIZE": "50000"},       "small morsel size"),
    ("mimalloc_purge",    {"MIMALLOC_PURGE_DELAY": "0"},                  "force mimalloc to return pages"),
    ("duckdb",            {"MODE": "duckdb"},                             "DuckDB same-machine reference"),
]


def warm():
    n = 0
    with open(PATH, "rb") as f:
        while c := f.read(1 << 24):
            n += len(c)


def parent():
    print("polars", pl.__version__, "| file", f"{os.path.getsize(PATH)/1e9:.2f} GB", "| cores", os.cpu_count())
    warm()
    results = {}
    TRIALS = 2
    for label, env, desc in CONFIGS:
        best = None
        for _ in range(TRIALS):
            e = dict(os.environ)
            e.update(env)
            e["MODE"] = env.get("MODE", "polars")
            proc = subprocess.run([sys.executable, __file__, "--child"], capture_output=True, text=True, env=e)
            line = next((l for l in proc.stdout.splitlines() if l.startswith("@@JSON@@")), None)
            if not line:
                best = {"error": proc.stderr[-300:]}
                break
            r = json.loads(line[len("@@JSON@@"):])
            if best is None or r["peak_maxrss_mb"] < best["peak_maxrss_mb"]:
                best = r
        results[label] = {"desc": desc, **best}
    json.dump(results, open(os.path.join(HERE, "knobs_results.json"), "w"))

    print("\n" + "=" * 108)
    print(f"{'config':<18}{'desc':<38}{'peak_maxrss':>13}{'inflight_ws':>13}{'shape':>9}{'collect':>10}{'fp':>15}")
    print("=" * 108)
    for label, _, _ in CONFIGS:
        r = results[label]
        if "error" in r:
            print(f"{label:<18} ERROR {r['error'][:60]}")
            continue
        print(f"{label:<18}{r['desc']:<38}{r['peak_maxrss_mb']:>10.0f}MB{r['inflight_ws_mb']:>10.0f}MB"
              f"{r['shape']:>9}{r['collect_ms']:>8.0f}ms  {r['fingerprint'][0]}")


if __name__ == "__main__":
    if "--child" in sys.argv:
        child()
    else:
        parent()
