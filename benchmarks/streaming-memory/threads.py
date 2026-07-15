#!/usr/bin/env python3
"""Does the streaming full-scan working set scale with thread-pool size?
Plus a correct DuckDB same-machine reference. One invocation, warm cache."""
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


def rss_mb():
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE / 1e6


def fingerprint(rows):
    def nv(x):
        return round(float(x), 4) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
    norm = sorted(tuple(sorted((str(k), nv(v)) for k, v in r.items())) for r in rows)
    return [hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12], len(norm)]


def sampled(fn):
    samples, stop, t0 = [], threading.Event(), time.perf_counter()

    def s():
        while not stop.is_set():
            samples.append(((time.perf_counter() - t0) * 1000, rss_mb()))
            time.sleep(0.002)
    th = threading.Thread(target=s, daemon=True); th.start()
    base = rss_mb(); t_pre = (time.perf_counter() - t0) * 1000
    tc = time.perf_counter(); rows = fn(); collect_ms = (time.perf_counter() - tc) * 1000
    t_post = (time.perf_counter() - t0) * 1000
    time.sleep(0.25); stop.set(); th.join()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000.0
    hi = max((m for t, m in samples if t_pre <= t <= t_post), default=0)
    return {"nrows": len(rows), "fingerprint": fingerprint(rows), "collect_ms": round(collect_ms, 1),
            "baseline_mb": round(base, 1), "peak_maxrss_mb": round(peak, 1),
            "inflight_ws_mb": round(hi - base, 1),
            "curve": [[round(t, 1), round(m, 1)] for t, m in samples]}


def child():
    mode = os.environ.get("MODE", "polars")
    if mode == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute(f"SET threads={os.environ.get('DUCK_THREADS','4')}")
        q = f"SELECT city, avg(salary) AS a, sum(qty) AS b FROM '{PATH}' GROUP BY city LIMIT 200"

        def fn():
            rel = con.sql(q)
            cols = rel.columns
            return [dict(zip(cols, r)) for r in rel.fetchall()]
        out = sampled(fn)
    else:
        lf = pl.scan_parquet(PATH).group_by(["city"], maintain_order=True).agg(
            [pl.col("salary").mean().alias("a"), pl.col("qty").sum().alias("b")]
        ).slice(0, 200).head(1_000_000)

        def fn():
            return lf.collect(engine="streaming").to_dicts()
        out = sampled(fn)
    out["threads"] = pl.thread_pool_size() if mode == "polars" else os.environ.get("DUCK_THREADS")
    print("@@JSON@@" + json.dumps(out))


CONFIGS = [
    ("polars_t1", {"POLARS_MAX_THREADS": "1"}),
    ("polars_t2", {"POLARS_MAX_THREADS": "2"}),
    ("polars_t4", {"POLARS_MAX_THREADS": "4"}),
    ("duckdb_t4", {"MODE": "duckdb", "DUCK_THREADS": "4"}),
    ("duckdb_t1", {"MODE": "duckdb", "DUCK_THREADS": "1"}),
]


def main():
    print("polars", pl.__version__, "| file", f"{os.path.getsize(PATH)/1e9:.2f} GB", "| cores", os.cpu_count())
    with open(PATH, "rb") as f:  # warm
        while c := f.read(1 << 24):
            pass
    R = {}
    for label, env in CONFIGS:
        best = None
        for _ in range(2):
            e = dict(os.environ); e.update(env); e["MODE"] = env.get("MODE", "polars")
            p = subprocess.run([sys.executable, __file__, "--child"], capture_output=True, text=True, env=e)
            line = next((l for l in p.stdout.splitlines() if l.startswith("@@JSON@@")), None)
            if not line:
                best = {"error": p.stderr[-300:]}; break
            r = json.loads(line[len("@@JSON@@"):])
            if best is None or r["peak_maxrss_mb"] < best["peak_maxrss_mb"]:
                best = r
        R[label] = {"env": env, **best}
    json.dump(R, open(os.path.join(HERE, "threads_results.json"), "w"))
    print("\n" + "=" * 92)
    print(f"{'config':<12}{'threads':>8}{'peak_maxrss':>14}{'inflight_ws':>14}{'collect':>11}{'fp':>16}")
    print("=" * 92)
    for label, _ in CONFIGS:
        r = R[label]
        if "error" in r:
            print(f"{label:<12} ERROR {r['error'][:60]}"); continue
        print(f"{label:<12}{str(r['threads']):>8}{r['peak_maxrss_mb']:>11.0f}MB{r['inflight_ws_mb']:>11.0f}MB"
              f"{r['collect_ms']:>9.0f}ms  {r['fingerprint'][0]}")


if __name__ == "__main__":
    child() if "--child" in sys.argv else main()
