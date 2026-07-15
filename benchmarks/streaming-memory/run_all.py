#!/usr/bin/env python3
"""Orchestrate the streaming/RSS investigation — all variants, one invocation, warm cache."""
from __future__ import annotations

import json
import os
import subprocess
import sys

import polars as pl

HERE = os.path.dirname(os.path.abspath(__file__))
HARNESS = os.path.join(HERE, "harness.py")
PATH = os.environ.get("BENCH_PATH", os.path.join(HERE, "bench_data.parquet"))
os.environ["BENCH_PATH"] = PATH

VARIANT_ORDER = [
    "full_current", "full_no_order", "full_inmem",
    "sel_current", "sel_no_order",
    "bare_true", "bare_false",
]
TRIALS = 2


def warm_cache():
    # touch every byte so the page cache is warm and equal for all variants
    n = 0
    with open(PATH, "rb") as f:
        while chunk := f.read(1 << 24):
            n += len(chunk)
    print(f"warmed page cache: read {n/1e9:.2f} GB", flush=True)


def child(name: str, verbose: bool) -> dict:
    env = dict(os.environ)
    if verbose:
        env["POLARS_VERBOSE"] = "1"
    proc = subprocess.run(
        [sys.executable, HARNESS, "--child", name],
        capture_output=True, text=True, env=env,
    )
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@JSON@@")), None)
    out = json.loads(line[len("@@JSON@@"):]) if line else {"name": name, "error": proc.stderr[-400:]}
    if verbose:
        out["verbose_stderr"] = proc.stderr
    return out


def main():
    print("polars", pl.__version__, "| file", f"{os.path.getsize(PATH)/1e9:.2f} GB",
          "| cores", os.cpu_count(), flush=True)
    warm_cache()

    # ---- peak RSS + curve, TRIALS per variant (trial-major so drift hits all equally)
    runs: dict[str, list[dict]] = {n: [] for n in VARIANT_ORDER}
    for t in range(TRIALS):
        print(f"trial {t+1}/{TRIALS}", flush=True)
        for name in VARIANT_ORDER:
            runs[name].append(child(name, verbose=False))

    # ---- one verbose pass per variant for the node graph
    verbose: dict[str, dict] = {}
    for name in VARIANT_ORDER:
        verbose[name] = child(name, verbose=True)

    # pick representative run per variant (median peak_maxrss); keep its curve
    def pick(name):
        rs = [r for r in runs[name] if "error" not in r]
        if not rs:
            return runs[name][0]
        return sorted(rs, key=lambda r: r["peak_maxrss_mb"])[len(rs) // 2]

    rep = {n: pick(n) for n in VARIANT_ORDER}
    out = {"polars": pl.__version__, "file_gb": os.path.getsize(PATH) / 1e9,
           "cores": os.cpu_count(), "trials": TRIALS,
           "runs": runs, "rep": rep, "verbose": verbose}
    with open(os.path.join(HERE, "results.json"), "w") as f:
        json.dump(out, f)

    # ================= REPORT =================
    def row(name):
        r = rep[name]
        if "error" in r:
            return f"{name:<16} ERROR: {r['error'][:60]}"
        peaks = [x["peak_maxrss_mb"] for x in runs[name] if "error" not in x]
        return (f"{name:<16} engine={r['engine']:<10} rows={r['nrows']:<4} "
                f"fp={r['fingerprint'][0]} | collect={r['collect_ms']:>7.1f}ms | "
                f"peak_maxrss={min(peaks):.0f}-{max(peaks):.0f}MB | "
                f"inflight_ws={r['inflight_working_set_mb']:>7.1f}MB | "
                f"post_plateau={r['post_plateau_mb']:>7.1f}MB")

    print("\n" + "=" * 100)
    print("PEAK RSS + IN-FLIGHT WORKING SET  (baseline subtracted for working set)")
    print("=" * 100)
    for name in VARIANT_ORDER:
        print(row(name))

    print("\n" + "=" * 100)
    print("CORRECTNESS  — fingerprints (digest, n_groups)")
    print("=" * 100)
    fp = {n: rep[n].get("fingerprint") for n in VARIANT_ORDER if "error" not in rep[n]}
    for n in VARIANT_ORDER:
        if "error" not in rep[n]:
            print(f"  {n:<16} {rep[n]['fingerprint']}")
    print("\n  full_current vs full_no_order identical as SET:",
          fp.get("full_current") == fp.get("full_no_order"))
    print("  sel_current  vs sel_no_order  identical as SET:",
          fp.get("sel_current") == fp.get("sel_no_order"))
    print("  full_current vs full_inmem    identical as SET:",
          fp.get("full_current") == fp.get("full_inmem"))

    print("\n" + "=" * 100)
    print("STREAMING NODE GRAPH (POLARS_VERBOSE) — 'running X in subgraph' lines")
    print("=" * 100)
    for name in VARIANT_ORDER:
        vs = verbose[name].get("verbose_stderr", "")
        nodes = [l.split("running", 1)[1].replace("in subgraph", "").strip()
                 for l in vs.splitlines() if "running" in l and "subgraph" in l]
        # dedup preserving order + count
        from collections import Counter
        c = Counter(nodes)
        fell_back = any("fallback" in l.lower() or "in-memory engine" in l.lower()
                        or "falling back" in l.lower() for l in vs.splitlines())
        print(f"  {name:<16} nodes={dict(c)}  fallback_msg={fell_back}")


if __name__ == "__main__":
    main()
