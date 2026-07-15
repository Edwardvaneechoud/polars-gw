#!/usr/bin/env python3
"""
polars-gw vs PyGWalker — a correctness-first, memory-and-latency benchmark.

Compares FOUR ways to answer Graphic Walker `IDataQueryPayload` queries over a
Parquet file:

  1. polars-gw (LazyFrame)          — scan_parquet, streaming collect  [lazy]
  2. polars-gw (DataFrame, eager)   — read_parquet once, query in memory [eager]
  3. pygwalker + DuckDB Connector   — GW's native SQL/DuckDB backend     [lazy]
  4. pygwalker (native Polars)      — GW's native PolarsDataFrameDataParser [eager]

What it measures, per path:
  * ONE-TIME cost      — load / connect, paid once per session
  * TIME-TO-FIRST      — one-time + the first query (what the user waits through)
  * PER-INTERACTION    — steady-state cost of one query, paid on every field drag
  * PEAK RSS           — feasibility; an OOM is a RESULT, not a slow number
  * RSS-over-time      — the memory profile of opening the file + first full-scan chart

Every path's result is FINGERPRINTED and compared. Timings across paths are only
meaningful if the answers agree; a mismatch is printed loudly and the run is
flagged. Each path runs in a FRESH SUBPROCESS so page-cache, thread-pool and
allocator state never leak between paths, and peak RSS is measured, not estimated.

Usage:
  python benchmark.py                         # 20M rows, warm cache, all paths
  python benchmark.py --rows 100000000        # the dramatic memory scale
  python benchmark.py --cold                  # evict page cache each rep (needs vmtouch/root)
  python benchmark.py --mem-limit-gb 4        # cap address space -> show the eager OOM
  python benchmark.py --no-viz                # skip chart generation
  python visualize.py                         # (re)draw charts from benchmark_results.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "bench_data.parquet")
PAGE = os.sysconf("SC_PAGE_SIZE")

# (label, is_lazy) — is_lazy drives the amortization table.
PATHS = [
    ("polars-gw (LazyFrame)", True),
    ("polars-gw (DataFrame, eager)", False),
    ("pygwalker + DuckDB Connector", True),
    ("pygwalker (native Polars)", False),
]
NAMES = [p[0] for p in PATHS]
IS_LAZY = dict(PATHS)

# Headline query for the RSS-over-time lifecycle curve.
HEADLINE = "full scan"


# ==========================================================================
# DATA
# ==========================================================================
def build(path: str, n_rows: int, chunk: int = 5_000_000) -> None:
    """Write a Parquet file with a SORTED key column (`ts`).

    Because `ts` is sorted its per-row-group min/max stats are tight, so a range
    filter on it lets the reader skip whole row groups — that is what makes the
    selective query fast. A BEST CASE, stated openly: an unsorted filter column
    would skip nothing.
    """
    import numpy as np
    import polars as pl
    import pyarrow.parquet as pq

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
                "pad1": rng.normal(0, 1, n),  # never queried -> projection pushdown
                "pad2": rng.normal(0, 1, n),
            }
        ).to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(path, tbl.schema, compression="snappy")
        writer.write_table(tbl, row_group_size=200_000)
    writer.close()
    print(f"wrote {n_rows:,} rows in {time.perf_counter() - t0:.0f}s -> {os.path.getsize(path)/1e9:.2f} GB")


def payloads(n_rows: int) -> dict[str, dict]:
    """The Graphic Walker IDataQueryPayloads, identical for every path."""
    hi = max(n_rows // 1000, 1)  # ~0.1% of rows
    return {
        # Interactive slice: a filtered aggregate. `ts` is sorted -> row groups skip.
        "selective": {
            "workflow": [
                {"type": "filter",
                 "filters": [{"fid": "ts", "rule": {"type": "range", "value": [0, hi]}}]},
                {"type": "view",
                 "query": [{"op": "aggregate", "groupBy": ["city"],
                            "measures": [{"field": "salary", "agg": "mean", "asFieldKey": "a"}]}]},
            ],
            "limit": 200,
        },
        # Worst case: aggregate every row. Nothing can skip.
        "full scan": {
            "workflow": [
                {"type": "view",
                 "query": [{"op": "aggregate", "groupBy": ["city"],
                            "measures": [{"field": "salary", "agg": "mean", "asFieldKey": "a"},
                                         {"field": "qty", "agg": "sum", "asFieldKey": "b"}]}]},
            ],
            "limit": 200,
        },
        # Distinct values of a dimension — what GW issues to populate a filter dropdown.
        "distinct": {
            "workflow": [
                {"type": "view",
                 "query": [{"op": "aggregate", "groupBy": ["city"], "measures": []}]},
            ],
            "limit": 200,
        },
    }


def drop_page_cache(path: str) -> bool:
    if subprocess.run(["which", "vmtouch"], capture_output=True).returncode == 0:
        subprocess.run(["vmtouch", "-e", path], capture_output=True)
        return True
    r = subprocess.run(["bash", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"], capture_output=True)
    return r.returncode == 0


# ==========================================================================
# CORRECTNESS
# ==========================================================================
def fingerprint(res) -> list:
    """Normalise any path's return value into a comparable [digest, nrows]."""
    rows = res
    try:
        import polars as pl
        if isinstance(res, pl.LazyFrame):
            rows = res.collect()
        if isinstance(rows, pl.DataFrame):
            rows = rows.to_dicts()
    except ImportError:
        pass
    if hasattr(rows, "to_dict") and not isinstance(rows, dict):   # pandas
        rows = rows.to_dict("records")
    if isinstance(rows, dict):                                    # column-oriented
        keys = list(rows)
        rows = [dict(zip(keys, vals)) for vals in zip(*(rows[k] for k in keys))]

    def norm_val(v):
        return round(float(v), 4) if isinstance(v, (int, float)) and not isinstance(v, bool) else v

    norm = sorted(tuple(sorted((str(k), norm_val(v)) for k, v in row.items())) for row in rows)
    digest = hashlib.sha1(json.dumps(norm, default=str).encode()).hexdigest()[:12]
    return [digest, len(norm)]


# ==========================================================================
# RSS sampler
# ==========================================================================
def rss_mb() -> float:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * PAGE / 1e6


class Sampler:
    def __init__(self, every=0.002):
        self.every, self.samples, self._stop = every, [], threading.Event()
        self.t0 = time.perf_counter()

    def _loop(self):
        while not self._stop.is_set():
            self.samples.append(((time.perf_counter() - self.t0) * 1000, rss_mb()))
            time.sleep(self.every)

    def __enter__(self):
        self.th = threading.Thread(target=self._loop, daemon=True)
        self.th.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self.th.join()

    def mark(self):
        return (time.perf_counter() - self.t0) * 1000


# ==========================================================================
# PATH FACTORIES — return a `run(payload)->result` callable, plus one_time_s
# ==========================================================================
def make_path(path_name: str):
    """Returns (run_fn, one_time_seconds). Raises if a dependency is missing."""
    import polars as pl

    if path_name == "polars-gw (LazyFrame)":
        from polars_gw import execute_workflow
        t = time.perf_counter()
        lf = pl.scan_parquet(PATH)            # metadata only
        return (lambda p: execute_workflow(lf, p)), (time.perf_counter() - t)

    if path_name == "polars-gw (DataFrame, eager)":
        from polars_gw import execute_workflow
        t = time.perf_counter()
        df = pl.read_parquet(PATH)            # materialise
        return (lambda p: execute_workflow(df, p)), (time.perf_counter() - t)

    if path_name == "pygwalker + DuckDB Connector":
        from pygwalker.data_parsers.database_parser import Connector, DatabaseDataParser
        t = time.perf_counter()
        conn = Connector("duckdb:///:memory:", f"SELECT * FROM '{PATH}'")
        dbp = DatabaseDataParser(conn, [], False, False, {})
        _ = dbp.field_metas
        return (lambda p: dbp.get_datas_by_payload(p)), (time.perf_counter() - t)

    if path_name == "pygwalker (native Polars)":
        import polars as pl
        from pygwalker.data_parsers.polars_parser import PolarsDataFrameDataParser
        t = time.perf_counter()
        df = pl.read_parquet(PATH)            # pygwalker REJECTS a LazyFrame -> mandatory
        parser = PolarsDataFrameDataParser(df, [], False, False, {})
        _ = parser.field_metas
        return (lambda p: parser.get_datas_by_payload(p)), (time.perf_counter() - t)

    raise ValueError(path_name)


def clear_result_cache():
    try:
        from polars_gw.executor import clear_cache
        clear_cache()
    except Exception:
        pass


# ==========================================================================
# CHILD MODES
# ==========================================================================
def child_timing(path_name: str, n_rows: int, reps: int, cold: bool, mem_limit_gb: float) -> dict:
    if mem_limit_gb:
        cap = int(mem_limit_gb * 1e9)
        resource.setrlimit(resource.RLIMIT_AS, (cap, cap))

    queries = payloads(n_rows)
    out = {"path": path_name, "one_time_s": 0.0, "first_ms": {}, "steady_ms": {}, "fingerprint": {}}
    try:
        run, one_time = make_path(path_name)
        out["one_time_s"] = one_time

        for name, p in queries.items():
            clear_result_cache()
            if cold:
                drop_page_cache(PATH)
            t = time.perf_counter()
            res = run(p)
            first = (time.perf_counter() - t) * 1000
            fp = fingerprint(res)
            ts = []
            for _ in range(reps):
                clear_result_cache()
                if cold:
                    drop_page_cache(PATH)
                t = time.perf_counter()
                run(p)
                ts.append((time.perf_counter() - t) * 1000)
            out["first_ms"][name] = first
            out["steady_ms"][name] = statistics.median(ts)
            out["fingerprint"][name] = fp
    except (MemoryError, OSError) as exc:
        out["error"] = "OOM"
        out["error_detail"] = f"{type(exc).__name__}: {exc}"
    except ImportError as exc:
        out["error"] = "MISSING_DEP"
        out["error_detail"] = str(exc)

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out["peak_rss_gb"] = rss / (1e6 if platform.system() == "Linux" else 1e9)
    return out


def child_curve(path_name: str, n_rows: int) -> dict:
    """Lifecycle RSS: sample from process start through load(+first HEADLINE query).
    This is the memory profile of 'open the file and render the first full-scan chart'."""
    queries = payloads(n_rows)
    out = {"path": path_name}
    try:
        with Sampler() as s:
            baseline = rss_mb()
            t_load0 = s.mark()
            run, one_time = make_path(path_name)   # LOAD happens here (eager) or ~0 (lazy)
            t_load1 = s.mark()
            run(queries[HEADLINE])                 # first full-scan query
            t_q1 = s.mark()
            time.sleep(0.25)                       # tail: expose allocator retention
        out.update(
            baseline_mb=round(baseline, 1),
            t_load_start=round(t_load0, 1), t_load_end=round(t_load1, 1), t_query_end=round(t_q1, 1),
            peak_mb=round(max(m for _, m in s.samples), 1),
            curve=[[round(t, 1), round(m, 1)] for t, m in s.samples],
        )
    except (MemoryError, OSError) as exc:
        out["error"] = "OOM"
    except ImportError:
        out["error"] = "MISSING_DEP"
    return out


# ==========================================================================
# PARENT
# ==========================================================================
def spawn(mode: str, path_name: str, args) -> dict:
    cmd = [sys.executable, os.path.abspath(__file__), "--_child", mode, "--_cpath", path_name,
           "--rows", str(args.rows), "--reps", str(args.reps),
           "--mem-limit-gb", str(args.mem_limit_gb)]
    if args.cold:
        cmd.append("--cold")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@JSON@@")), None)
    if line:
        return json.loads(line[len("@@JSON@@"):])
    return {"path": path_name, "error": "KILLED", "error_detail": proc.stderr.strip()[-200:],
            "one_time_s": 0.0, "peak_rss_gb": 0.0, "first_ms": {}, "steady_ms": {}, "fingerprint": {}}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=20_000_000)
    ap.add_argument("--trials", type=int, default=3, help="independent subprocesses per path")
    ap.add_argument("--reps", type=int, default=5, help="timed iterations within each subprocess (medianed)")
    ap.add_argument("--cold", action="store_true", help="evict page cache before each rep")
    ap.add_argument("--mem-limit-gb", type=float, default=0.0, help="cap address space; demonstrates the eager OOM")
    ap.add_argument("--session", type=str, default="1,10,50,200,1000")
    ap.add_argument("--mix", type=float, default=0.8, help="fraction of a session that is SELECTIVE queries")
    ap.add_argument("--no-viz", action="store_true", help="skip chart generation")
    ap.add_argument("--keep-data", action="store_true", help="keep the generated parquet file")
    # child dispatch (hidden)
    ap.add_argument("--_child", choices=["timing", "curve"], help=argparse.SUPPRESS)
    ap.add_argument("--_cpath", help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args._child == "timing":
        print("@@JSON@@" + json.dumps(child_timing(args._cpath, args.rows, args.reps, args.cold, args.mem_limit_gb)))
        return
    if args._child == "curve":
        print("@@JSON@@" + json.dumps(child_curve(args._cpath, args.rows)))
        return

    # -------------------- parent --------------------
    import polars as pl
    build(PATH, args.rows)
    qnames = list(payloads(args.rows))
    print(f"file: {os.path.getsize(PATH)/1e9:.2f} GB on disk, {args.rows:,} rows | polars {pl.__version__}")
    print(f"cores: {os.cpu_count()}  trials: {args.trials}  reps: {args.reps}  "
          f"cache: {'COLD' if args.cold else 'warm'}")
    if args.cold and not drop_page_cache(PATH):
        print("  !! cannot evict page cache (need vmtouch or root) — results are WARM")
    print()

    # warm the cache once, equally for all paths
    with open(PATH, "rb") as f:
        while f.read(1 << 24):
            pass

    # ---- timing: trial-major so machine drift hits every path equally
    trials: dict[str, list[dict]] = {n: [] for n in NAMES}
    for t in range(args.trials):
        print(f"  timing trial {t + 1}/{args.trials} ...", flush=True)
        for name in NAMES:
            trials[name].append(spawn("timing", name, args))

    # ---- RSS-over-time lifecycle curve, once per path
    print("  capturing RSS-over-time curves ...", flush=True)
    curves = {name: spawn("curve", name, args) for name in NAMES}

    # ---- aggregate
    def stat(runs, getter):
        vals = [getter(r) for r in runs if not r.get("error")]
        return (statistics.median(vals), min(vals), max(vals)) if vals else (0.0, 0.0, 0.0)

    R = {}
    for name in NAMES:
        runs = trials[name]
        errs = [r for r in runs if r.get("error")]
        rec: dict = {"path": name, "n": len(runs), "lazy": IS_LAZY[name]}
        if errs:
            rec["error"] = errs[0]["error"]
            rec["error_detail"] = errs[0].get("error_detail", "")
            rec["error_n"] = len(errs)
        rec["one_time_s"] = stat(runs, lambda r: r["one_time_s"])[0]
        rec["peak_rss_gb"] = stat(runs, lambda r: r["peak_rss_gb"])[0]
        rec["steady"] = {q: stat(runs, lambda r, q=q: r["steady_ms"][q]) for q in qnames}
        rec["first"] = {q: stat(runs, lambda r, q=q: r["first_ms"][q]) for q in qnames}
        fps = {q: {tuple(r["fingerprint"][q]) for r in runs if not r.get("error")} for q in qnames}
        rec["fp"] = {q: (list(fps[q])[0] if len(fps[q]) == 1 else None) for q in qnames}
        R[name] = rec

    out = {
        "meta": {
            "rows": args.rows, "file_gb": os.path.getsize(PATH) / 1e9, "cores": os.cpu_count(),
            "polars": pl.__version__, "trials": args.trials, "reps": args.reps,
            "cold": args.cold, "cache": "cold" if args.cold else "warm",
            "session_ns": [int(x) for x in args.session.split(",")], "mix": args.mix,
            "queries": qnames, "headline": HEADLINE, "names": NAMES, "is_lazy": IS_LAZY,
            "platform": platform.platform(),
        },
        "results": R, "curves": curves,
    }
    with open(os.path.join(HERE, "benchmark_results.json"), "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nwrote {os.path.join(HERE, 'benchmark_results.json')}")

    _print_report(R, out["meta"])

    if not args.keep_data:
        os.remove(PATH)

    if not args.no_viz:
        try:
            import visualize
            visualize.render(os.path.join(HERE, "benchmark_results.json"))
        except Exception as exc:  # noqa: BLE001
            print(f"  (viz skipped: {exc}; run `python visualize.py` after installing matplotlib)")


def _print_report(R: dict, meta: dict) -> None:
    qnames = meta["queries"]
    w = 32
    ok = [n for n in NAMES if not R[n].get("error")]

    print("\n" + "=" * 78)
    print("0. CORRECTNESS — do the paths agree?")
    print("=" * 78)
    for q in qnames:
        seen = {n: R[n]["fp"][q] for n in ok}
        distinct = {tuple(v) for v in seen.values() if v}
        if len(distinct) == 1:
            d, nrow = next(iter(distinct))
            print(f"  {q:<12} OK  all {len(ok)} paths agree (digest {d}, {nrow} rows)")
        else:
            print(f"  {q:<12} MISMATCH — timings below are NOT comparable:")
            for n, v in seen.items():
                print(f"      {n:<32} {v}")

    print("\n" + "=" * 78)
    print("1. ONE-TIME + FEASIBILITY (paid once per session)")
    print("=" * 78)
    print(f"{'path':<{w}}{'load':>9}{'peak RSS':>11}   status")
    for n in NAMES:
        r = R[n]
        status = f"{r['error']} ({r.get('error_n','?')}/{r['n']})" if r.get("error") else "ok"
        print(f"{n:<{w}}{r['one_time_s']:>8.2f}s{r['peak_rss_gb']:>8.2f} GB   {status}")

    print("\n" + "=" * 78)
    print("2. PER-INTERACTION (median ms; min-max in parens) — paid on every drag")
    print("=" * 78)
    print(f"{'path':<{w}}" + "".join(f"{q:>22}" for q in qnames))
    for n in NAMES:
        r = R[n]
        row = f"{n:<{w}}"
        for q in qnames:
            if r.get("error"):
                row += f"{r['error']:>22}"
            else:
                med, lo, hi = r["steady"][q]
                row += f"{med:>10.1f}ms {f'({lo:.0f}-{hi:.0f})':>10}"
        print(row)
    print("\n  If two ranges OVERLAP, do not claim one path is faster.")


if __name__ == "__main__":
    main()
