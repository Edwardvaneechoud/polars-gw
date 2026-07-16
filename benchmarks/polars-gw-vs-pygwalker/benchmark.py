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

`--rows` takes a comma-separated LADDER of scales, so one run answers the real
question — *when* to use what — instead of a single point. Charts 6 and 7 plot
latency and peak memory against data size across the ladder.

Usage:
  python benchmark.py                         # full ladder: 100K -> 200M rows (long run!)
  python benchmark.py --rows 20_000_000       # one scale, quick
  python benchmark.py --rows 1_000_000,100_000_000   # your own ladder
  python benchmark.py --rows 500_000_000 --merge     # add one rung to existing results
  python benchmark.py --cold                  # evict page cache each rep (needs vmtouch/root)
  python benchmark.py --mem-limit-gb 4        # cap address space -> show the eager MemoryError
  python benchmark.py --path-timeout 600      # kill a path subprocess after 10 min (thrash guard)
  python benchmark.py --no-viz                # skip chart generation
  python visualize.py                         # (re)draw charts from benchmark_results.json

Disk note: each scale builds its own parquet file (200M rows ~ 7.1 GB); files are
deleted after their scale finishes unless --keep-data.
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
PAGE = os.sysconf("SC_PAGE_SIZE")


def data_path(n_rows: int) -> str:
    """Per-scale data file, so a ladder run can build/evict each scale independently."""
    return os.path.join(HERE, f"bench_data_{n_rows}.parquet")


# Module-level so every helper sees the current scale's file. The parent sets it
# per ladder rung; a child sets it once from its --rows before running.
PATH = data_path(0)

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
# RSS sampler  (cross-platform: psutil if present, else Linux /proc, else rusage)
# ==========================================================================
try:
    import psutil

    _PROC = psutil.Process()

    def rss_mb() -> float:
        return _PROC.memory_info().rss / 1e6
except ImportError:
    def rss_mb() -> float:
        try:  # Linux
            with open("/proc/self/statm") as f:
                return int(f.read().split()[1]) * PAGE / 1e6
        except OSError:  # macOS/other without psutil: monotonic high-water mark
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return rss / (1e3 if platform.system() == "Linux" else 1e6)


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
    except (MemoryError, OSError):
        out["error"] = "OOM"
    except ImportError:
        out["error"] = "MISSING_DEP"
    return out


# ==========================================================================
# PARENT
# ==========================================================================
def _err_record(path_name: str, error: str, detail: str) -> dict:
    return {"path": path_name, "error": error, "error_detail": detail,
            "one_time_s": 0.0, "peak_rss_gb": 0.0, "first_ms": {}, "steady_ms": {}, "fingerprint": {}}


def spawn(mode: str, path_name: str, n_rows: int, args) -> dict:
    cmd = [sys.executable, os.path.abspath(__file__), "--_child", mode, "--_cpath", path_name,
           "--rows", str(n_rows), "--reps", str(args.reps),
           "--mem-limit-gb", str(args.mem_limit_gb)]
    if args.cold:
        cmd.append("--cold")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=args.path_timeout)
    except subprocess.TimeoutExpired:
        # A RESULT, not a crash: at scales near RAM the eager paths swap-thrash
        # rather than finishing. The timeout converts "unusably slow" into data.
        return _err_record(path_name, "TIMEOUT",
                           f"exceeded --path-timeout {args.path_timeout:.0f}s (likely swap-thrash at this scale)")
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("@@JSON@@")), None)
    if line:
        return json.loads(line[len("@@JSON@@"):])
    return _err_record(path_name, "KILLED", proc.stderr.strip()[-200:])


def aggregate(trials: dict[str, list[dict]], qnames: list[str]) -> dict:
    """Median/min/max per path across the trial subprocesses."""
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
    return R


DEFAULT_LADDER = "100_000,1_000_000,10_000_000,100_000_000,200_000_000"


def main() -> None:
    global PATH
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=str, default=DEFAULT_LADDER,
                    help="comma-separated ladder of row counts (underscores ok); one value = single scale")
    ap.add_argument("--trials", type=int, default=3, help="independent subprocesses per path, per scale")
    ap.add_argument("--reps", type=int, default=5, help="timed iterations within each subprocess (medianed)")
    ap.add_argument("--cold", action="store_true", help="evict page cache before each rep")
    ap.add_argument("--mem-limit-gb", type=float, default=0.0,
                    help="cap address space; forces the eager MemoryError failure mode")
    ap.add_argument("--path-timeout", type=float, default=900.0,
                    help="kill any single path subprocess after this many seconds (swap-thrash guard)")
    ap.add_argument("--session", type=str, default="1,10,50,200,1000")
    ap.add_argument("--mix", type=float, default=0.8, help="fraction of a session that is SELECTIVE queries")
    ap.add_argument("--merge", action="store_true",
                    help="merge this run's scales into an existing benchmark_results.json "
                         "instead of overwriting it (re-run scales are replaced)")
    ap.add_argument("--no-viz", action="store_true", help="skip chart generation")
    ap.add_argument("--keep-data", action="store_true", help="keep the generated parquet files")
    # child dispatch (hidden)
    ap.add_argument("--_child", choices=["timing", "curve"], help=argparse.SUPPRESS)
    ap.add_argument("--_cpath", help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args._child:  # children always receive a single scale in --rows
        n = int(args.rows)
        PATH = data_path(n)
        if args._child == "timing":
            res = child_timing(args._cpath, n, args.reps, args.cold, args.mem_limit_gb)
        else:
            res = child_curve(args._cpath, n)
        print("@@JSON@@" + json.dumps(res))
        return

    # -------------------- parent --------------------
    import polars as pl
    scales = [int(s) for s in args.rows.split(",") if s.strip()]
    qnames = list(payloads(scales[0]))
    try:
        import psutil
        total_ram_gb = psutil.virtual_memory().total / 1e9
    except ImportError:
        total_ram_gb = None

    print(f"scales: {', '.join(f'{n:,}' for n in scales)}")
    print(f"polars {pl.__version__} | cores {os.cpu_count()}"
          + (f" | RAM {total_ram_gb:.0f} GB" if total_ram_gb else "")
          + f" | trials {args.trials} x reps {args.reps} | cache {'COLD' if args.cold else 'warm'}"
          + f" | path-timeout {args.path_timeout:.0f}s")

    scales_out: dict[str, dict] = {}
    for n in scales:
        PATH = data_path(n)
        print("\n" + "#" * 78)
        print(f"# SCALE: {n:,} rows")
        print("#" * 78)
        build(PATH, n)
        file_gb = os.path.getsize(PATH) / 1e9
        if args.cold and not drop_page_cache(PATH):
            print("  !! cannot evict page cache (need vmtouch or root) — results are WARM")

        # warm the cache once, equally for all paths
        with open(PATH, "rb") as f:
            while f.read(1 << 24):
                pass

        # Trial-major so machine drift hits every path equally. A path that fails
        # hard (TIMEOUT / OOM / KILLED) is skipped for its remaining trials at
        # this scale — at near-RAM scales each doomed eager trial would otherwise
        # burn the full --path-timeout again to learn the same thing.
        trials: dict[str, list[dict]] = {name: [] for name in NAMES}
        failed: dict[str, dict] = {}
        for t in range(args.trials):
            print(f"  timing trial {t + 1}/{args.trials} ...", flush=True)
            for name in NAMES:
                if name in failed:
                    trials[name].append(failed[name])
                    continue
                res = spawn("timing", name, n, args)
                trials[name].append(res)
                if res.get("error") in ("TIMEOUT", "OOM", "KILLED"):
                    print(f"    !! {name}: {res['error']} — skipping its remaining trials at this scale",
                          flush=True)
                    failed[name] = res

        print("  capturing RSS-over-time curves ...", flush=True)
        curves = {name: (dict(failed[name]) if name in failed else spawn("curve", name, n, args))
                  for name in NAMES}

        R = aggregate(trials, qnames)
        scales_out[str(n)] = {"rows": n, "file_gb": file_gb, "trials": args.trials, "reps": args.reps,
                              "results": R, "curves": curves}
        _print_report(R, qnames, f"{n:,} rows / {file_gb:.2f} GB")

        if not args.keep_data:
            os.remove(PATH)

    results_path = os.path.join(HERE, "benchmark_results.json")
    if args.merge and os.path.exists(results_path):
        with open(results_path) as f:
            prior = json.load(f)
        if "scales" in prior:
            # comparability check: merged numbers only mean something on the same setup
            now = {"cores": os.cpu_count(), "polars": pl.__version__,
                   "cache": "cold" if args.cold else "warm", "platform": platform.platform()}
            pm = prior.get("meta", {})
            for k, v in now.items():
                if pm.get(k) is not None and pm.get(k) != v:
                    print(f"  !! merge warning: prior results have {k}={pm.get(k)!r}, this run {v!r} — "
                          "cross-scale charts will mix setups")
            merged = dict(prior["scales"])
            merged.update(scales_out)  # re-run scales replace their prior entry
            scales_out = merged
            print(f"merged into existing results: {len(scales_out)} scale(s) total")
        else:
            print("  (existing results file has no 'scales' key — overwriting)")
    elif os.path.exists(results_path):
        try:
            with open(results_path) as f:
                prior_keys = set(json.load(f).get("scales", {}))
        except (json.JSONDecodeError, OSError):
            prior_keys = set()
        dropped = prior_keys - {str(n) for n in scales}
        if dropped:
            print(f"note: overwriting previous results that also had scales "
                  f"{sorted(int(s) for s in dropped)} — pass --merge to combine instead")

    out = {
        "meta": {
            "rows_scales": sorted(int(k) for k in scales_out),
            "cores": os.cpu_count(), "total_ram_gb": total_ram_gb,
            "polars": pl.__version__, "trials": args.trials, "reps": args.reps,
            "cold": args.cold, "cache": "cold" if args.cold else "warm",
            "path_timeout_s": args.path_timeout,
            "session_ns": [int(x) for x in args.session.split(",")], "mix": args.mix,
            "queries": qnames, "headline": HEADLINE, "names": NAMES, "is_lazy": IS_LAZY,
            "platform": platform.platform(),
        },
        "scales": scales_out,
    }
    with open(results_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nwrote {results_path}")

    _print_scaling_summary(scales_out, qnames)

    if not args.no_viz:
        try:
            import visualize
            visualize.render(os.path.join(HERE, "benchmark_results.json"))
        except Exception as exc:  # noqa: BLE001
            print(f"  (viz skipped: {exc}; run `python visualize.py` after installing matplotlib)")


def _print_report(R: dict, qnames: list[str], subtitle: str) -> None:
    w = 32
    ok = [n for n in NAMES if not R[n].get("error")]

    print("\n" + "=" * 78)
    print(f"0. CORRECTNESS — do the paths agree?  [{subtitle}]")
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


SHORT = {
    "polars-gw (LazyFrame)": "pgw-lazy",
    "polars-gw (DataFrame, eager)": "pgw-eager",
    "pygwalker + DuckDB Connector": "duckdb",
    "pygwalker (native Polars)": "gw-polars",
}


def _print_scaling_summary(scales_out: dict[str, dict], qnames: list[str]) -> None:
    """The 'when to use what' table: per scale, fastest path per query + the memory split."""
    if len(scales_out) < 2:
        return
    print("\n" + "=" * 96)
    print("SCALING SUMMARY — fastest path per query at each scale (see charts 6-7)")
    print("=" * 96)
    print(f"{'rows':>13}{'file':>9}" + "".join(f"{q:>24}" for q in qnames) + f"{'RSS lazy|eager':>18}")
    print("-" * 96)
    failures: list[str] = []
    for key in sorted(scales_out, key=int):
        sc = scales_out[key]
        R = sc["results"]
        row = f"{sc['rows']:>13,}{sc['file_gb']:>7.2f}GB"
        for q in qnames:
            best, best_ms = None, None
            for name in NAMES:
                r = R[name]
                if r.get("error"):
                    continue
                ms = r["steady"][q][0]
                if best_ms is None or ms < best_ms:
                    best, best_ms = name, ms
            row += f"{SHORT.get(best, '—'):>14}{f'{best_ms:,.1f}ms' if best_ms is not None else '':>10}"
        lazy_peaks = [R[n]["peak_rss_gb"] for n in NAMES if IS_LAZY[n] and not R[n].get("error")]
        eager_peaks = [R[n]["peak_rss_gb"] for n in NAMES if not IS_LAZY[n] and not R[n].get("error")]
        lz = f"{max(lazy_peaks):.1f}" if lazy_peaks else "—"
        eg = f"{max(eager_peaks):.1f}" if eager_peaks else "—"
        row += f"{lz + '|' + eg + ' GB':>18}"
        print(row)
        for name in NAMES:
            if R[name].get("error"):
                failures.append(f"  ✗ {name} at {sc['rows']:,} rows: {R[name]['error']}"
                                f" ({R[name].get('error_detail', '')[:70]})")
    if failures:
        print("\nPaths that did not complete:")
        for f_line in failures:
            print(f_line)
    print("\n  Winners flip with scale — that is the point. Small data: everything is fast and")
    print("  eager is fine. As rows approach RAM, the eager paths fall off a cliff (swap-thrash")
    print("  or timeout) while the lazy paths keep working. Charts 6-7 draw this.")


if __name__ == "__main__":
    main()
