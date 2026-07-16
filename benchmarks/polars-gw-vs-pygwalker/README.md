# polars-gw vs PyGWalker — a benchmark you can run for the blog

Two files. One runs the benchmark, one draws the charts.

```bash
pip install -r requirements.txt          # or: uv pip install -r requirements.txt

python benchmark.py                       # 20M rows, warm cache, all four paths
python benchmark.py --rows 100000000      # the dramatic memory scale (needs ~10 GB free RAM)
python benchmark.py --cold                # evict page cache each rep (needs vmtouch or root)
python benchmark.py --mem-limit-gb 4      # cap address space → force the eager OOM

python visualize.py                       # (re)draw charts from benchmark_results.json
```

`benchmark.py` writes **`benchmark_results.json`** and, unless `--no-viz`, calls
`visualize.py`, which writes five PNG+SVG charts into **`charts/`**. Both the JSON and the
`charts/` directory are generated outputs and are **git-ignored** — run the benchmark to produce
them on your own machine; they are not committed.

## What it compares

Four ways to answer the same Graphic Walker `IDataQueryPayload` queries over one Parquet file:

| # | path | how | lazy? |
|---|------|-----|-------|
| 1 | **polars-gw (LazyFrame)** | `scan_parquet` → streaming `collect` | lazy |
| 2 | **polars-gw (DataFrame, eager)** | `read_parquet` once, query in memory | eager |
| 3 | **pygwalker + DuckDB Connector** | GW's native SQL backend over DuckDB | lazy |
| 4 | **pygwalker (native Polars)** | GW's native `PolarsDataFrameDataParser` | eager |

Paths 3 and 4 are **PyGWalker's own native implementations** — the honest baseline polars-gw
has to beat. All four run the identical GW payloads so the answers are directly comparable.

Three query shapes, each a real GW interaction:

- **selective** — a filtered aggregate (`ts BETWEEN …` then `group_by(city).mean(salary)`). The
  common interactive slice. `ts` is sorted, so the lazy scan skips row groups.
- **full scan** — `group_by(city).agg(mean(salary), sum(qty))` over every row. The worst case:
  nothing can be skipped.
- **distinct** — `group_by(city)` with no measures: what GW issues to fill a filter dropdown.

## What it measures — and why in this shape

The tool separates four things the eye tends to blur together:

1. **One-time cost** — load / connect. Paid once when the notebook cell runs.
2. **Per-interaction** — steady-state cost of one query. Paid on *every* field drag.
3. **Time to first chart** — one-time + the *first* query. What the user actually waits through
   (most benchmarks measure this and then discard it as "warmup").
4. **Peak RSS + RSS-over-time** — feasibility. A path that OOMs is not slow, it is *unavailable*;
   you do not average that away.

Then it combines 1 and 2 into `session(n) = one_time + n · per_interaction` and shows where the
lines cross, so "which is faster" becomes "faster *for how long a session*".

**Correctness first.** Every path's result is fingerprinted (sorted rows, 4-dp rounding, SHA-1)
and compared. If the fingerprints diverge, the run is flagged and the timings are not to be
trusted — a benchmark that does not check the answers may be timing a query that silently does
nothing.

**Isolation.** Each path runs in a **fresh subprocess**, in **trial-major order** (one trial of
every path, then the next), so page-cache warming, thread-pool spin-up and allocator state never
leak between paths and machine drift hits everyone equally. Peak RSS is read from the kernel
(`ru_maxrss`); the RSS-over-time curve is sampled from `/proc/self/statm` every 2 ms.

## The five charts

| file | what it shows |
|------|---------------|
| `charts/1_rss_over_time.png` | RSS from process start through load + first full-scan chart — the memory *profile* of each path |
| `charts/2_peak_rss.png` | peak memory per path (an OOM is drawn as unavailable, not as a bar) |
| `charts/3_per_interaction_latency.png` | steady-state ms per query, one panel per query shape, min–max whiskers |
| `charts/4_time_to_first_chart.png` | load + first query — the latency the user experiences on open |
| `charts/5_session_amortization.png` | `session(n)` for a realistic 80% selective / 20% full-scan mix, log-n |

Colors are the data-viz skill's validated categorical set (blue / green / magenta / yellow),
CVD-checked all-pairs; the two lower-contrast slots also carry a distinct line style + marker +
direct value labels, so identity never rests on color alone.

## Honest caveats (put these in the post)

- **Warm cache flatters the lazy paths.** They re-read the file on every interaction, which is
  nearly free from the OS page cache and expensive from disk or object storage. Run `--cold`
  before trusting the per-interaction column on full scans.
- **`ts` is sorted**, so the selective filter skips row groups — a stated best case. An unsorted
  filter column would skip nothing.
- **PyGWalker cannot accept a `LazyFrame`** (it needs a materialised frame or a SQL source), so
  its eager path's `read_parquet` is mandatory, not a choice — which is why its ceiling is your
  RAM: as the frame approaches available memory the OS spills to swap and it thrashes (a swapless
  box OOM-kills, which is what `--mem-limit-gb` simulates by capping address space).
- **Peak RSS is per-process and per-shape, and which fast path wins is hardware-dependent.**
  Report your own machine's numbers; do not inherit anyone else's.
- Polars ships mimalloc, which does not return freed pages to the OS promptly, so RSS staying
  high after a collect can be the allocator, not the live working set. The RSS-over-time curve is
  read *during* the collect for exactly this reason.

## Results from this run

50,000,000 rows / 1.78 GB, 4 cores, polars 1.39.3, **warm** cache, median of 3×5. Regenerate
with your own `--rows` and machine — these numbers are hardware-specific.

| path | peak RSS | load | selective | full scan | distinct |
|---|---|---|---|---|---|
| **polars-gw (LazyFrame)** | **1.18 GB** | 0.00s | **7 ms** | 424 ms | 276 ms |
| polars-gw (DataFrame, eager) | 5.23 GB | 0.78s | 19 ms | 301 ms | 729 ms |
| pygwalker + DuckDB Connector | 1.18 GB | 0.11s | 14 ms | **202 ms** | **68 ms** |
| pygwalker (native Polars) | 5.40 GB | 1.63s | 675 ms | 1039 ms | 976 ms |

Correctness: all four paths agree on every query (fingerprints match), so the timings are
comparable. Takeaways from *this* machine (yours will differ):

- **polars-gw (LazyFrame) wins the selective query** — the common interactive slice — at ~7 ms,
  and ties the DuckDB Connector for lowest memory (1.18 GB, both lazy). It also reaches the first
  chart fastest of the Polars options (chart 4).
- **The two eager paths cost ~4× the memory** (5.2 GB vs 1.2 GB) — they hold the whole frame
  resident, so their ceiling is your RAM: past it they thrash on swap (and, on a swapless box,
  OOM-kill). PyGWalker's native parsers *require* a materialised frame, so that is their only door
  in. `--mem-limit-gb 4` reproduces the hard-cap failure.
- **PyGWalker's native Polars parser is the slowest path on every query** (671 / 1023 / 986 ms)
  *and* eager. polars-gw beats it on latency and memory simultaneously.
- **The DuckDB Connector is genuinely strong** on full scans (198 ms, and the leanest single-query
  memory profile in chart 1) — but it needs a table/file to point `SELECT … FROM` at; it can't
  consume a derived in-memory `LazyFrame`. On full scans polars-gw's streaming pays for the
  re-read (426 ms); on filtered slices it pulls ahead.

The picture in one line: **polars-gw gives you the lazy path's low memory with the fastest
filtered-query latency, and — unlike either PyGWalker backend — accepts a `LazyFrame` that has no
file behind it.**
