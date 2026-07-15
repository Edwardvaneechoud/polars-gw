# Does polars-gw actually stream, and why is peak RSS ~3.5× DuckDB's?

**Verdict up front:** Streaming **is** running — there is no silent fallback. `maintain_order=True`
is **not** the cause (removing it is a measured wash). The ~1.5 GB peak is a **real, live,
linearly-accumulating working set** inside Polars' new streaming engine when it does a
*full-scan* `parquet → group_by`. No user-facing Polars knob or available version fixes it, and
DuckDB does the byte-identical query in ~0.11 GB. **The memory argument against DuckDB is
therefore dead on unfiltered full scans** — but it holds on *filtered* queries (the common
interactive case), where Polars streams flat at ~0.1 GB.

Every number below was produced in this session on one machine; nothing is inherited from the
prompt's table.

## Setup (pinned)

| | |
|---|---|
| Machine | 4 cores, 15 GB RAM, Linux 6.18.5, warm page cache |
| **Polars** | **1.39.3** (installed) — cross-checked on **1.42.1** (the benchmark's pin): identical behaviour |
| DuckDB / PyArrow / NumPy | 1.5.4 / 25.0.0 / 2.4.6 |
| Data | 100,000,000 rows, **3.56 GB** on disk — byte-identical to the benchmark's `build()` (seed 0, `row_group_size=200_000`, snappy) |
| Query | full scan: `group_by("city").agg(mean(salary) AS a, sum(qty) AS b)`, `limit 200` |

The streaming engine was rewritten before 1.39; 1.39.3 and 1.42.1 both use the new engine, and
both show the same behaviour, so the finding is not a pre-rewrite artefact. Versions newer than
1.42.1 are not available through this environment's index, so I make **no** claim about whether a
later release fixes it.

Reconstruction is faithful: the real `execute_workflow(pl.scan_parquet(PATH), payload)` returns
fingerprint `85381da9d6c1` at **peak 1564 MB / in-flight 1528 MB**, matching the reconstructed
plan I measured against (`1530–1570 MB`).

---

## Task 1 — Is streaming actually running? **Yes.**

Verbatim plan for the exact executor plan (`scan → group_by(maintain_order=True) → slice(0,200)
→ head(1e6)`):

```
AGGREGATE[maintain_order: true]
  [col("salary").mean().alias("a"), col("qty").sum().alias("b")] BY [col("city")]
  FROM
  Parquet SCAN [.../bench_data.parquet]
  PROJECT 3/7 COLUMNS          <- projection pushdown works: 3 of 7 columns read
  ESTIMATED ROWS: 100000000
```

In 1.39.3, `explain(engine="streaming")` only echoes the logical plan — the *physical* streaming
graph surfaces via `POLARS_VERBOSE=1`. That graph, for the current plan:

```
source = multi-scan[parquet]   (a STREAMING parquet source, not in-memory-source)
nodes  = {multi-scan[parquet]:1, group-by:3, simple-projection:2,
          with-row-index:1, in-memory-map:3, in-memory-sink:1, streaming-slice:1}
fallback-to-in-memory message = NONE  (checked on every variant)
```

The source is a streaming `multi-scan[parquet]` and there is **no fallback message**. The
silent-fallback hypothesis is **falsified**. Streaming runs the whole pipeline. (For contrast,
the `engine="in-memory"` run shows an `in-memory-source` node — that is what a non-streaming
source looks like.)

## Task 2 — In-flight working set: a RAMP, not a plateau

RSS sampled from `/proc/self/statm` every 2 ms across a single `collect`, +300 ms after:

```
streaming full scan:  54 MB  ──▁▂▃▄▅▆▇█ linear ramp ──▶ ~1.6 GB  ──(collect ends)──▶ drops to ~460 MB
```

- A genuine streaming pipeline holds a **low, flat plateau**. This is a **clean linear ramp** that
  tracks the fraction of the file consumed — i.e. the engine accumulates input faster than the
  group-by drains it.
- It is **not** just mimalloc holding freed pages: the ramp happens *during* the collect (live
  growth), and immediately after the collect ~1.1 GB is released back (the result is only 200
  rows; the ~460 MB that remains is the allocator's retained floor). Setting
  `MIMALLOC_PURGE_DELAY=0` did **not** lower the peak — the ~1.5 GB is live, not allocator noise.

See `rss_curves.png` — the streaming ramp (red) against DuckDB (flat ~0.2 GB), the selective
query (flat ~0.1 GB), and the in-memory engine (ramps to 4.1 GB, retains 3.0 GB).

## Task 3 — `maintain_order`: tested directly, **exonerated**

Same process, back-to-back, warm cache:

| plan | peak RSS | in-flight WS | collect | node added | result (as set) |
|---|---|---|---|---|---|
| `maintain_order=True` (current) | 1541 MB | 1525 MB | 969 ms | `with-row-index` | `85381da9d6c1` |
| `maintain_order=False` | 1524 MB | 1505 MB | 862 ms | — | `85381da9d6c1` |

- **Identical as a set of rows** (same fingerprint). Correctness-safe for this shape.
- `maintain_order=True` does add a `with-row-index` node — but it costs **~1% RSS** (16 MB) and a
  few ms. Dropping it does **not** move peak RSS off ~1.5 GB. **It is not load-bearing for the
  memory problem.**

## Task 4 — Bisection + knob sweep: the culprit is `multi-scan[parquet] → group_by` itself

Strip to the barest plan (`scan → group_by → collect`, no slice/head) and it still ramps:

| variant | in-flight WS | shape |
|---|---|---|
| `bare_true`  (group_by maintain_order=True, nothing else) | 1527 MB | RAMP |
| `bare_false` (group_by maintain_order=False, nothing else) | 1509 MB | RAMP |

So it is neither the `slice`, the `head`, nor `maintain_order`. **Every Polars knob leaves it a
~1.5 GB ramp:**

| knob | peak RSS | shape |
|---|---|---|
| baseline (streaming) | 1534 MB | RAMP |
| `scan_parquet(low_memory=True)` | 1553 MB | RAMP |
| `POLARS_PREFETCH_SIZE=1` / `=2` | 1543 / 1526 MB | RAMP |
| `POLARS_STREAMING_CHUNK_SIZE=50000` | 1456 MB | RAMP |
| `MIMALLOC_PURGE_DELAY=0` | 1545 MB | RAMP |

The one thing that moves it is **thread count** — the in-flight buffer scales with parallelism:

| threads | in-flight WS | collect |
|---|---|---|
| 1 | 1247 MB | 3178 ms |
| 2 | 1328 MB | 1676 ms |
| 4 | 1514 MB | 904 ms |

That is why the prompt's 12-core box saw **2.40 GB** while this 4-core box sees ~1.5 GB —
extrapolating the trend lands right around 2.4 GB at 12 threads. But note it is **already 1.25 GB
single-threaded**: the buffering is fundamental, thread count only amplifies it. Dropping threads
to cut memory is a bad trade (t1 is 3.5× slower and still 1.25 GB).

**The selective (filtered) query is the control that proves Polars *can* stream flat:**

| query | peak RSS | in-flight WS | shape |
|---|---|---|---|
| selective (0.1% range filter on sorted `ts`) | 103 MB | 47 MB | **FLAT** |

Predicate pushdown skips ~all row groups, so almost nothing is read and the working set stays
flat and tiny. When Polars can skip the scan it streams beautifully; on a full scan it does not.

## Reference points (same machine, same file, **identical result** `85381da9d6c1`)

| path | peak RSS | in-flight WS | collect |
|---|---|---|---|
| DuckDB (4 threads) | **202 MB** | **113 MB** | 462 ms |
| DuckDB (1 thread) | 186 MB | 97 MB | 1535 ms |
| Polars streaming (4 threads) | 1530 MB | 1514 MB | 904 ms |
| Polars in-memory (4 threads) | 4084 MB | 4127 MB | 1198 ms |

DuckDB does the byte-identical aggregation in **~13× less** in-flight memory *and* faster. The
low-memory version is not hypothetical — DuckDB demonstrates it. Polars' streaming (1.5 GB) is
still a real **2.7× win over its own in-memory engine** (4.1 GB) — so `collect(engine="streaming")`
is doing genuine work and is the correct call; it just does not get near DuckDB on this shape.

---

## Task 5 — The fix, and what it costs

### The change that was under test (drop `maintain_order=True`) — measured, and **not** recommended

```diff
--- a/polars_gw/executor.py
@@ _apply_aggregate
-    if not measures and group_by:
-        return lf.select(group_by).unique(maintain_order=True)
+    if not measures and group_by:
+        return lf.select(group_by).unique()
@@
-    if group_by:
-        return lf.group_by(group_by, maintain_order=True).agg(agg_exprs)
+    if group_by:
+        return lf.group_by(group_by).agg(agg_exprs)
```

- **Measured effect:** peak RSS 1541 → 1524 MB (**−1.1%, inside run-to-run noise**); removes the
  `with-row-index` node; result identical as a set.
- **Stated cost:** when a query's group cardinality **exceeds its `limit`** and it carries **no
  `sort` step**, the returned subset of groups becomes **non-deterministic across runs** (hash
  order instead of first-appearance). For `groups ≤ limit` (the common case, and this benchmark's
  200 cities ≤ 200) there is no observable change.
- **Recommendation: keep `maintain_order=True`.** The investigation's job was to test whether it
  is the memory culprit; the data says no. Since removal buys ~1% and introduces a
  limit-subset determinism change GW users could notice ("the same chart returns different bars
  on refresh"), there is no reason to take the risk.

### What actually holds, stated plainly

1. **Keep `collect(engine="streaming")`.** It is correct and is a real 2.7× memory win over the
   in-memory engine (the eager `DataFrame` path pays 4–6 GB; the lazy streaming path pays ~1.5 GB).
2. **The full-scan memory gap vs DuckDB cannot be closed in `executor.py`.** It is a property of
   Polars 1.39–1.42's streaming engine (`multi-scan[parquet] → group_by` accumulates a live buffer
   that ramps with rows × threads), not of polars-gw's translation. No knob, no available version,
   and no plan rewrite I could find flattens it. So **do not claim a memory advantage over DuckDB
   on unfiltered full scans** — it is a ~1.5 GB vs ~0.11 GB loss.
3. **Pivot the positioning to the arguments that survive measurement:**
   - **Selective-query behaviour** — the dominant interactive pattern. Filtered queries stream
     **flat at ~0.1 GB** here (and the prompt's 3.5 ms vs 9.6 ms latency edge on non-overlapping
     ranges is real).
   - **Accepting a derived `LazyFrame` with no file URI** — polars-gw can run GW payloads against
     a frame produced by upstream transforms; PyGWalker's DuckDB `Connector` needs a table/file to
     point `SELECT … FROM` at.

### Caveat on latency

The memory investigation is warm-cache by design. The latency figures above are **warm** only —
this environment has no `vmtouch`/root to evict the page cache, so the full-scan latency numbers
need a `--cold` pass on real storage before they mean anything (warm re-reads the file from page
cache each interaction, which flatters the lazy path). The *memory* conclusions do not depend on
cache state.

---

## Reproduce

```bash
uv pip install numpy pyarrow duckdb matplotlib
cd investigations/streaming-memory
python build_data.py bench_data.parquet 100000000   # ~80s, writes 3.56 GB (gitignored)

python run_all.py        # task 1-4: plan/node-graph, peak RSS + curves, correctness  -> results.json
python analyze_curves.py # ascii ramp/plateau sparklines from results.json
python knobs.py          # low_memory / prefetch / chunk / mimalloc sweep + DuckDB    -> knobs_results.json
python threads.py        # thread-count scaling + DuckDB reference                    -> threads_results.json
python verify_real.py    # proves reconstruction == real execute_workflow() + plan dump
python plot_curves.py    # -> rss_curves.png

# cross-version check (index caps at 1.42.1 here):
BENCH_PATH=$PWD/bench_data.parquet uv run --no-project --with 'polars==1.42.1' python measure_one.py
```

Each script isolates every variant in a fresh subprocess (clean per-process peak RSS), launched
from one parent invocation with a warm page cache, per the benchmark's order-effect guardrails.
