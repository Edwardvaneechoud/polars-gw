# polars-gw

Native Polars computation engine for [Graphic Walker](https://github.com/Kanaries/graphic-walker).

Translates Graphic Walker `IDataQueryPayload` workflow steps directly into Polars operations — no DuckDB, no SQL intermediate.

## Installation

```bash
pip install polars-gw              # core translator only
pip install 'polars-gw[viz]'       # core + built-in walk() UI
```

The core install only pulls in `polars`.  The `[viz]` extra adds FastAPI,
uvicorn and pydantic so you can launch a local Graphic Walker against a
DataFrame in one line.

## Interactive UI

```python
import polars as pl
from gw_polars import walk

df = pl.read_parquet("sales.parquet")
handle = walk(df)          # opens http://127.0.0.1:<free-port> in your browser
# ...
handle.stop()
```

`walk()` starts a FastAPI server in a background daemon thread, serves
the Graphic Walker UI **bundled inside the wheel** (no CDN, no network
required), and wires its computation callback to `execute_workflow`.

### Export / Import chart specs

Charts you build in the UI can be saved and restored across sessions:

```python
# Export the current charts to a JSON file
handle.export("my_charts.json")

# Or just grab the spec as a Python list
spec = handle.export()
```

Next time, pass the file back to `walk()` to restore exactly where you left off:

```python
handle = walk(df, spec_file="my_charts.json")
```

The spec file is a plain JSON array of Graphic Walker `IChart` objects — safe
to version-control or share with collaborators.

### Logs

By default `walk()` prints one line per compute call so you can see
what GW is asking the backend for:

```
13:42:11  INFO    gw_polars.viz: Graphic Walker running on http://127.0.0.1:54221 — 12 000 000 rows x 19 cols, max_rows=1000000
13:42:18  INFO    gw_polars.viz: compute: 2 step(s) → 847 row(s) in 142.3 ms
13:42:24  INFO    gw_polars.viz: compute: 1 step(s) → 1000000 row(s) in 1284.7 ms [CAPPED]
```

`[CAPPED]` means the result hit `max_rows` and was truncated — Graphic
Walker shows its **"Data Limit Reached"** toast in the UI when this
happens.  Default cap is 1 000 000 rows; tune it with `max_rows=` or
disable with `max_rows=None`.

Quiet things down with `log_level="warning"`, or get more detail with
`log_level="debug"`:

```python
walk(df, log_level="warning")   # only warnings and errors
walk(df, max_rows=None)         # no row cap (use with care on big data)
```

If you've already configured Python logging yourself (`logging.basicConfig`,
a framework, pytest's `caplog`, etc.), `walk()` won't overwrite it —
it only attaches a console handler when nothing else owns logging.

## Usage

```python
import polars as pl
from gw_polars import execute_workflow, get_fields

df = pl.read_csv("data.csv")

# Get field definitions for Graphic Walker
fields = get_fields(df)
# [{"fid": "city", "name": "city", "semanticType": "nominal", "analyticType": "dimension"}, ...]

# Execute a Graphic Walker computation payload
payload = {
    "workflow": [
        {"type": "filter", "filters": [
            {"fid": "age", "rule": {"type": "range", "value": [18, 65]}}
        ]},
        {"type": "view", "query": [
            {"op": "aggregate", "groupBy": ["city"], "measures": [
                {"field": "salary", "agg": "mean", "asFieldKey": "avg_salary"}
            ]}
        ]},
        {"type": "sort", "sort": "descending", "by": ["avg_salary"]}
    ],
    "limit": 100
}
results = execute_workflow(df, payload)
# Returns list[dict] — ready to return as IRow[] to Graphic Walker
```

## Supported Operations

### Workflow Steps

| Step Type | Description |
|-----------|-------------|
| `filter` | Range, temporal range, one of, not in, regexp |
| `view/aggregate` | Group by + aggregation (sum, count, mean, median, min, max, variance, stdev, distinctCount) |
| `view/fold` | Unpivot (wide to long) |
| `view/bin` | Numeric binning |
| `view/raw` | Column selection |
| `sort` | Ascending/descending sort |
| `transform` | Computed fields (bin, log/log2/log10, binCount, dateTimeDrill, dateTimeFeature, one, expr) |

### Field Inference

`get_fields()` maps Polars dtypes to Graphic Walker field types:

| Polars Type | Semantic Type | Analytic Type |
|-------------|---------------|---------------|
| Float*, Decimal | quantitative | measure |
| Int*, UInt* | quantitative | dimension |
| Date, Datetime, Time, Duration | temporal | dimension |
| Utf8, Categorical, Boolean, etc. | nominal | dimension |

## How It Differs from PyGWalker

PyGWalker (and panel-graphic-walker) always route through DuckDB — even for Polars DataFrames:

```
DataFrame → DuckDB → SQL → execute → dicts
```

polars-gw translates directly to Polars operations:

```
DataFrame → Polars expressions → execute → dicts
```

No DuckDB dependency. No SQL intermediate. Just Polars.

## polars-gw vs PyGWalker — which should you use?

Different tools, different jobs.  `polars-gw` is a focused compute engine
+ a standalone browser UI.  PyGWalker is a broader product with richer
UX surface area.

### Use `polars-gw` when

- You're already in the **Polars ecosystem** (e.g. LazyFrames, streaming,
  or tools like [Flowfile](https://github.com/edwardvaneechoud/Flowfile))
  and don't want a DuckDB round-trip in the middle.
- You need **type fidelity** for `Categorical`, `Decimal`, `Duration`,
  `List`, `Struct` — they pass through as-is instead of degrading through
  SQL.
- You want a **lean install** (~40 MB core, no DuckDB).
- You're building your **own frontend** and just need the translator
  (`execute_workflow` + `get_fields`).
- You want **debuggable** query plans (Polars expressions, not generated
  SQL).
- You need a **standalone browser UI** (`walk(df)`) without notebook
  dependencies.

### Use PyGWalker when

- You want the **Jupyter inline widget** — PyGWalker renders the UI
  *inside* a notebook cell via anywidget.  `gw_polars.walk()` pops a
  browser tab.
- You need first-party **framework integrations** (Streamlit, Gradio,
  Dash).  PyGWalker has these; polars-gw does not.
- You rely on **chart persistence** — saving/loading chart specs,
  exporting HTML/PNG, `vis_spec` round-tripping.
- You want **heterogeneous input support** (pandas + Polars + parquet +
  SQL tables under one DuckDB layer).
- Your data is **too large to collect into memory** and you need DuckDB's
  battle-tested out-of-core execution.
- You're using **Kanaries cloud features** (sharing, cloud chat, etc.).

### At a glance

| Concern | polars-gw | PyGWalker |
|---|---|---|
| Backend | Polars expressions | DuckDB SQL |
| Install weight | Lean (polars only) | DuckDB + heavier deps |
| Jupyter inline widget | ❌ (browser tab) | ✅ |
| Streamlit / Gradio / Dash | ❌ | ✅ |
| Standalone browser UI | ✅ (`walk()`) | ✅ |
| Chart save/load/export | ❌ (defers to GW client) | ✅ |
| LazyFrame native | ✅ | via DuckDB |
| Polars Categorical/Decimal/Duration fidelity | ✅ | lossy through SQL |
| New GW payload ops | Requires translator update | Often works via SQL |
| Heterogeneous inputs (pandas/parquet/SQL) | Polars-only | ✅ |

Short version: if you're all-in on Polars and want the fast, native path,
use `polars-gw`.  If you want inline-notebook, Streamlit, or chart
persistence out of the box, use PyGWalker.

## Development

### Python

```bash
uv sync --extra viz           # runtime + viz + dev deps
uv run pytest                 # 60+ tests
uv run ruff check .
```

### Bundling the viz assets

The `walk()` UI ships a pre-built JS/CSS bundle under
`gw_polars/viz_assets/` (committed to the repo) so end users don't need
Node to `pip install polars-gw[viz]`.

Maintainers rebuild when bumping Graphic Walker:

```bash
cd js
npm install
npm run build                 # one-shot production build
```

Or iterate with watch mode (rebuilds JS + CSS on save, source maps on):

```bash
npm run dev                   # in one shell
uv run python example/walk_demo.py   # in another — refresh browser to pick up changes
```

Bundle layout:

- `graphic-walker.js` — Graphic Walker + React 19.2.0, minified IIFE (~4.4 MB)
- `graphic-walker.css` — Tailwind-compiled stylesheet (~57 KB)
- `versions.json` — pinned npm versions + build mode + timestamp

See `js/README.md` for details (including why React is pinned to
exactly `19.2.0`).

## License

MIT
