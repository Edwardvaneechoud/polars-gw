# gw-polars

Native Polars computation engine for [Graphic Walker](https://github.com/Kanaries/graphic-walker).

Translates Graphic Walker `IDataQueryPayload` workflow steps directly into Polars operations — no DuckDB, no SQL intermediate.

## Installation

```bash
pip install gw-polars              # core translator only
pip install 'gw-polars[viz]'       # core + built-in walk() UI
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
the Graphic Walker UI (loaded from jsdelivr), and wires its computation
callback to `execute_workflow`.

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
| Int*, UInt*, Float*, Decimal | quantitative | measure |
| Date, Datetime, Time, Duration | temporal | dimension |
| Utf8, Categorical, Boolean, etc. | nominal | dimension |

## How It Differs from PyGWalker

PyGWalker (and panel-graphic-walker) always route through DuckDB — even for Polars DataFrames:

```
DataFrame → DuckDB → SQL → execute → dicts
```

gw-polars translates directly to Polars operations:

```
DataFrame → Polars expressions → execute → dicts
```

No DuckDB dependency. No SQL intermediate. Just Polars.

## gw-polars vs PyGWalker — which should you use?

Different tools, different jobs.  `gw-polars` is a focused compute engine
+ a standalone browser UI.  PyGWalker is a broader product with richer
UX surface area.

### Use `gw-polars` when

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
  Dash).  PyGWalker has these; gw-polars does not.
- You rely on **chart persistence** — saving/loading chart specs,
  exporting HTML/PNG, `vis_spec` round-tripping.
- You want **heterogeneous input support** (pandas + Polars + parquet +
  SQL tables under one DuckDB layer).
- Your data is **too large to collect into memory** and you need DuckDB's
  battle-tested out-of-core execution.
- You're using **Kanaries cloud features** (sharing, cloud chat, etc.).

### At a glance

| Concern | gw-polars | PyGWalker |
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
use `gw-polars`.  If you want inline-notebook, Streamlit, or chart
persistence out of the box, use PyGWalker.

## License

MIT
