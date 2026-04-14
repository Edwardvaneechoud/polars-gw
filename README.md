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

## License

MIT
