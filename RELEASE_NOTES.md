# polars-gw v0.1.3 — Paired with Flowfile 0.9.3

Companion release to Flowfile 0.9.3, which embeds polars-gw in its worker
service to power the new "library of visualizations" feature on catalog
tables. No API changes; the version bump keeps the Flowfile and polars-gw
release lines aligned per the paired-bump policy.

---

# polars-gw v0.1.0 — Initial Release

Native Polars computation engine for [Graphic Walker](https://github.com/Kanaries/graphic-walker). Translates GW workflow payloads directly into Polars expressions — no DuckDB, no SQL intermediate.

## Highlights

- **`execute_workflow()`** — full translation of Graphic Walker `IDataQueryPayload` into Polars lazy query plans: filters (range, temporal, one-of, not-in, regexp), aggregations, fold/unpivot, binning, sort, transforms, and raw field selection
- **`get_fields()`** — automatic Polars dtype to Graphic Walker field-type inference
- **`walk(df)`** — standalone browser UI with a bundled Graphic Walker JS/CSS bundle (no CDN, no Node required). Install with `pip install 'polars-gw[viz]'`
- **Chart export/import** — save and restore chart specs across sessions via `handle.export()` and `spec_file=`
- **LazyFrame support** — pass DataFrames or LazyFrames
- **Python 3.10–3.13**

## Install

```bash
pip install polars-gw           # core only
pip install 'polars-gw[viz]'    # core + interactive UI
```
