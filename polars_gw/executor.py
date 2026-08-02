"""Translate Graphic Walker IDataQueryPayload workflow steps into Polars operations."""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import threading
import uuid
import weakref
from typing import Any

import polars as pl

from polars_gw.types import (
    AggQuery,
    BinQuery,
    FieldTransform,
    FilterRule,
    FoldQuery,
    IDataQueryPayload,
    RawQuery,
    SortDirection,
    TransformExpression,
    ViewQuery,
    VisFilter,
)

logger = logging.getLogger(__name__)

# Per-request ID so concurrent execute_workflow calls can be disentangled in the logs.
_request_id: contextvars.ContextVar[str] = contextvars.ContextVar("polars_gw_request_id", default="")


def _log(level: int, msg: str, *args: Any) -> None:
    rid = _request_id.get()
    logger.log(level, (f"[{rid}] " if rid else "") + msg, *args)


DEFAULT_MAX_ROWS: int = 1_000_000

_CACHE_MAX_ENTRIES: int = 64
# Maps key -> (validator, rows); validator is None for self-validating content keys.
_cache: dict[str, tuple[weakref.ref | None, list[dict[str, Any]]]] = {}
# Guards the evict+store mutation only — /api/compute runs in uvicorn's threadpool.
_cache_lock = threading.Lock()


def _cache_key(prefix: str, payload: IDataQueryPayload, max_rows: int | None) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()
    return f"{prefix}|{digest}|{max_rows}"


def _content_key(df: pl.DataFrame | pl.LazyFrame) -> str | None:
    """Content hash of a *scan-based* LazyFrame's query plan, else ``None``.

    A serialized scan plan is tiny and deterministic, so two fresh
    ``scan_parquet(path)`` frames hash equal and share one cache entry — unlike
    ``id()``, which recycles.  Eager DataFrames and in-memory LazyFrames (the
    ``"DF ["`` plan marker) return ``None`` and take the id()+weakref path;
    serializing them would copy the whole dataset.  Misclassification only ever
    costs performance: any failure here falls back to id(), never a stale hit.
    """
    if not isinstance(df, pl.LazyFrame):
        return None
    try:
        plan = df.explain(optimized=False)  # cheap; does not materialise data
    except Exception:  # noqa: BLE001 - any failure just disables content-keying
        return None
    if "DF [" in plan:
        return None
    try:
        blob = df.serialize(format="binary")
    except Exception:  # noqa: BLE001 - unserialisable plan -> fall back to id()
        return None
    return hashlib.md5(blob, usedforsecurity=False).hexdigest()


def clear_cache() -> None:
    """Drop all cached query results."""
    _cache.clear()


def build_query(
    df: pl.DataFrame | pl.LazyFrame,
    payload: IDataQueryPayload,
    *,
    max_rows: int | None = DEFAULT_MAX_ROWS,
) -> pl.LazyFrame:
    """Build the lazy query plan for a Graphic Walker payload without collecting.

    Applies every workflow step (filter, view, sort, transform) plus the
    payload's limit/offset and the ``max_rows`` cap, returning the uncollected
    plan so callers can ``.explain()`` or ``.collect()`` it themselves.
    :func:`execute_workflow` wraps this with result caching and JSON sanitisation.

    Args: see :func:`execute_workflow`.
    """
    lf = df.lazy()

    workflow = payload.get("workflow", [])
    _log(logging.INFO, "build_query: %d step(s), max_rows=%s", len(workflow), max_rows)
    _log(logging.DEBUG, "payload=%r", payload)

    for i, step in enumerate(workflow):
        step_type = step.get("type")
        if step_type == "filter":
            filters = step.get("filters", [])
            _log(
                logging.INFO,
                "  step %d: filter — %s",
                i,
                ", ".join(f"{f.get('fid')} {f.get('rule', {}).get('type')}" for f in filters) or "(none)",
            )
            lf = _apply_filters(lf, filters)
        elif step_type == "view":
            queries = step.get("query", [])
            _log(
                logging.INFO,
                "  step %d: view — %s",
                i,
                ", ".join(_describe_view_query(q) for q in queries) or "(none)",
            )
            lf = _apply_view_queries(lf, queries)
        elif step_type == "sort":
            by = step.get("by", [])
            direction = step.get("sort", "ascending")
            _log(logging.INFO, "  step %d: sort — by=%s %s", i, by, direction)
            lf = _apply_sort(lf, by, direction)
        elif step_type == "transform":
            transforms = step.get("transform", [])
            _log(
                logging.INFO,
                "  step %d: transform — %s",
                i,
                ", ".join(f"{t.get('expression', {}).get('op')}->{t.get('key')}" for t in transforms) or "(none)",
            )
            lf = _apply_transforms(lf, transforms)
        else:
            _log(logging.WARNING, "  step %d: unknown step type %r", i, step_type)

    limit = payload.get("limit")
    if limit is not None:
        offset = payload.get("offset", 0) or 0
        _log(logging.INFO, "  slice: offset=%d, limit=%d", offset, limit)
        lf = lf.slice(offset, limit)

    if max_rows is not None:
        lf = lf.head(max_rows)

    return lf


def execute_workflow(
    df: pl.DataFrame | pl.LazyFrame,
    payload: IDataQueryPayload,
    *,
    max_rows: int | None = DEFAULT_MAX_ROWS,
) -> list[dict[str, Any]]:
    """Execute a Graphic Walker IDataQueryPayload against a Polars DataFrame.

    Builds the lazy plan via :func:`build_query`, collects it once, and returns
    JSON-safe row dicts.  Results are cached by payload so duplicate queries
    (common when reshuffling fields in the UI) return instantly; a scan-based
    LazyFrame is keyed by its serialized scan plan, so a file changing on disk
    mid-session may serve a stale result until the entry is evicted (max 64).

    Args:
        df: The source DataFrame (or LazyFrame) to query.
        payload: A Graphic Walker IDataQueryPayload dict (workflow steps plus
            optional limit/offset).
        max_rows: Hard cap on rows returned, applied after all workflow steps
            and the payload limit/offset.  ``None`` disables it.  Defaults to
            :data:`DEFAULT_MAX_ROWS` (1 000 000).

    Returns:
        A list of row dicts (IRow[]) suitable for returning to Graphic Walker.
    """
    rid_token = _request_id.set(uuid.uuid4().hex[:8])
    try:
        # Content key for scan-based frames; otherwise id()+weakref, so a recycled id() can't serve stale rows.
        validator: pl.DataFrame | pl.LazyFrame | None
        ckey = _content_key(df)
        if ckey is not None:
            key = _cache_key(f"C|{ckey}", payload, max_rows)
            validator = None  # content keys self-validate
        else:
            key = _cache_key(f"I|{id(df)}", payload, max_rows)
            validator = df
        entry = _cache.get(key)
        if entry is not None:
            wref, cached = entry
            # None => content-keyed; otherwise the weakref must still point at this df.
            if wref is None or wref() is validator:
                _log(logging.INFO, "execute_workflow: cache hit (%d row(s))", len(cached))
                return cached

        result = _sanitize_for_json(build_query(df, payload, max_rows=max_rows))

        if max_rows is not None and len(result) == max_rows:
            _log(
                logging.WARNING,
                "Result capped at max_rows=%d — output may be truncated. "
                "Pass a larger max_rows or max_rows=None to disable.",
                max_rows,
            )
        _log(logging.INFO, "execute_workflow: returned %d row(s)", len(result))
        _log(logging.DEBUG, f"execute_workflow: returned {str(result[:min(len(result), 20)])}")

        if validator is not None:
            try:
                wref = weakref.ref(df)
            except TypeError:
                # Not weak-referenceable; skip caching rather than store an unvalidatable entry.
                return result
        else:
            wref = None

        with _cache_lock:
            if len(_cache) >= _CACHE_MAX_ENTRIES and key not in _cache:
                try:
                    del _cache[next(iter(_cache))]
                except (StopIteration, KeyError):
                    pass
            _cache[key] = (wref, result)

        return result
    finally:
        _request_id.reset(rid_token)


def _describe_view_query(query: ViewQuery) -> str:
    """Short human-readable summary of a view query for logging."""
    op = query.get("op")
    if op == "aggregate":
        group_by = query.get("groupBy", [])
        measures = [f"{m.get('agg')}({m.get('field')})" for m in query.get("measures", [])]
        if not measures and group_by:
            return f"distinct {group_by}"
        return f"aggregate by={group_by} measures=[{', '.join(measures)}]"
    if op == "fold":
        return f"fold on={query.get('foldBy', [])}"
    if op == "bin":
        return f"bin {query.get('binBy')} size={query.get('binSize', 10)}"
    if op == "raw":
        return f"raw fields={query.get('fields', [])}"
    return f"{op}?"


# Filters


def _apply_filters(lf: pl.LazyFrame | pl.DataFrame, filters: list[VisFilter]) -> pl.LazyFrame:
    """Combine all filter predicates into a single .filter() call."""
    schema = lf.collect_schema()
    exprs: list[pl.Expr] = []
    for f in filters:
        fid = f.get("fid")
        rule = f.get("rule", {})
        if not fid or fid not in schema:
            continue
        expr = _build_filter_expr(fid, rule, schema)
        if expr is not None:
            exprs.append(expr)
    if exprs:
        combined = exprs[0]
        for e in exprs[1:]:
            combined = combined & e
        lf = lf.filter(combined)
    return lf


def _build_filter_expr(fid: str, rule: FilterRule, schema: pl.Schema) -> pl.Expr | None:
    rule_type = rule.get("type")
    value = rule.get("value")

    if rule_type == "range":
        low, high = value[0], value[1]
        col = pl.col(fid)
        if low is not None and high is not None:
            return col.is_between(low, high)
        if low is not None:
            return col >= low
        if high is not None:
            return col <= high

    elif rule_type == "temporal range":
        low, high = value[0], value[1]
        # Shift the user-supplied bounds so they align with the column's UTC epoch-ms.
        offset_min = rule.get("offset") or 0
        offset_ms = offset_min * 60_000
        dtype = schema[fid]
        col = pl.col(fid)
        if dtype == pl.Date:
            col = col.cast(pl.Datetime).dt.timestamp("ms")
        elif dtype.base_type() == pl.Datetime:
            col = col.dt.timestamp("ms")
        if low is not None:
            low = low - offset_ms
        if high is not None:
            high = high - offset_ms
        if low is not None and high is not None:
            return col.is_between(low, high)
        if low is not None:
            return col >= low
        if high is not None:
            return col <= high

    elif rule_type == "one of":
        if value is not None and len(value) > 0:
            return pl.col(fid).is_in(value)

    elif rule_type == "not in":
        if value is not None and len(value) > 0:
            return ~pl.col(fid).is_in(value)

    elif rule_type == "regexp":
        pattern = rule.get("value", "")
        if pattern:
            # Graphic Walker's caseSensitive flag — default True when absent.
            if rule.get("caseSensitive") is False and not pattern.startswith("(?i)"):
                pattern = f"(?i){pattern}"
            return pl.col(fid).cast(pl.Utf8).str.contains(pattern)

    return None


# View queries (aggregate, fold, bin, raw)


def _apply_view_queries(lf: pl.LazyFrame | pl.DataFrame, queries: list[ViewQuery]) -> pl.LazyFrame:
    for query in queries:
        op = query.get("op")
        if op == "aggregate":
            lf = _apply_aggregate(lf, query)
        elif op == "fold":
            lf = _apply_fold(lf, query)
        elif op == "bin":
            lf = _apply_bin(lf, query)
        elif op == "raw":
            lf = _apply_raw(lf, query)
    return lf


def _apply_aggregate(lf: pl.LazyFrame | pl.DataFrame, query: AggQuery) -> pl.LazyFrame:
    schema = lf.collect_schema()
    group_by = [g for g in query.get("groupBy", []) if g in schema]
    measures = query.get("measures", [])

    # No measures = GW fetching a dimension's distinct values (e.g. a filter dropdown).
    if not measures and group_by:
        return lf.select(group_by).unique(maintain_order=True)

    agg_exprs: list[pl.Expr] = []
    for m in measures:
        field = m.get("field")
        agg = m.get("agg")
        alias = m.get("asFieldKey") or field or agg or "value"

        # GW sends field="*" (or empty) for SQL-style count(*).
        if agg == "count" and (not field or field == "*"):
            agg_exprs.append(pl.len().alias(alias))
            continue

        # agg="expr": arbitrary SQL, e.g. "SUM(a) / SUM(b)".
        if agg == "expr":
            sql = m.get("expression") or m.get("expr") or field
            parsed = _parse_sql_expr(sql)
            if parsed is None:
                _log(logging.WARNING, "  skipping measure: agg='expr' expression=%r (could not parse)", sql)
                continue
            agg_exprs.append(parsed.alias(alias))
            continue

        if not field or field not in schema:
            _log(logging.WARNING, "  skipping measure: field=%r agg=%r (field not in schema)", field, agg)
            continue
        expr = _build_agg_expr(field, agg)
        if expr is None:
            _log(logging.WARNING, "  skipping measure: field=%r agg=%r (unsupported aggregator)", field, agg)
            continue
        agg_exprs.append(expr.alias(alias))

    if not agg_exprs:
        _log(logging.WARNING, "  aggregate: no valid measures, returning input unchanged")
        return lf

    if group_by:
        return lf.group_by(group_by, maintain_order=True).agg(agg_exprs)
    return lf.select(agg_exprs)


_AGG_MAP: dict[str, str] = {
    "sum": "sum",
    "count": "count",
    "max": "max",
    "min": "min",
    "mean": "mean",
    "average": "mean",
    "median": "median",
    "variance": "var",
    "stdev": "std",
    "distinctCount": "n_unique",
}


def _build_agg_expr(field: str, agg: str) -> pl.Expr | None:
    if agg in _AGG_MAP:
        return getattr(pl.col(field), _AGG_MAP[agg])()
    return None


def _apply_fold(lf: pl.LazyFrame | pl.DataFrame, query: FoldQuery) -> pl.LazyFrame:
    schema = lf.collect_schema()
    fold_by = [f for f in query.get("foldBy", []) if f in schema]
    key_col = query.get("newFoldKeyCol", "key")
    value_col = query.get("newFoldValueCol", "value")
    if not fold_by:
        return lf
    return lf.unpivot(on=fold_by, variable_name=key_col, value_name=value_col)


def _apply_bin(lf: pl.LazyFrame | pl.DataFrame, query: BinQuery) -> pl.LazyFrame:
    bin_by = query.get("binBy")
    new_col = query.get("newBinCol", f"{bin_by}_bin")
    bin_size = query.get("binSize", 10)
    if not isinstance(bin_size, int|float) or bin_size <= 0:
        bin_size = 10
    if not bin_by or bin_by not in lf.collect_schema():
        return lf
    col = pl.col(bin_by)
    return lf.with_columns(
        ((col - col.min()) / bin_size).floor().cast(pl.Int64, strict=False).fill_null(0).alias(new_col)
    )


def _apply_raw(lf: pl.LazyFrame | pl.DataFrame, query: RawQuery) -> pl.LazyFrame:
    schema = lf.collect_schema()
    fields = [f for f in query.get("fields", []) if f in schema]
    if fields:
        return lf.select(fields)
    return lf


def _apply_sort(lf: pl.LazyFrame | pl.DataFrame, by: list[str], sort_dir: SortDirection) -> pl.LazyFrame:
    schema = lf.collect_schema()
    by = [b for b in by if b in schema]
    if not by:
        return lf
    return lf.sort(by=by, descending=sort_dir == "descending")


def _apply_transforms(lf: pl.LazyFrame | pl.DataFrame, transforms: list[FieldTransform]) -> pl.LazyFrame:
    for t in transforms:
        key = t.get("key")
        expression = t.get("expression", {})
        if not key or not expression:
            continue
        expr = _build_transform_expr(expression, lf.collect_schema())
        if expr is None:
            _log(
                logging.WARNING,
                "  skipping transform: op=%r params=%r (unsupported op or missing field)",
                expression.get("op"),
                expression.get("params"),
            )
            continue
        lf = lf.with_columns(expr.alias(expression.get("as", key)))
    return lf


def _param_to_str(param: Any) -> str | None:
    """Extract a string from a transform parameter.

    Graphic Walker sometimes sends params as plain strings ("date_col",
    "month") and sometimes as dicts ({"field": "date_col"}, {"value":
    "month"}).  This helper normalises both shapes.
    """
    if isinstance(param, str):
        return param
    if isinstance(param, dict):
        for key in ("field", "fid", "value", "name"):
            v = param.get(key)
            if isinstance(v, str):
                return v
    return None


def _parse_sql_expr(sql: Any) -> pl.Expr | None:
    """Translate a SQL-ish expression string to a Polars expression.

    Used for GW's ``expr`` aggregator and ``expr`` transform op, where the
    payload carries an arbitrary expression the user typed in the UI.
    """
    if not isinstance(sql, str) or not sql.strip():
        return None
    try:
        return pl.sql_expr(sql)
    except Exception as e:  # noqa: BLE001 - we genuinely want to swallow parse errors
        _log(logging.WARNING, "  pl.sql_expr failed for %r: %s", sql, e)
        return None


_DATETIME_FEATURE_MAP: dict[str, str] = {
    # GW granularity/feature label → Polars .dt method name
    "year": "year",
    "quarter": "quarter",
    "month": "month",
    "week": "week",
    "day": "day",
    "dayOfMonth": "day",
    "dayOfYear": "ordinal_day",
    "dayOfWeek": "weekday",
    "weekday": "weekday",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
}


_DATETIME_DRILL_MAP: dict[str, str] = {
    # GW drill unit → Polars dt.truncate interval string
    "year": "1y",
    "quarter": "1q",
    "month": "1mo",
    "week": "1w",
    "day": "1d",
    "hour": "1h",
    "minute": "1m",
    "second": "1s",
}


def _param_display_offset(params: list) -> int:
    """Return the displayOffset (or offset) param as an int, or 0 if absent.

    GW sends timezone offsets in JS ``Date.getTimezoneOffset()`` convention —
    minutes, with positive meaning *behind* UTC.  We prefer ``displayOffset``
    (the user's display TZ) and fall back to ``offset``.
    """
    chosen: int | None = None
    fallback: int | None = None
    for p in params:
        if not isinstance(p, dict):
            continue
        ptype = p.get("type")
        if ptype == "displayOffset":
            v = p.get("value")
            if isinstance(v, int | float):
                chosen = int(v)
        elif ptype == "offset":
            v = p.get("value")
            if isinstance(v, int | float):
                fallback = int(v)
    if chosen is not None:
        return chosen
    if fallback is not None:
        return fallback
    return 0


def _bin_expr(field: str, num_bins: int) -> pl.Expr:
    """Equal-width binning, returning a per-row ``[lower, upper]`` pair.

    Mirrors ``bin()`` in graphic-walker's ``src/lib/execExp.ts``, which emits a
    2-tuple in the field's native numeric scale for the frontend to render as
    the chart's category label.

    Nulls follow the reference's JS coercion, where ``null`` compares and
    arithmetics as 0: they widen the bounds towards zero and then land in
    whichever bin contains 0.  Leaving them null would emit a ``[null, null]``
    bucket that corrupts the frontend's histogram reconstruction, which reads
    bin bounds as numbers.  NaN and inf rows cast to null and collapse to the
    first bin, matching ``if (Number.isNaN(bIndex)) bIndex = 0``.  Degenerate
    columns (constant, single-row, all-null) have step == 0 and collapse to a
    single bucket instead of blowing up the Int64 cast.
    """
    col = pl.col(field).fill_null(0)
    col_min = col.min()
    step = (col.max() - col_min) / num_bins
    # clip() keeps col.max() in the last bin rather than a phantom bin N.
    idx = (
        pl.when(step > 0)
        .then(((col - col_min) / step).floor().cast(pl.Int64, strict=False).fill_null(0).clip(0, num_bins - 1))
        .otherwise(0)
    )
    lower = (col_min + idx * step).cast(pl.Float64)
    upper = (col_min + (idx + 1) * step).cast(pl.Float64)
    return pl.concat_list([lower, upper])


def _bin_count_expr(field: str, num_bins: int) -> pl.Expr:
    """Equal-frequency (quantile) binning, returning a 1-indexed rank in 1..num.

    Mirrors ``binCount()`` in graphic-walker's ``src/lib/execExp.ts``: rows are
    sorted by value and split into ``num`` contiguous groups of ~N/num rows.

    The reference sorts with ``(a, b) => a.val - b.val``, so nulls order as 0
    and still occupy a rank slot — hence the same ``fill_null(0)`` as
    :func:`_bin_expr`, and a group size over the full row count.
    """
    col = pl.col(field).fill_null(0)
    # Ordinal rank breaks ties by input order, matching the reference's stable sort.
    order_index = col.rank(method="ordinal") - 1
    group_size = pl.len() / num_bins
    # Non-strict cast: the then-branch is eager, so an empty frame yields NaN here.
    return (
        pl.when(group_size > 0)
        .then((order_index / group_size).floor().cast(pl.Int64, strict=False).clip(0, num_bins - 1) + 1)
        .otherwise(1)
    )


def _build_transform_expr(expression: TransformExpression, schema: pl.Schema) -> pl.Expr | None:
    op = expression.get("op")
    params = expression.get("params", [])

    # GW's "Row Count" field: a constant 1, summed by a downstream aggregate.
    if op == "one":
        return pl.lit(1, dtype=pl.Int64)

    # Arbitrary SQL string: check the params shape first, then top-level fields.
    if op == "expr":
        sql = None
        for p in params:
            if isinstance(p, dict) and p.get("type") in ("sql", "expression", "value"):
                sql = p.get("value")
                if isinstance(sql, str):
                    break
        if sql is None:
            sql = expression.get("sql") or expression.get("expression")
        return _parse_sql_expr(sql)

    # The paint tool's mapping structure is undocumented — skip rather than crash.
    if op == "paint":
        _log(logging.WARNING, "  paint transform is not supported — skipping")
        return None

    if op == "bin":
        field = _param_to_str(params[0]) if params else None
        if field and field in schema:
            return _bin_expr(field, expression.get("num", 10))

    elif op in ("log", "log2", "log10"):
        field = _param_to_str(params[0]) if params else None
        base_map = {"log": 2.718281828459045, "log2": 2, "log10": 10}
        if field and field in schema:
            return pl.col(field).log(base=base_map[op])

    elif op == "binCount":
        field = _param_to_str(params[0]) if params else None
        if field and field in schema:
            return _bin_count_expr(field, expression.get("num", 10))

    elif op == "dateTimeDrill":
        # Truncates to the unit's start; dateTimeFeature is the one returning components.
        field = _param_to_str(params[0]) if params else None
        time_unit = _param_to_str(params[1]) if len(params) > 1 else "year"
        if field and field in schema:
            interval = _DATETIME_DRILL_MAP.get(time_unit or "year")
            if interval is None:
                _log(logging.WARNING, "  dateTimeDrill: unknown unit %r — skipping", time_unit)
                return None
            display_offset = _param_display_offset(params)
            expr = pl.col(field)
            # Truncate in the user's display TZ, then shift back to the source timezone.
            if display_offset:
                shift = pl.duration(minutes=display_offset)
                expr = (expr - shift).dt.truncate(interval) + shift
            else:
                expr = expr.dt.truncate(interval)
            return expr

    elif op == "dateTimeFeature":
        # Extract a numeric component (e.g. month → 3, dayOfWeek → 1).
        field = _param_to_str(params[0]) if params else None
        time_unit = _param_to_str(params[1]) if len(params) > 1 else "year"
        if field and field in schema:
            method = _DATETIME_FEATURE_MAP.get(time_unit or "year", "year")
            return getattr(pl.col(field).dt, method)()

    return None


def _sanitize_for_json(lf: pl.LazyFrame | pl.DataFrame) -> list[dict[str, Any]]:
    """Collect the lazy plan and convert to JSON-safe dicts.

    Batches all type casts into a single with_columns call.
    """
    schema = lf.collect_schema()
    cast_exprs: list[pl.Expr] = []
    for col_name, dtype in schema.items():
        if dtype == pl.Date or dtype.base_type() == pl.Datetime:
            cast_exprs.append(pl.col(col_name).cast(pl.Utf8))
        elif dtype == pl.Time:
            cast_exprs.append(pl.col(col_name).cast(pl.Utf8))
        elif dtype.base_type() == pl.Duration:
            cast_exprs.append(pl.col(col_name).dt.total_milliseconds())
        elif dtype.base_type() == pl.Decimal:
            cast_exprs.append(pl.col(col_name).cast(pl.Float64))
    if cast_exprs:
        lf = lf.with_columns(cast_exprs)
    if isinstance(lf, pl.LazyFrame):
        return lf.collect(engine="streaming").to_dicts()
    elif isinstance(lf, pl.DataFrame):
        return lf.to_dicts()
