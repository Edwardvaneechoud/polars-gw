"""Translate Graphic Walker IDataQueryPayload workflow steps into Polars operations."""

from __future__ import annotations

import contextvars
import logging
import uuid
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Per-request ID so concurrent execute_workflow calls can be disentangled in the logs.
_request_id: contextvars.ContextVar[str] = contextvars.ContextVar("gw_polars_request_id", default="")


def _log(level: int, msg: str, *args: Any) -> None:
    rid = _request_id.get()
    logger.log(level, (f"[{rid}] " if rid else "") + msg, *args)


DEFAULT_MAX_ROWS: int = 1_000_000


def execute_workflow(
    df: pl.DataFrame | pl.LazyFrame,
    payload: dict[str, Any],
    *,
    max_rows: int | None = DEFAULT_MAX_ROWS,
) -> list[dict[str, Any]]:
    """Execute a Graphic Walker IDataQueryPayload against a Polars DataFrame.

    The entire workflow is built as a lazy query plan and collected once at
    the end, letting Polars optimise predicate push-down and projection.

    Args:
        df: The source DataFrame (or LazyFrame) to query.
        payload: A Graphic Walker IDataQueryPayload dict with keys:
            - workflow: list of workflow steps
            - limit: optional row limit
            - offset: optional row offset
        max_rows: Hard cap on the number of rows returned.  Applied after all
            workflow steps and payload limit/offset.  Set to ``None`` to
            disable.  Defaults to :data:`DEFAULT_MAX_ROWS` (1 000 000).

    Returns:
        A list of row dicts (IRow[]) suitable for returning to Graphic Walker.
    """
    rid_token = _request_id.set(uuid.uuid4().hex[:8])
    try:
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df

        workflow = payload.get("workflow", [])
        _log(logging.INFO, "execute_workflow: %d step(s), max_rows=%s", len(workflow), max_rows)
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

        result = _sanitize_for_json(lf)

        if max_rows is not None and len(result) == max_rows:
            _log(
                logging.WARNING,
                "Result capped at max_rows=%d — output may be truncated. "
                "Pass a larger max_rows or max_rows=None to disable.",
                max_rows,
            )
        _log(logging.INFO, "execute_workflow: returned %d row(s)", len(result))
        _log(logging.DEBUG, f"execute_workflow: returned {str(result[:min(len(result), 20)])}")

        return result
    finally:
        _request_id.reset(rid_token)


def _describe_view_query(query: dict) -> str:
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


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _apply_filters(lf: pl.LazyFrame, filters: list[dict]) -> pl.LazyFrame:
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


def _build_filter_expr(fid: str, rule: dict, schema: pl.Schema) -> pl.Expr | None:
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
        # Optional timezone/offset in minutes — shift the user-supplied bounds
        # so they align with the column's UTC epoch-ms representation.
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


# ---------------------------------------------------------------------------
# View queries (aggregate, fold, bin, raw)
# ---------------------------------------------------------------------------


def _apply_view_queries(lf: pl.LazyFrame, queries: list[dict]) -> pl.LazyFrame:
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


def _apply_aggregate(lf: pl.LazyFrame, query: dict) -> pl.LazyFrame:
    schema = lf.collect_schema()
    group_by = [g for g in query.get("groupBy", []) if g in schema]
    measures = query.get("measures", [])

    # No measures requested: Graphic Walker uses this to fetch the distinct
    # values of a dimension (e.g. to populate a filter dropdown).  Without
    # this branch we would fall through and return the entire DataFrame.
    if not measures and group_by:
        return lf.select(group_by).unique(maintain_order=True)

    agg_exprs: list[pl.Expr] = []
    for m in measures:
        field = m.get("field")
        agg = m.get("agg")
        alias = m.get("asFieldKey") or field or agg or "value"

        # SQL-style count(*): Graphic Walker sends field="*" (or empty) for
        # "count all rows".  Polars' pl.len() is the direct equivalent.
        if agg == "count" and (not field or field == "*"):
            agg_exprs.append(pl.len().alias(alias))
            continue

        # agg="expr": arbitrary SQL expression, e.g. "SUM(a) / SUM(b)".  The
        # expression is taken from the measure's "expression" or "expr" field.
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


def _apply_fold(lf: pl.LazyFrame, query: dict) -> pl.LazyFrame:
    schema = lf.collect_schema()
    fold_by = [f for f in query.get("foldBy", []) if f in schema]
    key_col = query.get("newFoldKeyCol", "key")
    value_col = query.get("newFoldValueCol", "value")
    if not fold_by:
        return lf
    return lf.unpivot(on=fold_by, variable_name=key_col, value_name=value_col)


def _apply_bin(lf: pl.LazyFrame, query: dict) -> pl.LazyFrame:
    bin_by = query.get("binBy")
    new_col = query.get("newBinCol", f"{bin_by}_bin")
    bin_size = query.get("binSize", 10)
    if not bin_by or bin_by not in lf.collect_schema():
        return lf
    col = pl.col(bin_by)
    return lf.with_columns(
        ((col - col.min()) / bin_size).floor().cast(pl.Int64).alias(new_col)
    )


def _apply_raw(lf: pl.LazyFrame, query: dict) -> pl.LazyFrame:
    schema = lf.collect_schema()
    fields = [f for f in query.get("fields", []) if f in schema]
    if fields:
        return lf.select(fields)
    return lf


# ---------------------------------------------------------------------------
# Sort
# ---------------------------------------------------------------------------


def _apply_sort(lf: pl.LazyFrame, by: list[str], sort_dir: str) -> pl.LazyFrame:
    schema = lf.collect_schema()
    by = [b for b in by if b in schema]
    if not by:
        return lf
    return lf.sort(by=by, descending=sort_dir == "descending")


# ---------------------------------------------------------------------------
# Transform (computed fields)
# ---------------------------------------------------------------------------


def _apply_transforms(lf: pl.LazyFrame, transforms: list[dict]) -> pl.LazyFrame:
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
            if isinstance(v, (int, float)):
                chosen = int(v)
        elif ptype == "offset":
            v = p.get("value")
            if isinstance(v, (int, float)):
                fallback = int(v)
    if chosen is not None:
        return chosen
    if fallback is not None:
        return fallback
    return 0


def _build_transform_expr(expression: dict, schema: pl.Schema) -> pl.Expr | None:
    op = expression.get("op")
    params = expression.get("params", [])

    # Graphic Walker's "Row Count" field: op="one" creates a constant-1 column
    # that is then summed in a downstream aggregate to yield row counts.
    if op == "one":
        return pl.lit(1, dtype=pl.Int64)

    # op="expr": arbitrary expression supplied as a SQL string.  Look for it
    # in the standard params shape first, otherwise in a top-level field.
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

    # op="paint": value/colour mapping used by GW's paint tool.  Mapping
    # structure is complex and undocumented; log and return input unchanged
    # (null expression) so the workflow doesn't crash.
    if op == "paint":
        _log(logging.WARNING, "  paint transform is not supported — skipping")
        return None

    if op == "bin":
        # GW `bin`: equal-width binning, returns per-row [lowerBound, upperBound]
        # in the field's native numeric scale.  The reference implementation is
        # graphic-walker/src/lib/execExp.ts — it emits a 2-tuple per row and
        # the frontend renders it as the chart's category label.
        field = _param_to_str(params[0]) if params else None
        num_bins = expression.get("num", 10)
        if field and field in schema:
            col = pl.col(field)
            col_min = col.min()
            step = (col.max() - col_min) / num_bins
            # Clip so col.max() falls into the last bin rather than a phantom
            # bin N (mirrors `if (bIndex === binSize) bIndex = binSize - 1;`).
            idx = (
                pl.when(step > 0)
                .then(((col - col_min) / step).floor().cast(pl.Int64).clip(0, num_bins - 1))
                .otherwise(0)
            )
            lower = (col_min + idx * step).cast(pl.Float64)
            upper = (col_min + (idx + 1) * step).cast(pl.Float64)
            return pl.concat_list([lower, upper])

    elif op in ("log", "log2", "log10"):
        field = _param_to_str(params[0]) if params else None
        base_map = {"log": 2.718281828459045, "log2": 2, "log10": 10}
        if field and field in schema:
            return pl.col(field).log(base=base_map[op])

    elif op == "binCount":
        # GW `binCount`: equal-frequency (quantile) binning, returns a
        # 1-indexed bucket rank in 1..num.  Per execExp.ts, rows are sorted by
        # value and split into `num` contiguous groups of ~N/num rows each.
        field = _param_to_str(params[0]) if params else None
        num_bins = expression.get("num", 10)
        if field and field in schema:
            col = pl.col(field)
            # ordinal rank breaks ties deterministically by input order, which
            # matches the reference's stable sort.
            order_index = col.rank(method="ordinal") - 1  # 0-indexed
            group_size = col.count() / num_bins
            return (
                pl.when(group_size > 0)
                .then((order_index / group_size).floor().cast(pl.Int64).clip(0, num_bins - 1) + 1)
                .otherwise(1)
            )

    elif op == "dateTimeDrill":
        # Truncate a datetime to the start of the requested unit (year, month,
        # day, …).  Returns a datetime/date — not an integer component (that's
        # what dateTimeFeature is for).
        field = _param_to_str(params[0]) if params else None
        time_unit = _param_to_str(params[1]) if len(params) > 1 else "year"
        if field and field in schema:
            interval = _DATETIME_DRILL_MAP.get(time_unit or "year")
            if interval is None:
                _log(logging.WARNING, "  dateTimeDrill: unknown unit %r — skipping", time_unit)
                return None
            display_offset = _param_display_offset(params)
            expr = pl.col(field)
            # Shift into the user's display TZ so day/week/etc. boundaries
            # align with the local calendar, then truncate, then shift back so
            # the returned values stay in the source column's timezone.
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


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _sanitize_for_json(lf: pl.LazyFrame) -> list[dict[str, Any]]:
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
    return lf.collect().to_dicts()
