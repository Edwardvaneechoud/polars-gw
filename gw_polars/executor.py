"""Translate Graphic Walker IDataQueryPayload workflow steps into Polars operations."""

from __future__ import annotations

from typing import Any

import polars as pl


def execute_workflow(df: pl.DataFrame | pl.LazyFrame, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute a Graphic Walker IDataQueryPayload against a Polars DataFrame.

    The entire workflow is built as a lazy query plan and collected once at
    the end, letting Polars optimise predicate push-down and projection.

    Args:
        df: The source DataFrame (or LazyFrame) to query.
        payload: A Graphic Walker IDataQueryPayload dict with keys:
            - workflow: list of workflow steps
            - limit: optional row limit
            - offset: optional row offset

    Returns:
        A list of row dicts (IRow[]) suitable for returning to Graphic Walker.
    """
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df

    for step in payload.get("workflow", []):
        step_type = step.get("type")
        if step_type == "filter":
            lf = _apply_filters(lf, step.get("filters", []))
        elif step_type == "view":
            lf = _apply_view_queries(lf, step.get("query", []))
        elif step_type == "sort":
            lf = _apply_sort(lf, step.get("by", []), step.get("sort", "ascending"))
        elif step_type == "transform":
            lf = _apply_transforms(lf, step.get("transform", []))

    limit = payload.get("limit")
    if limit is not None:
        offset = payload.get("offset", 0) or 0
        lf = lf.slice(offset, limit)

    return _sanitize_for_json(lf)


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
        dtype = schema[fid]
        col = pl.col(fid)
        if dtype == pl.Date:
            col = col.cast(pl.Datetime).dt.timestamp("ms")
        elif dtype.base_type() == pl.Datetime:
            col = col.dt.timestamp("ms")
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

    agg_exprs: list[pl.Expr] = []
    for m in measures:
        field = m.get("field")
        agg = m.get("agg")
        alias = m.get("asFieldKey", field)
        if not field or field not in schema:
            continue
        expr = _build_agg_expr(field, agg)
        if expr is not None:
            agg_exprs.append(expr.alias(alias))

    if not agg_exprs:
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
        if expr is not None:
            lf = lf.with_columns(expr.alias(expression.get("as", key)))
    return lf


def _build_transform_expr(expression: dict, schema: pl.Schema) -> pl.Expr | None:
    op = expression.get("op")
    params = expression.get("params", [])

    if op == "bin":
        field = params[0] if params else None
        num_bins = expression.get("num", 10)
        if field and field in schema:
            col = pl.col(field)
            span = col.max() - col.min()
            return (
                pl.when(span > 0)
                .then(((col - col.min()) / (span / num_bins)).floor().cast(pl.Int64).clip(0, num_bins - 1))
                .otherwise(0)
            )

    elif op in ("log", "log2", "log10"):
        field = params[0] if params else None
        base_map = {"log": 2.718281828459045, "log2": 2, "log10": 10}
        if field and field in schema:
            return pl.col(field).log(base=base_map[op])

    elif op == "binCount":
        field = params[0] if params else None
        if field and field in schema:
            return pl.col(field)

    elif op == "dateTimeDrill":
        field = params[0] if params else None
        time_unit = params[1] if len(params) > 1 else "year"
        if field and field in schema:
            col = pl.col(field)
            drill_map = {
                "year": col.dt.year(),
                "quarter": col.dt.quarter(),
                "month": col.dt.month(),
                "week": col.dt.week(),
                "day": col.dt.day(),
                "dayOfWeek": col.dt.weekday(),
                "hour": col.dt.hour(),
                "minute": col.dt.minute(),
                "second": col.dt.second(),
            }
            return drill_map.get(time_unit, col.dt.year())

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
