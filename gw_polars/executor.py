"""Translate Graphic Walker IDataQueryPayload workflow steps into Polars operations."""

from __future__ import annotations

from typing import Any

import polars as pl


def execute_workflow(df: pl.LazyFrame, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute a Graphic Walker IDataQueryPayload against a Polars DataFrame.

    Args:
        df: The source DataFrame to query.
        payload: A Graphic Walker IDataQueryPayload dict with keys:
            - workflow: list of workflow steps
            - limit: optional row limit
            - offset: optional row offset

    Returns:
        A list of row dicts (IRow[]) suitable for returning to Graphic Walker.
    """
    for step in payload.get("workflow", []):
        step_type = step.get("type")
        if step_type == "filter":
            df = _apply_filters(df, step.get("filters", []))
        elif step_type == "view":
            df = _apply_view_queries(df, step.get("query", []))
        elif step_type == "sort":
            df = _apply_sort(df, step.get("by", []), step.get("sort", "ascending"))
        elif step_type == "transform":
            df = _apply_transforms(df, step.get("transform", []))

    limit = payload.get("limit")
    if limit is not None:
        offset = payload.get("offset", 0) or 0
        df = df.slice(offset, limit)

    return _sanitize_for_json(df)


def _apply_filters(df: pl.DataFrame, filters: list[dict]) -> pl.DataFrame:
    for f in filters:
        fid = f.get("fid")
        rule = f.get("rule", {})
        if not fid or fid not in df.columns:
            continue
        df = _apply_single_filter(df, fid, rule)
    return df


def _apply_single_filter(df: pl.DataFrame, fid: str, rule: dict) -> pl.DataFrame:
    rule_type = rule.get("type")
    value = rule.get("value")

    if rule_type == "range":
        low, high = value[0], value[1]
        expr = pl.col(fid)
        if low is not None and high is not None:
            df = df.filter(expr.is_between(low, high))
        elif low is not None:
            df = df.filter(expr >= low)
        elif high is not None:
            df = df.filter(expr <= high)

    elif rule_type == "temporal range":
        low, high = value[0], value[1]
        col_expr = pl.col(fid)
        dtype = df[fid].dtype
        if dtype == pl.Date:
            col_expr = col_expr.cast(pl.Datetime).dt.timestamp("ms")
        elif dtype in (pl.Datetime, pl.Datetime("ns"), pl.Datetime("us"), pl.Datetime("ms")):
            col_expr = col_expr.dt.timestamp("ms")
        if low is not None and high is not None:
            df = df.filter(col_expr.is_between(low, high))
        elif low is not None:
            df = df.filter(col_expr >= low)
        elif high is not None:
            df = df.filter(col_expr <= high)

    elif rule_type == "one of":
        if value is not None and len(value) > 0:
            df = df.filter(pl.col(fid).is_in(value))

    elif rule_type == "not in":
        if value is not None and len(value) > 0:
            df = df.filter(~pl.col(fid).is_in(value))

    elif rule_type == "regexp":
        pattern = rule.get("value", "")
        if pattern:
            df = df.filter(pl.col(fid).cast(pl.Utf8).str.contains(pattern))

    return df


# ---------------------------------------------------------------------------
# View queries (aggregate, fold, bin, raw)
# ---------------------------------------------------------------------------


def _apply_view_queries(df: pl.LazyFrame, queries: list[dict]) -> pl.LazyFrame:
    for query in queries:
        op = query.get("op")
        if op == "aggregate":
            df = _apply_aggregate(df, query)
        elif op == "fold":
            df = _apply_fold(df, query)
        elif op == "bin":
            df = _apply_bin(df, query)
        elif op == "raw":
            df = _apply_raw(df, query)
    return df


def _apply_aggregate(df: pl.LazyFrame, query: dict) -> pl.LazyFrame:
    group_by = [g for g in query.get("groupBy", []) if g in df.columns]
    measures = query.get("measures", [])

    agg_exprs = []
    for m in measures:
        field = m.get("field")
        agg = m.get("agg")
        alias = m.get("asFieldKey", field)
        if not field or field not in df.columns:
            continue
        expr = _build_agg_expr(field, agg)
        if expr is not None:
            agg_exprs.append(expr.alias(alias))

    if not agg_exprs:
        return df

    if group_by:
        return df.group_by(group_by, maintain_order=True).agg(agg_exprs)
    else:
        return df.select(agg_exprs)


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
    col = pl.col(field)
    if agg in _AGG_MAP:
        return getattr(col, _AGG_MAP[agg])()
    return None


def _apply_fold(df: pl.DataFrame, query: dict) -> pl.LazyFrame:
    fold_by = [f for f in query.get("foldBy", []) if f in df.columns]
    key_col = query.get("newFoldKeyCol", "key")
    value_col = query.get("newFoldValueCol", "value")
    if not fold_by:
        return df
    return df.unpivot(on=fold_by, variable_name=key_col, value_name=value_col)


def _apply_bin(df: pl.DataFrame, query: dict) -> pl.DataFrame:
    bin_by = query.get("binBy")
    new_col = query.get("newBinCol", f"{bin_by}_bin")
    bin_size = query.get("binSize", 10)

    if not bin_by or bin_by not in df.columns:
        return df

    min_val = df[bin_by].min()
    max_val = df[bin_by].max()

    if min_val is None or max_val is None or min_val == max_val:
        return df.with_columns(pl.lit(0).alias(new_col))

    return df.with_columns(
        ((pl.col(bin_by) - min_val) / bin_size).floor().cast(pl.Int64).alias(new_col)
    )


def _apply_raw(df: pl.DataFrame, query: dict) -> pl.DataFrame:
    fields = [f for f in query.get("fields", []) if f in df.columns]
    if fields:
        return df.select(fields)
    return df


# ---------------------------------------------------------------------------
# Sort
# ---------------------------------------------------------------------------


def _apply_sort(df: pl.DataFrame, by: list[str], sort_dir: str) -> pl.DataFrame:
    by = [b for b in by if b in df.columns]
    if not by:
        return df
    descending = sort_dir == "descending"
    return df.sort(by=by, descending=descending)


# ---------------------------------------------------------------------------
# Transform (computed fields)
# ---------------------------------------------------------------------------


def _apply_transforms(df: pl.DataFrame, transforms: list[dict]) -> pl.DataFrame:
    for t in transforms:
        key = t.get("key")
        expression = t.get("expression", {})
        if not key or not expression:
            continue
        df = _apply_single_transform(df, key, expression)
    return df


def _apply_single_transform(df: pl.DataFrame, key: str, expression: dict) -> pl.DataFrame:
    op = expression.get("op")
    params = expression.get("params", [])
    as_field = expression.get("as", key)

    if op == "bin":
        field = params[0] if params else None
        num_bins = expression.get("num", 10)
        if field and field in df.columns:
            min_val = df[field].min()
            max_val = df[field].max()
            if min_val is not None and max_val is not None and max_val > min_val:
                bin_width = (max_val - min_val) / num_bins
                df = df.with_columns(
                    ((pl.col(field) - min_val) / bin_width).floor().cast(pl.Int64).clip(0, num_bins - 1).alias(as_field)
                )
            else:
                df = df.with_columns(pl.lit(0).alias(as_field))

    elif op in ("log", "log2", "log10"):
        field = params[0] if params else None
        base_map = {"log": 2.718281828459045, "log2": 2, "log10": 10}
        if field and field in df.columns:
            df = df.with_columns(pl.col(field).log(base=base_map[op]).alias(as_field))

    elif op == "binCount":
        field = params[0] if params else None
        if field and field in df.columns:
            df = df.with_columns(pl.col(field).alias(as_field))

    elif op == "dateTimeDrill":
        field = params[0] if params else None
        time_unit = params[1] if len(params) > 1 else "year"
        if field and field in df.columns:
            col_expr = pl.col(field)
            drill_map = {
                "year": col_expr.dt.year(),
                "quarter": col_expr.dt.quarter(),
                "month": col_expr.dt.month(),
                "week": col_expr.dt.week(),
                "day": col_expr.dt.day(),
                "dayOfWeek": col_expr.dt.weekday(),
                "hour": col_expr.dt.hour(),
                "minute": col_expr.dt.minute(),
                "second": col_expr.dt.second(),
            }
            expr = drill_map.get(time_unit, col_expr.dt.year())
            df = df.with_columns(expr.alias(as_field))

    return df


def _sanitize_for_json(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame to a JSON-safe list of dicts.

    Handles temporal types, Decimal, Duration, etc.
    """
    for col_name in df.columns:
        dtype = df[col_name].dtype
        if dtype in (pl.Date, pl.Datetime) or str(dtype).startswith("Datetime"):
            df = df.with_columns(pl.col(col_name).cast(pl.Utf8))
        elif dtype == pl.Time:
            df = df.with_columns(pl.col(col_name).cast(pl.Utf8))
        elif dtype == pl.Duration or str(dtype).startswith("Duration"):
            df = df.with_columns(pl.col(col_name).dt.total_milliseconds())
        elif dtype == pl.Decimal or str(dtype).startswith("Decimal"):
            df = df.with_columns(pl.col(col_name).cast(pl.Float64))
    return df.to_dicts()
