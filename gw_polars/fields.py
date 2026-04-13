"""Convert Polars DataFrame schema to Graphic Walker IMutField[] format."""

from __future__ import annotations

from typing import Any

import polars as pl

from gw_polars.types import AnalyticType, SemanticType

_QUANTITATIVE_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)

_TEMPORAL_DTYPES = (
    pl.Date, pl.Datetime, pl.Time, pl.Duration,
)


def get_fields(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame schema to Graphic Walker IMutField[] format.

    Args:
        df: The DataFrame whose schema to convert.

    Returns:
        A list of dicts matching GW's IMutField interface, e.g.:
        [{"fid": "age", "name": "age", "semanticType": "quantitative", "analyticType": "measure"}, ...]
    """
    fields = []
    for name, dtype in df.schema.items():
        semantic = _dtype_to_semantic(dtype)
        analytic = _semantic_to_analytic(semantic)
        fields.append({
            "fid": name,
            "name": name,
            "semanticType": semantic,
            "analyticType": analytic,
        })
    return fields


def _dtype_to_semantic(dtype: pl.DataType) -> SemanticType:
    """Map a Polars dtype to a Graphic Walker semantic type."""
    if dtype in _QUANTITATIVE_DTYPES:
        return "quantitative"
    if str(dtype).startswith("Decimal"):
        return "quantitative"
    if dtype in _TEMPORAL_DTYPES:
        return "temporal"
    if str(dtype).startswith("Datetime") or str(dtype).startswith("Duration"):
        return "temporal"
    if dtype == pl.Boolean:
        return "nominal"
    # Utf8, Categorical, Enum, List, Struct, etc.
    return "nominal"


def _semantic_to_analytic(semantic: SemanticType) -> AnalyticType:
    """Map a semantic type to a Graphic Walker analytic type."""
    return "measure" if semantic == "quantitative" else "dimension"
