"""Convert Polars DataFrame schema to Graphic Walker IMutField[] format."""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from gw_polars.types import AnalyticType, SemanticType

logger = logging.getLogger(__name__)

_INTEGER_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
)

_FLOAT_DTYPES = (pl.Float32, pl.Float64)

_TEMPORAL_DTYPES = (pl.Date, pl.Datetime, pl.Time, pl.Duration)


def get_fields(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    field_overrides: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame schema to Graphic Walker IMutField[] format.

    Args:
        df: The DataFrame whose schema to convert.
        field_overrides: Optional ``{column_name: {key: value, ...}}`` map
            that is shallow-merged into each field after dtype inference.
            Useful for forcing a specific ``analyticType`` or
            ``semanticType`` when the dtype rule gets it wrong (e.g. an
            integer that should be a measure, or an int rating that should
            be ordinal).

    Returns:
        A list of dicts matching GW's IMutField interface, e.g.::

            [{"fid": "VendorID", "name": "VendorID", "basename": "VendorID",
              "semanticType": "quantitative", "analyticType": "dimension",
              "offset": 0}, ...]
    """
    overrides = field_overrides or {}
    column_names = set(df.schema)
    unknown = set(overrides) - column_names
    if unknown:
        logger.warning(
            "field_overrides: ignoring unknown column(s): %s",
            ", ".join(sorted(unknown)),
        )

    fields: list[dict[str, Any]] = []
    for name, dtype in df.schema.items():
        semantic, analytic = _dtype_classify(dtype)
        field: dict[str, Any] = {
            "fid": name,
            "name": name,
            "basename": name,
            "semanticType": semantic,
            "analyticType": analytic,
            "offset": 0,
        }
        if name in overrides:
            field.update(overrides[name])
        # aggName defaults to "sum" on measures, but a user-supplied aggName
        # (in either analytic role) is left untouched.
        if field["analyticType"] == "measure" and "aggName" not in field:
            field["aggName"] = "sum"
        fields.append(field)

    return fields


def _dtype_classify(dtype: pl.DataType) -> tuple[SemanticType, AnalyticType]:
    """Pure dtype → (semanticType, analyticType) — matches PyGWalker."""
    if dtype in _INTEGER_DTYPES:
        # quantitative-but-discrete: PyGWalker treats these as dimensions
        return "quantitative", "dimension"
    if dtype in _FLOAT_DTYPES or str(dtype).startswith("Decimal"):
        return "quantitative", "measure"
    if dtype in _TEMPORAL_DTYPES:
        return "temporal", "dimension"
    if str(dtype).startswith("Datetime") or str(dtype).startswith("Duration"):
        return "temporal", "dimension"
    if dtype == pl.Boolean:
        return "nominal", "dimension"
    # Utf8, Categorical, Enum, List, Struct, etc.
    return "nominal", "dimension"
