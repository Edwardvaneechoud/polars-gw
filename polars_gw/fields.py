"""Convert Polars DataFrame schema to Graphic Walker IMutField[] format."""

from __future__ import annotations

import logging
from collections.abc import Mapping

import polars as pl

from polars_gw.types import (
    AnalyticType,
    ClassifyIntegers,
    IMutField,
    IMutFieldOverride,
    SemanticType,
)

logger = logging.getLogger(__name__)

_INTEGER_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
)

_FLOAT_DTYPES = (pl.Float32, pl.Float64)

_TEMPORAL_DTYPES = (pl.Date, pl.Datetime, pl.Time, pl.Duration)

# PyGWalker parity constants (pygwalker/data_parsers/base.py + polars_parser.py):
# fields are inferred from ``df[:1000]`` and an integer column with
# ``<= 16`` distinct values is a dimension, otherwise a measure.
_SAMPLE_ROWS = 1000
_DIMENSION_MAX_CARDINALITY = 16
_INT_MODES: tuple[ClassifyIntegers, ...] = ("sample", "scan", "measure")

# analyticType used for integer columns when the distinct-count pass raises.
# "measure" mirrors classify_integers="measure" and graphic-walker's native
# numeric default; it avoids labeling an unknown-cardinality integer a
# dimension (which would drop it on an axis and trigger a group-by over
# millions of groups downstream).
_COUNT_FAILURE_ANALYTIC: AnalyticType = "measure"


def get_fields(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    field_overrides: Mapping[str, IMutFieldOverride] | None = None,
    classify_integers: ClassifyIntegers = "sample",
) -> list[IMutField]:
    """Convert a Polars DataFrame schema to Graphic Walker IMutField[] format.

    Args:
        df: The DataFrame (or LazyFrame) whose schema to convert.
        field_overrides: Optional ``{column_name: IMutFieldOverride}`` map
            that is shallow-merged into each field after dtype inference.
            Useful for forcing a specific ``analyticType``/``semanticType``/
            ``aggName``/``name`` when the dtype rule gets it wrong (e.g. an
            integer that should be a measure, or an int rating that should be
            ordinal). A column whose override pins ``analyticType`` also
            *skips* the integer distinct-count pass below — its analytic role
            is predetermined, so there is no reason to touch its data.
        classify_integers: How integer columns are split into dimension vs
            measure. ``semanticType`` stays ``"quantitative"`` in every mode;
            only ``analyticType`` is decided. Floats/Decimal are always
            measures and other dtypes are unaffected.

            - ``"sample"`` (default): approximate ``approx_n_unique`` over
              the first ``1000`` rows; ``<= 16`` distinct → dimension, else
              measure. Uses PyGWalker's ``df[:1000]`` + ``<= 16`` window and
              threshold, so it touches up to 1000 rows at call time (a small
              collect for a LazyFrame).
            - ``"scan"``: ``approx_n_unique`` over the *whole* frame (one
              streaming pass); same threshold. Correct even on sorted or
              clustered columns where the head sample is unrepresentative.
              Costs real time on large or complex lazy plans.
            - ``"measure"``: every integer becomes a measure. Zero data
              access — the escape hatch for lazy pipelines too expensive to
              touch even once.

    Returns:
        A list of dicts matching GW's IMutField interface, e.g.::

            [{"fid": "VendorID", "name": "VendorID", "basename": "VendorID",
              "semanticType": "quantitative", "analyticType": "measure",
              "aggName": "sum", "offset": 0}, ...]

    Raises:
        ValueError: if *classify_integers* is not one of "sample", "scan",
            or "measure".
    """
    if classify_integers not in _INT_MODES:
        raise ValueError(f"classify_integers must be one of {_INT_MODES}, got {classify_integers!r}")

    overrides: Mapping[str, IMutFieldOverride] = field_overrides or {}
    schema = df.collect_schema()
    column_names = set(schema)
    unknown = set(overrides) - column_names
    if unknown:
        logger.warning(
            "field_overrides: ignoring unknown column(s): %s",
            ", ".join(sorted(unknown)),
        )

    # Integer columns whose analyticType is NOT already pinned by an override.
    # Pinned columns are skipped entirely — no data access for them (so the
    # distinct-count cost "disappears" wherever the caller overrides it).
    int_cols_to_count = [
        name
        for name, dtype in schema.items()
        if dtype in _INTEGER_DTYPES and "analyticType" not in overrides.get(name, {})
    ]
    int_analytic = _classify_integers(df, int_cols_to_count, classify_integers)

    fields: list[IMutField] = []
    for name, dtype in schema.items():
        semantic, analytic = _dtype_classify(dtype)
        # Cardinality-driven analyticType for counted integer columns; set as
        # the pre-merge base so an explicit field override still wins below.
        if name in int_analytic:
            analytic = int_analytic[name]
        field: IMutField = {
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


def _classify_integers(
    df: pl.DataFrame | pl.LazyFrame,
    cols: list[str],
    mode: ClassifyIntegers,
) -> dict[str, AnalyticType]:
    """Map each integer column in *cols* to ``"dimension"`` or ``"measure"``.

    Performs at most ONE batched collect over the frame. Returns ``{}``
    without touching data when there is nothing to count or ``mode`` is
    ``"measure"``. On collect failure it degrades every column to
    ``_COUNT_FAILURE_ANALYTIC`` and logs a warning — field inference must
    never crash (a raise here means ``walk()`` never launches).
    """
    out: dict[str, AnalyticType] = {}
    if not cols:
        return out
    if mode == "measure":
        for c in cols:
            out[c] = "measure"
        return out

    if mode == "sample":
        frame = df.head(_SAMPLE_ROWS)
        exprs = [pl.col(c).approx_n_unique().alias(c) for c in cols]
    else:  # "scan"
        frame = df
        exprs = [pl.col(c).approx_n_unique().alias(c) for c in cols]

    try:
        selected = frame.select(exprs)
        if isinstance(selected, pl.LazyFrame):
            selected = selected.collect(engine="streaming") if mode == "scan" else selected.collect()
        counts = selected.row(0, named=True)
    except Exception as exc:  # noqa: BLE001 - degrade; never crash field inference
        logger.warning(
            "get_fields(classify_integers=%r): distinct-count pass failed (%s); "
            "defaulting %d integer column(s) to analyticType=%r: %s",
            mode,
            exc,
            len(cols),
            _COUNT_FAILURE_ANALYTIC,
            ", ".join(cols),
        )
        for c in cols:
            out[c] = _COUNT_FAILURE_ANALYTIC
        return out

    for c in cols:
        out[c] = "dimension" if counts[c] <= _DIMENSION_MAX_CARDINALITY else "measure"
    return out


def _dtype_classify(dtype: pl.DataType) -> tuple[SemanticType, AnalyticType]:
    """Pure dtype → (semanticType, analyticType).

    For integer dtypes the returned ``analyticType`` is only a placeholder:
    ``get_fields`` always overwrites it (via ``classify_integers`` cardinality
    inference, or a user override). ``semanticType`` is the real final value.
    """
    if dtype in _INTEGER_DTYPES:
        # analyticType is decided by cardinality in get_fields; "dimension" is
        # a placeholder. semanticType is quantitative (matches PyGWalker).
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
