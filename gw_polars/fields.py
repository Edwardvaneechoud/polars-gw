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

# Smart-inference thresholds — exposed as module constants so tests/users
# can patch them. Picked conservatively so the rule only fires when the
# evidence is strong.
SMART_ABSOLUTE_DISTINCT_MAX = 20      # ≤ this many distinct values → dimension
SMART_DISTINCT_RATIO_MAX = 0.005      # < 0.5% distinct/non-null → dimension


def get_fields(
    df: pl.DataFrame,
    *,
    smart_schema_identification: bool = False,
    field_overrides: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame schema to Graphic Walker IMutField[] format.

    Args:
        df: The DataFrame whose schema to convert.
        smart_schema_identification: When ``True``, refine the
            dimension-vs-measure split for numeric columns using cardinality
            (distinct count + distinct/non-null ratio) computed from the
            actual data. Default ``False`` — matches PyGWalker's pure-dtype
            behaviour exactly.
        field_overrides: Optional ``{column_name: {key: value, ...}}`` map
            that is shallow-merged into each field after inference. Useful
            for forcing a specific ``analyticType`` or ``semanticType`` when
            neither dtype nor smart inference gets it right.

    Returns:
        A list of dicts matching GW's IMutField interface, e.g.::

            [{"fid": "VendorID", "name": "VendorID", "basename": "VendorID",
              "semanticType": "quantitative", "analyticType": "dimension",
              "offset": 0}, ...]
    """
    # 1. dtype-based defaults
    base = []
    for name, dtype in df.schema.items():
        semantic, analytic = _dtype_classify(dtype)
        base.append((name, dtype, semantic, analytic))

    # 2. optional smart override for numeric columns
    smart_overrides: dict[str, AnalyticType] = {}
    if smart_schema_identification:
        smart_overrides = _smart_classify_numerics(df, base)
        if smart_overrides:
            n_changed = sum(
                1
                for name, _dt, _sem, analytic in base
                if smart_overrides.get(name, analytic) != analytic
            )
            logger.info(
                "smart_schema_identification: reclassified %d/%d numeric column(s)",
                n_changed,
                sum(1 for _n, dt, _s, _a in base if _is_numeric(dt)),
            )

    # 3. assemble fields, then apply user overrides
    overrides = field_overrides or {}
    unknown = set(overrides) - {n for n, *_ in base}
    if unknown:
        logger.warning(
            "field_overrides: ignoring unknown column(s): %s",
            ", ".join(sorted(unknown)),
        )

    fields: list[dict[str, Any]] = []
    for name, _dtype, semantic, analytic in base:
        analytic = smart_overrides.get(name, analytic)
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


def _is_numeric(dtype: pl.DataType) -> bool:
    return (
        dtype in _INTEGER_DTYPES
        or dtype in _FLOAT_DTYPES
        or str(dtype).startswith("Decimal")
    )


def _smart_classify_numerics(
    df: pl.DataFrame,
    base: list[tuple[str, pl.DataType, SemanticType, AnalyticType]],
) -> dict[str, AnalyticType]:
    """Cardinality-based override for numeric columns. Returns {col: analytic}."""
    numeric = [(name, dt, default) for name, dt, _sem, default in base if _is_numeric(dt)]
    if not numeric:
        return {}

    exprs: list[pl.Expr] = [pl.len().alias("__total")]
    for name, _dt, _default in numeric:
        exprs.append(pl.col(name).n_unique().alias(f"__nuniq__{name}"))
        exprs.append(pl.col(name).null_count().alias(f"__nulls__{name}"))
    stats = df.select(exprs).row(0, named=True)
    n_total: int = stats["__total"]

    out: dict[str, AnalyticType] = {}
    for name, _dt, default in numeric:
        n_unique: int = stats[f"__nuniq__{name}"]
        n_null: int = stats[f"__nulls__{name}"]
        out[name] = _smart_analytic(n_total, n_unique, n_null, default)
    return out


def _smart_analytic(
    n_total: int,
    n_unique: int,
    n_null: int,
    default: AnalyticType,
) -> AnalyticType:
    """Decide dimension vs measure for a numeric column from cardinality."""
    n_non_null = n_total - n_null
    if n_non_null == 0:
        return default  # no signal — keep dtype default

    # Every value distinct → likely an ID
    if n_unique == n_non_null and n_total > 1:
        return "dimension"
    # Few distinct values absolute → categorical
    if n_unique <= SMART_ABSOLUTE_DISTINCT_MAX:
        return "dimension"
    # Low cardinality at scale (e.g. 100 distinct in 1M rows)
    if n_unique / n_non_null < SMART_DISTINCT_RATIO_MAX:
        return "dimension"
    # Otherwise: continuous-like
    return "measure"
