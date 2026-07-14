"""Tests for polars_gw.fields — Polars schema to GW IMutField conversion."""

import datetime
from decimal import Decimal

import polars as pl
import pytest

from polars_gw.fields import get_fields


def _boom(s: pl.Series) -> pl.Series:
    """UDF that passes schema resolution (empty probe) but raises on real data.

    Used to build "poisoned" LazyFrames whose ``collect_schema()`` succeeds but
    whose ``collect()`` fails — proving whether get_fields touches the data.
    """
    if s.len() == 0:
        return s
    raise RuntimeError("boom: this frame must not be collected")


def _poison_int_lazyframe() -> pl.LazyFrame:
    """LazyFrame with one integer column that raises if actually collected."""
    return pl.LazyFrame({"n": pl.Series([1, 2, 3], dtype=pl.Int64)}).with_columns(
        pl.col("n").map_batches(_boom, return_dtype=pl.Int64)
    )


def _poison_string_lazyframe() -> pl.LazyFrame:
    """LazyFrame with NO integer columns that raises if actually collected."""
    return pl.LazyFrame({"s": ["x", "y", "z"]}).with_columns(
        pl.col("s").map_batches(_boom, return_dtype=pl.String)
    )


class TestGetFields:
    def test_integer_columns_are_dimensions(self):
        # PyGWalker parity: low-cardinality ints (<=16 distinct in the first
        # 1000 rows) are quantitative *dimensions*; high-cardinality ints are
        # measures. Both columns here have only a few distinct values.
        df = pl.DataFrame({
            "int_col": pl.Series([1, 2, 3], dtype=pl.Int64),
            "float_col": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float64),
        })
        fields = get_fields(df)
        field_map = {f["fid"]: f for f in fields}
        assert field_map["int_col"]["semanticType"] == "quantitative"
        assert field_map["int_col"]["analyticType"] == "dimension"
        assert "aggName" not in field_map["int_col"]
        assert field_map["float_col"]["semanticType"] == "quantitative"
        assert field_map["float_col"]["analyticType"] == "measure"
        assert field_map["float_col"]["aggName"] == "sum"

    def test_string_types_are_nominal(self):
        df = pl.DataFrame({"name": ["Alice", "Bob"]})
        fields = get_fields(df)
        assert fields[0]["semanticType"] == "nominal"
        assert fields[0]["analyticType"] == "dimension"
        assert "aggName" not in fields[0]

    def test_temporal_types(self):
        df = pl.DataFrame({
            "date_col": [datetime.date(2024, 1, 1)],
            "datetime_col": [datetime.datetime(2024, 1, 1, 12, 0)],
        })
        fields = get_fields(df)
        field_map = {f["fid"]: f for f in fields}
        assert field_map["date_col"]["semanticType"] == "temporal"
        assert field_map["date_col"]["analyticType"] == "dimension"
        assert field_map["datetime_col"]["semanticType"] == "temporal"
        assert field_map["datetime_col"]["analyticType"] == "dimension"

    def test_boolean_is_nominal(self):
        df = pl.DataFrame({"flag": [True, False]})
        fields = get_fields(df)
        assert fields[0]["semanticType"] == "nominal"
        assert fields[0]["analyticType"] == "dimension"

    def test_field_structure_includes_basename_and_offset(self):
        df = pl.DataFrame({
            "age": pl.Series([25, 30], dtype=pl.Int64),
            "price": pl.Series([1.5, 2.5], dtype=pl.Float64),
        })
        fields = get_fields(df)
        field_map = {f["fid"]: f for f in fields}
        for name in ("age", "price"):
            f = field_map[name]
            assert f["fid"] == name
            assert f["name"] == name
            assert f["basename"] == name
            assert f["offset"] == 0
            assert "semanticType" in f
            assert "analyticType" in f
        # only the float column carries aggName
        assert field_map["price"]["aggName"] == "sum"
        assert "aggName" not in field_map["age"]

    def test_empty_dataframe(self):
        df = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
        fields = get_fields(df)
        assert len(fields) == 1
        assert fields[0]["fid"] == "a"
        assert fields[0]["semanticType"] == "quantitative"
        assert fields[0]["analyticType"] == "dimension"

    def test_multiple_columns_order_preserved(self):
        df = pl.DataFrame({"z_col": [1], "a_col": ["x"], "m_col": [True]})
        fields = get_fields(df)
        fids = [f["fid"] for f in fields]
        assert fids == ["z_col", "a_col", "m_col"]

    def test_all_int_variants_are_dimensions(self):
        # Dimensions because each column has 1 distinct value (<=16), not
        # because integers are unconditionally dimensions — see
        # TestClassifyIntegers for the cardinality rule.
        df = pl.DataFrame({
            "i8": pl.Series([1], dtype=pl.Int8),
            "i16": pl.Series([1], dtype=pl.Int16),
            "i32": pl.Series([1], dtype=pl.Int32),
            "i64": pl.Series([1], dtype=pl.Int64),
            "u8": pl.Series([1], dtype=pl.UInt8),
            "u16": pl.Series([1], dtype=pl.UInt16),
            "u32": pl.Series([1], dtype=pl.UInt32),
            "u64": pl.Series([1], dtype=pl.UInt64),
        })
        fields = get_fields(df)
        for f in fields:
            assert f["semanticType"] == "quantitative", f["fid"]
            assert f["analyticType"] == "dimension", f["fid"]
            assert "aggName" not in f, f["fid"]

    def test_float_variants_are_measures_with_aggname(self):
        df = pl.DataFrame({
            "f32": pl.Series([1.0], dtype=pl.Float32),
            "f64": pl.Series([1.0], dtype=pl.Float64),
        })
        fields = get_fields(df)
        for f in fields:
            assert f["semanticType"] == "quantitative", f["fid"]
            assert f["analyticType"] == "measure", f["fid"]
            assert f["aggName"] == "sum", f["fid"]

    def test_decimal_is_measure(self):
        df = pl.DataFrame({
            "amount": pl.Series([Decimal("1.50"), Decimal("2.50")]),
        })
        fields = get_fields(df)
        assert fields[0]["semanticType"] == "quantitative"
        assert fields[0]["analyticType"] == "measure"
        assert fields[0]["aggName"] == "sum"


class TestFieldOverrides:
    def test_override_analytic_type(self):
        df = pl.DataFrame({"VendorID": pl.Series([1, 2, 3], dtype=pl.Int64)})
        fields = get_fields(
            df,
            field_overrides={"VendorID": {"analyticType": "measure"}},
        )
        assert fields[0]["analyticType"] == "measure"
        # aggName auto-added because final analyticType is measure
        assert fields[0]["aggName"] == "sum"

    def test_override_semantic_type(self):
        df = pl.DataFrame({"rating": pl.Series([1, 2, 3, 4, 5], dtype=pl.Int64)})
        fields = get_fields(
            df,
            field_overrides={"rating": {"semanticType": "ordinal"}},
        )
        assert fields[0]["semanticType"] == "ordinal"
        assert fields[0]["analyticType"] == "dimension"  # unchanged

    def test_override_aggname_on_measure(self):
        df = pl.DataFrame({"price": pl.Series([1.0, 2.0], dtype=pl.Float64)})
        fields = get_fields(
            df,
            field_overrides={"price": {"aggName": "mean"}},
        )
        assert fields[0]["aggName"] == "mean"  # user value wins, not "sum"

    def test_override_dimension_to_measure_adds_aggname(self):
        df = pl.DataFrame({"code": pl.Series([1, 2, 3], dtype=pl.Int64)})
        fields = get_fields(
            df,
            field_overrides={"code": {"analyticType": "measure"}},
        )
        assert fields[0]["analyticType"] == "measure"
        assert fields[0]["aggName"] == "sum"

    def test_unknown_override_warns(self, caplog):
        df = pl.DataFrame({"x": [1]})
        with caplog.at_level("WARNING", logger="polars_gw.fields"):
            get_fields(df, field_overrides={"nope": {"analyticType": "measure"}})
        assert any("nope" in rec.message for rec in caplog.records)

    def test_semantic_only_override_still_counts(self):
        # A semanticType-only override does NOT pin analyticType, so the column
        # is still counted: 100 distinct ints -> measure, with semantic ordinal.
        df = pl.DataFrame({"code": pl.Series(range(100), dtype=pl.Int64)})
        fields = get_fields(df, field_overrides={"code": {"semanticType": "ordinal"}})
        assert fields[0]["semanticType"] == "ordinal"
        assert fields[0]["analyticType"] == "measure"
        assert fields[0]["aggName"] == "sum"


class TestClassifyIntegers:
    def test_high_cardinality_int_is_measure(self):
        # >16 distinct -> measure, still quantitative, with aggName default.
        df = pl.DataFrame({"id": pl.Series(range(100), dtype=pl.Int64)})
        f = get_fields(df)[0]  # default "sample"
        assert f["semanticType"] == "quantitative"
        assert f["analyticType"] == "measure"
        assert f["aggName"] == "sum"

    def test_sample_boundary(self):
        # Pins the <=16 operator: 16 distinct -> dimension, 17 -> measure.
        at = pl.DataFrame({"v": pl.Series(range(16), dtype=pl.Int64)})
        over = pl.DataFrame({"v": pl.Series(range(17), dtype=pl.Int64)})
        assert get_fields(at)[0]["analyticType"] == "dimension"
        assert "aggName" not in get_fields(at)[0]
        assert get_fields(over)[0]["analyticType"] == "measure"

    def test_classify_integers_measure_forces_measure(self):
        df = pl.DataFrame({"few": pl.Series([1, 2, 3], dtype=pl.Int64)})
        f = get_fields(df, classify_integers="measure")[0]
        assert f["analyticType"] == "measure"
        assert f["aggName"] == "sum"

    def test_scan_beats_sample_on_clustered_column(self):
        # First 1000 rows are all 0 (1 distinct), but the column has 100
        # distinct overall. "sample" is fooled -> dimension; "scan" sees the
        # whole frame -> measure. This is the reason "scan" exists.
        data = [0] * 1000 + list(range(100))
        lf = pl.LazyFrame({"n": pl.Series(data, dtype=pl.Int64)})
        assert get_fields(lf, classify_integers="sample")[0]["analyticType"] == "dimension"
        assert get_fields(lf, classify_integers="scan")[0]["analyticType"] == "measure"

    def test_all_null_int_is_dimension(self):
        df = pl.DataFrame({"n": pl.Series([None, None], dtype=pl.Int64)})
        f = get_fields(df)[0]
        assert f["semanticType"] == "quantitative"
        assert f["analyticType"] == "dimension"

    def test_invalid_classify_integers_raises(self):
        df = pl.DataFrame({"n": pl.Series([1, 2, 3], dtype=pl.Int64)})
        with pytest.raises(ValueError, match="classify_integers"):
            get_fields(df, classify_integers="bogus")  # type: ignore[arg-type]


class TestNoDataAccessGuarantees:
    """These use a 'poisoned' LazyFrame that raises if actually collected."""

    def test_zero_int_columns_skips_collect(self):
        # No integer columns -> no distinct-count pass -> the poison never fires.
        fields = get_fields(_poison_string_lazyframe(), classify_integers="scan")
        assert fields[0]["fid"] == "s"
        assert fields[0]["analyticType"] == "dimension"

    def test_analytic_override_skips_count(self):
        # Pinning analyticType removes the column from counting -> no collect.
        fields = get_fields(
            _poison_int_lazyframe(),
            field_overrides={"n": {"analyticType": "dimension"}},
            classify_integers="scan",
        )
        assert fields[0]["analyticType"] == "dimension"

    def test_measure_mode_zero_data_access(self):
        # "measure" never inspects data, so the poison never fires.
        fields = get_fields(_poison_int_lazyframe(), classify_integers="measure")
        assert fields[0]["analyticType"] == "measure"
        assert fields[0]["aggName"] == "sum"

    def test_count_failure_falls_back_to_measure(self, caplog):
        # When the count pass raises, degrade to measure + WARNING, never crash.
        with caplog.at_level("WARNING", logger="polars_gw.fields"):
            fields = get_fields(_poison_int_lazyframe(), classify_integers="scan")
        assert fields[0]["analyticType"] == "measure"
        assert fields[0]["aggName"] == "sum"
        assert any("distinct-count pass failed" in rec.message for rec in caplog.records)


class TestPyGWalkerParity:
    """Opt-in cross-check against PyGWalker's real field inference.

    Skipped unless pygwalker is installed (e.g. ``uv run --with pygwalker
    pytest tests/test_fields.py -k pygwalker``). Makes the parity claim
    verifiable by behavior rather than asserted in prose.
    """

    def test_sample_matches_pygwalker(self):
        pytest.importorskip("pygwalker")
        from pygwalker.services.data_parsers import get_parser

        df = pl.DataFrame({
            "low": pl.Series(list(range(10)) * 100, dtype=pl.Int64),  # 10 distinct
            "high": pl.Series(range(1000), dtype=pl.Int64),           # 1000 distinct
            "price": pl.Series([1.0] * 1000, dtype=pl.Float64),
            "city": pl.Series(["x"] * 1000, dtype=pl.String),
        })

        ours = {f["fid"]: f for f in get_fields(df, classify_integers="sample")}
        theirs = {f["fid"]: f for f in get_parser(df).raw_fields}

        for name in df.columns:
            assert ours[name]["analyticType"] == theirs[name]["analyticType"], name
            assert ours[name]["semanticType"] == theirs[name]["semanticType"], name
