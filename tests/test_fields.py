"""Tests for gw_polars.fields — Polars schema to GW IMutField conversion."""

import datetime
from decimal import Decimal

import polars as pl

from gw_polars.fields import get_fields


class TestGetFields:
    def test_integer_columns_are_dimensions(self):
        # PyGWalker parity: ints are quantitative *dimensions*, not measures.
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
        with caplog.at_level("WARNING", logger="gw_polars.fields"):
            get_fields(df, field_overrides={"nope": {"analyticType": "measure"}})
        assert any("nope" in rec.message for rec in caplog.records)
