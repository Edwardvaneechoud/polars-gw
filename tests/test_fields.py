"""Tests for gw_polars.fields — Polars schema to GW IMutField conversion."""

import datetime

import polars as pl

from gw_polars.fields import get_fields


class TestGetFields:
    def test_numeric_types_are_quantitative(self):
        df = pl.DataFrame({
            "int_col": pl.Series([1, 2, 3], dtype=pl.Int64),
            "float_col": pl.Series([1.0, 2.0, 3.0], dtype=pl.Float64),
        })
        fields = get_fields(df)
        field_map = {f["fid"]: f for f in fields}
        assert field_map["int_col"]["semanticType"] == "quantitative"
        assert field_map["int_col"]["analyticType"] == "measure"
        assert field_map["float_col"]["semanticType"] == "quantitative"
        assert field_map["float_col"]["analyticType"] == "measure"

    def test_string_types_are_nominal(self):
        df = pl.DataFrame({"name": ["Alice", "Bob"]})
        fields = get_fields(df)
        assert fields[0]["semanticType"] == "nominal"
        assert fields[0]["analyticType"] == "dimension"

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

    def test_field_structure(self):
        df = pl.DataFrame({"age": [25, 30]})
        fields = get_fields(df)
        f = fields[0]
        assert f["fid"] == "age"
        assert f["name"] == "age"
        assert "semanticType" in f
        assert "analyticType" in f

    def test_empty_dataframe(self):
        df = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
        fields = get_fields(df)
        assert len(fields) == 1
        assert fields[0]["fid"] == "a"
        assert fields[0]["semanticType"] == "quantitative"

    def test_multiple_columns_order_preserved(self):
        df = pl.DataFrame({"z_col": [1], "a_col": ["x"], "m_col": [True]})
        fields = get_fields(df)
        fids = [f["fid"] for f in fields]
        assert fids == ["z_col", "a_col", "m_col"]

    def test_all_int_variants(self):
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
            assert f["semanticType"] == "quantitative", f"{f['fid']} should be quantitative"
