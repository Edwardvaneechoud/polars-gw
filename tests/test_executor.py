"""Tests for gw_polars.executor — GW workflow → Polars translation."""

import datetime

import polars as pl

from gw_polars.executor import execute_workflow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "city": ["Amsterdam", "Berlin", "Amsterdam", "Berlin", "Paris"],
        "category": ["A", "B", "A", "B", "A"],
        "sales": [100, 200, 150, 250, 300],
        "quantity": [10, 20, 15, 25, 30],
    })


def _temporal_df() -> pl.DataFrame:
    return pl.DataFrame({
        "date": [
            datetime.date(2024, 1, 15),
            datetime.date(2024, 3, 20),
            datetime.date(2024, 6, 10),
            datetime.date(2024, 9, 5),
            datetime.date(2024, 12, 25),
        ],
        "value": [10, 20, 30, 40, 50],
    })


# ---------------------------------------------------------------------------
# Filter tests
# ---------------------------------------------------------------------------


class TestFilterRange:
    def test_range_inclusive(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "sales", "rule": {"type": "range", "value": [150, 300]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 4
        assert all(150 <= r["sales"] <= 300 for r in result)

    def test_range_open_low(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "sales", "rule": {"type": "range", "value": [None, 200]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert all(r["sales"] <= 200 for r in result)

    def test_range_open_high(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "sales", "rule": {"type": "range", "value": [200, None]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert all(r["sales"] >= 200 for r in result)


class TestFilterOneOf:
    def test_one_of(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "city", "rule": {"type": "one of", "value": ["Amsterdam", "Paris"]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 3
        assert all(r["city"] in ("Amsterdam", "Paris") for r in result)

    def test_not_in(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "city", "rule": {"type": "not in", "value": ["Berlin"]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert all(r["city"] != "Berlin" for r in result)


class TestFilterRegexp:
    def test_regexp_match(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "city", "rule": {"type": "regexp", "value": "^A"}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert all(r["city"].startswith("A") for r in result)


class TestFilterTemporalRange:
    def test_temporal_range(self):
        # Filter dates between March and September 2024 (as unix ms)
        low_ms = int(datetime.datetime(2024, 3, 1).timestamp() * 1000)
        high_ms = int(datetime.datetime(2024, 9, 30).timestamp() * 1000)
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "date", "rule": {"type": "temporal range", "value": [low_ms, high_ms]}}
                ]}
            ]
        }
        result = execute_workflow(_temporal_df(), payload)
        assert len(result) == 3  # March, June, September


class TestFilterUnknownColumn:
    def test_unknown_column_skipped(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "nonexistent", "rule": {"type": "range", "value": [0, 100]}}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 5  # No filtering applied


# ---------------------------------------------------------------------------
# Aggregate tests
# ---------------------------------------------------------------------------


class TestAggregate:
    def test_sum_group_by(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": ["city"],
                        "measures": [{"field": "sales", "agg": "sum", "asFieldKey": "total_sales"}],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        result_map = {r["city"]: r["total_sales"] for r in result}
        assert result_map["Amsterdam"] == 250
        assert result_map["Berlin"] == 450
        assert result_map["Paris"] == 300

    def test_count_no_group(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": [],
                        "measures": [{"field": "sales", "agg": "count", "asFieldKey": "n"}],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 1
        assert result[0]["n"] == 5

    def test_mean_and_median(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": [],
                        "measures": [
                            {"field": "sales", "agg": "mean", "asFieldKey": "avg_sales"},
                            {"field": "sales", "agg": "median", "asFieldKey": "med_sales"},
                        ],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert result[0]["avg_sales"] == 200.0
        assert result[0]["med_sales"] == 200.0

    def test_distinct_count(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": [],
                        "measures": [{"field": "city", "agg": "distinctCount", "asFieldKey": "n_cities"}],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert result[0]["n_cities"] == 3

    def test_min_max(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": [],
                        "measures": [
                            {"field": "sales", "agg": "min", "asFieldKey": "min_sales"},
                            {"field": "sales", "agg": "max", "asFieldKey": "max_sales"},
                        ],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert result[0]["min_sales"] == 100
        assert result[0]["max_sales"] == 300

    def test_unknown_field_skipped(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": ["city"],
                        "measures": [{"field": "nonexistent", "agg": "sum", "asFieldKey": "x"}],
                    }
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        # No valid agg exprs → returns original df unchanged
        assert len(result) == 5


# ---------------------------------------------------------------------------
# Fold tests
# ---------------------------------------------------------------------------


class TestFold:
    def test_fold_basic(self):
        df = pl.DataFrame({"id": [1, 2], "q1": [10, 20], "q2": [30, 40]})
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "fold",
                        "foldBy": ["q1", "q2"],
                        "newFoldKeyCol": "quarter",
                        "newFoldValueCol": "amount",
                    }
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert len(result) == 4
        keys = {r["quarter"] for r in result}
        assert keys == {"q1", "q2"}

    def test_fold_unknown_column(self):
        df = pl.DataFrame({"id": [1], "a": [10]})
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {
                        "op": "fold",
                        "foldBy": ["nonexistent"],
                        "newFoldKeyCol": "k",
                        "newFoldValueCol": "v",
                    }
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert len(result) == 1  # Unchanged


# ---------------------------------------------------------------------------
# Bin tests
# ---------------------------------------------------------------------------


class TestBin:
    def test_bin_basic(self):
        df = pl.DataFrame({"val": list(range(0, 100))})
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {"op": "bin", "binBy": "val", "newBinCol": "val_bin", "binSize": 10}
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert all("val_bin" in r for r in result)
        # bin 0 should contain values 0-9 (floor((val - 0) / 10))
        assert result[0]["val_bin"] == 0
        assert result[15]["val_bin"] == 1

    def test_bin_constant_column(self):
        df = pl.DataFrame({"val": [5, 5, 5]})
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {"op": "bin", "binBy": "val", "newBinCol": "b", "binSize": 10}
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert all(r["b"] == 0 for r in result)


# ---------------------------------------------------------------------------
# Raw tests
# ---------------------------------------------------------------------------


class TestRaw:
    def test_raw_select_fields(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {"op": "raw", "fields": ["city", "sales"]}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert set(result[0].keys()) == {"city", "sales"}

    def test_raw_empty_fields_returns_all(self):
        payload = {
            "workflow": [
                {"type": "view", "query": [
                    {"op": "raw", "fields": []}
                ]}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        assert set(result[0].keys()) == {"city", "category", "sales", "quantity"}


# ---------------------------------------------------------------------------
# Sort tests
# ---------------------------------------------------------------------------


class TestSort:
    def test_sort_ascending(self):
        payload = {
            "workflow": [
                {"type": "sort", "by": ["sales"], "sort": "ascending"}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        sales = [r["sales"] for r in result]
        assert sales == sorted(sales)

    def test_sort_descending(self):
        payload = {
            "workflow": [
                {"type": "sort", "by": ["sales"], "sort": "descending"}
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        sales = [r["sales"] for r in result]
        assert sales == sorted(sales, reverse=True)


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------


class TestTransform:
    def test_bin_transform(self):
        df = pl.DataFrame({"age": [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]})
        payload = {
            "workflow": [
                {"type": "transform", "transform": [
                    {"key": "age_bin", "expression": {"op": "bin", "params": ["age"], "as": "age_bin", "num": 5}}
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert all("age_bin" in r for r in result)
        # 5 bins over 5-95, width = 18. bin(5)=0, bin(95)=4
        assert result[0]["age_bin"] == 0
        assert result[-1]["age_bin"] == 4

    def test_log_transform(self):
        df = pl.DataFrame({"val": [1.0, 10.0, 100.0]})
        payload = {
            "workflow": [
                {"type": "transform", "transform": [
                    {"key": "log_val", "expression": {"op": "log10", "params": ["val"], "as": "log_val"}}
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        assert abs(result[0]["log_val"] - 0.0) < 0.01
        assert abs(result[1]["log_val"] - 1.0) < 0.01
        assert abs(result[2]["log_val"] - 2.0) < 0.01

    def test_datetime_drill(self):
        df = _temporal_df()
        payload = {
            "workflow": [
                {"type": "transform", "transform": [
                    {"key": "month", "expression": {"op": "dateTimeDrill", "params": ["date", "month"], "as": "month"}}
                ]}
            ]
        }
        result = execute_workflow(df, payload)
        months = [r["month"] for r in result]
        assert months == [1, 3, 6, 9, 12]


# ---------------------------------------------------------------------------
# Workflow chain tests
# ---------------------------------------------------------------------------


class TestWorkflowChain:
    def test_filter_then_aggregate_then_sort(self):
        payload = {
            "workflow": [
                {"type": "filter", "filters": [
                    {"fid": "sales", "rule": {"type": "range", "value": [100, 300]}}
                ]},
                {"type": "view", "query": [
                    {
                        "op": "aggregate",
                        "groupBy": ["city"],
                        "measures": [{"field": "sales", "agg": "sum", "asFieldKey": "total"}],
                    }
                ]},
                {"type": "sort", "by": ["total"], "sort": "descending"},
            ]
        }
        result = execute_workflow(_sample_df(), payload)
        totals = [r["total"] for r in result]
        assert totals == sorted(totals, reverse=True)

    def test_empty_workflow(self):
        result = execute_workflow(_sample_df(), {"workflow": []})
        assert len(result) == 5


# ---------------------------------------------------------------------------
# Limit / offset tests
# ---------------------------------------------------------------------------


class TestLimitOffset:
    def test_limit(self):
        payload = {"workflow": [], "limit": 2}
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 2

    def test_limit_with_offset(self):
        payload = {"workflow": [], "limit": 2, "offset": 1}
        result = execute_workflow(_sample_df(), payload)
        assert len(result) == 2
        assert result[0]["city"] == "Berlin"  # Second row (offset 1)


# ---------------------------------------------------------------------------
# JSON serialization tests
# ---------------------------------------------------------------------------


class TestSanitization:
    def test_temporal_serialized_as_string(self):
        df = _temporal_df()
        result = execute_workflow(df, {"workflow": []})
        # Date columns should be converted to strings
        assert isinstance(result[0]["date"], str)

    def test_nullable_values(self):
        df = pl.DataFrame({"a": [1, None, 3], "b": ["x", None, "z"]})
        result = execute_workflow(df, {"workflow": []})
        assert result[1]["a"] is None
        assert result[1]["b"] is None


# ---------------------------------------------------------------------------
# max_rows tests
# ---------------------------------------------------------------------------


class TestMaxRows:
    def test_custom_max_rows(self):
        result = execute_workflow(_sample_df(), {"workflow": []}, max_rows=3)
        assert len(result) == 3

    def test_max_rows_none_disables(self):
        result = execute_workflow(_sample_df(), {"workflow": []}, max_rows=None)
        assert len(result) == 5

    def test_max_rows_smaller_than_payload_limit(self):
        """max_rows caps even when payload limit is larger."""
        payload = {"workflow": [], "limit": 4}
        result = execute_workflow(_sample_df(), payload, max_rows=2)
        assert len(result) == 2

    def test_payload_limit_smaller_than_max_rows(self):
        """Payload limit wins when it is smaller than max_rows."""
        payload = {"workflow": [], "limit": 2}
        result = execute_workflow(_sample_df(), payload, max_rows=100)
        assert len(result) == 2

    def test_default_cap_applied(self):
        """Default max_rows (1M) is applied — result is capped, not unlimited."""
        from gw_polars.executor import DEFAULT_MAX_ROWS

        assert DEFAULT_MAX_ROWS == 1_000_000
        # Just verify the parameter default works (don't allocate 1M+ rows)
        result = execute_workflow(_sample_df(), {"workflow": []})
        assert len(result) == 5  # 5 < 1M, so all rows returned
