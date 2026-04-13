"""Type hints for Graphic Walker payload structures (GW 0.4.77)."""

from typing import Any, Literal

# --- Filter types ---

FilterRuleType = Literal["range", "temporal range", "one of", "not in", "regexp"]

# --- Aggregator types ---

Aggregator = Literal[
    "sum", "count", "max", "min", "mean", "median",
    "variance", "stdev", "distinctCount", "expr",
]

# --- View query op types ---

ViewQueryOp = Literal["aggregate", "fold", "bin", "raw"]

# --- Workflow step type discriminators ---

WorkflowStepType = Literal["filter", "view", "sort", "transform"]

# --- Semantic / Analytic types for fields ---

SemanticType = Literal["quantitative", "temporal", "nominal", "ordinal"]
AnalyticType = Literal["measure", "dimension"]

# All payload structures are typed as dicts since they come from JSON.
# The type aliases below document the expected shapes.

# IDataQueryPayload
# {
#     "workflow": list[WorkflowStep],
#     "limit": int | None,
#     "offset": int | None,
# }

# WorkflowStep (discriminated by "type"):
# FilterStep:    {"type": "filter", "filters": list[VisFilter]}
# ViewStep:      {"type": "view", "query": list[ViewQuery]}
# SortStep:      {"type": "sort", "by": list[str], "sort": "ascending" | "descending"}
# TransformStep: {"type": "transform", "transform": list[FieldTransform]}

# VisFilter:
# {"fid": str, "rule": FilterRule}

# FilterRule (discriminated by "type"):
# {"type": "range", "value": [number | None, number | None]}
# {"type": "temporal range", "value": [number | None, number | None], "offset": int | None}
# {"type": "one of", "value": list[Any]}
# {"type": "not in", "value": list[Any]}
# {"type": "regexp", "value": str, "caseSensitive": bool | None}

# ViewQuery (discriminated by "op"):
# AggQuery:  {"op": "aggregate", "groupBy": list[str], "measures": list[Measure]}
# FoldQuery: {"op": "fold", "foldBy": list[str], "newFoldKeyCol": str, "newFoldValueCol": str}
# BinQuery:  {"op": "bin", "binBy": str, "newBinCol": str, "binSize": number}
# RawQuery:  {"op": "raw", "fields": list[str]}

# Measure:
# {"field": str, "agg": Aggregator, "asFieldKey": str, "format": str | None, "offset": int | None}

Payload = dict[str, Any]
