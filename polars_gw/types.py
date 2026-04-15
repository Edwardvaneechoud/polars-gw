"""Type hints for Graphic Walker payload structures (GW 0.4.77)."""

from typing import Any, Literal, TypedDict

# ---------------------------------------------------------------------------
# Literal discriminator types
# ---------------------------------------------------------------------------

FilterRuleType = Literal["range", "temporal range", "one of", "not in", "regexp"]

Aggregator = Literal[
    "sum", "count", "max", "min", "mean", "median",
    "variance", "stdev", "distinctCount", "expr",
]

ViewQueryOp = Literal["aggregate", "fold", "bin", "raw"]

WorkflowStepType = Literal["filter", "view", "sort", "transform"]

SemanticType = Literal["quantitative", "temporal", "nominal", "ordinal"]
AnalyticType = Literal["measure", "dimension"]

TransformOp = Literal[
    "one", "expr", "paint", "bin", "log", "log2", "log10",
    "binCount", "dateTimeDrill", "dateTimeFeature",
]

SortDirection = Literal["ascending", "descending"]

DateTimeDrillUnit = Literal[
    "year", "quarter", "month", "week", "day", "hour", "minute", "second",
]

DateTimeFeatureKey = Literal[
    "year", "quarter", "month", "week", "day", "dayOfMonth",
    "dayOfYear", "dayOfWeek", "weekday", "hour", "minute", "second",
]

# ---------------------------------------------------------------------------
# Payload TypedDicts — machine-checked types for GW JSON structures.
# These are plain dicts at runtime (zero overhead); the type annotations
# are consumed by mypy / pyright for static checking.
#
# For TypedDicts with optional fields, we use the two-class inheritance
# pattern (required base + total=False subclass) for Python 3.10 compat
# (typing.NotRequired was added in 3.11).
# ---------------------------------------------------------------------------

# --- Filter types ---


class _FilterRuleRequired(TypedDict):
    type: FilterRuleType
    value: Any  # list[float | None] for range, list[Any] for one-of/not-in, str for regexp


class FilterRule(_FilterRuleRequired, total=False):
    caseSensitive: bool
    offset: int  # timezone offset in minutes for temporal range


class VisFilter(TypedDict):
    fid: str
    rule: FilterRule


# --- Measure ---


class _MeasureRequired(TypedDict):
    field: str
    agg: Aggregator
    asFieldKey: str


class Measure(_MeasureRequired, total=False):
    expression: str  # SQL expression for agg="expr"
    expr: str  # alternative key for SQL expression
    format: str
    offset: int


# --- View queries (discriminated union by "op") ---


class AggQuery(TypedDict):
    op: Literal["aggregate"]
    groupBy: list[str]
    measures: list[Measure]


class _FoldQueryRequired(TypedDict):
    op: Literal["fold"]
    foldBy: list[str]


class FoldQuery(_FoldQueryRequired, total=False):
    newFoldKeyCol: str  # defaults to "key"
    newFoldValueCol: str  # defaults to "value"


class _BinQueryRequired(TypedDict):
    op: Literal["bin"]
    binBy: str


class BinQuery(_BinQueryRequired, total=False):
    newBinCol: str  # defaults to "{binBy}_bin"
    binSize: int  # defaults to 10


class RawQuery(TypedDict):
    op: Literal["raw"]
    fields: list[str]


ViewQuery = AggQuery | FoldQuery | BinQuery | RawQuery

# --- Transform types ---


class _TransformExpressionRequired(TypedDict):
    op: TransformOp
    params: list[Any]
    # Note: GW payload also includes an "as" key (Python keyword).
    # It is accessed via .get("as", ...) at runtime.


class TransformExpression(_TransformExpressionRequired, total=False):
    num: int
    sql: str
    expression: str


class FieldTransform(TypedDict):
    key: str
    expression: TransformExpression


# --- Workflow steps (discriminated union by "type") ---


class FilterStep(TypedDict):
    type: Literal["filter"]
    filters: list[VisFilter]


class ViewStep(TypedDict):
    type: Literal["view"]
    query: list[ViewQuery]


class _SortStepRequired(TypedDict):
    type: Literal["sort"]
    by: list[str]


class SortStep(_SortStepRequired, total=False):
    sort: SortDirection  # defaults to "ascending"


class TransformStep(TypedDict):
    type: Literal["transform"]
    transform: list[FieldTransform]


WorkflowStep = FilterStep | ViewStep | SortStep | TransformStep

# --- Top-level payload ---


class _IDataQueryPayloadRequired(TypedDict):
    workflow: list[WorkflowStep]


class IDataQueryPayload(_IDataQueryPayloadRequired, total=False):
    limit: int
    offset: int


# --- IMutField (returned by get_fields) ---


class _IMutFieldRequired(TypedDict):
    fid: str
    name: str
    basename: str
    semanticType: SemanticType
    analyticType: AnalyticType
    offset: int


class IMutField(_IMutFieldRequired, total=False):
    aggName: str  # only present on measures, typically "sum"
