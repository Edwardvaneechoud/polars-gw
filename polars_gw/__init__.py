"""polars-gw: Native Polars computation engine for Graphic Walker."""

from polars_gw.executor import DEFAULT_MAX_ROWS, execute_workflow
from polars_gw.fields import get_fields
from polars_gw.types import (
    Aggregator,
    AnalyticType,
    IDataQueryPayload,
    IMutField,
    Measure,
    SemanticType,
    ViewQuery,
    WorkflowStep,
)

__all__ = [
    "Aggregator",
    "AnalyticType",
    "DEFAULT_MAX_ROWS",
    "IDataQueryPayload",
    "IMutField",
    "Measure",
    "SemanticType",
    "ViewQuery",
    "WorkflowStep",
    "execute_workflow",
    "get_fields",
    "walk",
]
__version__ = "0.1.3"


def __getattr__(name: str):
    if name == "walk":
        from polars_gw.viz import walk

        return walk
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
