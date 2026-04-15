"""polars-gw: Native Polars computation engine for Graphic Walker."""

from gw_polars.executor import DEFAULT_MAX_ROWS, execute_workflow
from gw_polars.fields import get_fields
from gw_polars.types import (
    Aggregator,
    AnalyticType,
    IDataQueryPayload,
    IMutField,
    Measure,
    SemanticType,
    ViewQuery,
    WorkflowStep,
)
from gw_polars.viz import walk

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
__version__ = "0.1.0"
