"""gw-polars: Native Polars computation engine for Graphic Walker."""

from gw_polars.executor import execute_workflow
from gw_polars.fields import get_fields

__all__ = ["execute_workflow", "get_fields"]
__version__ = "0.1.0"
