"""gw-polars: Native Polars computation engine for Graphic Walker."""

from gw_polars.executor import DEFAULT_MAX_ROWS, execute_workflow
from gw_polars.fields import get_fields
from gw_polars.viz import walk

__all__ = ["DEFAULT_MAX_ROWS", "execute_workflow", "get_fields", "walk"]
__version__ = "0.1.0"
