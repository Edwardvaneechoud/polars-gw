import pytest

from gw_polars.executor import clear_cache


@pytest.fixture(autouse=True)
def _clear_executor_cache():
    clear_cache()
