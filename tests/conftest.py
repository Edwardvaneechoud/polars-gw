import pytest

from polars_gw.executor import clear_cache


@pytest.fixture(autouse=True)
def _clear_executor_cache():
    clear_cache()
