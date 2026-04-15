"""Tests for polars_gw.viz — skipped entirely if the viz extras are absent."""

from __future__ import annotations

import socket
import time
import urllib.request
from urllib.error import URLError

import polars as pl
import pytest

# Skip the whole module when the viz extras aren't installed.
pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")


from polars_gw import walk  # noqa: E402


def _wait_until_up(url: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url) as r:  # noqa: S310 - localhost only
                if r.status == 200:
                    return
        except URLError as e:
            last = e
            time.sleep(0.05)
    raise AssertionError(f"server did not come up in {timeout}s: {last}")


def _post(url: str, body: bytes = b"") -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:  # noqa: S310 - localhost only
        return r.status, r.read()


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "city": ["Amsterdam", "Berlin", "Paris"],
        "sales": [100, 200, 300],
    })


class TestWalk:
    def test_walk_serves_ui(self, sample_df: pl.DataFrame) -> None:
        handle = walk(sample_df, open_browser=False)
        try:
            _wait_until_up(handle.url)
            with urllib.request.urlopen(handle.url) as r:  # noqa: S310 - localhost
                body = r.read().decode()
            assert "graphic-walker" in body.lower()
            assert r.status == 200
        finally:
            handle.stop()

    def test_walk_serves_bundled_assets(self, sample_df: pl.DataFrame) -> None:
        """The bundled JS/CSS are served out of /static/ (no CDN)."""
        handle = walk(sample_df, open_browser=False)
        try:
            _wait_until_up(handle.url)
            with urllib.request.urlopen(f"{handle.url}/static/graphic-walker.js") as r:  # noqa: S310
                assert r.status == 200
                body = r.read()
                # Should be the minified bundle — expect the entry helper name.
                assert b"__gwpRender" in body
                assert len(body) > 100_000  # bundle is several MB
            with urllib.request.urlopen(f"{handle.url}/static/graphic-walker.css") as r:  # noqa: S310
                assert r.status == 200
                assert len(r.read()) > 1_000
        finally:
            handle.stop()

    def test_api_fields(self, sample_df: pl.DataFrame) -> None:
        import json

        handle = walk(sample_df, open_browser=False)
        try:
            _wait_until_up(handle.url)
            status, body = _post(f"{handle.url}/api/fields")
            assert status == 200
            fields = json.loads(body)
            fids = [f["fid"] for f in fields]
            assert fids == ["city", "sales"]
        finally:
            handle.stop()

    def test_api_compute_empty_workflow(self, sample_df: pl.DataFrame) -> None:
        import json

        handle = walk(sample_df, open_browser=False)
        try:
            _wait_until_up(handle.url)
            status, body = _post(f"{handle.url}/api/compute", b'{"workflow":[]}')
            assert status == 200
            rows = json.loads(body)
            assert len(rows) == 3
            assert {r["city"] for r in rows} == {"Amsterdam", "Berlin", "Paris"}
        finally:
            handle.stop()

    def test_handle_stop_frees_port(self, sample_df: pl.DataFrame) -> None:
        handle = walk(sample_df, open_browser=False)
        _wait_until_up(handle.url)
        port = int(handle.url.rsplit(":", 1)[1])
        handle.stop()
        # Port should be reusable shortly after stop().  Try a few times
        # because uvicorn may not release the socket instantly.
        for _ in range(20):
            try:
                with socket.socket() as s:
                    s.bind(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.1)
        pytest.fail(f"port {port} was not released after handle.stop()")


def test_walk_without_viz_extras_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate the extras missing: _VIZ_IMPORT_ERROR set → walk() raises."""
    from polars_gw import viz

    monkeypatch.setattr(viz, "_VIZ_IMPORT_ERROR", ImportError("No module named 'fastapi'"))
    with pytest.raises(ImportError, match=r"polars-gw\[viz\]"):
        viz.walk(pl.DataFrame({"a": [1]}))


class TestLogging:
    """walk() should surface compute logs + cap warnings to the console."""

    def test_compute_log_emitted(self, caplog: pytest.LogCaptureFixture) -> None:
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        handle = walk(df, open_browser=False)
        try:
            _wait_until_up(handle.url)
            with caplog.at_level("INFO", logger="polars_gw"):
                _post(f"{handle.url}/api/compute", b'{"workflow":[]}')
            messages = [r.getMessage() for r in caplog.records]
            assert any("compute:" in m and "row(s)" in m and "ms" in m for m in messages), messages
        finally:
            handle.stop()

    def test_max_rows_cap_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """When max_rows truncates a result, log marks it [CAPPED]."""
        df = pl.DataFrame({"a": list(range(100))})
        handle = walk(df, open_browser=False, max_rows=10)
        try:
            _wait_until_up(handle.url)
            with caplog.at_level("INFO", logger="polars_gw"):
                _post(f"{handle.url}/api/compute", b'{"workflow":[]}')
            messages = [r.getMessage() for r in caplog.records]
            assert any("[CAPPED]" in m for m in messages), messages
        finally:
            handle.stop()
