"""Optional UI entrypoint: launch a local Graphic Walker UI against a Polars DataFrame.

Requires the ``viz`` extras::

    pip install 'gw-polars[viz]'

Usage::

    import polars as pl
    from gw_polars import walk

    df = pl.read_parquet("sales.parquet")
    handle = walk(df)
    ...
    handle.stop()
"""

from __future__ import annotations

import logging
import socket
import threading
import time
import webbrowser
from dataclasses import dataclass
from importlib import resources
from typing import Any

import polars as pl

from gw_polars.executor import DEFAULT_MAX_ROWS, execute_workflow
from gw_polars.fields import get_fields

logger = logging.getLogger(__name__)

try:
    import uvicorn
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel

    _VIZ_IMPORT_ERROR: ImportError | None = None

    class ComputeRequest(BaseModel):
        """Body of /api/compute — mirrors Graphic Walker's IDataQueryPayload."""

        workflow: list = []
        limit: int | None = None
        offset: int | None = None

except ImportError as _exc:  # pragma: no cover - exercised only without extras installed
    uvicorn = None  # type: ignore[assignment]
    FastAPI = None  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    StaticFiles = None  # type: ignore[assignment]
    ComputeRequest = None  # type: ignore[assignment]
    _VIZ_IMPORT_ERROR = _exc


# Location of the bundled viz assets inside the package.  Built from
# `js/` by `npm run build` and shipped inside the wheel — see
# `gw_polars/viz_assets/versions.json` for the exact pinned versions.
_ASSETS_PACKAGE = "gw_polars.viz_assets"


def _assets_dir() -> str:
    """Return an absolute filesystem path to the bundled viz assets.

    Works in both editable installs (reads straight from the repo) and
    wheel installs (reads from site-packages).  Raises a clear error if
    the bundle is missing — typically means the repo is checked out
    without having run `npm run build` in `js/`.
    """
    try:
        path = resources.files(_ASSETS_PACKAGE)
    except ModuleNotFoundError as exc:  # pragma: no cover - misbuilt wheel
        raise RuntimeError(
            "gw-polars viz bundle is missing — did you `pip install` from a "
            "source checkout without building? Run `npm install && npm run "
            "build` inside `js/`, or reinstall from a published wheel."
        ) from exc
    # importlib.resources returns a Traversable; MultiplexedPath/PosixPath
    # both str() cleanly to a filesystem path for our use case.
    return str(path)


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Graphic Walker — gw-polars</title>
  <link rel="stylesheet" href="/static/graphic-walker.css">
  <style>
    html, body, #root { margin: 0; padding: 0; height: 100%; width: 100%; }
    body { font-family: system-ui, sans-serif; }
    #gwp-error {
      padding: 1rem 1.25rem; margin: 1rem; border: 1px solid #f5c2c7;
      background: #f8d7da; color: #842029; border-radius: 4px;
      font-family: ui-monospace, Menlo, Consolas, monospace; white-space: pre-wrap;
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script src="/static/graphic-walker.js"></script>
  <script>
    (async () => {
      try {
        await window.__gwpRender(document.getElementById("root"), {
          fieldsUrl: "/api/fields",
          computeUrl: "/api/compute",
          appearance: "light",
        });
      } catch (e) {
        const el = document.createElement("div");
        el.id = "gwp-error";
        el.textContent = "Failed to load Graphic Walker:\\n" + (e && e.stack || e);
        document.body.appendChild(el);
        throw e;
      }
    })();
  </script>
</body>
</html>
"""


def _require_viz() -> None:
    if _VIZ_IMPORT_ERROR is not None:
        raise ImportError(
            "The walk() feature requires extra dependencies. "
            "Install with: pip install 'gw-polars[viz]'"
        ) from _VIZ_IMPORT_ERROR


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@dataclass
class WalkHandle:
    """Handle returned by :func:`walk` — exposes the URL and a stop method."""

    url: str
    _server: Any  # uvicorn.Server when extras installed
    _thread: threading.Thread

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background server to shut down and wait for it."""
        self._server.should_exit = True
        self._thread.join(timeout=timeout)

    # Convenience for interactive/REPL use
    def __repr__(self) -> str:  # pragma: no cover - display only
        return f"WalkHandle(url={self.url!r})"


def walk(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    host: str = "127.0.0.1",
    port: int | None = None,
    open_browser: bool = True,
    max_rows: int | None = DEFAULT_MAX_ROWS,
) -> WalkHandle:
    """Launch a local Graphic Walker UI connected to ``df``.

    Starts a FastAPI server in a background daemon thread that serves the
    Graphic Walker UI and wires its ``computation`` callback to
    :func:`gw_polars.execute_workflow`.

    Args:
        df: The DataFrame (or LazyFrame) to explore.  LazyFrames are
            collected eagerly so the field schema is stable.
        host: Interface to bind (default ``127.0.0.1``).
        port: Port to bind; an ephemeral free port is picked if ``None``.
        open_browser: Whether to open the URL in the default browser.
        max_rows: Hard row cap applied to every compute response — see
            :func:`execute_workflow`.  Defaults to
            :data:`gw_polars.DEFAULT_MAX_ROWS`.

    Returns:
        A :class:`WalkHandle` with ``.url`` and ``.stop()``.

    Raises:
        ImportError: if the ``viz`` extras are not installed.
    """
    _require_viz()

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    fields = get_fields(df)

    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount(
        "/static",
        StaticFiles(directory=_assets_dir()),
        name="gwp-static",
    )

    @app.get("/", response_class=HTMLResponse)
    def _index() -> str:
        return _HTML_TEMPLATE

    @app.post("/api/fields")
    def _api_fields() -> list[dict[str, Any]]:
        return fields

    @app.post("/api/compute")
    def _api_compute(request: ComputeRequest) -> list[dict[str, Any]]:
        return execute_workflow(df, request.model_dump(), max_rows=max_rows)

    bind_port = _free_port() if port is None else port
    config = uvicorn.Config(app, host=host, port=bind_port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True, name="gw-polars-viz")
    thread.start()

    # Wait briefly for the server to finish starting so the browser
    # doesn't get connection-refused on the first request.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not server.started:
        time.sleep(0.05)

    url = f"http://{host}:{bind_port}"
    logger.info("Graphic Walker running on %s (%d rows x %d cols)", url, df.shape[0], df.shape[1])

    if open_browser:
        try:
            webbrowser.open(url)
        except Exception as e:  # noqa: BLE001 - browser failures are non-fatal
            logger.warning("Could not open browser automatically: %s", e)

    return WalkHandle(url=url, _server=server, _thread=thread)
