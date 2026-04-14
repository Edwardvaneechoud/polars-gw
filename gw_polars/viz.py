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


def _ensure_console_logging(level: str) -> None:
    """Attach a stderr handler to the ``gw_polars`` logger if none exists.

    Library code normally shouldn't add handlers, but ``walk()`` is an
    interactive entrypoint — without this, users running in the REPL or
    a Jupyter notebook see nothing when execute_workflow logs request
    timings or warns about row caps.

    No-op if anyone (the user, a framework, pytest's caplog, etc.) has
    already configured a handler on the package logger or the root
    logger.  Idempotent: a sentinel attr makes repeated calls cheap.
    """
    pkg_logger = logging.getLogger("gw_polars")
    pkg_logger.setLevel(level.upper())

    if getattr(pkg_logger, "_gwp_console_attached", False):
        return  # already installed by a previous walk() call
    if pkg_logger.handlers or logging.getLogger().handlers:
        return  # someone else owns logging output — don't double-print

    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")
    )
    pkg_logger.addHandler(h)
    pkg_logger._gwp_console_attached = True  # type: ignore[attr-defined]


def walk(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    host: str = "127.0.0.1",
    port: int | None = None,
    open_browser: bool = True,
    max_rows: int | None = DEFAULT_MAX_ROWS,
    log_level: str = "info",
    field_overrides: dict[str, dict[str, Any]] | None = None,
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
            :data:`gw_polars.DEFAULT_MAX_ROWS` (1 000 000).  When the cap
            is hit, a WARNING is emitted and Graphic Walker shows its
            "Data Limit Reached" toast in the UI.  Pass ``None`` to
            disable the cap entirely.
        log_level: Python logging level for the ``gw_polars`` logger
            and for uvicorn's request logs.  Defaults to ``"info"`` so
            you see compute timings + cap warnings in the REPL.  Set
            to ``"warning"`` for less noise, ``"debug"`` for more.
        field_overrides: Forwarded to :func:`get_fields` — shallow-merge
            override for any per-field key (``analyticType``,
            ``semanticType``, ``aggName``, …). Useful when the dtype rule
            mis-labels a column (e.g. an int code that should be a measure).

    Returns:
        A :class:`WalkHandle` with ``.url`` and ``.stop()``.

    Raises:
        ImportError: if the ``viz`` extras are not installed.
    """
    _require_viz()
    _ensure_console_logging(log_level)

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    fields = get_fields(df, field_overrides=field_overrides)

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
        t0 = time.monotonic()
        rows = execute_workflow(df, request.model_dump(), max_rows=max_rows)
        elapsed_ms = (time.monotonic() - t0) * 1000
        n_steps = len(request.workflow)
        logger.info(
            "compute: %d step(s) → %d row(s) in %.1f ms%s",
            n_steps,
            len(rows),
            elapsed_ms,
            " [CAPPED]" if max_rows is not None and len(rows) == max_rows else "",
        )
        return rows

    bind_port = _free_port() if port is None else port
    config = uvicorn.Config(app, host=host, port=bind_port, log_level=log_level.lower())
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True, name="gw-polars-viz")
    thread.start()

    # Wait briefly for the server to finish starting so the browser
    # doesn't get connection-refused on the first request.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not server.started:
        time.sleep(0.05)

    url = f"http://{host}:{bind_port}"
    logger.info(
        "Graphic Walker running on %s — %d rows x %d cols, max_rows=%s",
        url, df.shape[0], df.shape[1], max_rows,
    )

    if open_browser:
        try:
            webbrowser.open(url)
        except Exception as e:  # noqa: BLE001 - browser failures are non-fatal
            logger.warning("Could not open browser automatically: %s", e)

    return WalkHandle(url=url, _server=server, _thread=thread)
