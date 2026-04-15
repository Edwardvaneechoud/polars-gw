"""Optional UI entrypoint: launch a local Graphic Walker UI against a Polars DataFrame.

Requires the ``viz`` extras::

    pip install 'polars-gw[viz]'

Usage::

    import polars as pl
    from polars_gw import walk

    df = pl.read_parquet("sales.parquet")
    handle = walk(df)
    ...
    handle.stop()
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import webbrowser
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

import polars as pl

from polars_gw.executor import DEFAULT_MAX_ROWS, execute_workflow
from polars_gw.fields import get_fields

logger = logging.getLogger(__name__)

try:
    import uvicorn
    from fastapi import FastAPI, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel

    _VIZ_IMPORT_ERROR: ImportError | None = None

    class ComputeRequest(BaseModel):
        """Body of /api/compute — mirrors Graphic Walker's IDataQueryPayload."""

        workflow: list[dict[str, Any]] = []
        limit: int | None = None
        offset: int | None = None

except ImportError as _exc:  # pragma: no cover - exercised only without extras installed
    uvicorn = None  # type: ignore[assignment]
    FastAPI = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    StaticFiles = None  # type: ignore[assignment]
    ComputeRequest = None  # type: ignore[assignment]
    _VIZ_IMPORT_ERROR = _exc


_ASSETS_PACKAGE = "polars_gw.viz_assets"


def _assets_dir() -> str:
    """Return an absolute filesystem path to the bundled viz assets."""
    try:
        path = resources.files(_ASSETS_PACKAGE)
    except ModuleNotFoundError as exc:  # pragma: no cover - misbuilt wheel
        raise RuntimeError(
            "polars-gw viz bundle is missing — did you `pip install` from a "
            "source checkout without building? Run `npm install && npm run "
            "build` inside `js/`, or reinstall from a published wheel."
        ) from exc
    return str(path)


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Graphic Walker — polars-gw</title>
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
          specUrl: "/api/spec",
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
            "Install with: pip install 'polars-gw[viz]'"
        ) from _VIZ_IMPORT_ERROR


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@dataclass
class WalkHandle:
    """Handle returned by :func:`walk` — exposes the URL and a stop method."""

    url: str
    _server: Any
    _thread: threading.Thread
    _spec_store: list = field(default_factory=list, repr=False)
    _spec_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background server to shut down and wait for it."""
        self._server.should_exit = True
        self._thread.join(timeout=timeout)

    def export(self, path: str | None = None) -> list[dict]:
        """Return the current chart spec as a list of IChart dicts.

        If *path* is given, the spec is also written to that file as
        pretty-printed JSON.
        """
        with self._spec_lock:
            spec = list(self._spec_store)
        if path is not None:
            p = Path(path)
            p.write_text(json.dumps(spec, indent=2, ensure_ascii=False))
            logger.info("Spec exported to %s (%d chart(s))", p, len(spec))
        return spec

    def __repr__(self) -> str:  # pragma: no cover
        return f"WalkHandle(url={self.url!r})"


def _ensure_console_logging(level: str) -> None:
    """Attach a stderr handler to the ``polars_gw`` logger if none exists.

    Library code normally shouldn't add handlers, but ``walk()`` is an
    interactive entrypoint — without this, users running in the REPL or
    a Jupyter notebook see nothing when execute_workflow logs request
    timings or warns about row caps.

    No-op if anyone (the user, a framework, pytest's caplog, etc.) has
    already configured a handler on the package logger or the root
    logger.  Idempotent: a sentinel attr makes repeated calls cheap.
    """
    pkg_logger = logging.getLogger("polars_gw")
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
    spec_file: str | None = None,
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
    :func:`polars_gw.execute_workflow`.

    Args:
        df: The DataFrame (or LazyFrame) to explore.  LazyFrames are
            collected eagerly so the field schema is stable.
        spec_file: Path to a JSON file containing a saved Graphic Walker
            chart spec (an ``IChart[]`` array).  When provided, the
            charts are restored in the UI on load.  Use
            :meth:`WalkHandle.export` to save a spec file.
        host: Interface to bind (default ``127.0.0.1``).
        port: Port to bind; an ephemeral free port is picked if ``None``.
        open_browser: Whether to open the URL in the default browser.
        max_rows: Hard row cap applied to every compute response — see
            :func:`execute_workflow`.  Defaults to
            :data:`polars_gw.DEFAULT_MAX_ROWS` (1 000 000).  When the cap
            is hit, a WARNING is emitted and Graphic Walker shows its
            "Data Limit Reached" toast in the UI.  Pass ``None`` to
            disable the cap entirely.
        log_level: Python logging level for the ``polars_gw`` logger
            and for uvicorn's request logs.  Defaults to ``"info"`` so
            you see compute timings + cap warnings in the REPL.  Set
            to ``"warning"`` for less noise, ``"debug"`` for more.
        field_overrides: Forwarded to :func:`get_fields` — shallow-merge
            override for any per-field key (``analyticType``,
            ``semanticType``, ``aggName``, …). Useful when the dtype rule
            mis-labels a column (e.g. an int code that should be a measure).

    Returns:
        A :class:`WalkHandle` with ``.url``, ``.stop()``, and ``.export()``.

    Raises:
        ImportError: if the ``viz`` extras are not installed.
        FileNotFoundError: if *spec_file* does not exist.
        ValueError: if *spec_file* does not contain a JSON array.
    """
    _require_viz()
    _ensure_console_logging(log_level)

    spec_lock = threading.Lock()
    spec_store: list[dict] = []
    if spec_file is not None:
        p = Path(spec_file)
        if not p.exists():
            raise FileNotFoundError(f"Spec file not found: {p}")
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"Spec file must contain a JSON array, got {type(raw).__name__}")
        spec_store = raw
        logger.info("Loaded %d chart(s) from %s", len(spec_store), p)

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

    @app.get("/api/spec")
    def _api_spec_get() -> list[dict[str, Any]]:
        with spec_lock:
            return list(spec_store)

    @app.post("/api/spec")
    async def _api_spec_post(request: Request) -> dict[str, str]:
        body = await request.json()
        if not isinstance(body, list):
            return {"status": "error", "detail": "expected a JSON array"}
        with spec_lock:
            spec_store.clear()
            spec_store.extend(body)
        logger.debug("Spec updated: %d chart(s)", len(body))
        return {"status": "ok"}

    bind_port = _free_port() if port is None else port
    config = uvicorn.Config(app, host=host, port=bind_port, log_level=log_level.lower(), timeout_keep_alive=60)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True, name="polars-gw-viz")
    thread.start()

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not server.started:
        time.sleep(0.05)

    url = f"http://{host}:{bind_port}"
    if isinstance(df, pl.DataFrame):
        logger.info(
            "Graphic Walker running on %s — %d rows x %d cols, max_rows=%s",
            url, df.shape[0], df.shape[1], max_rows,
        )
    elif isinstance(df, pl.LazyFrame):
        df: pl.LazyFrame
        logger.info(
            "Graphic Walker running on %s — n rows x %d cols, max_rows=%s",
            url, len(df.collect_schema().names()), max_rows,
        )
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception as e:  # noqa: BLE001 - browser failures are non-fatal
            logger.warning("Could not open browser automatically: %s", e)

    return WalkHandle(url=url, _server=server, _thread=thread, _spec_store=spec_store, _spec_lock=spec_lock)
