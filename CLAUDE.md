# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gw-polars** is a native Polars computation engine for [Graphic Walker](https://github.com/Kanaries/graphic-walker). It translates Graphic Walker `IDataQueryPayload` workflow steps directly into Polars expressions, bypassing the DuckDB/SQL path used by PyGWalker.

## Common Commands

```bash
# Install dependencies
uv sync --extra viz            # runtime + viz + dev deps
uv sync                        # runtime + dev (no viz extras)

# Run all tests
uv run pytest

# Run a single test file or test
uv run pytest tests/test_executor.py -v
uv run pytest tests/test_executor.py::TestAggregateQuery::test_sum -v

# Lint and format
uv run ruff check .
uv run ruff format .

# Rebuild viz JS/CSS assets (maintainers only, requires Node)
cd js && npm install && npm run build
```

## Architecture

### Core Package (`gw_polars/`)

- **`executor.py`** — Central translation engine. Converts GW workflow payloads into Polars lazy query plans. Supports filters (range, temporal, one-of, not-in, regexp), aggregations, fold (unpivot), bin, raw field selection, sort, and transforms (bin, log, dateTime drill/feature, arbitrary SQL via `pl.sql_expr()`). Builds the full query lazily, then collects once at the end.

- **`fields.py`** — Converts Polars DataFrame schema to GW `IMutField` format. Classification matches PyGWalker: int→quantitative/dimension, float→quantitative/measure (with `aggName:"sum"`), temporal→temporal/dimension, string/bool/categorical→nominal/dimension. Supports user-provided field overrides.

- **`types.py`** — Literal type definitions for GW payload structures (FilterRuleType, Aggregator, ViewQueryOp, SemanticType, AnalyticType, etc.).

- **`viz.py`** — Optional `walk()` entrypoint (behind `[viz]` extra). Starts a FastAPI server on a background daemon thread, serves pre-built Graphic Walker JS/CSS from `viz_assets/`, exposes `/api/fields` and `/api/compute` endpoints. Returns a `WalkHandle` for cleanup.

### JS Bundle (`js/`)

Pre-built Graphic Walker + React IIFE bundle shipped inside the wheel so users don't need Node. Built with esbuild + tailwindcss. React 19.2.0 is pinned exactly (GW 0.5.0 runtime check). Output goes to `gw_polars/viz_assets/`.

### Key Patterns

- **Lazy evaluation**: Full Polars query plan built before collecting for optimization.
- **Context vars**: Per-request logging IDs for tracing concurrent `execute_workflow()` calls.
- **Graceful degradation**: Unknown columns/fields are silently skipped rather than crashing.
- **JSON safety**: Temporal and Decimal types are cast to JSON-serializable formats before returning.

## Configuration

- **Python**: 3.10+ required. Ruff configured with line-length 120, rules: F, E, W, I, UP, B.
- **CI**: Tests run on Python 3.10–3.13 via GitHub Actions. CI runs `ruff check` then `pytest`.
- **Publishing**: PyPI via OIDC on GitHub release events.
