# js/ — viz bundle build

Builds the `@kanaries/graphic-walker` UI + React into a single
self-contained JS/CSS pair that ships inside the `gw-polars` wheel
under `gw_polars/viz_assets/`.

End users **do not need Node** — they install `gw-polars[viz]` and the
pre-built assets come along.  This directory is only for maintainers
who bump Graphic Walker.

## Setup

```bash
cd js
npm install           # one-time, pulls Node deps
```

## Production build

```bash
npm run build         # minified, no source maps, mode=production
```

Outputs committed to the repo:

- `gw_polars/viz_assets/graphic-walker.js`   (minified IIFE, ~4.4 MB)
- `gw_polars/viz_assets/graphic-walker.css`  (~57 KB)
- `gw_polars/viz_assets/versions.json`       (pinned versions + build date + mode)

## Dev / watch mode

For iterating on `entry.jsx`, `entry.css`, or the Tailwind config:

```bash
npm run dev           # watches entry.jsx + entry.css + GW source
                      # rebuilds JS + CSS on change
                      # unminified + inline source maps (~17 MB)
```

Pair it with the Python server in another shell:

```bash
uv run python example/walk_demo.py
```

The Python server reads `gw_polars/viz_assets/` on every page load,
so refreshing the browser tab picks up the latest watch-mode build.
Devtools will show real source positions (entry.jsx, GW src files).

**Don't commit a dev build** — versions.json includes `"mode": "dev"`
as a marker.  Always finish with `npm run build` before committing.

## Bumping Graphic Walker

1. Edit `dependencies` in `js/package.json`.
2. `rm -rf node_modules package-lock.json && npm install`
3. `npm run build`
4. Sanity-check in the browser (`uv run python example/walk_demo.py`).
5. `git add gw_polars/viz_assets js/package.json js/package-lock.json`

## Why .npmrc has `legacy-peer-deps=true`

GW 0.5.0's top-level peer dep declares `react@>=19`, but its own
transitive deps (`@headlessui/react`, `react-leaflet`,
`mobx-react-lite`, `react-resizable-panels`, …) still cap at
`react@^18`.  npm strict-peer-resolution refuses this combination.
We accept the override — empirically the bundle works against React
19.2.0 (the version GW's prebuilt dist is baked against).

## Why `react` and `react-dom` are pinned to exactly `19.2.0`

GW 0.5.0's prebuilt dist inlines react-dom and runtime-checks the
React version with `if (z !== "19.2.0") throw Error(527, …)`.
Any other React version breaks the bundle at load time.
