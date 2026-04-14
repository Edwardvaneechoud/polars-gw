# js/ — viz bundle build

Builds the `@kanaries/graphic-walker` UI + React into a single
self-contained JS/CSS pair that ships inside the `gw-polars` wheel
under `gw_polars/viz_assets/`.

End users **do not need Node** — they install `gw-polars[viz]` and the
pre-built assets come along.  This directory is only for maintainers
who bump Graphic Walker.

## Build

```bash
cd js
npm install           # one-time, pulls Node deps
npm run build         # bundles + writes gw_polars/viz_assets/
```

Outputs committed to the repo:

- `gw_polars/viz_assets/graphic-walker.js`   (minified IIFE, ~few MB)
- `gw_polars/viz_assets/graphic-walker.css`
- `gw_polars/viz_assets/versions.json`       (pinned versions + build date)

## Bumping Graphic Walker

1. Edit `dependencies` in `js/package.json`.
2. `rm -rf node_modules package-lock.json && npm install`
3. `npm run build`
4. `git add gw_polars/viz_assets js/package.json js/package-lock.json`
5. Sanity-check in the browser (`uv run python example/walk_demo.py`).
