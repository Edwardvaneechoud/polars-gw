// Entry point for the gw-polars viz bundle.
//
// Exposes a single global `window.__gwpRender(rootEl, opts)` helper that
// boots Graphic Walker against a pair of JSON API endpoints (defaults:
// /api/fields and /api/compute).  The Python `walk()` server serves the
// bundled JS + CSS and a tiny HTML page that calls this helper.

import React from "react";
import { createRoot } from "react-dom/client";
import { GraphicWalker } from "@kanaries/graphic-walker";

const DEFAULTS = {
  fieldsUrl: "/api/fields",
  computeUrl: "/api/compute",
  appearance: "light",
};

async function render(rootEl, userOpts = {}) {
  const opts = { ...DEFAULTS, ...userOpts };

  const computation = async (payload) => {
    const r = await fetch(opts.computeUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error(`compute failed: ${r.status}`);
    return r.json();
  };

  const fieldsResp = await fetch(opts.fieldsUrl, { method: "POST" });
  if (!fieldsResp.ok) throw new Error(`fields failed: ${fieldsResp.status}`);
  const fields = await fieldsResp.json();

  const root = createRoot(rootEl);
  root.render(
    React.createElement(GraphicWalker, {
      fields,
      computation,
      appearance: opts.appearance,
    }),
  );
  return root;
}

window.__gwpRender = render;
