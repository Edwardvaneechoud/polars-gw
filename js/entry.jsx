import React from "react";
import { createRoot } from "react-dom/client";
import { GraphicWalker } from "@kanaries/graphic-walker";

const DEFAULTS = {
  fieldsUrl: "/api/fields",
  computeUrl: "/api/compute",
  specUrl: "/api/spec",
  appearance: "light",
};

function GWApp({ fields, computation, appearance, initialChart, specSaveUrl }) {
  const storeRef = React.useRef(null);
  const lastSpecJson = React.useRef("");

  React.useEffect(() => {
    const interval = setInterval(() => {
      if (!storeRef.current) return;
      try {
        const spec = storeRef.current.exportCode();
        const json = JSON.stringify(spec);
        if (json !== lastSpecJson.current) {
          lastSpecJson.current = json;
          fetch(specSaveUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: json,
          });
        }
      } catch (e) {}
    }, 2000);
    return () => clearInterval(interval);
  }, [specSaveUrl]);

  return React.createElement(GraphicWalker, {
    fields,
    computation,
    appearance,
    chart: initialChart || undefined,
    storeRef,
  });
}

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

  let initialChart = null;
  try {
    const specResp = await fetch(opts.specUrl);
    if (specResp.ok) {
      const spec = await specResp.json();
      if (Array.isArray(spec) && spec.length > 0) {
        initialChart = spec;
      }
    }
  } catch (e) {}

  const root = createRoot(rootEl);
  root.render(
    React.createElement(GWApp, {
      fields,
      computation,
      appearance: opts.appearance,
      initialChart,
      specSaveUrl: opts.specUrl,
    }),
  );
  return root;
}

window.__gwpRender = render;
