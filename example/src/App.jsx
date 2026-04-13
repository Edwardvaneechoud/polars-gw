import React, { useState, useEffect } from "react";
import { GraphicWalker } from "@kanaries/graphic-walker";

const API = "http://localhost:8787";

/**
 * The computation function that Graphic Walker calls on every user interaction.
 * It forwards the IDataQueryPayload to our FastAPI backend (powered by gw-polars).
 */
async function computation(payload) {
  const res = await fetch(`${API}/api/compute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    console.error("Compute failed:", res.status, await res.text());
    return [];
  }
  return res.json();
}

export default function App() {
  const [fields, setFields] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch(`${API}/api/fields`, { method: "POST" })
      .then((r) => r.json())
      .then(setFields)
      .catch((e) => setError(`Cannot connect to backend at ${API} — is server.py running?\n\n${e.message}`));
  }, []);

  if (error) {
    return (
      <div style={{ padding: 40, color: "#c00" }}>
        <h2>Backend not reachable</h2>
        <pre>{error}</pre>
        <p>
          Start the backend first:
          <code style={{ display: "block", margin: "12px 0", padding: 8, background: "#f5f5f5" }}>
            cd gw_polars/example && python server.py
          </code>
        </p>
      </div>
    );
  }

  if (!fields) {
    return <div style={{ padding: 40 }}>Loading fields from backend...</div>;
  }

  return (
    <div style={{ height: "100vh" }}>
      <GraphicWalker
        computation={computation}
        fields={fields}
        appearance="light"
      />
    </div>
  );
}
