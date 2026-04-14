// Bundles Graphic Walker + React + our entry point into a single IIFE
// JS file and copies the matching CSS into gw_polars/viz_assets/.
//
// Run with `npm run build` (from the js/ directory) or
// `python -m build_viz` via a helper.  The output files are committed
// to the repo so `pip install` never needs Node.

import { build } from "esbuild";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const outDir = resolve(repoRoot, "gw_polars", "viz_assets");

await mkdir(outDir, { recursive: true });

// ---------------------------------------------------------------- JS
// Force a single React / React-DOM instance for the whole bundle.  The
// error #527 "Incompatible React versions" comes from two copies of
// React ending up in the same bundle (one from our root node_modules,
// another nested inside a transitive dep).  The `alias` map guarantees
// every `import "react"` or `import "react-dom/*"` resolves to exactly
// one path on disk.
const reactRoot = resolve(__dirname, "node_modules", "react");
const reactDomRoot = resolve(__dirname, "node_modules", "react-dom");

await build({
  entryPoints: [resolve(__dirname, "entry.jsx")],
  bundle: true,
  minify: true,
  sourcemap: false,
  format: "iife",
  target: ["es2020"],
  platform: "browser",
  loader: { ".js": "jsx", ".jsx": "jsx" },
  jsx: "automatic",
  alias: {
    react: reactRoot,
    "react-dom": reactDomRoot,
    "react/jsx-runtime": resolve(reactRoot, "jsx-runtime.js"),
    "react/jsx-dev-runtime": resolve(reactRoot, "jsx-dev-runtime.js"),
    "react-dom/client": resolve(reactDomRoot, "client.js"),
  },
  define: {
    "process.env.NODE_ENV": '"production"',
    "process.env.DEBUG": "false",
    "process.platform": '"browser"',
    "process.browser": "true",
    global: "globalThis",
  },
  // Some deps still read `process.env.<FOO>` for unknown keys.  Inject a
  // tiny shim so those reads return undefined instead of crashing.
  banner: {
    js: "var process=typeof process!=='undefined'?process:{env:{},platform:'browser',browser:true};",
  },
  logLevel: "info",
  outfile: resolve(outDir, "graphic-walker.js"),
});

// --------------------------------------------------------------- CSS
// GW 0.5.x ships source-only CSS.  We run Tailwind CLI against GW's
// source tree + our entry.css to produce the final stylesheet.
console.log("Building Tailwind CSS bundle…");
const tailwindBin = resolve(__dirname, "node_modules", ".bin", "tailwindcss");
execFileSync(
  tailwindBin,
  [
    "-c", resolve(__dirname, "tailwind.config.cjs"),
    "-i", resolve(__dirname, "entry.css"),
    "-o", resolve(outDir, "graphic-walker.css"),
    "--postcss", resolve(__dirname, "postcss.config.cjs"),
    "--minify",
  ],
  { stdio: "inherit", cwd: __dirname },
);

// -------------------------------------------------------- version marker
const pkg = JSON.parse(
  await readFile(resolve(__dirname, "package.json"), "utf8"),
);
const versions = {
  "@kanaries/graphic-walker": pkg.dependencies["@kanaries/graphic-walker"],
  react: pkg.dependencies.react,
  "react-dom": pkg.dependencies["react-dom"],
  builtAt: new Date().toISOString(),
};
await writeFile(
  resolve(outDir, "versions.json"),
  JSON.stringify(versions, null, 2) + "\n",
);

console.log("gw-polars viz bundle written to:", outDir);
console.log(versions);
