// Bundles Graphic Walker + React + our entry point into a single IIFE
// JS file and copies the matching CSS into polars_gw/viz_assets/.
//
// Modes:
//   node build.mjs            # one-shot production build (minified)
//   node build.mjs --watch    # dev mode: watch + rebuild + source maps
//
// The production output files are committed to the repo so
// `pip install` never needs Node.

import { context, build } from "esbuild";
import { spawn } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const outDir = resolve(repoRoot, "polars_gw", "viz_assets");
const isWatch = process.argv.includes("--watch");

await mkdir(outDir, { recursive: true });

// -------------------------------------------------------- esbuild options
// Force a single React / React-DOM instance for the whole bundle.  The
// error #527 "Incompatible React versions" comes from two copies of
// React ending up in the same bundle (one from our root node_modules,
// another nested inside a transitive dep).  The `alias` map guarantees
// every `import "react"` or `import "react-dom/*"` resolves to exactly
// one path on disk.
const reactRoot = resolve(__dirname, "node_modules", "react");
const reactDomRoot = resolve(__dirname, "node_modules", "react-dom");

const esbuildOptions = {
  entryPoints: [resolve(__dirname, "entry.jsx")],
  bundle: true,
  minify: !isWatch,
  sourcemap: isWatch ? "inline" : false,
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
    "process.env.NODE_ENV": isWatch ? '"development"' : '"production"',
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
};

// --------------------------------------------------------------- versions
async function writeVersions() {
  const pkg = JSON.parse(
    await readFile(resolve(__dirname, "package.json"), "utf8"),
  );
  const versions = {
    "@kanaries/graphic-walker": pkg.dependencies["@kanaries/graphic-walker"],
    react: pkg.dependencies.react,
    "react-dom": pkg.dependencies["react-dom"],
    mode: isWatch ? "dev" : "production",
    builtAt: new Date().toISOString(),
  };
  await writeFile(
    resolve(outDir, "versions.json"),
    JSON.stringify(versions, null, 2) + "\n",
  );
  return versions;
}

// ---------------------------------------------------------- tailwind helpers
const tailwindBin = resolve(__dirname, "node_modules", ".bin", "tailwindcss");
const tailwindBaseArgs = [
  "-c", resolve(__dirname, "tailwind.config.cjs"),
  "-i", resolve(__dirname, "entry.css"),
  "-o", resolve(outDir, "graphic-walker.css"),
  "--postcss", resolve(__dirname, "postcss.config.cjs"),
];

function spawnTailwind(extraArgs) {
  const args = [...tailwindBaseArgs, ...extraArgs];
  const child = spawn(tailwindBin, args, { stdio: "inherit", cwd: __dirname });
  return new Promise((resolveProm, rejectProm) => {
    child.on("exit", (code) => (code === 0 ? resolveProm() : rejectProm(new Error(`tailwindcss exited with ${code}`))));
    child.on("error", rejectProm);
  });
}

// ================================================================ run
if (isWatch) {
  console.log("🔎  gw-polars viz bundle — WATCH MODE");
  console.log("    JS + CSS rebuild on file changes; source maps enabled.");
  console.log("    Reload the browser tab after a rebuild.\n");

  // esbuild watcher
  const ctx = await context(esbuildOptions);
  await ctx.watch();

  // tailwind watcher (spawned in parallel, long-running)
  const tailwindChild = spawn(
    tailwindBin,
    [...tailwindBaseArgs, "--watch"],
    { stdio: "inherit", cwd: __dirname },
  );

  await writeVersions();
  console.log("gw-polars viz bundle (dev) watching →", outDir);

  // Cleanly shut down on Ctrl+C
  const shutdown = async () => {
    tailwindChild.kill("SIGINT");
    await ctx.dispose();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
} else {
  // one-shot production build
  await build(esbuildOptions);

  console.log("Building Tailwind CSS bundle…");
  await spawnTailwind(["--minify"]);

  const versions = await writeVersions();
  console.log("gw-polars viz bundle written to:", outDir);
  console.log(versions);
}
