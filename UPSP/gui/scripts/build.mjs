import { mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const guiRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const assetsRoot = resolve(guiRoot, "assets", "markdown");
const common = {
  bundle: true,
  write: false,
  platform: "browser",
  target: "es2022",
  charset: "ascii",
  legalComments: "none",
  minify: false,
  sourcemap: false,
  logLevel: "silent",
};

const results = await Promise.all([
  build({
    ...common,
    entryPoints: [resolve(guiRoot, "src", "app.ts")],
    outfile: resolve(guiRoot, "app.js"),
    format: "iife",
    supported: { "template-literal": false },
    banner: { js: "/* Generated from src/app.ts. Do not edit app.js directly. */" },
  }),
  build({
    ...common,
    entryPoints: [resolve(guiRoot, "src", "markdown-mermaid.ts")],
    outfile: resolve(guiRoot, "markdown-mermaid.js"),
    format: "esm",
    minify: true,
    banner: { js: "/* Generated from src/markdown-mermaid.ts. Do not edit directly. */" },
  }),
  build({
    ...common,
    entryPoints: [resolve(guiRoot, "src", "markdown.css")],
    outfile: resolve(guiRoot, "markdown.css"),
    loader: { ".woff2": "file", ".woff": "file", ".ttf": "file" },
    assetNames: "assets/markdown/[name]",
    plugins: [{
      name: "katex-woff2-only",
      setup(context) {
        context.onLoad({ filter: /katex(?:\.min)?\.css$/ }, async ({ path }) => ({
          contents: (await readFile(path, "utf8"))
            .replace(/,url\([^)]*\.(?:woff|ttf)\)\s*format\("[^"]+"\)/g, ""),
          loader: "css",
          resolveDir: dirname(path),
        }));
      },
    }],
  }),
]);

const generated = new Map();
for (const result of results) {
  for (const output of result.outputFiles) {
    generated.set(resolve(output.path), output.contents);
  }
}

async function currentAssetFiles() {
  try {
    return (await readdir(assetsRoot, { withFileTypes: true }))
      .filter((entry) => entry.isFile())
      .map((entry) => resolve(assetsRoot, entry.name));
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
}

const expectedAssets = new Set([...generated.keys()].filter((path) => dirname(path) === assetsRoot));
const existingAssets = await currentAssetFiles();

if (process.argv.includes("--check")) {
  const failures = [];
  for (const [path, contents] of generated) {
    try {
      if (!(await readFile(path)).equals(contents)) failures.push(`stale ${relative(guiRoot, path)}`);
    } catch (error) {
      if (error?.code === "ENOENT") failures.push(`missing ${relative(guiRoot, path)}`);
      else throw error;
    }
  }
  for (const path of existingAssets) {
    if (!expectedAssets.has(path)) failures.push(`extra ${relative(guiRoot, path)}`);
  }
  if (failures.length) {
    console.error(`generated GUI assets are stale:\n${failures.join("\n")}`);
    process.exitCode = 1;
  }
} else {
  await mkdir(assetsRoot, { recursive: true });
  for (const path of existingAssets) {
    if (!expectedAssets.has(path)) await unlink(path);
  }
  for (const [path, contents] of generated) {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, contents);
  }
}
