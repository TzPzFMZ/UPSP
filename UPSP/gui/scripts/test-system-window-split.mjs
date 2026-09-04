import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const result = await build({
  entryPoints: [fileURLToPath(new URL("../src/system-window-split-model.ts", import.meta.url))],
  bundle: true,
  write: false,
  format: "esm",
  platform: "node",
  target: "es2022",
  logLevel: "silent",
});
const encoded = Buffer.from(result.outputFiles[0].text).toString("base64");
const model = await import(`data:text/javascript;base64,${encoded}`);

const closeTo = (actual, expected, epsilon = 1e-9) => {
  assert.ok(Math.abs(actual - expected) <= epsilon, `${actual} != ${expected}`);
};

assert.equal(model.parseStoredSystemWindowRatio(null), 0.5);
assert.equal(model.parseStoredSystemWindowRatio(""), 0.5);
assert.equal(model.parseStoredSystemWindowRatio("broken"), 0.5);
assert.equal(model.parseStoredSystemWindowRatio("0.29"), 0.5);
assert.equal(model.parseStoredSystemWindowRatio("0.71"), 0.5);
assert.equal(model.parseStoredSystemWindowRatio("0.3"), 0.3);
assert.equal(model.parseStoredSystemWindowRatio("0.7"), 0.7);

const desktop = model.computeSystemWindowSplitGeometry(1424, 0.5);
assert.equal(desktop.availableWidth, 1400);
assert.equal(desktop.systemWidth, 700);
assert.equal(desktop.effectiveRatio, 0.5);
closeTo(desktop.minRatio, 480 / 1400);
closeTo(desktop.maxRatio, 1 - 520 / 1400);

const wide = model.computeSystemWindowSplitGeometry(1860, 0.5);
assert.equal(wide.minRatio, 0.3);
assert.equal(wide.maxRatio, 0.7);
assert.equal(model.computeSystemWindowSplitGeometry(1860, 0.1).effectiveRatio, 0.3);
assert.equal(model.computeSystemWindowSplitGeometry(1860, 0.9).effectiveRatio, 0.7);

const narrow = model.computeSystemWindowSplitGeometry(900, 0.67);
assert.equal(narrow.minRatio, 0.5);
assert.equal(narrow.maxRatio, 0.5);
assert.equal(narrow.effectiveRatio, 0.5);

closeTo(model.systemWindowRatioFromPointer(1424, 700), 0.5);
closeTo(model.systemWindowRatioFromPointer(1424, 0), desktop.minRatio);
closeTo(model.systemWindowRatioFromPointer(1424, 1400), desktop.maxRatio);
assert.equal(model.systemWindowRatioFromPointer(900, 100), 0.5);

assert.equal(model.systemWindowRatioFromKey(desktop, "ArrowRight", false), 0.52);
assert.equal(model.systemWindowRatioFromKey(desktop, "ArrowLeft", false), 0.48);
assert.equal(model.systemWindowRatioFromKey(desktop, "ArrowRight", true), 0.6);
assert.equal(model.systemWindowRatioFromKey(desktop, "Home", false), desktop.minRatio);
assert.equal(model.systemWindowRatioFromKey(desktop, "End", false), desktop.maxRatio);
assert.equal(model.systemWindowRatioFromKey(desktop, "Enter", false), null);
assert.equal(model.systemWindowRatioFromKey(narrow, "ArrowRight", false), 0.5);

console.log("System window split model tests passed");
