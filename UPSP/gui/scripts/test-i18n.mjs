import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { build } from "esbuild";

const guiRoot = resolve(import.meta.dirname, "..");
const temporary = await mkdtemp(join(tmpdir(), "upsp-i18n-"));
try {
  const result = await build({
    entryPoints: [resolve(guiRoot, "src", "i18n.ts")],
    bundle: true,
    write: false,
    format: "esm",
    platform: "node",
    target: "node22",
    logLevel: "silent",
  });
  const modulePath = join(temporary, "i18n.mjs");
  await writeFile(modulePath, result.outputFiles[0].contents);
  const i18n = await import(`${pathToFileURL(modulePath).href}?v=${Date.now()}`);

  assert.equal(i18n.systemLocale(["zh-Hant-TW", "en-US"]), "zh-CN");
  assert.equal(i18n.systemLocale(["en-GB"]), "en-US");
  assert.equal(i18n.systemLocale(["fr-FR"]), "zh-CN");
  assert.equal(i18n.initialLocale(["zh-CN"]), "zh-CN");
  assert.equal(i18n.initialLocale(["en-US"]), "en-US");
  assert.equal(i18n.configuredLocale("system", ["en-US"]), "en-US");
  assert.equal(i18n.configuredLocale("zh-CN", ["en-US"]), "zh-CN");
  assert.equal(i18n.configuredLocale("invalid", ["fr-FR"]), "zh-CN");
  assert.equal(i18n.LOCALE_STORAGE_KEY, undefined);
  assert.equal(i18n.storedLocale, undefined);

  for (const [key, value] of Object.entries(i18n.english)) {
    assert.ok(key.trim(), "Chinese dictionary key must not be empty");
    assert.ok(value.trim(), `English translation must not be empty: ${key}`);
    const parameters = (text) => [...text.matchAll(/\{([^}]+)\}/g)].map((match) => match[1]).sort();
    assert.deepEqual(parameters(value), parameters(key), `Interpolation parameters differ: ${key}`);
  }

  i18n.setLocale("en-US");
  assert.equal(i18n.t("轮次 {round} 事件目录", { round: 578 }), "Round 578 event index");
  assert.equal(i18n.runtimeTerm("cleanup"), "Cleanup");
  assert.equal(i18n.runtimeTerm("close_requested"), "Close requested");
  assert.equal(i18n.runtimeTerm("cleanup_pending"), "Cleanup pending");
  assert.equal(i18n.runtimeTerm("closed"), "Closed");
  assert.equal(i18n.runtimeTerm("pending"), "Pending settlement");
  assert.equal(i18n.runtimeTerm("future_state"), "future_state");
  i18n.setLocale("zh-CN");
  assert.equal(i18n.runtimeTerm("cleanup"), "善后");
  assert.equal(i18n.runtimeTerm("close_requested"), "待闭合");
  assert.equal(i18n.runtimeTerm("cleanup_pending"), "善后中");
  assert.equal(i18n.runtimeTerm("closed"), "已闭合");
  assert.equal(i18n.runtimeTerm("pending"), "待结算");

  const sourceFiles = ["index.html", "src/bootstrap.ts", "src/state.ts", "src/view.ts"];
  const forbidden = [
    "OUTLINER / RUNTIME",
    "STATUSBAR SNAPSHOT",
    "CONTENT DETAIL",
    "TEN-LAYER LEDGER",
    "RULES / REGISTRY",
    "DOCS / REGISTRY",
    "模型与 Provider",
    "UI 主题",
  ];
  for (const file of sourceFiles) {
    const source = await readFile(resolve(guiRoot, file), "utf8");
    for (const phrase of forbidden) assert.ok(!source.includes(phrase), `${file} contains governed product copy: ${phrase}`);
  }
  const i18nSource = await readFile(resolve(guiRoot, "src", "i18n.ts"), "utf8");
  assert.ok(!i18nSource.includes("localStorage"), "locale truth must not use localStorage");

  const manuals = [
    "audit-tools.md",
    "base-serial.md",
    "content-window.md",
    "context-layers.md",
    "intro.md",
    "memory-bus.md",
    "step-wheel.md",
    "work-containers.md",
  ];
  for (const manual of manuals) {
    const englishManual = manual.replace(/\.md$/, ".en-US.md");
    assert.ok((await readFile(resolve(guiRoot, "manual", manual), "utf8")).trim());
    assert.ok((await readFile(resolve(guiRoot, "manual", englishManual), "utf8")).trim());
  }
} finally {
  await rm(temporary, { recursive: true, force: true });
}
