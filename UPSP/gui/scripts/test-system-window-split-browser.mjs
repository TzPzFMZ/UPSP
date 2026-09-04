import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "esbuild";
import { availablePort, browserExecutable as browser, connectCdp, pollJson } from "./browser-test-utils.mjs";

assert.ok(browser, "Edge or Chrome is required for the system window split browser test");
const guiRoot = resolve(import.meta.dirname, "..");
const result = await build({
  stdin: {
    contents: `
      import { applySystemWindowSplit, initSystemWindowSplit } from "./src/system-window-split.ts";
      import { els, state } from "./src/state.ts";
      import { renderStage } from "./src/view.ts";
      state.systemWindowOpen = true;
      els.app.classList.add("system-open");
      renderStage("run");
      initSystemWindowSplit();
      let initialPanel = document.querySelector(".system-window");
      const initialChat = document.querySelector("#chatPane");
      window.qa = {
        ready: true,
        apply: applySystemWindowSplit,
        setOpen(open) {
          state.systemWindowOpen = open;
          els.app.classList.toggle("system-open", open);
          applySystemWindowSplit();
        },
        requestRender(pageId) {
          renderStage(pageId);
        },
        acceptCurrentPanel() {
          initialPanel = document.querySelector(".system-window");
        },
        snapshot() {
          const stage = document.querySelector("#mainStage");
          const panel = document.querySelector(".system-window");
          const chat = document.querySelector("#chatPane");
          const split = document.querySelector("#systemWindowSplitter");
          return {
            stageWidth: stage.getBoundingClientRect().width,
            panelWidth: panel.getBoundingClientRect().width,
            chatWidth: chat.getBoundingClientRect().width,
            chatX: chat.getBoundingClientRect().x,
            hidden: split.hidden,
            tabIndex: split.tabIndex,
            ariaNow: Number(split.getAttribute("aria-valuenow")),
            ariaMin: Number(split.getAttribute("aria-valuemin")),
            ariaMax: Number(split.getAttribute("aria-valuemax")),
            dragging: split.classList.contains("dragging"),
            activePage: panel.dataset.activePage || "",
            samePanel: panel === initialPanel,
            sameChat: chat === initialChat,
          };
        },
      };
    `,
    resolveDir: guiRoot,
    sourcefile: "system-window-split-browser-test.ts",
    loader: "ts",
  },
  bundle: true,
  write: false,
  format: "iife",
  platform: "browser",
  target: "es2022",
  logLevel: "silent",
});

const stubElements = `
  <main id="bootstrapRoot"></main><div id="appShell">
  <aside class="left-rail"></aside><div id="personaTabs"></div>
  <details id="personaMoreMenu"><summary id="personaMoreToggle"></summary><div id="personaNameOptions"></div></details>
  <button id="createPersonaButton"></button><div id="instanceTabs"></div>
  <details id="instanceMoreMenu"><summary id="instanceMoreToggle"></summary><div id="instanceOptions"></div></details>
  <button id="createInstanceButton"></button><div id="identityFeedback"></div><div id="statusReadouts"></div>
  <div id="productVersionName"></div><div id="productVersionNumber"></div><div id="surfaceNav"></div>
  <button id="navLockToggle"></button><div id="pageCode"></div><div id="pageTitle"></div>
  <main id="mainStage"><section id="stagePage"><article class="system-window"></article></section>
    <section id="chatPane"><div id="chatThread"></div></section>
    <div id="systemWindowSplitter" role="separator" aria-orientation="vertical" tabindex="-1" hidden></div>
  </main>
  <div id="overviewContent"></div><div id="overviewPane"></div><button id="overviewToggle"></button>
  <button id="globalSettingsToggle"></button><div id="globalSettingsOverlay"></div><div id="globalSettingsContent"></div>
  <div id="manualOverlay"></div><div id="manualTitle"></div><div id="manualSummary"></div><div id="manualPageLabel"></div>
  <div id="manualSources"></div><div id="manualBody"></div><div id="runtimeState"><span></span></div><div id="commsSource"></div>
  <form id="runtimeComposer"><textarea id="messageInput"></textarea><select id="permissionLevel"></select><button id="sendButton"></button></form>
  <button id="stopButton"></button><button id="configureModelButton"></button><div id="sendFeedback"></div><output id="contextUsage"></output>
  <div id="ledgerRound"></div><div id="ledgerContext"></div><div id="ledgerFrame"></div><div id="ledgerSettlement"></div></div>`;

const html = `<!doctype html><meta charset="utf-8"><style>
  * { box-sizing: border-box; }
  html, body { margin: 0; width: 100%; height: 100%; overflow: hidden; }
  #appShell { --system-window-width: calc((100% - 24px) / 2); width: 100%; height: 100%; }
  #mainStage { position: relative; width: 100vw; height: 100vh; overflow: hidden; }
  #stagePage { position: absolute; inset: 0; }
  .system-window { position: absolute; inset: 12px auto 12px 12px; width: var(--system-window-width); background: #122; }
  #chatPane { position: absolute; inset: 0; left: calc(var(--system-window-width) + 24px); background: #011; }
  #systemWindowSplitter { position: absolute; z-index: 6; top: 12px; bottom: 12px; left: calc(var(--system-window-width) + 12px); width: 12px; cursor: col-resize; touch-action: none; }
  @media (max-width: 760px) { #appShell { --system-window-width: 100%; } #systemWindowSplitter { display: none !important; } .system-window { inset: 0; width: 100%; } }
</style>${stubElements}<script>${result.outputFiles[0].text}</script>`;

const temporary = await mkdtemp(join(tmpdir(), "upsp-system-split-test-"));
const port = await availablePort();
const debugPort = await availablePort();
const server = createServer((request, response) => {
  response.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
  response.end(html);
});
await new Promise((resolveReady, reject) => {
  server.once("error", reject);
  server.listen(port, "127.0.0.1", resolveReady);
});
const browserProcess = spawn(browser, [
  "--headless=new", "--disable-gpu", "--disable-extensions", "--no-first-run",
  `--user-data-dir=${join(temporary, "profile")}`,
  `--remote-debugging-port=${debugPort}`,
  "about:blank",
], { stdio: "ignore", windowsHide: true });

let cdp;
const evaluate = async (expression) => {
  const response = await cdp.call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
  if (response.exceptionDetails) throw new Error(response.exceptionDetails.text);
  return response.result.value;
};
const waitReady = async () => {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await evaluate("Boolean(window.qa?.ready)")) return;
    await new Promise((resolveWait) => setTimeout(resolveWait, 25));
  }
  throw new Error("system split browser test did not become ready");
};
const setViewport = async (width, height) => {
  await cdp.call("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
  await new Promise((resolveWait) => setTimeout(resolveWait, 60));
};

try {
  const pages = await pollJson(`http://127.0.0.1:${debugPort}/json`);
  const target = pages.find((candidate) => candidate.type === "page");
  assert.ok(target?.webSocketDebuggerUrl, "headless browser did not expose a page target");
  cdp = await connectCdp(target.webSocketDebuggerUrl);
  await setViewport(1440, 960);
  await cdp.call("Page.navigate", { url: `http://127.0.0.1:${port}/` });
  await waitReady();

  let snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.stageWidth, 1440);
  assert.equal(snapshot.panelWidth, 708);
  assert.equal(snapshot.ariaNow, 50);
  assert.equal(snapshot.hidden, false);
  assert.equal(snapshot.tabIndex, 0);

  const startX = 12 + snapshot.panelWidth + 6;
  const initialPanelWidth = snapshot.panelWidth;
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: startX, y: 400 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: startX, y: 400, button: "left", buttons: 1, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.panelWidth, initialPanelWidth);
  assert.equal(snapshot.ariaNow, 50);
  assert.equal(snapshot.dragging, true);
  await evaluate("window.qa.requestRender('run')");
  await evaluate("window.qa.requestRender('mem')");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.activePage, "run");
  assert.equal(snapshot.samePanel, true);
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: startX, y: 400, button: "left", buttons: 0, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.activePage, "mem");
  assert.equal(snapshot.samePanel, false);
  await evaluate("window.qa.acceptCurrentPanel()");

  const targetX = startX + (1440 - 24) * 0.1;
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: startX, y: 400 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: startX, y: 400, button: "left", buttons: 1, clickCount: 1 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: targetX, y: 400, button: "left", buttons: 1 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: targetX, y: 400, button: "left", buttons: 0, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 60);
  assert.equal(snapshot.dragging, false);
  assert.equal(snapshot.samePanel, true);
  assert.equal(snapshot.sameChat, true);

  await setViewport(900, 700);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 50);
  assert.equal(snapshot.ariaMin, 50);
  assert.equal(snapshot.ariaMax, 50);
  await setViewport(1440, 960);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 60);

  await cdp.call("Page.reload", { ignoreCache: true });
  await waitReady();
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 60);

  await evaluate("document.querySelector('#systemWindowSplitter').focus()");
  await cdp.call("Input.dispatchKeyEvent", { type: "keyDown", key: "ArrowLeft", code: "ArrowLeft" });
  await cdp.call("Input.dispatchKeyEvent", { type: "keyUp", key: "ArrowLeft", code: "ArrowLeft" });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 58);
  await cdp.call("Input.dispatchKeyEvent", { type: "keyDown", key: "ArrowLeft", code: "ArrowLeft", modifiers: 8 });
  await cdp.call("Input.dispatchKeyEvent", { type: "keyUp", key: "ArrowLeft", code: "ArrowLeft", modifiers: 8 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 48);
  await cdp.call("Input.dispatchKeyEvent", { type: "keyDown", key: "End", code: "End" });
  await cdp.call("Input.dispatchKeyEvent", { type: "keyUp", key: "End", code: "End" });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, snapshot.ariaMax);

  await evaluate("document.querySelector('#systemWindowSplitter').dispatchEvent(new MouseEvent('dblclick',{bubbles:true}))");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 50);
  await cdp.call("Page.reload", { ignoreCache: true });
  await waitReady();
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 50);

  await setViewport(1366, 768);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.panelWidth, 671);
  assert.equal(snapshot.chatWidth, 671);
  assert.equal(snapshot.ariaNow, 50);
  await setViewport(1920, 1080);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.panelWidth, 948);
  assert.equal(snapshot.chatWidth, 948);
  assert.equal(snapshot.ariaNow, 50);

  await evaluate("window.qa.setOpen(false)");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.hidden, true);
  assert.equal(snapshot.tabIndex, -1);
  await evaluate("window.qa.setOpen(true)");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 50);

  await setViewport(390, 844);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.hidden, true);
  assert.equal(snapshot.tabIndex, -1);
  assert.equal(snapshot.panelWidth, 390);

  console.log("System window split browser tests passed");
} finally {
  await cdp?.call("Browser.close").catch(() => undefined);
  cdp?.close();
  if (browserProcess.exitCode === null) browserProcess.kill();
  await Promise.race([once(browserProcess, "exit"), new Promise((resolveWait) => setTimeout(resolveWait, 2_000))]);
  await new Promise((resolveClosed) => server.close(resolveClosed));
  await rm(temporary, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}
