import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { extname, join, relative, resolve } from "node:path";

import { build } from "esbuild";
import { availablePort, browserExecutable as browser, connectCdp, pollJson } from "./browser-test-utils.mjs";

assert.ok(browser, "Edge or Chrome is required for the system window split browser test");
const guiRoot = resolve(import.meta.dirname, "..");
const result = await build({
  stdin: {
    contents: `
      import { applySystemWindowSplit, initSystemWindowSplit } from "./src/system-window-split.ts";
      import { els, state } from "./src/state.ts";
      import { closeSystemWindow, renderStage } from "./src/view.ts";
      els.app.hidden = false;
      els.bootstrapRoot.hidden = true;
      state.systemWindowOpen = true;
      els.app.classList.add("system-open");
      renderStage("run");
      initSystemWindowSplit();
      let initialPanel = document.querySelector(".system-window");
      const initialChat = document.querySelector("#chatPane");
      window.qa = {
        ready: true,
        apply: applySystemWindowSplit,
        close: closeSystemWindow,
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
            stageX: stage.getBoundingClientRect().x,
            railX: document.querySelector('.left-rail').getBoundingClientRect().x,
            panelWidth: panel.getBoundingClientRect().width,
            panelRight: panel.getBoundingClientRect().right,
            chatWidth: chat.getBoundingClientRect().width,
            chatX: chat.getBoundingClientRect().x,
            splitX: split.getBoundingClientRect().x + split.getBoundingClientRect().width / 2,
            splitY: split.getBoundingClientRect().y + split.getBoundingClientRect().height / 2,
            role: split.getAttribute('role'),
            orientation: split.getAttribute('aria-orientation'),
            touchAction: getComputedStyle(split).touchAction,
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

const html = (await readFile(join(guiRoot, 'index.html'), 'utf8'))
  .replace(/<script[^>]*src="\.\/app\.js[^\"]*"[^>]*><\/script>/, '<script src="/test.js"></script>');
assert.ok(html.includes('/test.js'));

const temporary = await mkdtemp(join(tmpdir(), "upsp-system-split-test-"));
const port = await availablePort();
const debugPort = await availablePort();
const server = createServer(async (request, response) => {
  const path = new URL(request.url, 'http://localhost').pathname;
  if (request.method !== 'GET' || path.startsWith('/api/')) { response.writeHead(403); response.end(); return; }
  response.setHeader('Cache-Control', 'no-store');
  if (path === '/') { response.setHeader('Content-Type', 'text/html; charset=utf-8'); response.end(html); return; }
  if (path === '/test.js') { response.setHeader('Content-Type', 'text/javascript'); response.end(result.outputFiles[0].text); return; }
  try {
    const file = resolve(guiRoot, '.' + path);
    assert.ok(!relative(guiRoot, file).startsWith('..'));
    response.setHeader('Content-Type', ({'.css':'text/css', '.png':'image/png', '.woff2':'font/woff2'})[extname(file)] || 'application/octet-stream');
    response.end(await readFile(file));
  } catch { response.writeHead(404); response.end(); }
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
    if (await evaluate("Boolean(window.qa?.ready)")) {
      await evaluate("document.fonts.ready.then(() => Promise.all(document.querySelector('#stagePage').getAnimations().map(a => a.finished)))");
      return;
    }
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
  await cdp.call('Network.enable');
  await cdp.call('Network.setBlockedURLs', { urls: ['https://*', 'http://*/api/*'] });
  await setViewport(1440, 960);
  await cdp.call("Page.navigate", { url: `http://127.0.0.1:${port}/` });
  await waitReady();

  let snapshot = await evaluate("window.qa.snapshot()");
  assert.ok(snapshot.stageWidth > 1000);
  assert.ok(Math.abs(snapshot.panelWidth - snapshot.chatWidth) <= 1);
  assert.ok(snapshot.chatX >= snapshot.panelRight);
  assert.equal(snapshot.ariaNow, 50);
  assert.equal(snapshot.hidden, false);
  assert.equal(snapshot.tabIndex, 0);
  assert.equal(snapshot.role, 'separator');
  assert.equal(snapshot.orientation, 'vertical');
  assert.equal(snapshot.touchAction, 'none');
  const anchors = { stageX: snapshot.stageX, railX: snapshot.railX };
  await evaluate("document.querySelector('.left-rail').classList.add('nav-expanded')");
  snapshot = await evaluate('window.qa.snapshot()');
  assert.equal(snapshot.stageX, anchors.stageX, 'navigation expansion moved the main stage');
  assert.equal(snapshot.railX, anchors.railX, 'navigation expansion moved its anchor');
  await evaluate("document.querySelector('.left-rail').classList.remove('nav-expanded')");
  assert.equal(await evaluate(`(() => {
    const stage = document.querySelector('#stagePage');
    const animation = stage.getAnimations()[0];
    if (!animation) throw new Error('opening the system window did not animate');
    window.qa.requestRender('run');
    return stage.getAnimations()[0] === animation;
  })()`), true, 'refresh restarted the opening animation');
  await evaluate('window.qa.acceptCurrentPanel()');

  const startX = snapshot.splitX;
  const pointerY = snapshot.splitY;
  const initialPanelWidth = snapshot.panelWidth;
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: startX, y: pointerY });
  await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: startX, y: pointerY, button: "left", buttons: 1, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.panelWidth, initialPanelWidth);
  assert.equal(snapshot.ariaNow, 50);
  assert.equal(snapshot.dragging, true);
  await evaluate("window.qa.requestRender('run')");
  await evaluate("window.qa.requestRender('mem')");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.activePage, "run");
  assert.equal(snapshot.samePanel, true);
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: startX, y: pointerY, button: "left", buttons: 0, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.activePage, "mem");
  assert.equal(snapshot.samePanel, false);
  await evaluate("window.qa.acceptCurrentPanel()");

  const targetX = startX + (snapshot.panelWidth + snapshot.chatWidth) * 0.1;
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: startX, y: pointerY });
  await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: startX, y: pointerY, button: "left", buttons: 1, clickCount: 1 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: targetX, y: pointerY, button: "left", buttons: 1 });
  await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: targetX, y: pointerY, button: "left", buttons: 0, clickCount: 1 });
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 60);
  assert.equal(snapshot.dragging, false);
  assert.equal(snapshot.samePanel, true);
  assert.equal(snapshot.sameChat, true);

  for (const event of ['pointercancel', 'lostpointercapture']) {
    const before = await evaluate('window.qa.snapshot()');
    await cdp.call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: before.splitX, y: before.splitY });
    await cdp.call('Input.dispatchMouseEvent', { type: 'mousePressed', x: before.splitX, y: before.splitY, button: 'left', buttons: 1, clickCount: 1 });
    await cdp.call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: before.splitX + 30, y: before.splitY, button: 'left', buttons: 1 });
    await evaluate(`document.querySelector('#systemWindowSplitter').dispatchEvent(new PointerEvent('${event}', {pointerId:1}))`);
    await cdp.call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: before.splitX + 30, y: before.splitY, button: 'left', buttons: 0, clickCount: 1 });
    snapshot = await evaluate('window.qa.snapshot()');
    assert.equal(snapshot.dragging, false, event);
    assert.equal(snapshot.ariaNow, before.ariaNow, event + ' did not restore the original ratio');
  }

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
  assert.ok(Math.abs(snapshot.panelWidth - snapshot.chatWidth) <= 1);
  assert.equal(snapshot.ariaNow, 50);
  await setViewport(1920, 1080);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.ok(Math.abs(snapshot.panelWidth - snapshot.chatWidth) <= 1);
  assert.equal(snapshot.ariaNow, 50);

  await evaluate("window.qa.setOpen(false)");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.hidden, true);
  assert.equal(snapshot.tabIndex, -1);
  assert.equal(snapshot.stageX, anchors.stageX);
  assert.ok(snapshot.chatWidth > snapshot.panelWidth);
  await evaluate("window.qa.setOpen(true)");
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.ariaNow, 50);

  await cdp.call('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-reduced-motion', value: 'reduce' }] });
  assert.equal(await evaluate(`(() => {
    window.qa.close();
    return document.getAnimations().every(a => Number(a.effect.getTiming().duration) <= 1);
  })()`), true, 'reduced motion left a long layout animation');
  await new Promise(resolveWait => setTimeout(resolveWait, 180));
  assert.equal(await evaluate("document.querySelector('#systemWindowSplitter').hidden && !document.querySelector('.system-window')"), true);
  await evaluate("window.qa.setOpen(true);window.qa.requestRender('run');document.querySelector('#systemWindowSplitter').focus()");
  await cdp.call('Input.dispatchKeyEvent', { type: 'keyDown', key: 'ArrowRight', code: 'ArrowRight' });
  assert.equal((await evaluate('window.qa.snapshot()')).ariaNow, 52, 'reduced motion disabled the accessible splitter');

  await setViewport(390, 844);
  snapshot = await evaluate("window.qa.snapshot()");
  assert.equal(snapshot.hidden, true);
  assert.equal(snapshot.tabIndex, -1);
  assert.equal(snapshot.panelWidth, snapshot.stageWidth);

  console.log("System window split browser tests passed");
} finally {
  await cdp?.call("Browser.close").catch(() => undefined);
  cdp?.close();
  if (browserProcess.exitCode === null) browserProcess.kill();
  await Promise.race([once(browserProcess, "exit"), new Promise((resolveWait) => setTimeout(resolveWait, 2_000))]);
  await new Promise((resolveClosed) => server.close(resolveClosed));
  await rm(temporary, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}
