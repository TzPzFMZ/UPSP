import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { build } from "esbuild";
import { availablePort, browserExecutable as browser, connectCdp, pollJson } from "./browser-test-utils.mjs";

const guiRoot = resolve(import.meta.dirname, "..");
assert.ok(browser, "Edge or Chrome is required for the conversation scroll regression test");

const temporary = await mkdtemp(join(tmpdir(), "upsp-scroll-test-"));
try {
  const result = await build({
    stdin: {
      contents: `
        import {
          mutateScrollLayout,
          scrollConversationToBottomIfSticky,
          updateConversationStickyState,
        } from "./src/conversation-scroll.ts";

        const assert = (condition, message) => { if (!condition) throw new Error(message); };
        const distance = (container) => container.scrollHeight - container.scrollTop - container.clientHeight;
        const frame = () => new Promise((resolve) => requestAnimationFrame(resolve));

        async function run() {
          const container = document.querySelector("#scroll");
          const content = document.querySelector("#content");
          const state = { conversationStickToBottom: true };
          const addBlock = (height = 80) => {
            const block = document.createElement("div");
            block.style.height = height + "px";
            content.appendChild(block);
            return block;
          };
          for (let index = 0; index < 5; index += 1) addBlock();

          scrollConversationToBottomIfSticky(container, state);
          assert(distance(container) <= 1, "initial render did not reach bottom");

          addBlock(120);
          scrollConversationToBottomIfSticky(container, state);
          assert(distance(container) <= 1, "stream growth did not remain sticky");

          container.scrollTop = 40;
          updateConversationStickyState(container, state);
          assert(state.conversationStickToBottom === false, "user scroll-up was not recorded");
          const readingTop = container.scrollTop;
          addBlock(160);
          scrollConversationToBottomIfSticky(container, state);
          assert(container.scrollTop === readingTop, "stream growth stole the reading position");

          container.scrollTop = container.scrollHeight;
          updateConversationStickyState(container, state);
          assert(state.conversationStickToBottom === true, "return to bottom did not restore sticky mode");
          addBlock(100);
          scrollConversationToBottomIfSticky(container, state);
          assert(distance(container) <= 1, "restored sticky mode did not follow growth");

          const hydrated = addBlock(40);
          scrollConversationToBottomIfSticky(container, state);
          mutateScrollLayout(container, hydrated, () => { hydrated.style.height = "180px"; }, () => state.conversationStickToBottom);
          await frame();
          await frame();
          assert(distance(container) <= 1, "async markdown-style hydration broke sticky mode");

          container.scrollTop = 100;
          updateConversationStickyState(container, state);
          const anchor = container.scrollTop;
          const first = content.firstElementChild;
          mutateScrollLayout(container, first, () => { first.style.height = "140px"; }, () => state.conversationStickToBottom);
          await frame();
          await frame();
          assert(container.scrollTop > anchor, "growth above the viewport did not preserve the reading anchor");

          document.body.dataset.testStatus = "passed";
          document.querySelector("#result").textContent = "conversation scroll behavior passed";
        }
        run().catch((error) => {
          document.body.dataset.testStatus = "failed";
          document.querySelector("#result").textContent = String(error?.stack || error);
        });
      `,
      resolveDir: guiRoot,
      sourcefile: "conversation-scroll-browser-test.ts",
      loader: "ts",
    },
    bundle: true,
    write: false,
    format: "iife",
    platform: "browser",
    target: "es2022",
    logLevel: "silent",
  });
  const html = `<!doctype html><meta charset="utf-8"><style>
    #scroll { height: 160px; width: 320px; overflow-y: auto; }
    #content > div { box-sizing: border-box; }
  </style><div id="scroll"><div id="content"></div></div><pre id="result">pending</pre><script>${result.outputFiles[0].text}</script>`;
  const page = join(temporary, "test.html");
  await writeFile(page, html, "utf8");
  const port = await availablePort();
  const browserProcess = spawn(browser, [
    "--headless=new",
    "--disable-gpu",
    "--disable-extensions",
    "--no-first-run",
    `--user-data-dir=${join(temporary, "profile")}`,
    `--remote-debugging-port=${port}`,
    "about:blank",
  ], { stdio: "ignore", windowsHide: true });
  let cdp;
  try {
    const pages = await pollJson(`http://127.0.0.1:${port}/json`);
    const target = pages.find((candidate) => candidate.type === "page");
    assert.ok(target?.webSocketDebuggerUrl, "headless browser did not expose a page target");
    cdp = await connectCdp(target.webSocketDebuggerUrl);
    await cdp.call("Page.navigate", { url: pathToFileURL(page).href });
    const deadline = Date.now() + 10_000;
    let status = "pending";
    let detail = "";
    while (Date.now() < deadline && status === "pending") {
      const evaluated = await cdp.call("Runtime.evaluate", {
        expression: `({ status: document.body?.dataset.testStatus || "pending", detail: document.querySelector("#result")?.textContent || "" })`,
        returnByValue: true,
      });
      ({ status, detail } = evaluated.result.value);
      if (status === "pending") await new Promise((resolveWait) => setTimeout(resolveWait, 25));
    }
    assert.equal(status, "passed", detail || "browser test timed out");
    assert.equal(detail, "conversation scroll behavior passed");
  } finally {
    await cdp?.call("Browser.close").catch(() => undefined);
    cdp?.close();
    if (browserProcess.exitCode === null) browserProcess.kill();
    await Promise.race([
      once(browserProcess, "exit"),
      new Promise((resolveWait) => setTimeout(resolveWait, 2_000)),
    ]);
  }
  console.log("Conversation scroll browser tests passed");
} finally {
  await rm(temporary, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}
