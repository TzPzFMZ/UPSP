import bash from "highlight.js/lib/languages/bash";
import css from "highlight.js/lib/languages/css";
import diff from "highlight.js/lib/languages/diff";
import javascript from "highlight.js/lib/languages/javascript";
import json from "highlight.js/lib/languages/json";
import markdown from "highlight.js/lib/languages/markdown";
import powershell from "highlight.js/lib/languages/powershell";
import python from "highlight.js/lib/languages/python";
import typescript from "highlight.js/lib/languages/typescript";
import xml from "highlight.js/lib/languages/xml";
import yaml from "highlight.js/lib/languages/yaml";
import { t } from "./i18n";
import rehypeHighlight from "rehype-highlight";
import rehypeKatex from "rehype-katex";
import rehypeSanitize, { defaultSchema } from "rehype-sanitize";
import type { Options as SanitizeOptions } from "rehype-sanitize";
import rehypeStringify from "rehype-stringify";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";
import remarkParse from "remark-parse";
import remarkRehype from "remark-rehype";
import type { Options as RemarkRehypeOptions } from "remark-rehype";
import { unified } from "unified";

interface MermaidModule {
  renderMermaid(id: string, source: string): Promise<string>;
}

interface ImageSize {
  width: number;
  height: number;
}

const htmlCache = new Map<string, { source: string; html: string }>();
const documentPrefixes = new Map<string, string>();
const approvedImageDocuments = new Set<string>();
const imageLoads = new Map<string, Promise<ImageSize>>();
const mermaidRenders = new Map<string, Promise<string>>();
let documentSequence = 0;
let mermaidSequence = 0;
let mermaidModule: Promise<MermaidModule> | null = null;
let interactionsReady = false;

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function documentPrefix(documentId: string): string {
  const existing = documentPrefixes.get(documentId);
  if (existing) return existing;
  const prefix = `md-${++documentSequence}-`;
  documentPrefixes.set(documentId, prefix);
  return prefix;
}

function remoteImage(value: unknown): { url: string; domain: string } | null {
  try {
    const url = new URL(String(value || ""));
    if (!["http:", "https:"].includes(url.protocol)) return null;
    return { url: url.href, domain: url.hostname };
  } catch {
    return null;
  }
}

const imageHandler: NonNullable<NonNullable<RemarkRehypeOptions["handlers"]>["image"]> = (_state, node) => {
  const image = remoteImage(node.url);
  const alt = String(node.alt || t("远程图片"));
  return {
    type: "element",
    tagName: "button",
    properties: {
      type: "button",
      className: ["md-image-placeholder"],
      disabled: !image,
      ariaLabel: image ? t("加载当前文档中的远程图片：{alt}", { alt }) : t("图片地址已拒绝：{alt}", { alt }),
      ...(image ? { dataMarkdownImageUrl: image.url } : {}),
    },
    children: [
      { type: "element", tagName: "strong", properties: {}, children: [{ type: "text", value: alt }] },
      {
        type: "element",
        tagName: "small",
        properties: {},
        children: [{
          type: "text",
          value: image ? t("点击加载当前文档图片 · {domain}", { domain: image.domain }) : t("仅允许 http / https 图片"),
        }],
      },
    ],
  };
};

const sanitizeSchema: SanitizeOptions = {
  ...defaultSchema,
  tagNames: [...(defaultSchema.tagNames || []).filter((tag) => !["img", "picture", "source"].includes(tag)), "button"],
  attributes: {
    ...defaultSchema.attributes,
    button: [
      ["type", "button"],
      ["className", "md-image-placeholder"],
      ["disabled", true],
      "ariaLabel",
      "dataMarkdownImageUrl",
    ],
  },
  protocols: {
    ...defaultSchema.protocols,
    href: ["http", "https", "mailto"],
  },
};

function renderMarkdown(source: string, documentId: string): string {
  const file = unified()
    .use(remarkParse)
    .use(remarkGfm)
    .use(remarkMath, { singleDollarTextMath: true })
    .use(remarkRehype, {
      allowDangerousHtml: false,
      clobberPrefix: documentPrefix(documentId),
      footnoteLabel: t("脚注"),
      footnoteBackLabel: t("返回正文"),
      handlers: { image: imageHandler },
    })
    .use(rehypeSanitize, sanitizeSchema)
    .use(rehypeKatex, {
      trust: false,
      strict: "ignore",
      maxExpand: 1_000,
      maxSize: 20,
      errorColor: "#d9a441",
    })
    .use(rehypeHighlight, {
      detect: false,
      languages: { bash, css, diff, javascript, json, markdown, powershell, python, typescript, xml, yaml },
      aliases: {
        bash: ["sh", "shell"],
        javascript: ["js", "jsx"],
        markdown: ["md"],
        powershell: ["ps1"],
        typescript: ["ts", "tsx"],
        xml: ["html"],
        yaml: ["yml"],
      },
      plainText: ["mermaid", "math", "text", "plaintext"],
    })
    .use(rehypeStringify)
    .processSync(source);
  return String(file);
}

export function renderMarkdownDocument(documentId: string, value: unknown): string {
  const source = String(value ?? "").trim();
  const cached = htmlCache.get(documentId);
  if (cached?.source === source) {
    return `<div class="md-document" data-markdown-document-id="${escapeHtml(documentId)}">${cached.html}</div>`;
  }
  let html: string;
  try {
    html = source ? renderMarkdown(source, documentId) : `<p class="md-empty">${t("无可展示内容。")}</p>`;
  } catch {
    html = `<pre class="md-render-fallback"><code>${escapeHtml(source || t("无可展示内容。"))}</code></pre>`;
  }
  htmlCache.set(documentId, { source, html });
  return `<div class="md-document" data-markdown-document-id="${escapeHtml(documentId)}">${html}</div>`;
}

export function memoryBodyMarkdown(value: unknown): string {
  const source = String(value ?? "").trim();
  if (!source) return `_${t("正文为空。")}_`;
  const lines = source.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const heading = lines[0]?.startsWith("## ") ? lines.shift() : "";
  const rows: [string, string][] = [];
  const remainder: string[] = [];
  for (const line of lines) {
    const normalized = line.replace(/^\*\*([^*]+)\*\*/, "$1");
    const separators = [normalized.indexOf("："), normalized.indexOf(":")].filter((index) => index > 0);
    const separator = separators.length ? Math.min(...separators) : -1;
    if (separator < 0) {
      remainder.push(line);
      continue;
    }
    rows.push([
      normalized.slice(0, separator).trim(),
      normalized.slice(separator + 1).trim() || "—",
    ]);
  }
  if (!rows.length) return source;
  const cell = (text: string): string => text.replaceAll("|", "\\|");
  const table = [
    `| ${t("字段")} | ${t("内容")} |`,
    "| --- | --- |",
    ...rows.map(([field, content]) => `| ${cell(field)} | ${cell(content)} |`),
  ].join("\n");
  return [heading, table, remainder.join("\n\n")].filter(Boolean).join("\n\n");
}

function jsonRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function homogeneousObjectArray(value: unknown[]): value is Record<string, unknown>[] {
  if (!value.length || !value.every(jsonRecord)) return false;
  const signature = Object.keys(value[0]).sort().join("\u0000");
  return value.every((item) => Object.keys(item).sort().join("\u0000") === signature);
}

function jsonPrimitive(value: unknown): string {
  if (value === null) return '<em class="structured-json-null">null</em>';
  if (typeof value === "string") return `<span class="structured-json-string">${escapeHtml(value)}</span>`;
  if (typeof value === "number" || typeof value === "boolean") {
    return `<code class="structured-json-primitive">${escapeHtml(String(value))}</code>`;
  }
  return `<span>${escapeHtml(String(value ?? ""))}</span>`;
}

function jsonNested(value: unknown): string {
  if (Array.isArray(value) || jsonRecord(value)) {
    const count = Array.isArray(value) ? value.length : Object.keys(value).length;
    const label = Array.isArray(value) ? t("数组 · {count} 项", { count }) : t("对象 · {count} 字段", { count });
    return `<details class="structured-json-nested"><summary>${label}</summary>${renderStructuredJson(value)}</details>`;
  }
  return jsonPrimitive(value);
}

export function renderStructuredJson(value: unknown): string {
  if (Array.isArray(value) && homogeneousObjectArray(value)) {
    const columns = Object.keys(value[0]);
    return `<div class="structured-json-scroll"><table class="structured-json-table"><thead><tr>${columns.map((key) => `<th scope="col">${escapeHtml(key)}</th>`).join("")}</tr></thead><tbody>${value.map((item) => `<tr>${columns.map((key) => `<td>${jsonNested(item[key])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  }
  if (Array.isArray(value)) {
    return `<div class="structured-json-scroll"><table class="structured-json-table"><thead><tr><th scope="col">${t("序号")}</th><th scope="col">${t("值")}</th></tr></thead><tbody>${value.map((item, index) => `<tr><th scope="row">${index}</th><td>${jsonNested(item)}</td></tr>`).join("")}</tbody></table></div>`;
  }
  if (jsonRecord(value)) {
    return `<div class="structured-json-scroll"><table class="structured-json-table"><thead><tr><th scope="col">${t("字段")}</th><th scope="col">${t("值")}</th></tr></thead><tbody>${Object.entries(value).map(([key, item]) => `<tr><th scope="row">${escapeHtml(key)}</th><td>${jsonNested(item)}</td></tr>`).join("")}</tbody></table></div>`;
  }
  return `<p class="structured-json-value">${jsonPrimitive(value)}</p>`;
}

export function renderStructuredJsonSource(source: string): string | null {
  try {
    return renderStructuredJson(JSON.parse(source));
  } catch {
    return null;
  }
}

function markdownDocument(target: Element): HTMLElement | null {
  return target.closest<HTMLElement>(".md-document[data-markdown-document-id]");
}

function scrollMutation(container: HTMLElement | null, element: HTMLElement, mutate: () => void): void {
  if (!container) {
    mutate();
    return;
  }
  const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
  const atBottom = distanceFromBottom <= 24;
  const containerTop = container.getBoundingClientRect().top;
  const before = element.getBoundingClientRect();
  const aboveViewport = before.bottom <= containerTop + 1;
  const oldHeight = element.offsetHeight;
  mutate();
  window.requestAnimationFrame(() => {
    if (atBottom) container.scrollTop = container.scrollHeight;
    else if (aboveViewport) container.scrollTop += element.offsetHeight - oldHeight;
  });
}

function preloadImage(url: string): Promise<ImageSize> {
  const existing = imageLoads.get(url);
  if (existing) return existing;
  const request = new Promise<ImageSize>((resolve, reject) => {
    const image = new Image();
    image.referrerPolicy = "no-referrer";
    image.addEventListener("load", () => resolve({ width: image.naturalWidth, height: image.naturalHeight }), { once: true });
    image.addEventListener("error", () => reject(new Error("remote_image_load_failed")), { once: true });
    image.src = url;
  }).catch((error: unknown) => {
    imageLoads.delete(url);
    throw error;
  });
  imageLoads.set(url, request);
  return request;
}

async function loadImage(button: HTMLButtonElement, scrollContainer: HTMLElement | null): Promise<void> {
  if (button.dataset.markdownImageLoading === "true") return;
  const image = remoteImage(button.dataset.markdownImageUrl);
  if (!image) return;
  button.dataset.markdownImageLoading = "true";
  const status = button.querySelector<HTMLElement>("small");
  if (status) status.textContent = t("正在加载 · {domain}", { domain: image.domain });
  try {
    const size = await preloadImage(image.url);
    if (!button.isConnected) return;
    const rendered = new Image();
    rendered.className = "md-remote-image";
    rendered.alt = button.querySelector("strong")?.textContent || t("远程图片");
    rendered.decoding = "async";
    rendered.referrerPolicy = "no-referrer";
    rendered.width = size.width;
    rendered.height = size.height;
    rendered.src = image.url;
    scrollMutation(scrollContainer, button, () => button.replaceWith(rendered));
  } catch {
    button.dataset.markdownImageLoading = "false";
    if (status) status.textContent = t("加载失败，点击重试 · {domain}", { domain: image.domain });
  }
}

function loadDocumentImages(documentRoot: HTMLElement, scrollContainer: HTMLElement | null): void {
  documentRoot.querySelectorAll<HTMLButtonElement>("button.md-image-placeholder[data-markdown-image-url]")
    .forEach((button) => void loadImage(button, scrollContainer));
}

function loadMermaidModule(): Promise<MermaidModule> {
  if (mermaidModule) return mermaidModule;
  const path = "./markdown-mermaid.js";
  mermaidModule = import(path).then((loaded: unknown) => {
    const candidate = loaded as Partial<MermaidModule>;
    if (typeof candidate.renderMermaid !== "function") throw new Error("mermaid_module_contract_mismatch");
    return candidate as MermaidModule;
  }).catch((error: unknown) => {
    mermaidModule = null;
    throw error;
  });
  return mermaidModule;
}

function mermaidHtml(documentId: string, position: number, source: string): Promise<string> {
  const key = `${documentId}:${position}:${source}`;
  const existing = mermaidRenders.get(key);
  if (existing) return existing;
  const request = loadMermaidModule()
    .then((loaded) => loaded.renderMermaid(`md-diagram-${++mermaidSequence}`, source))
    .catch((error: unknown) => {
      mermaidRenders.delete(key);
      throw error;
    });
  mermaidRenders.set(key, request);
  return request;
}

async function renderMermaid(
  figure: HTMLElement,
  documentId: string,
  position: number,
  source: string,
  scrollContainer: HTMLElement | null,
): Promise<void> {
  try {
    const svg = await mermaidHtml(documentId, position, source);
    if (!figure.isConnected) return;
    scrollMutation(scrollContainer, figure, () => {
      figure.dataset.mermaidState = "ready";
      figure.innerHTML = svg;
    });
  } catch {
    if (!figure.isConnected) return;
    scrollMutation(scrollContainer, figure, () => {
      figure.dataset.mermaidState = "error";
      figure.innerHTML = `<p class="md-mermaid-status">${t("图表渲染失败，已保留源码。")}</p><pre><code>${escapeHtml(source)}</code></pre>`;
    });
    enhanceCodeBlocks(figure);
  }
}

function enhanceMermaid(documentRoot: HTMLElement, scrollContainer: HTMLElement | null): void {
  const documentId = documentRoot.dataset.markdownDocumentId || "markdown";
  documentRoot.querySelectorAll<HTMLElement>("pre > code.language-mermaid").forEach((code, position) => {
    const pre = code.parentElement;
    if (!(pre instanceof HTMLPreElement)) return;
    const source = code.textContent || "";
    const figure = document.createElement("figure");
    figure.className = "md-mermaid";
    figure.dataset.mermaidState = "loading";
    figure.innerHTML = `<p class="md-mermaid-status">${t("正在渲染图表…")}</p>`;
    pre.replaceWith(figure);
    void renderMermaid(figure, documentId, position, source, scrollContainer);
  });
}

function enhanceTables(documentRoot: HTMLElement): void {
  documentRoot.querySelectorAll<HTMLTableElement>("table").forEach((table) => {
    if (table.parentElement?.classList.contains("md-table-scroll")) return;
    const wrapper = document.createElement("div");
    wrapper.className = "md-table-scroll";
    table.replaceWith(wrapper);
    wrapper.append(table);
  });
}

function enhanceLinks(documentRoot: HTMLElement): void {
  documentRoot.querySelectorAll<HTMLAnchorElement>("a[href]").forEach((link) => {
    try {
      const url = new URL(link.href, window.location.href);
      if (["http:", "https:"].includes(url.protocol) && url.origin !== window.location.origin) {
        link.target = "_blank";
        link.rel = "noopener noreferrer";
      }
    } catch {
      link.removeAttribute("href");
    }
  });
}

function enhanceCodeBlocks(root: ParentNode): void {
  root.querySelectorAll<HTMLElement>("pre > code").forEach((code) => {
    const pre = code.parentElement;
    if (!(pre instanceof HTMLPreElement) || pre.dataset.markdownCodeEnhanced === "true") return;
    pre.dataset.markdownCodeEnhanced = "true";
    const language = [...code.classList].find((name) => name.startsWith("language-"))?.slice(9) || "text";
    const tools = document.createElement("span");
    tools.className = "md-code-tools";
    const label = document.createElement("span");
    label.textContent = language;
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.markdownCopy = "true";
    button.textContent = t("复制");
    button.setAttribute("aria-label", t("复制 {language} 代码", { language }));
    tools.append(label, button);
    pre.prepend(tools);
  });
}

export function hydrateLedgerJsonTables(root: ParentNode): void {
  root.querySelectorAll<HTMLElement>(".md-document pre > code.language-json").forEach((code) => {
    const pre = code.parentElement;
    if (!(pre instanceof HTMLPreElement) || pre.dataset.ledgerJsonRaw === "true") return;
    const source = code.textContent || "";
    const table = renderStructuredJsonSource(source);
    if (table === null) return;
    const section = document.createElement("section");
    section.className = "structured-json-view";
    section.innerHTML = `${table}<details class="structured-json-raw"><summary>${t("原始 JSON")}</summary><pre data-ledger-json-raw="true"><code class="language-json">${escapeHtml(source)}</code></pre></details>`;
    pre.replaceWith(section);
    enhanceCodeBlocks(section);
  });
}

export function hydrateMarkdownDocuments(root: ParentNode, scrollContainer: HTMLElement | null = null): void {
  root.querySelectorAll<HTMLElement>(".md-document[data-markdown-document-id]").forEach((documentRoot) => {
    enhanceTables(documentRoot);
    enhanceLinks(documentRoot);
    enhanceMermaid(documentRoot, scrollContainer);
    enhanceCodeBlocks(documentRoot);
    if (approvedImageDocuments.has(documentRoot.dataset.markdownDocumentId || "")) {
      loadDocumentImages(documentRoot, scrollContainer);
    }
  });
}

export function initMarkdownInteractions(): void {
  if (interactionsReady) return;
  interactionsReady = true;
  document.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    const imageButton = target.closest<HTMLButtonElement>("button.md-image-placeholder[data-markdown-image-url]");
    if (imageButton) {
      const documentRoot = markdownDocument(imageButton);
      if (!documentRoot) return;
      const documentId = documentRoot.dataset.markdownDocumentId || "";
      approvedImageDocuments.add(documentId);
      const scrollContainer = documentRoot.closest<HTMLElement>(".chat-thread, .manual-body, .chat-tool-code, .runtime-context-workspace article");
      loadDocumentImages(documentRoot, scrollContainer);
      return;
    }
    const copyButton = target.closest<HTMLButtonElement>("button[data-markdown-copy]");
    if (!copyButton) return;
    const code = copyButton.closest("pre")?.querySelector("code");
    if (!code) return;
    navigator.clipboard.writeText(code.textContent || "").then(() => {
      copyButton.textContent = t("已复制");
      window.setTimeout(() => { copyButton.textContent = t("复制"); }, 1_200);
    }).catch(() => {
      copyButton.textContent = t("复制失败");
      window.setTimeout(() => { copyButton.textContent = t("复制"); }, 1_200);
    });
  });
}
