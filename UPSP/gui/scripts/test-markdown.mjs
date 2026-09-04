import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { build } from "esbuild";

const guiRoot = resolve(import.meta.dirname, "..");
const temporary = await mkdtemp(join(tmpdir(), "upsp-markdown-"));
try {
  const result = await build({
    entryPoints: [resolve(guiRoot, "src", "markdown.ts")],
    bundle: true,
    write: false,
    format: "esm",
    platform: "node",
    target: "node22",
    logLevel: "silent",
  });
  const modulePath = join(temporary, "markdown.mjs");
  await writeFile(modulePath, result.outputFiles[0].contents);
  const {
    memoryBodyMarkdown,
    renderMarkdownDocument,
    renderStructuredJson,
    renderStructuredJsonSource,
  } = await import(pathToFileURL(modulePath).href);

  const rich = renderMarkdownDocument("test:rich", `# 标题

~~删除~~、**粗体**与脚注[^1]。

- 一级
  - 二级
- [x] 已完成

| A | B |
| - | - |
| 1 | 2 |

行内公式 $x^2$。

$$
E = mc^2
$$

\`\`\`typescript
const answer: number = 42;
\`\`\`

\`\`\`unknown-language
plain <unsafe>
\`\`\`

\`\`\`mermaid
flowchart LR
  A --> B
\`\`\`

![图一](https://images.example.test/a.png)
![拒绝](javascript:alert(1))

[^1]: 脚注正文
`);
  assert.match(rich, /<h1>标题<\/h1>/);
  assert.match(rich, /<del>删除<\/del>/);
  assert.match(rich, /type="checkbox" checked disabled/);
  assert.match(rich, /<table>/);
  assert.match(rich, /class="katex"/);
  assert.match(rich, /class="hljs-/);
  assert.doesNotMatch(rich, /language-unknown-language[^>]*hljs/);
  assert.match(rich, /class="language-mermaid"/);
  assert.match(rich, /data-footnotes/);
  assert.match(rich, /data-markdown-image-url="https:\/\/images\.example\.test\/a\.png"/);
  assert.match(rich, /仅允许 http \/ https 图片/);

  const unsafe = renderMarkdownDocument(
    "test:unsafe",
    '<script>alert(1)</script>\n\n<img src="https://tracker.example/pixel">\n\n[危险](javascript:alert(1))',
  );
  assert.doesNotMatch(unsafe, /<script|<img|javascript:/i);
  assert.match(unsafe, />危险</);

  const firstFootnote = renderMarkdownDocument("test:footnote-a", "A[^1]\n\n[^1]: first");
  const secondFootnote = renderMarkdownDocument("test:footnote-b", "B[^1]\n\n[^1]: second");
  const firstId = firstFootnote.match(/id="([^"]*fn-1)"/)?.[1];
  const secondId = secondFootnote.match(/id="([^"]*fn-1)"/)?.[1];
  assert.ok(firstId && secondId && firstId !== secondId);

  const invalidMath = renderMarkdownDocument("test:bad-math", "$\\notacommand{$");
  assert.match(invalidMath, /notacommand|katex-error/);

  const memory = renderMarkdownDocument("memory:MEM-TEST", memoryBodyMarkdown(`## MEM-TEST [S] 权重3
**交互对象**：TzPz
**摘要**（≤512字）：保留 | 字符并自动换行
关联容器：`));
  assert.match(memory, /<table>/);
  assert.match(memory, /<th>字段<\/th>/);
  assert.match(memory, /<td>TzPz<\/td>/);
  assert.match(memory, /保留 \| 字符并自动换行/);
  assert.match(memory, /<td>—<\/td>/);

  const objectTable = renderStructuredJson({
    name: "<unsafe>",
    enabled: true,
    count: 3,
    missing: null,
    nested: { key: "value" },
  });
  assert.match(objectTable, /<th scope="col">字段<\/th>/);
  assert.match(objectTable, /&lt;unsafe&gt;/);
  assert.match(objectTable, /structured-json-null/);
  assert.match(objectTable, /<details class="structured-json-nested">/);

  const rowTable = renderStructuredJson([
    { id: 1, label: "一" },
    { id: 2, label: "二" },
  ]);
  assert.match(rowTable, /<th scope="col">id<\/th>/);
  assert.match(rowTable, /<th scope="col">label<\/th>/);
  assert.doesNotMatch(rowTable, /序号/);

  const mixedArray = renderStructuredJson(["text", { nested: [1, 2] }]);
  assert.match(mixedArray, /序号/);
  assert.match(mixedArray, /数组 · 2 项/);
  assert.equal(renderStructuredJsonSource("{ invalid"), null);
  assert.match(renderStructuredJsonSource('{"long":"' + "x".repeat(500) + '"}'), /structured-json-string/);
  console.log("Markdown renderer tests passed");
} finally {
  await rm(temporary, { recursive: true, force: true });
}
