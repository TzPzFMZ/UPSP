import mermaid from "mermaid";

mermaid.initialize({
  startOnLoad: false,
  securityLevel: "strict",
  htmlLabels: false,
  suppressErrorRendering: true,
  theme: "dark",
  logLevel: "fatal",
  maxTextSize: 50_000,
  flowchart: { htmlLabels: false, useMaxWidth: true },
});

export async function renderMermaid(id: string, source: string): Promise<string> {
  const { svg } = await mermaid.render(id, source);
  return svg;
}
