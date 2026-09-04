type ComponentKey = "thinking" | "tools" | "activity" | "streaming";
type ScenarioId = "multi_tool" | "first_byte_wait" | "no_reasoning" | "tool_retry" | "user_stop" | "long_markdown";
type ProcessStage = "idle" | "connecting" | "thinking" | "progressing" | "tool_running" | "tool_approval" | "answering" | "completed" | "stopped";
type ToolState = "running" | "approval" | "succeeded" | "failed" | "stopped";
type TextKind = "progress" | "answer";

type DemoEvent =
  | { atMs: number; type: "stage"; stage: ProcessStage }
  | { atMs: number; type: "reasoning"; blockId: string; text: string }
  | { atMs: number; type: "progress"; blockId: string; text: string }
  | { atMs: number; type: "tool_start"; callId: string; toolId: string; args: string }
  | { atMs: number; type: "tool_approval"; callId: string; message: string }
  | { atMs: number; type: "tool_result"; callId: string; result: string }
  | { atMs: number; type: "tool_error"; callId: string; result: string }
  | { atMs: number; type: "answer"; blockId: string; text: string }
  | { atMs: number; type: "complete" }
  | { atMs: number; type: "stopped" };

interface DemoScenario {
  id: ScenarioId;
  label: string;
  userText: string;
  durationMs: number;
  events: DemoEvent[];
}

interface ToolSnapshot {
  callId: string;
  toolId: string;
  args: string;
  result: string;
  state: ToolState;
  startedAtMs: number;
  endedAtMs: number | null;
}

interface ReasoningSegment {
  id: string;
  label: string;
  text: string;
  startedAtMs: number;
  endedAtMs: number | null;
  active: boolean;
}

interface TextSegment {
  id: string;
  kind: TextKind;
  text: string;
  active: boolean;
}

type TimelineItem =
  | { kind: "reasoning"; id: string; value: ReasoningSegment }
  | { kind: "progress" | "answer"; id: string; value: TextSegment }
  | { kind: "tool"; id: string; value: ToolSnapshot };

interface DemoSnapshot {
  elapsedMs: number;
  stage: ProcessStage;
  reasoningSegments: ReasoningSegment[];
  tools: ToolSnapshot[];
  textSegments: TextSegment[];
  timeline: TimelineItem[];
  terminalMessage: string;
}

interface PreviewSurface {
  timeline: HTMLElement;
  activity: HTMLElement;
}

type VariantSet = Record<ComponentKey, string>;

interface VariantDefinition {
  id: string;
  marker: string;
  title: string;
  description: string;
}

interface SelectionState {
  thinking?: string;
  tools?: string;
  activity?: string;
  streaming?: string;
}

interface StoredState {
  schema_version: "upsp_conversation_showcase_state.v1";
  scenario: ScenarioId;
  speed: number;
  selections: SelectionState;
}

const STORAGE_KEY = "upsp.conversationShowcase.v1";
const STATE_SCHEMA = "upsp_conversation_showcase_state.v1";
const SELECTION_SCHEMA = "upsp_conversation_showcase_selection.v1";
const COMPONENTS: ComponentKey[] = ["thinking", "tools", "activity", "streaming"];

const componentLabels: Record<ComponentKey, string> = {
  thinking: "思考折叠",
  tools: "工具调用",
  activity: "正在处理",
  streaming: "流式输出",
};

const variants: Record<ComponentKey, VariantDefinition[]> = {
  thinking: [
    { id: "thinking_inline_disclosure", marker: "A", title: "极简折叠行", description: "思考块始终收起；只在手动点击时展开全文。" },
    { id: "thinking_auto_card", marker: "B", title: "自动收束预览卡", description: "活动块展开；离开后收成两行预览，仍可手动展开。" },
    { id: "thinking_phase_timeline", marker: "C", title: "全事件时间线", description: "用连续轨道保留思考、进展、工具与回复的前后关系。" },
  ],
  tools: [
    { id: "tools_compact_records", marker: "A", title: "紧凑记录", description: "一个工具一行；参数与结果按需展开。" },
    { id: "tools_execution_cards", marker: "B", title: "执行卡片", description: "调用和返回上下分区，状态一眼可辨。" },
    { id: "tools_execution_timeline", marker: "C", title: "执行时间线", description: "逐项保留调用、失败、重试与完成顺序。" },
  ],
  activity: [
    { id: "activity_pulse_dots", marker: "A", title: "三点脉冲", description: "最轻量的活动反馈，紧邻 assistant 身份。" },
    { id: "activity_breath_timer", marker: "B", title: "呼吸环计时", description: "用当前阶段与累计用时减少等待的不确定感。" },
    { id: "activity_stage_rail", marker: "C", title: "四阶段轨", description: "显式区分连接、思考、工具与回复。" },
  ],
  streaming: [
    { id: "streaming_direct_delta", marker: "A", title: "原始增量", description: "按 provider 批次原样追加，保留真实突发感。" },
    { id: "streaming_smoothed_phrases", marker: "B", title: "短语平滑", description: "将批次放入队列，以稳定节拍释放。" },
    { id: "streaming_block_commit", marker: "C", title: "块级提交", description: "活动段保持纯文本，完成后再提交 Markdown。" },
  ],
};

const BASELINE_VARIANTS: VariantSet = {
  thinking: "thinking_inline_disclosure",
  tools: "tools_compact_records",
  activity: "activity_pulse_dots",
  streaming: "streaming_direct_delta",
};

function answerEvents(startAtMs: number, intervalMs: number, chunks: string[], blockId = "final-answer"): DemoEvent[] {
  return chunks.map((text, index) => ({
    atMs: startAtMs + index * intervalMs,
    type: "answer" as const,
    blockId,
    text,
  }));
}

function progressEvents(startAtMs: number, intervalMs: number, blockId: string, chunks: string[]): DemoEvent[] {
  return chunks.map((text, index) => ({
    atMs: startAtMs + index * intervalMs,
    type: "progress" as const,
    blockId,
    text,
  }));
}

const scenarios: DemoScenario[] = [
  {
    id: "multi_tool",
    label: "正常多工具回复",
    userText: "帮我核对这个判断，并把证据说清楚。",
    durationMs: 11_800,
    events: [
      { atMs: 0, type: "stage", stage: "connecting" },
      { atMs: 650, type: "stage", stage: "thinking" },
      { atMs: 900, type: "reasoning", blockId: "reasoning-1", text: "先拆开问题中的结论与证据要求。" },
      { atMs: 1_400, type: "reasoning", blockId: "reasoning-1", text: "现有摘要不足以支撑精确结论，需要先找候选。" },
      { atMs: 1_600, type: "stage", stage: "progressing" },
      ...progressEvents(1_650, 180, "progress-1", ["我先从相关记忆里", "找定位候选。"]),
      { atMs: 2_100, type: "stage", stage: "tool_running" },
      { atMs: 2_100, type: "tool_start", callId: "call-search", toolId: "memory_search", args: "query_terms=[\"缓存命中\", \"测试结论\"]" },
      { atMs: 2_850, type: "tool_result", callId: "call-search", result: "找到 3 个定位候选；片段不是证据。" },
      { atMs: 3_000, type: "stage", stage: "thinking" },
      { atMs: 3_100, type: "reasoning", blockId: "reasoning-2", text: "候选指向同一条记忆，继续读取完整正文。" },
      { atMs: 3_300, type: "stage", stage: "progressing" },
      ...progressEvents(3_350, 180, "progress-2", ["有三条候选，但片段不是证据；", "我继续打开完整正文。"]),
      { atMs: 3_800, type: "stage", stage: "tool_running" },
      { atMs: 3_800, type: "tool_start", callId: "call-read", toolId: "memory_content_read", args: "mem_id=MEM-7A10C2D4, mount_mode=temporary" },
      { atMs: 4_650, type: "tool_result", callId: "call-read", result: "已读取完整正文；来源坐标为 meta / R000619。" },
      { atMs: 4_850, type: "stage", stage: "thinking" },
      { atMs: 5_000, type: "reasoning", blockId: "reasoning-3", text: "正文给出了范围和限定，但日期仍需回查原始语料。" },
      { atMs: 5_250, type: "stage", stage: "progressing" },
      ...progressEvents(5_300, 180, "progress-3", ["正文还缺精确日期；", "我继续回查创建分身的原始轮审计。"]),
      { atMs: 5_900, type: "stage", stage: "tool_running" },
      { atMs: 5_900, type: "tool_start", callId: "call-grep", toolId: "file_grep", args: "root=persona://active, query=缓存读取比例" },
      { atMs: 6_750, type: "tool_result", callId: "call-grep", result: "命中原始轮审计 2 处，覆盖完整。" },
      { atMs: 7_000, type: "stage", stage: "answering" },
      ...answerEvents(7_150, 520, [
        "可以确认：",
        "这次提升主要发生在连续调用的稳定前缀，",
        "并不等于所有场景都会得到同样的命中率。\n\n",
        "证据链是：记忆定位 → 完整正文 → 原始轮审计。",
      ]),
      { atMs: 9_750, type: "complete" },
    ],
  },
  {
    id: "first_byte_wait",
    label: "首字等待",
    userText: "先仔细想清楚再回答。",
    durationMs: 10_800,
    events: [
      { atMs: 0, type: "stage", stage: "connecting" },
      { atMs: 3_600, type: "stage", stage: "thinking" },
      { atMs: 4_200, type: "reasoning", blockId: "reasoning-1", text: "正在建立回答结构，并核对是否需要外部证据。" },
      { atMs: 6_600, type: "stage", stage: "answering" },
      ...answerEvents(6_900, 650, ["我先给结论：", "当前证据只支持有限判断，", "还不能把它外推成一般规律。"]),
      { atMs: 9_300, type: "complete" },
    ],
  },
  {
    id: "no_reasoning",
    label: "模型未返回 reasoning",
    userText: "把这段信息整理成两点。",
    durationMs: 7_800,
    events: [
      { atMs: 0, type: "stage", stage: "connecting" },
      { atMs: 550, type: "stage", stage: "progressing" },
      ...progressEvents(650, 180, "progress-1", ["我先展开索引，", "确认已有信息的结构。"]),
      { atMs: 1_250, type: "stage", stage: "tool_running" },
      { atMs: 1_250, type: "tool_start", callId: "call-index", toolId: "index_view", args: "scope=ltm_inverted, offset=0, limit=8" },
      { atMs: 2_150, type: "tool_result", callId: "call-index", result: "返回 8 个索引项。" },
      { atMs: 2_500, type: "stage", stage: "answering" },
      ...answerEvents(2_650, 600, ["一是保留事实边界；", "二是把尚未核验的内容明确标成待确认。"]),
      { atMs: 5_300, type: "complete" },
    ],
  },
  {
    id: "tool_retry",
    label: "工具失败、审批与重试",
    userText: "查一下原始文件，失败的话换路径继续。",
    durationMs: 13_200,
    events: [
      { atMs: 0, type: "stage", stage: "thinking" },
      { atMs: 400, type: "reasoning", blockId: "reasoning-1", text: "先尝试正文检索；如果覆盖不完整，再用受控命令核验。" },
      { atMs: 750, type: "stage", stage: "progressing" },
      { atMs: 800, type: "progress", blockId: "progress-1", text: "我先从原始语料做一次字面检索。" },
      { atMs: 1_300, type: "stage", stage: "tool_running" },
      { atMs: 1_300, type: "tool_start", callId: "call-grep-fail", toolId: "file_grep", args: "root=persona://active, query=原始结论" },
      { atMs: 2_250, type: "tool_error", callId: "call-grep-fail", result: "目标文件无法解码；覆盖不完整。" },
      { atMs: 2_500, type: "stage", stage: "thinking" },
      { atMs: 2_650, type: "reasoning", blockId: "reasoning-2", text: "零命中不能当作不存在；改查文本导出目录。" },
      { atMs: 2_900, type: "stage", stage: "progressing" },
      ...progressEvents(2_950, 160, "progress-2", ["第一次检索覆盖不完整，", "我换一条需要审批的路径继续。"]),
      { atMs: 3_350, type: "stage", stage: "tool_running" },
      { atMs: 3_350, type: "tool_start", callId: "call-shell", toolId: "shell_command", args: "command=rg --text 原始结论 export/, purpose=核验历史文本" },
      { atMs: 4_000, type: "stage", stage: "tool_approval" },
      { atMs: 4_000, type: "tool_approval", callId: "call-shell", message: "等待用户允许本次执行。" },
      { atMs: 5_800, type: "stage", stage: "tool_running" },
      { atMs: 6_500, type: "tool_result", callId: "call-shell", result: "退出码 0；命中 2 行并返回来源路径。" },
      { atMs: 6_800, type: "stage", stage: "answering" },
      ...answerEvents(7_000, 600, ["第一次检索覆盖不完整，", "切换到已审批的文本核验后找到了两处来源。", "所以结论应以第二次结果为准。"]),
      { atMs: 10_700, type: "complete" },
    ],
  },
  {
    id: "user_stop",
    label: "用户主动停止",
    userText: "先查资料；如果我停止，就别继续。",
    durationMs: 8_400,
    events: [
      { atMs: 0, type: "stage", stage: "thinking" },
      { atMs: 500, type: "reasoning", blockId: "reasoning-1", text: "需要读取一份较长资料，再汇总结论。" },
      { atMs: 850, type: "stage", stage: "progressing" },
      { atMs: 900, type: "progress", blockId: "progress-1", text: "我先读取这份长资料。" },
      { atMs: 1_400, type: "stage", stage: "tool_running" },
      { atMs: 1_400, type: "tool_start", callId: "call-long-read", toolId: "file_read", args: "path=persona://active/files/raw/long-note.md" },
      { atMs: 3_100, type: "tool_result", callId: "call-long-read", result: "已返回第一窗口；正文尚未读完。" },
      { atMs: 3_300, type: "stage", stage: "progressing" },
      { atMs: 3_450, type: "progress", blockId: "progress-2", text: "我已经读到前半部分，初步看——" },
      { atMs: 4_450, type: "stopped" },
    ],
  },
  {
    id: "long_markdown",
    label: "长 Markdown 回复",
    userText: "用列表、代码和表格给我一个完整示例。",
    durationMs: 17_200,
    events: [
      { atMs: 0, type: "stage", stage: "thinking" },
      { atMs: 500, type: "reasoning", blockId: "reasoning-1", text: "按结论、执行步骤、示例代码和对照表组织。" },
      { atMs: 1_500, type: "stage", stage: "answering" },
      ...answerEvents(1_700, 720, [
        "### 建议方案\n\n",
        "先把状态与正文分开：\n\n",
        "- 思考过程独立折叠\n- 工具调用逐项显示\n- 回复正文单独流入\n\n",
        "```ts\nconst state = { phase: \"answering\" };\nrenderFrame(state);\n```\n\n",
        "| 阶段 | 对用户可见 |\n| --- | --- |\n| 思考 | 可折叠 |\n| 工具 | 逐项卡片 |\n| 回复 | 流式正文 |\n\n",
        "这样既保留透明度，也不会把审计结构直接倾倒进聊天窗口。",
      ]),
      { atMs: 12_000, type: "complete" },
    ],
  },
];

const scenarioById = new Map(scenarios.map((scenario) => [scenario.id, scenario]));

const stageLabels: Record<ProcessStage, string> = {
  idle: "等待播放",
  connecting: "正在连接模型",
  thinking: "正在思考",
  progressing: "正在输出轮中进展",
  tool_running: "正在执行工具",
  tool_approval: "等待工具审批",
  answering: "正在组织回复",
  completed: "本次演示已完成",
  stopped: "已按用户要求停止",
};

function requiredElement<T extends HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (!(node instanceof HTMLElement)) throw new Error(`missing showcase element: ${id}`);
  return node as T;
}

function createElement<K extends keyof HTMLElementTagNameMap>(
  tagName: K,
  className = "",
  text = "",
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tagName);
  if (className) node.className = className;
  if (text) node.textContent = text;
  return node;
}

function formatSeconds(milliseconds: number): string {
  return `${(Math.max(0, milliseconds) / 1000).toFixed(1)} 秒`;
}

function normalizeSpeed(value: unknown): number {
  const speed = Number(value);
  return [0.5, 1, 2].includes(speed) ? speed : 1;
}

function isScenarioId(value: unknown): value is ScenarioId {
  return typeof value === "string" && scenarioById.has(value as ScenarioId);
}

function validVariant(component: ComponentKey, value: unknown): value is string {
  return typeof value === "string" && variants[component].some((variant) => variant.id === value);
}

function deriveSnapshot(scenario: DemoScenario, elapsedMs: number): DemoSnapshot {
  let stage: ProcessStage = elapsedMs > 0 ? "connecting" : "idle";
  let terminalMessage = "";
  const reasoningOrder: string[] = [];
  const reasoningMap = new Map<string, ReasoningSegment>();
  const textOrder: string[] = [];
  const textMap = new Map<string, TextSegment>();
  const toolMap = new Map<string, ToolSnapshot>();
  const timelineRefs: Array<{ kind: "reasoning" | "progress" | "answer" | "tool"; id: string; atMs: number }> = [];
  const timelineKeys = new Set<string>();

  const appendTimeline = (kind: "reasoning" | "progress" | "answer" | "tool", id: string, atMs: number): void => {
    const key = `${kind}:${id}`;
    if (timelineKeys.has(key)) return;
    timelineKeys.add(key);
    timelineRefs.push({ kind, id, atMs });
  };

  for (const event of scenario.events) {
    if (elapsedMs <= 0 || event.atMs > elapsedMs) break;
    if (event.type === "stage") {
      stage = event.stage;
    } else if (event.type === "reasoning") {
      let segment = reasoningMap.get(event.blockId);
      if (!segment) {
        reasoningOrder.push(event.blockId);
        segment = {
          id: event.blockId,
          label: `思考片段 ${reasoningOrder.length}`,
          text: "",
          startedAtMs: event.atMs,
          endedAtMs: null,
          active: false,
        };
        reasoningMap.set(event.blockId, segment);
        appendTimeline("reasoning", event.blockId, event.atMs);
      }
      segment.text += event.text;
    } else if (event.type === "progress" || event.type === "answer") {
      const key = `${event.type}:${event.blockId}`;
      let segment = textMap.get(key);
      if (!segment) {
        textOrder.push(key);
        segment = {
          id: key,
          kind: event.type,
          text: "",
          active: false,
        };
        textMap.set(key, segment);
        appendTimeline(event.type, key, event.atMs);
      }
      segment.text += event.text;
    } else if (event.type === "tool_start") {
      toolMap.set(event.callId, {
        callId: event.callId,
        toolId: event.toolId,
        args: event.args,
        result: "",
        state: "running",
        startedAtMs: event.atMs,
        endedAtMs: null,
      });
      appendTimeline("tool", event.callId, event.atMs);
    } else if (event.type === "tool_approval") {
      const tool = toolMap.get(event.callId);
      if (tool) {
        tool.state = "approval";
        tool.result = event.message;
      }
    } else if (event.type === "tool_result" || event.type === "tool_error") {
      const tool = toolMap.get(event.callId);
      if (tool) {
        tool.state = event.type === "tool_result" ? "succeeded" : "failed";
        tool.result = event.result;
        tool.endedAtMs = event.atMs;
      }
    } else if (event.type === "complete") {
      stage = "completed";
    } else if (event.type === "stopped") {
      stage = "stopped";
      terminalMessage = "用户已停止本轮；只保留停止前已经显示的内容。";
    }
  }

  if (stage === "stopped") {
    toolMap.forEach((tool) => {
      if (["running", "approval"].includes(tool.state)) {
        tool.state = "stopped";
        tool.endedAtMs = elapsedMs;
        tool.result = terminalMessage || "处理已停止。";
      }
    });
  }

  const terminal = stage === "completed" || stage === "stopped";
  timelineRefs.forEach((ref, index) => {
    const next = timelineRefs[index + 1];
    const end = next?.atMs ?? (terminal ? elapsedMs : null);
    const isLast = index === timelineRefs.length - 1;
    if (ref.kind === "reasoning") {
      const segment = reasoningMap.get(ref.id);
      if (segment) {
        segment.endedAtMs = end;
        segment.active = isLast && stage === "thinking";
      }
    } else if (ref.kind === "progress" || ref.kind === "answer") {
      const segment = textMap.get(ref.id);
      if (segment) {
        segment.active = isLast && (stage === (ref.kind === "progress" ? "progressing" : "answering"));
      }
    }
  });

  const reasoningSegments = reasoningOrder.map((id) => reasoningMap.get(id)).filter((value): value is ReasoningSegment => Boolean(value));
  const textSegments = textOrder.map((id) => textMap.get(id)).filter((value): value is TextSegment => Boolean(value));
  const timeline: TimelineItem[] = [];
  timelineRefs.forEach((ref) => {
    if (ref.kind === "reasoning") {
      const value = reasoningMap.get(ref.id);
      if (value) timeline.push({ kind: "reasoning", id: ref.id, value });
    } else if (ref.kind === "tool") {
      const value = toolMap.get(ref.id);
      if (value) timeline.push({ kind: "tool", id: ref.id, value });
    } else {
      const value = textMap.get(ref.id);
      if (value) timeline.push({ kind: ref.kind, id: ref.id, value });
    }
  });

  return {
    elapsedMs,
    stage,
    reasoningSegments,
    tools: [...toolMap.values()],
    textSegments,
    timeline,
    terminalMessage,
  };
}

const scenarioSelect = requiredElement<HTMLSelectElement>("scenarioSelect");
const playButton = requiredElement<HTMLButtonElement>("playButton");
const pauseButton = requiredElement<HTMLButtonElement>("pauseButton");
const resetButton = requiredElement<HTMLButtonElement>("resetButton");
const timelineStatus = requiredElement<HTMLElement>("timelineStatus");
const timelineProgress = requiredElement<HTMLProgressElement>("timelineProgress");
const thinkingVariants = requiredElement<HTMLElement>("thinkingVariants");
const toolVariants = requiredElement<HTMLElement>("toolVariants");
const activityVariants = requiredElement<HTMLElement>("activityVariants");
const streamingVariants = requiredElement<HTMLElement>("streamingVariants");
const combinationMissing = requiredElement<HTMLElement>("combinationMissing");
const combinationPreview = requiredElement<HTMLElement>("combinationPreview");
const selectionSummary = requiredElement<HTMLElement>("selectionSummary");
const selectionFeedback = requiredElement<HTMLElement>("selectionFeedback");
const clearSelectionButton = requiredElement<HTMLButtonElement>("clearSelectionButton");
const copySelectionButton = requiredElement<HTMLButtonElement>("copySelectionButton");
const exportSelectionButton = requiredElement<HTMLButtonElement>("exportSelectionButton");

const focusedSurfaces: Record<ComponentKey, Map<string, PreviewSurface>> = {
  thinking: new Map(),
  tools: new Map(),
  activity: new Map(),
  streaming: new Map(),
};
let combinationSurface: PreviewSurface | null = null;
let selections: SelectionState = {};
let currentScenarioId: ScenarioId = "multi_tool";
let playbackSpeed = 1;
let elapsedMs = 0;
let playing = false;
let animationFrame = 0;
let previousTimestamp: number | null = null;
const smoothedText = new Map<string, string>();
let activeTab = "thinking";

function currentScenario(): DemoScenario {
  return scenarioById.get(currentScenarioId) || scenarios[0];
}

function readStoredState(): StoredState | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const data = JSON.parse(raw) as Partial<StoredState>;
    if (data.schema_version !== STATE_SCHEMA || !isScenarioId(data.scenario)) return null;
    const safeSelections: SelectionState = {};
    COMPONENTS.forEach((component) => {
      const candidate = data.selections?.[component];
      if (validVariant(component, candidate)) safeSelections[component] = candidate;
    });
    return {
      schema_version: STATE_SCHEMA,
      scenario: data.scenario,
      speed: normalizeSpeed(data.speed),
      selections: safeSelections,
    };
  } catch {
    return null;
  }
}

function persistState(): void {
  const state: StoredState = {
    schema_version: STATE_SCHEMA,
    scenario: currentScenarioId,
    speed: playbackSpeed,
    selections,
  };
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    selectionFeedback.textContent = "浏览器未允许保存本地选型；本页仍可继续比较。";
  }
}

function selectionPayload(): Record<string, string> | null {
  if (!COMPONENTS.every((component) => validVariant(component, selections[component]))) return null;
  return {
    schema_version: SELECTION_SCHEMA,
    thinking: selections.thinking || "",
    tools: selections.tools || "",
    activity: selections.activity || "",
    streaming: selections.streaming || "",
  };
}

function selectedDefinition(component: ComponentKey): VariantDefinition | null {
  return variants[component].find((variant) => variant.id === selections[component]) || null;
}

function readableSelection(): string {
  const chosen = COMPONENTS
    .map((component) => {
      const definition = selectedDefinition(component);
      return definition ? `${componentLabels[component]}：${definition.marker}｜${definition.title}` : "";
    })
    .filter(Boolean);
  return chosen.length ? chosen.join("；") : "尚未选择任何方案。";
}

function buildMiniThread(root: HTMLElement): PreviewSurface {
  const thread = createElement("div", "mini-thread");
  const user = createElement("div", "mini-user");
  user.dataset.showcaseUser = "true";
  thread.append(user);
  const assistant = createElement("div", "mini-assistant");
  assistant.append(createElement("div", "mini-assistant-label", "UPSP · ASSISTANT"));
  const timeline = createElement("div", "round-event-stream");
  timeline.setAttribute("aria-label", "本轮可观察事件时间线");
  const activity = createElement("div", "activity-mount round-activity-tail");
  assistant.append(timeline, activity);
  thread.append(assistant);
  root.append(thread);
  return { timeline, activity };
}

function buildVariantCard(component: ComponentKey, definition: VariantDefinition): HTMLElement {
  const card = createElement("article", "variant-card");
  card.dataset.component = component;
  card.dataset.variantId = definition.id;
  const header = createElement("header");
  header.append(createElement("span", "", `方案 ${definition.marker}`));
  header.append(createElement("h3", "", definition.title));
  header.append(createElement("p", "", definition.description));
  const preview = createElement("div", "variant-preview");
  focusedSurfaces[component].set(definition.id, buildMiniThread(preview));
  const footer = createElement("footer");
  const select = createElement("button", "variant-select", "选择此方案");
  select.type = "button";
  select.dataset.selectComponent = component;
  select.dataset.selectVariant = definition.id;
  select.setAttribute("aria-pressed", "false");
  footer.append(select);
  card.append(header, preview, footer);
  return card;
}

function buildVariantGrids(): void {
  const roots: Record<ComponentKey, HTMLElement> = {
    thinking: thinkingVariants,
    tools: toolVariants,
    activity: activityVariants,
    streaming: streamingVariants,
  };
  COMPONENTS.forEach((component) => {
    roots[component].replaceChildren(...variants[component].map((definition) => buildVariantCard(component, definition)));
  });
}

function updateUserText(): void {
  document.querySelectorAll<HTMLElement>("[data-showcase-user]").forEach((node) => {
    node.textContent = currentScenario().userText;
  });
}

function disclosureOpen(toggle: HTMLButtonElement, automatic: boolean): boolean {
  if (toggle.dataset.manual === "true") return toggle.getAttribute("aria-expanded") === "true";
  return automatic;
}

function createThinkingUnit(variantId: string, segmentId: string): HTMLElement {
  const variantClass = variantId === "thinking_auto_card"
    ? "variant-b"
    : variantId === "thinking_phase_timeline" ? "variant-c" : "";
  const unit = createElement("section", `thinking-unit ${variantClass}`);
  unit.dataset.thinkingVariant = variantId;
  unit.dataset.reasoningId = segmentId;
  const toggle = createElement("button", "thinking-toggle") as HTMLButtonElement;
  toggle.type = "button";
  toggle.setAttribute("aria-expanded", "false");
  toggle.append(createElement("strong", "", "思考片段"));
  toggle.append(createElement("span", "thinking-meta", "等待 reasoning"));
  const content = createElement("p", "thinking-content");
  content.hidden = true;
  toggle.addEventListener("click", () => {
    const open = toggle.getAttribute("aria-expanded") !== "true";
    toggle.dataset.manual = "true";
    toggle.setAttribute("aria-expanded", String(open));
    content.classList.remove("is-preview");
    content.hidden = !open;
  });
  unit.append(toggle, content);
  return unit;
}

function renderThinkingUnit(unit: HTMLElement, variantId: string, segment: ReasoningSegment, snapshot: DemoSnapshot): void {
  const toggle = unit.querySelector<HTMLButtonElement>(".thinking-toggle");
  const meta = unit.querySelector<HTMLElement>(".thinking-meta");
  const title = unit.querySelector<HTMLElement>(".thinking-toggle strong");
  const content = unit.querySelector<HTMLElement>(".thinking-content");
  if (!toggle || !meta || !content) return;
  const automaticOpen = variantId !== "thinking_inline_disclosure" && segment.active;
  const expanded = disclosureOpen(toggle, automaticOpen);
  const preview = variantId === "thinking_auto_card" && !segment.active && toggle.dataset.manual !== "true";
  toggle.setAttribute("aria-expanded", String(expanded));
  content.hidden = !expanded && !preview;
  content.classList.toggle("is-preview", preview);
  const end = segment.endedAtMs ?? snapshot.elapsedMs;
  if (title) title.textContent = segment.label;
  const stateLabel = segment.active ? "思考中" : preview ? "两行预览" : "已结束";
  meta.textContent = `${stateLabel} · ${formatSeconds(end - segment.startedAtMs)}`;
  content.textContent = segment.text;
  unit.classList.toggle("is-active", segment.active);
  unit.dataset.state = segment.active ? "active" : "settled";
}

function renderThinking(root: HTMLElement, variantId: string, snapshot: DemoSnapshot, segments = snapshot.reasoningSegments): void {
  root.classList.add("thinking-mount", "thinking-list");
  root.classList.toggle("thinking-timeline", variantId === "thinking_phase_timeline");
  segments.forEach((segment) => {
    let unit = [...root.querySelectorAll<HTMLElement>("[data-reasoning-id]")]
      .find((candidate) => candidate.dataset.reasoningId === segment.id);
    if (!unit || unit.dataset.thinkingVariant !== variantId) {
      unit?.remove();
      unit = createThinkingUnit(variantId, segment.id);
      root.append(unit);
    }
    renderThinkingUnit(unit, variantId, segment, snapshot);
  });
  const activeIds = new Set(segments.map((segment) => segment.id));
  root.querySelectorAll<HTMLElement>("[data-reasoning-id]").forEach((unit) => {
    if (!activeIds.has(unit.dataset.reasoningId || "")) unit.remove();
  });
  if (!segments.length) {
    let empty = root.querySelector<HTMLElement>(".runtime-empty-copy");
    if (!empty) {
      empty = createElement("p", "runtime-empty-copy");
      root.append(empty);
    }
    empty.textContent = snapshot.stage === "completed" || snapshot.stage === "stopped"
      ? "Provider 未返回 reasoning；未生成思考节点。"
      : "当前尚未收到 reasoning；不生成占位思考节点。";
  }
  if (segments.length) root.querySelector(".runtime-empty-copy")?.remove();
}

function toolStateLabel(tool: ToolSnapshot, elapsedMsValue: number): string {
  const labels: Record<ToolState, string> = {
    running: "执行中",
    approval: "等待审批",
    succeeded: "已完成",
    failed: "执行失败",
    stopped: "已停止",
  };
  const end = tool.endedAtMs ?? elapsedMsValue;
  return `${labels[tool.state]} · ${formatSeconds(end - tool.startedAtMs)}`;
}

function createToolItem(variantId: string, tool: ToolSnapshot): HTMLElement {
  if (variantId === "tools_execution_cards") {
    const card = createElement("article", "tool-item tool-card-b");
    card.dataset.callId = tool.callId;
    const header = createElement("header");
    header.append(createElement("code"), createElement("span", "tool-state"));
    const detail = createElement("div", "tool-detail");
    const call = createElement("div");
    call.append(createElement("b", "", "调用参数"), createElement("pre", "tool-args"));
    const result = createElement("div");
    result.append(createElement("b", "", "执行结果"), createElement("p", "tool-result"));
    detail.append(call, result);
    card.append(header, detail);
    return card;
  }
  const details = createElement("details", "tool-item");
  details.dataset.callId = tool.callId;
  const summary = createElement("summary");
  summary.append(createElement("strong"), createElement("span", "tool-state"));
  const detail = createElement("div", "tool-detail");
  const call = createElement("div");
  call.append(createElement("b", "", "调用参数"), createElement("pre", "tool-args"));
  const result = createElement("div");
  result.append(createElement("b", "", "执行结果"), createElement("p", "tool-result"));
  detail.append(call, result);
  details.append(summary, detail);
  return details;
}

function renderToolNode(root: HTMLElement, variantId: string, tool: ToolSnapshot, snapshot: DemoSnapshot): void {
  let item = root.querySelector<HTMLElement>("[data-call-id]");
  if (!item || item.dataset.callId !== tool.callId || item.dataset.toolVariant !== variantId) {
    item?.remove();
    item = createToolItem(variantId, tool);
    item.dataset.toolVariant = variantId;
    root.append(item);
  }
  item.dataset.state = tool.state;
  const name = item.querySelector("strong, code");
  const state = item.querySelector<HTMLElement>(".tool-state");
  const args = item.querySelector<HTMLElement>(".tool-args");
  const result = item.querySelector<HTMLElement>(".tool-result");
  if (name) name.textContent = tool.toolId;
  if (state) state.textContent = toolStateLabel(tool, snapshot.elapsedMs);
  if (args) args.textContent = tool.args;
  if (result) result.textContent = tool.result || "等待返回……";
}

function createActivitySurface(variantId: string): HTMLElement {
  const surface = createElement("div", "activity-surface");
  surface.dataset.activityVariant = variantId;
  if (variantId === "activity_pulse_dots") {
    const dots = createElement("div", "pulse-dots");
    dots.setAttribute("aria-hidden", "true");
    dots.append(createElement("i"), createElement("i"), createElement("i"));
    surface.append(dots, createElement("div", "activity-copy"));
  } else if (variantId === "activity_breath_timer") {
    surface.append(createElement("div", "breath-ring"), createElement("div", "activity-copy"));
  } else {
    const rail = createElement("div", "stage-rail");
    [
      ["connecting", "连接模型"],
      ["thinking", "思考"],
      ["tool_running", "执行工具"],
      ["answering", "组织回复"],
    ].forEach(([stage, label]) => {
      const row = createElement("div", "stage-node");
      row.dataset.processStage = stage;
      row.append(createElement("i"), createElement("span", "", label), createElement("em", "", "等待"));
      rail.append(row);
    });
    surface.append(rail);
  }
  return surface;
}

function stageRank(stage: ProcessStage): number {
  if (stage === "connecting" || stage === "idle") return 0;
  if (stage === "thinking") return 1;
  if (stage === "tool_running" || stage === "tool_approval") return 2;
  if (stage === "progressing") return 3;
  return 3;
}

function renderActivity(root: HTMLElement, variantId: string, snapshot: DemoSnapshot): void {
  let surface = root.querySelector<HTMLElement>("[data-activity-variant]");
  if (!surface || surface.dataset.activityVariant !== variantId) {
    root.replaceChildren(createActivitySurface(variantId));
    surface = root.querySelector<HTMLElement>("[data-activity-variant]");
  }
  if (!surface) return;
  const label = stageLabels[snapshot.stage];
  surface.classList.toggle("is-active", !["idle", "completed", "stopped"].includes(snapshot.stage));
  if (variantId === "activity_pulse_dots") {
    const copy = surface.querySelector<HTMLElement>(".activity-copy");
    if (copy) copy.textContent = label;
  } else if (variantId === "activity_breath_timer") {
    const ring = surface.querySelector<HTMLElement>(".breath-ring");
    const copy = surface.querySelector<HTMLElement>(".activity-copy");
    if (ring) ring.textContent = `${(snapshot.elapsedMs / 1000).toFixed(0)}s`;
    if (copy) copy.textContent = label;
  } else {
    const currentRank = stageRank(snapshot.stage);
    surface.querySelectorAll<HTMLElement>("[data-process-stage]").forEach((node, index) => {
      let state = index < currentRank ? "done" : index === currentRank ? "active" : "waiting";
      if (snapshot.stage === "idle") state = "waiting";
      if (["completed", "stopped"].includes(snapshot.stage)) state = snapshot.stage === "completed" ? "done" : index < currentRank ? "done" : "waiting";
      node.dataset.stageState = state;
      const status = node.querySelector("em");
      if (status) status.textContent = state === "done" ? "完成" : state === "active" ? label : "等待";
    });
  }
  surface.setAttribute("aria-label", `${label}，已用 ${formatSeconds(snapshot.elapsedMs)}`);
  surface.setAttribute("role", "status");
}

function createStreamingSurface(variantId: string): HTMLElement {
  if (variantId === "streaming_block_commit") {
    const surface = createElement("section", "reply-object stream-output block-stream-output");
    surface.dataset.streamingVariant = variantId;
    surface.append(createElement("div", "committed-blocks"), createElement("div", "active-stream-block"));
    return surface;
  }
  const output = createElement("section", "reply-object stream-output is-empty");
  output.dataset.streamingVariant = variantId;
  return output;
}

function splitStreamBlocks(text: string, terminal: boolean): { complete: string[]; active: string } {
  if (!text) return { complete: [], active: "" };
  if (terminal) return { complete: text.split(/\n\n+/).filter(Boolean), active: "" };
  const boundary = text.lastIndexOf("\n\n");
  if (boundary < 0) return { complete: [], active: text };
  return {
    complete: text.slice(0, boundary).split(/\n\n+/).filter(Boolean),
    active: text.slice(boundary + 2),
  };
}

function appendCommittedBlock(root: HTMLElement, block: string): void {
  const trimmed = block.trim();
  if (!trimmed) return;
  if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
    const lines = trimmed.split("\n");
    const pre = createElement("pre");
    const code = createElement("code", "", lines.slice(1, -1).join("\n"));
    pre.append(code);
    root.append(pre);
    return;
  }
  if (trimmed.startsWith("### ")) {
    root.append(createElement("h3", "", trimmed.slice(4)));
    return;
  }
  if (trimmed.split("\n").every((line) => line.startsWith("- "))) {
    const list = createElement("ul");
    trimmed.split("\n").forEach((line) => list.append(createElement("li", "", line.slice(2))));
    root.append(list);
    return;
  }
  const lines = trimmed.split("\n");
  if (lines.length >= 3 && lines[0].includes("|") && /^\|?\s*---/.test(lines[1])) {
    const table = createElement("table");
    const splitRow = (line: string): string[] => line.replace(/^\||\|$/g, "").split("|").map((cell) => cell.trim());
    const thead = createElement("thead");
    const headRow = createElement("tr");
    splitRow(lines[0]).forEach((cell) => headRow.append(createElement("th", "", cell)));
    thead.append(headRow);
    const tbody = createElement("tbody");
    lines.slice(2).forEach((line) => {
      const row = createElement("tr");
      splitRow(line).forEach((cell) => row.append(createElement("td", "", cell)));
      tbody.append(row);
    });
    table.append(thead, tbody);
    root.append(table);
    return;
  }
  root.append(createElement("p", "", trimmed));
}

function createTextEvent(segment: TextSegment): HTMLElement {
  const event = createElement("section", `text-event text-event-${segment.kind}`);
  event.dataset.textId = segment.id;
  const header = createElement("header");
  header.append(
    createElement("strong", "", segment.kind === "progress" ? "轮中进展" : "最终回复"),
    createElement("span", "text-event-state"),
  );
  event.append(header, createElement("div", "text-event-mount"));
  return event;
}

function renderTextNode(root: HTMLElement, variantId: string, segment: TextSegment): void {
  let event = root.querySelector<HTMLElement>("[data-text-id]");
  if (!event || event.dataset.textId !== segment.id) {
    event?.remove();
    event = createTextEvent(segment);
    root.append(event);
  }
  event.className = `text-event text-event-${segment.kind}`;
  event.dataset.state = segment.active ? "streaming" : "settled";
  const state = event.querySelector<HTMLElement>(".text-event-state");
  const mount = event.querySelector<HTMLElement>(".text-event-mount");
  if (!mount) return;
  if (state) state.textContent = segment.active ? "输出中" : "已结算";
  let surface = mount.querySelector<HTMLElement>("[data-streaming-variant]");
  if (!surface || surface.dataset.streamingVariant !== variantId) {
    mount.replaceChildren(createStreamingSurface(variantId));
    surface = mount.querySelector<HTMLElement>("[data-streaming-variant]");
  }
  if (!surface) return;
  const terminal = !segment.active;
  const text = variantId === "streaming_smoothed_phrases" ? smoothedText.get(segment.id) || "" : segment.text;
  if (variantId === "streaming_block_commit") {
    const blocksRoot = surface.querySelector<HTMLElement>(".committed-blocks");
    const active = surface.querySelector<HTMLElement>(".active-stream-block");
    if (!blocksRoot || !active) return;
    const blocks = splitStreamBlocks(text, terminal);
    const signature = JSON.stringify(blocks.complete);
    if (blocksRoot.dataset.signature !== signature) {
      blocksRoot.replaceChildren();
      blocks.complete.forEach((block) => appendCommittedBlock(blocksRoot, block));
      blocksRoot.dataset.signature = signature;
    }
    active.textContent = blocks.active;
    active.hidden = !blocks.active;
    active.classList.toggle("is-streaming", !terminal && Boolean(blocks.active));
  } else {
    surface.textContent = text || "等待文本……";
    surface.classList.toggle("is-empty", !text);
    surface.classList.toggle("is-streaming", segment.active && Boolean(text));
  }
  event.setAttribute("aria-label", `${segment.kind === "progress" ? "轮中进展" : "最终回复"}，${segment.active ? "输出中" : "已结算"}`);
}

function renderStreaming(
  root: HTMLElement,
  variantId: string,
  snapshot: DemoSnapshot,
  segments = snapshot.textSegments,
  includeTerminal = true,
): void {
  root.classList.add("streaming-mount", "text-event-list");
  segments.forEach((segment) => {
    let mount = [...root.querySelectorAll<HTMLElement>("[data-text-mount]")]
      .find((candidate) => candidate.dataset.textMount === segment.id);
    if (!mount) {
      mount = createElement("div", "text-node-mount");
      mount.dataset.textMount = segment.id;
      root.append(mount);
    }
    renderTextNode(mount, variantId, segment);
  });
  const activeIds = new Set(segments.map((segment) => segment.id));
  root.querySelectorAll<HTMLElement>("[data-text-mount]").forEach((node) => {
    if (!activeIds.has(node.dataset.textMount || "")) node.remove();
  });
  if (!segments.length && !root.querySelector(".runtime-empty-copy")) {
    root.append(createElement("p", "runtime-empty-copy", "等待轮中进展或最终回复……"));
  }
  if (segments.length) root.querySelector(".runtime-empty-copy")?.remove();
  let terminal = root.querySelector<HTMLElement>(".stream-terminal-note");
  if (includeTerminal && snapshot.terminalMessage) {
    if (!terminal) {
      terminal = createElement("p", "stream-terminal-note");
      root.append(terminal);
    }
    terminal.textContent = snapshot.terminalMessage;
  } else {
    terminal?.remove();
  }
}

function resetDynamicSurfaces(): void {
  document.querySelectorAll<HTMLElement>("[data-reasoning-id]").forEach((node) => node.remove());
  document.querySelectorAll<HTMLElement>("[data-text-mount]").forEach((node) => node.remove());
  document.querySelectorAll<HTMLElement>("[data-round-event-key]").forEach((node) => node.remove());
  document.querySelectorAll<HTMLElement>("[data-activity-variant]").forEach((node) => node.remove());
  document.querySelectorAll(".runtime-empty-copy").forEach((node) => node.remove());
  document.querySelectorAll(".stream-terminal-note").forEach((node) => node.remove());
}

function buildCombinationPreview(): void {
  combinationSurface = null;
  const missing = COMPONENTS.filter((component) => !validVariant(component, selections[component]));
  if (missing.length) {
    combinationMissing.hidden = false;
    combinationPreview.hidden = true;
    combinationPreview.replaceChildren();
    combinationMissing.textContent = `还需选择：${missing.map((component) => componentLabels[component]).join("、")}。`;
    return;
  }
  combinationMissing.hidden = true;
  combinationPreview.hidden = false;
  combinationPreview.replaceChildren();
  combinationSurface = buildMiniThread(combinationPreview);
  updateUserText();
}

function createRoundEventNode(item: TimelineItem): HTMLElement {
  const node = createElement("section", `round-event-node round-event-${item.kind}`);
  node.dataset.roundEventKey = `${item.kind}:${item.id}`;
  node.dataset.eventKind = item.kind;
  node.append(createElement("div", "round-event-marker"), createElement("div", "round-event-mount"));
  return node;
}

function renderRoundStream(surface: PreviewSurface, snapshot: DemoSnapshot, variantSet: VariantSet): void {
  const { timeline, activity } = surface;
  const useTimeline = variantSet.thinking === "thinking_phase_timeline"
    || variantSet.tools === "tools_execution_timeline";
  timeline.classList.toggle("is-timeline", useTimeline);
  timeline.dataset.thinkingVariant = variantSet.thinking;
  timeline.dataset.toolsVariant = variantSet.tools;
  timeline.dataset.streamingVariant = variantSet.streaming;
  snapshot.timeline.forEach((item) => {
    const key = `${item.kind}:${item.id}`;
    let node = [...timeline.querySelectorAll<HTMLElement>("[data-round-event-key]")]
      .find((candidate) => candidate.dataset.roundEventKey === key);
    if (!node) {
      node = createRoundEventNode(item);
      timeline.append(node);
    }
    const mount = node.querySelector<HTMLElement>(".round-event-mount");
    if (!mount) return;
    if (item.kind === "reasoning") {
      renderThinking(mount, variantSet.thinking, snapshot, [item.value]);
    } else if (item.kind === "tool") {
      mount.className = `round-event-mount tool-list ${variantSet.tools === "tools_execution_timeline" ? "tool-timeline" : ""}`;
      renderToolNode(mount, variantSet.tools, item.value, snapshot);
    } else if (item.kind === "progress" || item.kind === "answer") {
      renderStreaming(mount, variantSet.streaming, snapshot, [item.value], false);
    }
  });
  const activeKeys = new Set(snapshot.timeline.map((item) => `${item.kind}:${item.id}`));
  timeline.querySelectorAll<HTMLElement>("[data-round-event-key]").forEach((node) => {
    if (!activeKeys.has(node.dataset.roundEventKey || "")) node.remove();
  });
  let terminal = timeline.querySelector<HTMLElement>(".round-terminal-note");
  if (snapshot.terminalMessage) {
    if (!terminal) {
      terminal = createElement("p", "round-terminal-note");
      timeline.append(terminal);
    }
    terminal.textContent = snapshot.terminalMessage;
  } else {
    terminal?.remove();
  }
  renderActivity(activity, variantSet.activity, snapshot);
}

function renderCombination(snapshot: DemoSnapshot): void {
  if (!combinationSurface) return;
  const variantSet: VariantSet = {
    thinking: selections.thinking || BASELINE_VARIANTS.thinking,
    tools: selections.tools || BASELINE_VARIANTS.tools,
    activity: selections.activity || BASELINE_VARIANTS.activity,
    streaming: selections.streaming || BASELINE_VARIANTS.streaming,
  };
  renderRoundStream(combinationSurface, snapshot, variantSet);
}

function updateSelectionUi(): void {
  document.querySelectorAll<HTMLElement>("[data-variant-id]").forEach((card) => {
    const component = card.dataset.component as ComponentKey;
    const selected = selections[component] === card.dataset.variantId;
    card.classList.toggle("is-selected", selected);
    const button = card.querySelector<HTMLButtonElement>(".variant-select");
    if (button) {
      button.setAttribute("aria-pressed", String(selected));
      button.textContent = selected ? "已选择" : "选择此方案";
    }
  });
  selectionSummary.textContent = readableSelection();
  const complete = Boolean(selectionPayload());
  copySelectionButton.disabled = !complete;
  exportSelectionButton.disabled = !complete;
  buildCombinationPreview();
  persistState();
  renderAll(deriveSnapshot(currentScenario(), elapsedMs));
}

function renderFocused(component: ComponentKey, snapshot: DemoSnapshot): void {
  focusedSurfaces[component].forEach((surface, variantId) => {
    renderRoundStream(surface, snapshot, { ...BASELINE_VARIANTS, [component]: variantId });
  });
}

function renderAll(snapshot: DemoSnapshot): void {
  COMPONENTS.forEach((component) => renderFocused(component, snapshot));
  renderCombination(snapshot);
  updateUserText();
  timelineStatus.textContent = `${stageLabels[snapshot.stage]} · ${formatSeconds(snapshot.elapsedMs)}`;
  timelineProgress.value = snapshot.stage === "completed" ? 1 : Math.min(1, snapshot.elapsedMs / currentScenario().durationMs);
  playButton.disabled = playing;
  pauseButton.disabled = !playing;
}

function resetPlayback(): void {
  playing = false;
  cancelAnimationFrame(animationFrame);
  previousTimestamp = null;
  elapsedMs = 0;
  smoothedText.clear();
  resetDynamicSurfaces();
  renderAll(deriveSnapshot(currentScenario(), elapsedMs));
}

function advanceSmoothedText(snapshot: DemoSnapshot, deltaMs: number): void {
  const release = Math.max(1, Math.floor(deltaMs * playbackSpeed * 0.12));
  snapshot.textSegments.forEach((segment) => {
    let current = smoothedText.get(segment.id) || "";
    if (!segment.text.startsWith(current)) current = "";
    if (current.length < segment.text.length) {
      current += segment.text.slice(current.length, current.length + release);
      smoothedText.set(segment.id, current);
    }
  });
}

function animationTick(timestamp: number): void {
  if (!playing) return;
  const last = previousTimestamp ?? timestamp;
  const delta = Math.min(120, timestamp - last);
  previousTimestamp = timestamp;
  const scenario = currentScenario();
  elapsedMs = Math.min(scenario.durationMs, elapsedMs + delta * playbackSpeed);
  const snapshot = deriveSnapshot(scenario, elapsedMs);
  advanceSmoothedText(snapshot, delta);
  renderAll(snapshot);
  const sourceComplete = ["completed", "stopped"].includes(snapshot.stage) || elapsedMs >= scenario.durationMs;
  const smoothedComplete = snapshot.textSegments.every((segment) => (smoothedText.get(segment.id) || "").length >= segment.text.length);
  if (sourceComplete && smoothedComplete) {
    playing = false;
    previousTimestamp = null;
    renderAll(snapshot);
    return;
  }
  animationFrame = requestAnimationFrame(animationTick);
}

function startPlayback(): void {
  const stage = deriveSnapshot(currentScenario(), elapsedMs).stage;
  if (["completed", "stopped"].includes(stage) || elapsedMs >= currentScenario().durationMs) resetPlayback();
  if (playing) return;
  playing = true;
  previousTimestamp = null;
  animationFrame = requestAnimationFrame(animationTick);
  renderAll(deriveSnapshot(currentScenario(), elapsedMs));
}

function pausePlayback(): void {
  playing = false;
  cancelAnimationFrame(animationFrame);
  previousTimestamp = null;
  renderAll(deriveSnapshot(currentScenario(), elapsedMs));
}

function setActiveTab(tabId: string, focus = false): void {
  const tabs = [...document.querySelectorAll<HTMLButtonElement>("[data-showcase-tab]")];
  if (!tabs.some((tab) => tab.dataset.showcaseTab === tabId)) return;
  activeTab = tabId;
  tabs.forEach((tab) => {
    const selected = tab.dataset.showcaseTab === activeTab;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
    if (selected && focus) tab.focus();
  });
  document.querySelectorAll<HTMLElement>("[data-showcase-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.showcasePanel !== activeTab;
  });
}

async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    const field = createElement("textarea") as HTMLTextAreaElement;
    field.value = text;
    field.style.position = "fixed";
    field.style.opacity = "0";
    document.body.append(field);
    field.select();
    const copied = document.execCommand("copy");
    field.remove();
    return copied;
  }
}

function exportSelection(): void {
  const payload = selectionPayload();
  if (!payload) return;
  const blob = new Blob([`${JSON.stringify(payload, null, 2)}\n`], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = createElement("a") as HTMLAnchorElement;
  link.href = url;
  link.download = "upsp-conversation-showcase-selection.json";
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  selectionFeedback.textContent = "已导出选型 JSON；它尚未写入产品配置。";
}

function bindEvents(): void {
  playButton.addEventListener("click", startPlayback);
  pauseButton.addEventListener("click", pausePlayback);
  resetButton.addEventListener("click", resetPlayback);
  scenarioSelect.addEventListener("change", () => {
    if (!isScenarioId(scenarioSelect.value)) return;
    currentScenarioId = scenarioSelect.value;
    persistState();
    resetPlayback();
  });
  document.querySelectorAll<HTMLInputElement>('input[name="speed"]').forEach((input) => {
    input.addEventListener("change", () => {
      if (!input.checked) return;
      playbackSpeed = normalizeSpeed(input.value);
      persistState();
    });
  });
  document.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const select = target?.closest<HTMLButtonElement>("[data-select-component][data-select-variant]");
    if (select) {
      const component = select.dataset.selectComponent as ComponentKey;
      const variantId = select.dataset.selectVariant;
      if (COMPONENTS.includes(component) && validVariant(component, variantId)) {
        selections = { ...selections, [component]: variantId };
        selectionFeedback.textContent = `${componentLabels[component]}已选择。`;
        updateSelectionUi();
      }
      return;
    }
    const tab = target?.closest<HTMLButtonElement>("[data-showcase-tab]");
    if (tab?.dataset.showcaseTab) setActiveTab(tab.dataset.showcaseTab);
  });
  const tabList = document.querySelector<HTMLElement>('[role="tablist"]');
  tabList?.addEventListener("keydown", (event) => {
    if (!(["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key))) return;
    const tabs = [...tabList.querySelectorAll<HTMLButtonElement>("[data-showcase-tab]")];
    const current = tabs.findIndex((tab) => tab.dataset.showcaseTab === activeTab);
    let next = current;
    if (event.key === "ArrowLeft") next = (current - 1 + tabs.length) % tabs.length;
    if (event.key === "ArrowRight") next = (current + 1) % tabs.length;
    if (event.key === "Home") next = 0;
    if (event.key === "End") next = tabs.length - 1;
    event.preventDefault();
    setActiveTab(tabs[next].dataset.showcaseTab || "thinking", true);
  });
  clearSelectionButton.addEventListener("click", () => {
    selections = {};
    selectionFeedback.textContent = "已清空四组选择。";
    updateSelectionUi();
  });
  copySelectionButton.addEventListener("click", async () => {
    const payload = selectionPayload();
    if (!payload) return;
    const text = `${readableSelection()}\n\n${JSON.stringify(payload, null, 2)}`;
    selectionFeedback.textContent = await copyText(text) ? "已复制选型摘要。" : "复制失败，请使用导出 JSON。";
  });
  exportSelectionButton.addEventListener("click", exportSelection);
}

function initialize(): void {
  scenarioSelect.replaceChildren(...scenarios.map((scenario) => {
    const option = createElement("option", "", scenario.label) as HTMLOptionElement;
    option.value = scenario.id;
    return option;
  }));
  const stored = readStoredState();
  if (stored) {
    currentScenarioId = stored.scenario;
    playbackSpeed = stored.speed;
    selections = stored.selections;
  }
  scenarioSelect.value = currentScenarioId;
  document.querySelectorAll<HTMLInputElement>('input[name="speed"]').forEach((input) => {
    input.checked = Number(input.value) === playbackSpeed;
  });
  buildVariantGrids();
  bindEvents();
  setActiveTab("thinking");
  updateSelectionUi();
  resetPlayback();
}

initialize();
