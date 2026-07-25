import type {
  ConsolePage,
  AboutProjection,
  BootstrapProjection,
  DepositionProjection,
  Elements,
  PageId,
  PageTab,
  PersonaProjection,
  PollingState,
  ProtocolProjection,
  RuntimeProjection,
  SettingsProjection,
  Shortcut,
  TaskProjection,
  UiState,
} from "./contracts";
import { locale } from "./i18n";

export const consolePages: ConsolePage[] = [
  { id: "run", code: "RUN.console", title: "运行台", icon: "M4 5h16v12H4zM8 21h8M12 17v4", manual: "intro.md" },
  { id: "persona", code: "PERSONA.core", title: "位格主体", icon: "M12 3l7 4v6c0 4-3 7-7 8-4-1-7-4-7-8V7zM9 11h6M9 15h6", manual: "intro.md", nav: false },
  { id: "mem", code: "MEM.bus", title: "记忆总线", icon: "M5 6h14M5 12h14M5 18h14M8 4v16M16 4v16", manual: "memory-bus.md" },
  { id: "relations", code: "REL.domain", title: "关系域", icon: "M7 7a3 3 0 116 0 3 3 0 01-6 0zM4 19c1-4 5-6 8-6s7 2 8 6M17 8h3M18.5 6.5v3", manual: "intro.md" },
  { id: "containers", code: "WB.containers", title: "容器工作台", icon: "M4 6h16v4H4zM4 14h7v4H4zM13 14h7v4h-7z", manual: "work-containers.md" },
  { id: "context", code: "CONTEXT.review", title: "上下文审阅", icon: "M5 5h14v14H5zM8 8h8M8 12h8M8 16h5", manual: "context-layers.md" },
  { id: "audit", code: "PROTOCOL.center", title: "协议中心", icon: "M6 5h12M6 10h12M6 15h8M17 15l2 2-4 4", manual: "audit-tools.md" },
  { id: "settings", code: "PERSONA.settings", title: "位格设置", icon: "M12 8a4 4 0 100 8 4 4 0 000-8zM4 12h2M18 12h2M12 4v2M12 18v2M6.3 6.3l1.4 1.4M16.3 16.3l1.4 1.4M17.7 6.3l-1.4 1.4M7.7 16.3l-1.4 1.4", manual: "base-serial.md" },
];

export const pageTabs: Record<PageId, PageTab[]> = {
  run: [
    { id: "round", label: "当前轮", code: "ROUND" },
    { id: "tools", label: "任务与证据", code: "TASK" },
    { id: "receipts", label: "回执与结算", code: "RECEIPT" },
    { id: "risks", label: "风险警报", code: "RISK" },
  ],
  persona: [
    { id: "core", label: "核心档案", code: "CORE" },
    { id: "state", label: "生命状态", code: "STATE" },
  ],
  mem: [
    { id: "map", label: "记忆图谱", code: "MAP" },
    { id: "search", label: "记忆检索", code: "SEARCH" },
    { id: "stm", label: "STM", code: "STM" },
    { id: "ltm", label: "LTM", code: "LTM" },
    { id: "mounts", label: "挂接链", code: "MOUNT" },
  ],
  relations: [
    { id: "graph", label: "关系图谱", code: "GRAPH" },
  ],
  containers: [
    { id: "project", label: "项目容器", code: "PRJ" },
    { id: "dc", label: "辩证链", code: "DC" },
    { id: "ec", label: "事件链", code: "EC" },
    { id: "skl", label: "技能容器", code: "SKL" },
    { id: "candidates", label: "候选容器", code: "CAND" },
  ],
  context: [
    { id: "guide", label: "分层导览", code: "GUIDE" },
    { id: "content", label: "内容详情", code: "DETAIL" },
  ],
  audit: [
    { id: "ledger", label: "动态账本", code: "LEDGER" },
    { id: "rules", label: "规则", code: "RULES" },
    { id: "docs", label: "文档", code: "DOCS" },
  ],
  settings: [
    { id: "routing", label: "模型路由", code: "ROUTING" },
    { id: "context", label: "上下文与存储", code: "CONTEXT" },
    { id: "runtime", label: "运行设置", code: "RUNTIME" },
  ],
};

export const shortcuts: Shortcut[] = [
  { id: "stm", label: "STM", name: "短时记忆", target: "mem", tab: "stm", icon: "M5 7h14M7 4h10M6 12h12v7H6z" },
  { id: "ltm", label: "LTM", name: "长期记忆", target: "mem", tab: "ltm", icon: "M4 6h16v4H4zM6 10h12v10H6z" },
  { id: "dc", label: "DC", name: "辩证容器", target: "containers", tab: "dc", icon: "M5 7h6v6H5zM13 11h6v6h-6zM11 10l2 2M11 14l2-2" },
  { id: "ec", label: "EC", name: "事件容器", target: "containers", tab: "ec", icon: "M5 5h14M7 9h10M9 13h8M11 17h4" },
  { id: "prj", label: "PRJ", name: "项目容器", target: "containers", tab: "project", icon: "M4 6h16M6 10h12M5 15h6M13 15h6" },
  { id: "skl", label: "SKL", name: "技能容器", target: "containers", tab: "skl", icon: "M12 4l7 4v8l-7 4-7-4V8zM12 4v16M5 8l7 4 7-4" },
];

export const runtimePages = new Set<PageId>(["run", "persona", "mem", "relations", "containers", "context", "audit", "settings"]);
export const depositionPages = new Set<PageId>(["mem", "relations", "containers"]);
export const runtimeProjection: RuntimeProjection = {
  host: "connecting",
  status: null,
  live: null,
  round: null,
  error: "",
  sendFeedback: "",
  exportFeedback: "",
  sending: false,
  stopping: false,
  awaitingProjection: false,
  submitBaseline: null,
  unlimitedConfirmed: false,
  fullRefreshNeeded: true,
  renderKey: "",
  conversationRounds: new Map(),
  conversationRoundOrder: [],
  conversationHistoryInitialized: false,
  conversationHistoryLatest: null,
  conversationHistoryError: "",
  conversationHistoryVersion: 0,
};

export const depositionProjection: DepositionProjection = {
  index: null,
  details: { memory: {}, container: {}, relation: {} },
  pendingDetails: new Set(),
  detailErrors: {},
  focusMutation: {
    pending: false,
    feedback: "",
    receipt: null,
  },
  loading: true,
  error: "",
  renderKey: "",
};

export const taskProjection: TaskProjection = {
  data: null,
  loading: true,
  error: "",
  relayPending: false,
  relayFeedback: "",
  renderKey: "",
};

export const protocolProjection: ProtocolProjection = {
  catalog: null,
  loading: true,
  error: "",
  renderKey: "",
};

export const personaProjection: PersonaProjection = {
  core: null,
  state: null,
  coreLoading: false,
  stateLoading: false,
  coreError: "",
  stateError: "",
  stateStale: false,
  coreRenderKey: "",
  stateRenderKey: "",
};

export const settingsProjection: SettingsProjection = {
  data: null,
  loading: true,
  pending: false,
  error: "",
  feedback: "",
  renderKey: "",
};

export const aboutProjection: AboutProjection = {
  data: null,
  loading: true,
  error: "",
};

export const bootstrapProjection: BootstrapProjection = {
  data: null,
  loading: true,
  pending: false,
  error: "",
  feedback: "",
  renderKey: "",
  selection: "choice",
  preview: false,
  testToken: "",
  skipModelSetup: false,
  draft: {
    name_zh: "",
    name_en: "",
    abbreviation: "",
    roles: ["", "", ""],
    axes: { S: 50, C: 50, V: 50, A: 50, R: 50, B: 50 },
    self_description: "",
    traits: ["", "", ""],
    instance_notes: "",
  },
};

export const polling: PollingState = {
  about: null,
  bootstrap: null,
  runtime: null,
  runtimeForceQueued: false,
  task: null,
  taskForceQueued: false,
  deposition: null,
  depositionForceQueued: false,
  settings: null,
  settingsForceQueued: false,
  personaCore: null,
  personaState: null,
};

export const state: UiState = {
  locale: locale(),
  activePage: "run",
  activeTabs: Object.fromEntries(Object.entries(pageTabs).map(([pageId, tabs]) => [pageId, tabs[0]?.id || ""])) as Record<PageId, string>,
  manualFile: "intro.md",
  overviewCollapsed: true,
  overviewSectionsCollapsed: new Set(),
  conversationDisclosure: new Map(),
  navCollapseLocked: localStorage.getItem("upsp.v4.navCollapseLocked") === "1",
  systemWindowOpen: false,
  globalSettingsOpen: false,
  globalSettingsTab: "models",
  editingConnectionId: null,
  editingModelId: null,
  selectedMemoryId: "",
  selectedContainerId: "",
  selectedRelationId: "",
  memoryQuery: "",
  activeRuntimePane: "00_call_header",
  selectedContextRound: null,
  selectedContextFrame: null,
  selectedLedgerRound: null,
};

function requiredElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`seed_gui_missing_dom:${selector}`);
  return element;
}

export const els: Elements = {
  bootstrapRoot: requiredElement<HTMLElement>("#bootstrapRoot"),
  app: requiredElement<HTMLElement>("#appShell"),
  leftRail: requiredElement<HTMLElement>(".left-rail"),
  personaNameSelector: requiredElement<HTMLDetailsElement>("#personaNameSelector"),
  personaNameSummary: requiredElement<HTMLElement>("#personaNameSummary"),
  personaNameValue: requiredElement<HTMLElement>("#personaNameValue"),
  personaNameOptions: requiredElement<HTMLElement>("#personaNameOptions"),
  statusReadouts: requiredElement<HTMLElement>("#statusReadouts"),
  productVersionName: requiredElement<HTMLElement>("#productVersionName"),
  productVersionNumber: requiredElement<HTMLElement>("#productVersionNumber"),
  surfaceNav: requiredElement<HTMLElement>("#surfaceNav"),
  navLockToggle: requiredElement<HTMLButtonElement>("#navLockToggle"),
  pageCode: requiredElement<HTMLElement>("#pageCode"),
  pageTitle: requiredElement<HTMLElement>("#pageTitle"),
  stagePage: requiredElement<HTMLElement>("#stagePage"),
  overviewContent: requiredElement<HTMLElement>("#overviewContent"),
  overviewPane: requiredElement<HTMLElement>("#overviewPane"),
  chatThread: requiredElement<HTMLElement>("#chatThread"),
  overviewToggle: requiredElement<HTMLButtonElement>("#overviewToggle"),
  globalSettingsToggle: requiredElement<HTMLButtonElement>("#globalSettingsToggle"),
  globalSettingsOverlay: requiredElement<HTMLElement>("#globalSettingsOverlay"),
  globalSettingsContent: requiredElement<HTMLElement>("#globalSettingsContent"),
  manualOverlay: requiredElement<HTMLElement>("#manualOverlay"),
  manualTitle: requiredElement<HTMLElement>("#manualTitle"),
  manualSummary: requiredElement<HTMLElement>("#manualSummary"),
  manualPageLabel: requiredElement<HTMLElement>("#manualPageLabel"),
  manualSources: requiredElement<HTMLElement>("#manualSources"),
  manualBody: requiredElement<HTMLElement>("#manualBody"),
  runtimeState: requiredElement<HTMLElement>("#runtimeState"),
  commsSource: requiredElement<HTMLElement>("#commsSource"),
  runtimeComposer: requiredElement<HTMLFormElement>("#runtimeComposer"),
  messageInput: requiredElement<HTMLTextAreaElement>("#messageInput"),
  permissionLevel: requiredElement<HTMLSelectElement>("#permissionLevel"),
  sendButton: requiredElement<HTMLButtonElement>("#sendButton"),
  stopButton: requiredElement<HTMLButtonElement>("#stopButton"),
  configureModelButton: requiredElement<HTMLButtonElement>("#configureModelButton"),
  sendFeedback: requiredElement<HTMLElement>("#sendFeedback"),
  ledgerRound: requiredElement<HTMLElement>("#ledgerRound"),
  ledgerContext: requiredElement<HTMLElement>("#ledgerContext"),
  ledgerFrame: requiredElement<HTMLElement>("#ledgerFrame"),
  ledgerSettlement: requiredElement<HTMLElement>("#ledgerSettlement"),
};
