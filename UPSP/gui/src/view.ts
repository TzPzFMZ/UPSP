import type {
  CallFrame,
  ConversationCard,
  LedgerItem,
  ContextPane,
  DepositionDetailItem,
  DepositionItem,
  DepositionKind,
  FocusReturnDescriptor,
  GlobalSettingsTab,
  BootstrapIdentity,
  ModelConnection,
  ModelProfile,
  ModelRouteSlot,
  PageId,
  PersonaNameVariant,
  PersonaStateField,
  ProviderErrorKind,
  ProtocolCatalogEntry,
  ProtocolDocumentPayload,
  RequestPrefixDiffPayload,
  RequestPrefixDiffTarget,
  RelayRuntimeState,
  SettingValue,
  SettingsFileId,
  SettingsPayload,
  SystemReturnFocus,
  TaskRecord,
  UiState,
} from "./contracts";
import {
  aboutProjection,
  bootstrapProjection,
  consolePages,
  depositionPages,
  depositionProjection,
  els,
  pageTabs,
  personaProjection,
  personaCatalogProjection,
  protocolProjection,
  runtimePages,
  runtimeProjection,
  settingsProjection,
  shortcuts,
  state,
  taskProjection,
} from "./state";
import { applySystemWindowSplit, deferSystemWindowRender } from "./system-window-split";
import {
  hydrateLedgerJsonTables,
  hydrateMarkdownDocuments,
  memoryBodyMarkdown,
  renderMarkdownDocument,
  renderStructuredJson,
} from "./markdown";
import { applyStaticTranslations, runtimeTerm, setLocale, t } from "./i18n";
import type { Locale, MessageKey } from "./i18n";
import { renderConversationTimeline } from "./conversation-timeline";

type LedgerRow = readonly [unknown, unknown, unknown, string];
type OverviewRow = readonly [unknown, unknown, unknown, PageId, string?, string?];
let navCollapseTimer = 0;
let manualReturnFocus: HTMLElement | null = null;
let systemCloseTimer = 0;
let systemReturnFocus: SystemReturnFocus = null;
let globalSettingsReturnFocus: HTMLElement | null = null;

function personaAbbreviation(): string {
  return bootstrapProjection.data?.identity?.abbreviation || "UPSP";
}

const personaNameVariants: PersonaNameVariant[] = ["name_zh", "name_en", "abbreviation"];
const personaNameStoragePrefix = "upsp.seed_gui.persona_name_variant.v1:";

function selectedPersonaNameVariant(
  pid: string,
  identity: Partial<BootstrapIdentity> | null | undefined,
): PersonaNameVariant {
  if (!pid) return "abbreviation";
  const stored = localStorage.getItem(`${personaNameStoragePrefix}${pid}`);
  if (
    personaNameVariants.includes(stored as PersonaNameVariant)
    && identity?.[stored as PersonaNameVariant]
  ) return stored as PersonaNameVariant;
  if (stored) localStorage.removeItem(`${personaNameStoragePrefix}${pid}`);
  return "abbreviation";
}

function personaDisplayName(pid: string, identity: Partial<BootstrapIdentity>): string {
  const variant = selectedPersonaNameVariant(pid, identity);
  return identity[variant]
    || identity.display_name
    || identity.name_zh
    || identity.name_en
    || identity.abbreviation
    || pid;
}

function personaFullNameTooltip(pid: string, identity: Partial<BootstrapIdentity>): string {
  const names = [identity.name_zh, identity.name_en, identity.abbreviation]
    .filter((value, index, values): value is string => Boolean(value) && values.indexOf(value) === index);
  return `${names.join(" / ") || pid} · ${pid}`;
}

export function selectPersonaNameVariant(variant: string): boolean {
  const catalog = personaCatalogProjection.data;
  const active = catalog?.personas.find((item) => item.pid === catalog.active.pid);
  const identity = active?.identity || bootstrapProjection.data?.identity;
  if (
    !catalog?.active.pid
    || !personaNameVariants.includes(variant as PersonaNameVariant)
    || !identity?.[variant as PersonaNameVariant]
  ) return false;
  localStorage.setItem(`${personaNameStoragePrefix}${catalog.active.pid}`, variant);
  renderIdentity();
  return true;
}

function identityMutationBusy(): boolean {
  if (personaCatalogProjection.pending) return true;
  const status = runtimeProjection.status;
  if (!status) return false;
  const stage = String(status.stage || "");
  return status.current_round != null
    || status.send_in_flight === true
    || status.relay_in_flight === true
    || status.mutation_in_flight === true
    || status.pending_tool_approval != null
    || status.restart_requested === true
    || (stage !== "" && stage !== "idle");
}

const identityHtmlCache = new WeakMap<HTMLElement, string>();

function renderIdentityHtml(element: HTMLElement, html: string): boolean {
  if (identityHtmlCache.get(element) === html) return false;
  identityHtmlCache.set(element, html);
  element.innerHTML = html;
  return true;
}

const personaGroupLabels: Record<string, MessageKey> = {
  meta: "元数据",
  core_axes: "核心轴",
  dynamic_axes: "动态轴",
  comfort_zone: "舒适区",
  core_speed_wheel: "核心速度轮",
  workhood_index: "工化指数",
  activity_mode: "活动模式",
  fatigue: "疲劳",
  token_usage: "令牌用量",
  identity: "身份状态",
  sleep_state: "睡眠状态",
  runtime: "运行状态",
  focus: "专注度",
  heartbeat_flags: "心跳旗标",
  alert_deferrals: "警报搁置",
  feeling_buffer: "感受缓冲",
  context_cache: "上下文缓存",
};

const personaFieldLabels: Record<string, MessageKey> = {
  total_round: "总轮次",
  daily_round: "当日轮次",
  last_rhythm_round: "上次节律轮次",
  last_heartbeat_at: "上次心跳时间",
  last_standby_round: "上次待命轮次",
  last_round_closed_at: "上次轮次闭合时间",
  last_external_input_at: "上次外部输入时间",
  last_update: "上次更新时间",
  version: "版本",
  last_calendar_check_at: "上次日历检查时间",
  next_settle_at: "下次结算时间",
  last_state_settlement_id: "上次状态结算标识",
  shelve_timer_at: "搁置计时开始时间",
  last_error: "上次错误",
  S: "S 轴",
  C: "C 轴",
  V: "V 轴",
  A: "A 轴",
  R: "R 轴",
  B: "B 轴",
  valence: "效价",
  arousal: "唤醒度",
  focus: "聚焦",
  mood: "心境",
  humor: "幽默",
  safety: "安全",
  value: "数值",
  current: "当前值",
  max: "上限",
  self_reference: "自我指涉",
  self_reflection: "自我反思",
  autonomy: "自主性",
  awake_since: "唤醒时间",
  current_tokens: "当前令牌",
  window_size: "窗口大小",
  usage_ratio: "用量比例",
  last_round_input: "上轮输入",
  last_round_output: "上轮输出",
  confirmed: "确认状态",
  confirmed_at: "确认时间",
  timeout_seconds: "身份超时",
  local_default_relation_id: "本地默认关系",
  current_relation_id: "当前关系",
  current_declared_name: "当前声明名",
  current_source: "当前来源",
  level: "睡眠级别",
  entered_at: "进入时间",
  phase: "运行阶段",
  standby_countdown: "待命倒计时",
  pending_relay_target: "待处理中继目标",
  relay_intents: "中继意图",
  relay_intent_seq: "中继意图序号",
  work_intent_debt: "工作意图债务",
  feeling_settle_due: "感受待结算",
  api_degraded: "API 降级",
  memory_compression_due: "记忆待压缩",
  user_message_waiting: "用户消息等待",
  rhythm_due: "节律到期",
  standby_due: "待命到期",
  continue_requested: "继续请求",
  shelve_timer_expired: "搁置计时到期",
  token_usage_warning: "令牌用量警告",
  context_pressure: "上下文压力",
  cache_compaction_due: "缓存压缩到期",
  calendar_day_due: "日节律到期",
  calendar_week_due: "周节律到期",
  calendar_month_due: "月节律到期",
  calendar_quarter_due: "季节律到期",
  calendar_year_due: "年节律到期",
  permanent_expired: "永固层过期",
  periodic_expired: "定期层过期",
  popup_active: "弹窗活动",
};

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function icon(path: string): string {
  return `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="${escapeHtml(path)}"/></svg>`;
}

export function setPage(pageId: PageId, tabId = ""): void {
  clearSystemCloseTimer();
  const page = consolePages.find((item) => item.id === pageId) || consolePages[0];
  state.activePage = page.id;
  state.systemWindowOpen = true;
  if (tabId) setActivePageTab(page.id, tabId);
  else setActivePageTab(page.id, getActivePageTab(page.id));
  state.manualFile = page.manual;
  renderPageChrome();
}

export function getActivePageTab(pageId: PageId = state.activePage): string {
  const tabs = pageTabs[pageId] || [];
  const active = state.activeTabs[pageId];
  return tabs.some((tab) => tab.id === active) ? active : tabs[0]?.id || "";
}

export function setActivePageTab(pageId: PageId, tabId: string): void {
  const tabs = pageTabs[pageId] || [];
  if (!tabs.length) return;
  state.activeTabs[pageId] = tabs.some((tab) => tab.id === tabId) ? tabId : tabs[0].id;
}

export function render(): void {
  document.documentElement.lang = state.locale;
  document.title = "UPSP";
  applyStaticTranslations();
  syncShellState();
  renderIdentity();
  renderNavigation();
  renderOverview();
  renderChat();
  renderSourceState();
  renderCurrentPage();
  renderGlobalSettings();
}

export function changeLocale(next: Locale): void {
  if (next === state.locale) return;
  setLocale(next);
  state.locale = next;
  render();
  document.dispatchEvent(new CustomEvent("upsp:locale-changed"));
  if (!els.manualOverlay.hidden && state.manualFile) void openManual(state.manualFile, { retry: true });
}

export function syncShellState(): void {
  els.app.classList.toggle("system-open", state.systemWindowOpen);
  applySystemWindowSplit();
  const page = consolePages.find((item) => item.id === state.activePage) || consolePages[0];
  els.app.classList.toggle("nav-locked", state.navCollapseLocked);
  if (state.navCollapseLocked) collapseNavNow();
  els.app.classList.toggle("overview-collapsed", state.overviewCollapsed);
  els.overviewToggle.setAttribute("aria-label", t(state.overviewCollapsed ? "展开概览" : "收起概览"));
  els.overviewToggle.setAttribute("aria-expanded", state.overviewCollapsed ? "false" : "true");
  els.overviewPane.setAttribute("aria-hidden", state.overviewCollapsed ? "true" : "false");
  els.overviewPane.toggleAttribute("inert", state.overviewCollapsed);
  els.navLockToggle.setAttribute("aria-pressed", state.navCollapseLocked ? "true" : "false");
  els.globalSettingsToggle.setAttribute("aria-expanded", String(state.globalSettingsOpen));
  els.navLockToggle.setAttribute("aria-label", t(state.navCollapseLocked ? "解锁左侧导航悬停展开" : "锁定左侧导航为收起态"));
  els.navLockToggle.title = t(state.navCollapseLocked ? "解锁悬停展开" : "锁定收起态");
  els.pageCode.textContent = state.locale === "zh-CN" ? "" : page.code;
  els.pageTitle.textContent = t(page.title);
}

export function openGlobalSettings(tab: GlobalSettingsTab = state.globalSettingsTab): void {
  globalSettingsReturnFocus = document.activeElement instanceof HTMLElement
    ? document.activeElement
    : els.globalSettingsToggle;
  state.globalSettingsTab = tab;
  state.globalSettingsOpen = true;
  renderGlobalSettings();
  window.requestAnimationFrame(() => {
    els.globalSettingsOverlay.querySelector<HTMLElement>(`[data-global-settings-tab="${tab}"]`)?.focus();
  });
}

export function closeGlobalSettings(): void {
  if (!state.globalSettingsOpen) return;
  state.globalSettingsOpen = false;
  state.editingConnectionId = null;
  state.editingModelId = null;
  renderGlobalSettings();
  (globalSettingsReturnFocus?.isConnected ? globalSettingsReturnFocus : els.globalSettingsToggle).focus();
}

function renderPageChrome(): void {
  syncShellState();
  renderNavigation();
  renderSourceState();
  renderCurrentPage();
}

function renderCurrentPage(): void {
  renderStage(state.activePage);
}

export function renderSourceState(): void {
  const live = runtimeProjection.live;
  const lifecycle = live?.round_lifecycle || {};
  const connected = runtimeProjection.host === "connected";
  els.runtimeState.classList.toggle("connected", connected);
  els.runtimeState.classList.toggle("failed", runtimeProjection.host === "error");
  els.runtimeState.querySelector("span")!.textContent = connected ? t("本地宿主已连接") : runtimeProjection.host === "connecting" ? t("宿主连接中") : t("本地宿主不可用");
  els.commsSource.textContent = runtimeProjection.round == null ? t("无轮次") : `${t("当前轮")} ${runtimeProjection.round}`;
  els.ledgerRound.textContent = runtimeProjection.round == null ? t("无轮次") : `R${String(runtimeProjection.round).padStart(6, "0")} / ${runtimeTerm(lifecycle.state, "running")}`;
  els.ledgerContext.textContent = `${live?.frame_catalog?.length ? 10 : 0} / 10`;
  els.ledgerFrame.textContent = live?.latest_frame_id || t("尚无帧次");
  els.ledgerSettlement.textContent = runtimeTerm(lifecycle.settlement_status || lifecycle.state, "unsettled");
  renderComposerState();
}

export function renderIdentity(): void {
  const product = aboutProjection.data?.product;
  els.productVersionName.textContent = product
    ? product.channel === "alpha" ? "Alpha" : product.channel
    : t("版本信息不可用");
  els.productVersionNumber.textContent = product?.version || "—";

  const catalog = personaCatalogProjection.data;
  const busy = identityMutationBusy();
  const activePid = catalog?.active.pid || "";
  const activePersona = catalog?.personas.find((item) => item.pid === activePid);
  const identity = activePersona?.identity || bootstrapProjection.data?.identity || {};
  const selectedVariant = selectedPersonaNameVariant(catalog?.active.pid || "", identity);
  const personaTabsChanged = renderIdentityHtml(els.personaTabs, (catalog?.personas || []).map((item) => {
    const active = item.pid === activePid;
    const name = personaDisplayName(item.pid, item.identity);
    const title = personaFullNameTooltip(item.pid, item.identity);
    return `<button class="identity-tab persona-tab ${active ? "active" : ""}" type="button"
      data-identity-tab data-tab-group="persona" data-activate-persona="${escapeHtml(item.pid)}"
      role="tab" aria-selected="${active ? "true" : "false"}" tabindex="${active ? "0" : "-1"}"
      aria-current="${active ? "page" : "false"}" title="${escapeHtml(title)}" ${busy && !active ? "disabled" : ""}>
      <span>${escapeHtml(name)}</span>
    </button>`;
  }).join(""));
  if (personaTabsChanged) window.requestAnimationFrame(() => {
    els.personaTabs.querySelector<HTMLElement>(".identity-tab.active")?.scrollIntoView({ block: "nearest", inline: "nearest" });
  });
  renderIdentityHtml(els.personaNameOptions, personaNameVariants.map((variant) => {
    const value = identity?.[variant] || "";
    const label = variant === "name_zh" ? t("中文名") : variant === "name_en" ? t("英文名") : t("缩写");
    return `
      <button class="identity-menu-option ${variant === selectedVariant ? "active" : ""}" type="button"
              data-persona-name-variant="${variant}" aria-pressed="${variant === selectedVariant ? "true" : "false"}" ${value ? "" : "disabled"}>
        <b>${escapeHtml(label)}</b>
        <span>${escapeHtml(value || t("未填写"))}</span>
      </button>
    `;
  }).join(""));
  els.createPersonaButton.disabled = busy;

  const activeInstance = activePersona?.instances.find(
    (item) => item.instance_id === catalog?.active.instance_id && !item.archived,
  );
  els.identityFeedback.textContent = personaCatalogProjection.pending
    ? t("正在处理")
    : personaCatalogProjection.error;
  els.identityFeedback.hidden = !els.identityFeedback.textContent;
  const liveInstances = (activePersona?.instances || []).filter((item) => !item.archived);
  const archivedInstances = (activePersona?.instances || []).filter((item) => item.archived);
  const instanceTabsChanged = renderIdentityHtml(els.instanceTabs, liveInstances.map((item) => {
    const active = item.instance_id === catalog?.active.instance_id;
    const label = item.label || item.instance_id;
    const source = item.kind === "meta" ? "meta" : `${item.source_instance_id || "meta"}/R${item.source_round}`;
    const title = `${label} · ${item.instance_id} · ${t("来源")} ${source}`;
    const slot = busy
      ? active ? `<span class="identity-tab-spinner" role="status" aria-label="${t("正在处理")}"></span>` : `<span class="identity-tab-slot" aria-hidden="true"></span>`
      : `<button class="instance-fork" type="button" data-fork-instance="${escapeHtml(item.instance_id)}"
          aria-label="${escapeHtml(t("从此分身创建分支"))}" title="${escapeHtml(t("从此分身创建分支"))}">
          <svg viewBox="0 0 20 20" aria-hidden="true"><circle cx="5" cy="4" r="1.5"/><circle cx="15" cy="6" r="1.5"/><circle cx="5" cy="16" r="1.5"/><path d="M5 5.5v9M6.5 12c5 0 7-1.8 7-4.5"/></svg>
        </button>`;
    return `<span class="identity-tab-shell instance-tab ${active ? "active" : ""}">
      <button class="identity-tab instance-tab-select" type="button" data-identity-tab data-tab-group="instance"
        data-activate-instance="${escapeHtml(item.instance_id)}" data-persona-id="${escapeHtml(activePersona?.pid || "")}"
        role="tab" aria-selected="${active ? "true" : "false"}" tabindex="${active ? "0" : "-1"}"
        aria-current="${active ? "page" : "false"}" title="${escapeHtml(title)}" ${busy && !active ? "disabled" : ""}>
        <span>${escapeHtml(label)}</span>
      </button>${slot}
    </span>`;
  }).join(""));
  if (instanceTabsChanged) window.requestAnimationFrame(() => {
    els.instanceTabs.querySelector<HTMLElement>(".identity-tab-shell.active")?.scrollIntoView({ block: "nearest", inline: "nearest" });
  });
  const instanceActions = [
    activeInstance?.kind === "branch"
      ? `<button class="identity-menu-option" type="button" data-archive-instance="${escapeHtml(activeInstance.instance_id)}" ${busy ? "disabled" : ""}><span>${t("归档分身")}</span></button>`
      : "",
    ...archivedInstances.map((item) => `<button class="identity-menu-option" type="button" data-restore-instance="${escapeHtml(item.instance_id)}" ${busy ? "disabled" : ""}><b>${t("恢复分身")}</b><span>${escapeHtml(item.label || item.instance_id)}</span></button>`),
  ].filter(Boolean);
  renderIdentityHtml(els.instanceOptions, instanceActions.length
    ? instanceActions.join("")
    : `<button class="identity-menu-option" type="button" disabled><span>${t("没有可用操作")}</span></button>`);
  els.createInstanceButton.disabled = busy;

  const live = runtimeProjection.live;
  const statusbar = live?.statusbar_projection || {};
  const lifecycle = live?.round_lifecycle || {};
  const roundType = statusbar.round?.type
    || runtimeProjection.status?.round_type
    || runtimeProjection.status?.cli?.data?.round_type;
  const statuses = live && runtimeProjection.round != null ? [
    { label: t("模式"), value: runtimeTerm(statusbar.mode || t("未投影")) },
    { label: t("轮型"), value: runtimeTerm(roundType || t("未投影")) },
    { label: t("当前轮"), value: `${statusbar.round?.id || `R${String(runtimeProjection.round).padStart(6, "0")}`} / ${runtimeTerm(lifecycle.state, "running")}` },
    { label: t("帧次"), value: live.latest_frame_id || t("尚无帧次") },
  ] : [
    { label: t("模式"), value: t("未投影") },
    { label: t("轮型"), value: runtimeTerm(roundType || t("未投影")) },
    { label: t("当前轮"), value: t("无轮次") },
    { label: t("帧次"), value: t("尚无帧次") },
  ];
  els.statusReadouts.innerHTML = statuses.map((status, index) => `
    <span class="status-tab status-readout ${index === 0 ? "active" : ""}">
      <strong>${escapeHtml(status.label)}</strong>
      <small>${escapeHtml(status.value)}</small>
    </span>
  `).join("");
}

export function renderNavigation(): void {
  const pages = consolePages.filter((page) => page.nav !== false).map((page) => navButton({
    active: state.systemWindowOpen && page.id === state.activePage,
    code: page.code,
    iconPath: page.icon,
    pageId: page.id,
    title: t(page.title).replace("Base ", ""),
  })).join("");

  const quicks = shortcuts.map((item) => navButton({
    active: state.systemWindowOpen && item.target === state.activePage && item.tab === getActivePageTab(item.target),
    code: item.label,
    iconPath: item.icon,
    pageId: item.target,
    tabId: item.tab,
    title: t(item.name),
    quick: true,
  })).join("");

  els.surfaceNav.innerHTML = `
    <div class="nav-primary">${pages}</div>
    <div class="nav-divider"><span>${t("工作容器")}</span></div>
    <div class="quick-grid">${quicks}</div>
  `;
  syncNavPointer();
}

export function syncNavPointer(): void {
  const activeButton = els.surfaceNav.querySelector(".nav-button.active");
  els.leftRail.classList.toggle("nav-has-active", Boolean(activeButton));
  if (!activeButton) {
    els.leftRail.style.removeProperty("--nav-pointer-top");
    return;
  }

  const railRect = els.leftRail.getBoundingClientRect();
  const buttonRect = activeButton.getBoundingClientRect();
  const pointerTop = buttonRect.top - railRect.top + (buttonRect.height / 2);
  els.leftRail.style.setProperty("--nav-pointer-top", `${pointerTop}px`);
}

function navButton({
  active,
  code,
  iconPath,
  pageId,
  tabId = "",
  title,
  quick = false,
}: {
  active: boolean;
  code: string;
  iconPath: string;
  pageId: PageId;
  tabId?: string;
  title: string;
  quick?: boolean;
}): string {
  return `
    <button class="nav-button ${quick ? "quick-button" : ""} ${active ? "active" : ""}" data-page="${pageId}" ${tabId ? `data-tab="${escapeHtml(tabId)}"` : ""} title="${escapeHtml(title)}" ${active ? 'aria-current="page"' : ""}>
      ${icon(iconPath)}
      <span class="nav-copy">
        <b>${escapeHtml(title)}</b>
        <small>${escapeHtml(code)}</small>
      </span>
      <em>${escapeHtml(code.split(".")[0])}</em>
    </button>
  `;
}

export function renderOverview(): void {
  const live = runtimeProjection.live;
  const frames = live?.frame_catalog || [];
  const lifecycle = live?.round_lifecycle || {};
  const sections: Array<{ id: string; title: string; rows: OverviewRow[] }> = runtimeProjection.round == null ? [{
    id: "round",
    title: t("运行概览"),
    rows: [[t("尚无轮次"), runtimeProjection.host === "connected" ? t("宿主已连接，等待真实事件") : t("本地宿主不可用"), "", "run", "", "round"]],
  }] : [
    {
      id: "round",
      title: `${t("当前轮")} ${runtimeProjection.round}`,
      rows: [[runtimeTerm(lifecycle.state, "running"), `${t("结算")}：${runtimeTerm(lifecycle.settlement_status, "unsettled")}`, String(live?.last_event_index || 0), "run", lifecycle.state === "unsettled" ? "warn" : "", "round"]],
    },
    {
      id: "frames",
      title: t("选择帧次"),
      rows: frames.slice(-6).reverse().map((frame) => [frame.label || frame.frame_id, `${runtimeTerm(frame.phase, "unknown")} / ${state.locale === "zh-CN" ? "事件" : "events"} ${frame.event_start_index}-${frame.event_end_index}`, runtimeTerm(frame.call_channel || ""), "context", "", "content"]),
    },
  ];

  els.overviewContent.innerHTML = sections.map((section) => {
    const collapsed = state.overviewSectionsCollapsed.has(section.id);
    return `
    <section class="overview-section">
      <button class="overview-section-toggle" type="button" data-overview-section="${escapeHtml(section.id)}" aria-expanded="${collapsed ? "false" : "true"}" aria-controls="overview-section-${escapeHtml(section.id)}">
        <span>${escapeHtml(section.title)}</span>
        ${icon("M9 6l6 6-6 6")}
      </button>
      <div class="overview-section-body" id="overview-section-${escapeHtml(section.id)}" ${collapsed ? "hidden" : ""}>
        ${section.rows.map((row) => objectRow(row)).join("")}
      </div>
    </section>
  `;
  }).join("");
}

function objectRow([title, desc, status, page, tone, tab]: OverviewRow): string {
  return `
    <button class="object-row ${tone === "warn" ? "warn" : ""}" data-page="${page}" ${tab ? `data-tab="${escapeHtml(tab)}"` : ""}>
      <i class="status-dot ${tone === "warn" ? "warn" : ""}"></i>
      <span><b>${escapeHtml(title)}</b><span>${escapeHtml(desc)}</span></span>
      <em>${escapeHtml(status)}</em>
    </button>
  `;
}

function fullLocalTime(value: unknown): { source: string; label: string; title: string } | null {
  const source = String(value || "").trim();
  const date = new Date(source);
  if (!source || Number.isNaN(date.getTime())) return null;
  const pad = (part: number): string => String(part).padStart(2, "0");
  const label = `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
  return { source, label, title: `${label} · ${source}` };
}

function renderFullTime(value: unknown, className = ""): string {
  const time = fullLocalTime(value);
  return time ? `<time class="${escapeHtml(className)}" datetime="${escapeHtml(time.source)}" title="${escapeHtml(time.title)}">${escapeHtml(time.label)}</time>` : "";
}

export function renderChat(): void {
  renderConversationTimeline();
}

interface StageScrollSnapshot {
  top: number;
  stickToBottom: boolean;
}

function captureStageScroll(): Map<string, StageScrollSnapshot> {
  const snapshots = new Map<string, StageScrollSnapshot>();
  els.stagePage.querySelectorAll<HTMLElement>("[data-stage-scroll-key]").forEach((element) => {
    const key = element.dataset.stageScrollKey;
    if (!key) return;
    const maxTop = Math.max(0, element.scrollHeight - element.clientHeight);
    snapshots.set(key, {
      top: element.scrollTop,
      stickToBottom: maxTop > 0 && maxTop - element.scrollTop <= 24,
    });
  });
  return snapshots;
}

function restoreStageScroll(snapshots: Map<string, StageScrollSnapshot>): void {
  els.stagePage.querySelectorAll<HTMLElement>("[data-stage-scroll-key]").forEach((element) => {
    const snapshot = snapshots.get(element.dataset.stageScrollKey || "");
    if (!snapshot) return;
    const maxTop = Math.max(0, element.scrollHeight - element.clientHeight);
    element.scrollTop = snapshot.stickToBottom
      ? maxTop
      : Math.max(0, Math.min(snapshot.top, maxTop));
  });
}

export function renderStage(pageId: PageId): void {
  if (deferSystemWindowRender(() => renderStage(pageId))) return;
  const renderers: Record<PageId, () => string> = {
    run: renderRunPage,
    persona: renderPersonaPage,
    context: renderContextPage,
    mem: renderMemoryPage,
    relations: renderRelationsPage,
    containers: renderContainersPage,
    audit: renderAuditPage,
    settings: renderSettingsPage,
  };
  const page = consolePages.find((item) => item.id === pageId) || consolePages[0];
  const activeTab = getActivePageTab(page.id);
  const activeTabId = `page-tab-${page.id}-${activeTab}`;
  const scrollSnapshots = captureStageScroll();
  const priorPersonaView = els.stagePage.querySelector<HTMLElement>("[data-persona-view]")?.dataset.personaView || "";
  const priorPersonaAllOpen = Boolean(els.stagePage.querySelector("details.persona-state-all[open]"));
  const openPersonaGroups = new Set(
    [...els.stagePage.querySelectorAll<HTMLDetailsElement>("details[data-persona-state-group][open]")]
      .map((details) => details.dataset.personaStateGroup || "")
      .filter(Boolean),
  );
  els.app.classList.toggle("system-open", state.systemWindowOpen);
  if (!state.systemWindowOpen) {
    els.stagePage.innerHTML = "";
    return;
  }
  els.stagePage.innerHTML = `
    <article class="system-window cut-panel" data-active-page="${escapeHtml(page.id)}" aria-labelledby="systemWindowTitle">
      <header class="system-window-head">
        <div>
          <span class="hud-label">${state.locale === "zh-CN" ? "" : escapeHtml(page.code)}</span>
          <h2 id="systemWindowTitle">${escapeHtml(t(page.title))}</h2>
        </div>
        <div class="system-window-actions" aria-label="${t("窗口控制")}">
          <button class="window-help-button" data-manual="${escapeHtml(page.manual)}" title="${t("打开当前页说明")}" aria-label="${t("打开当前页说明")}">
            <span>?</span>
          </button>
          <button class="window-close-button" data-close-system-window title="${t("关闭当前系统窗")}" aria-label="${t("关闭当前系统窗")}">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>
          </button>
        </div>
      </header>
      <div class="system-window-body" id="systemWindowPanel" role="tabpanel" aria-labelledby="${escapeHtml(activeTabId)}" tabindex="0" data-stage-scroll-key="${escapeHtml(`body:${page.id}:${activeTab || "default"}`)}" ${page.id === "context" ? `aria-busy="${runtimeProjection.host === "connecting" ? "true" : "false"}"` : ""}>
        ${runtimePages.has(page.id) ? "" : `<p class="static-design-notice">${t("静态设计页｜尚未接入运行时")}</p>`}
        ${(renderers[pageId] || renderRunPage)()}
      </div>
      ${renderPageTabs(pageId)}
    </article>
  `;
  const contextScroll = els.stagePage.querySelector<HTMLElement>(".runtime-context-workspace article");
  if (contextScroll) {
    hydrateMarkdownDocuments(contextScroll, contextScroll);
    if (contextInstrumentPaneIds.includes(state.activeRuntimePane)) hydrateLedgerJsonTables(contextScroll);
  }
  if (pageId === "persona") {
    const personaBody = els.stagePage.querySelector<HTMLElement>(".system-window-body");
    if (personaBody) hydrateMarkdownDocuments(personaBody, personaBody);
    const nextPersonaView = els.stagePage.querySelector<HTMLElement>("[data-persona-view]")?.dataset.personaView || "";
    if (priorPersonaView && priorPersonaView === nextPersonaView && personaBody) {
      const personaAll = personaBody.querySelector<HTMLDetailsElement>("details.persona-state-all");
      if (personaAll && priorPersonaAllOpen) personaAll.open = true;
      openPersonaGroups.forEach((group) => {
        const details = personaBody.querySelector<HTMLDetailsElement>(`details[data-persona-state-group="${CSS.escape(group)}"]`);
        if (details) details.open = true;
      });
    }
  }
  restoreStageScroll(scrollSnapshots);
  const contextNav = els.stagePage.querySelector<HTMLElement>(".runtime-context-workspace > nav");
  const contextRail = els.stagePage.querySelector<HTMLElement>(".context-diff-rail");
  if (contextNav && contextRail) contextRail.scrollTop = contextNav.scrollTop;
}

export function renderStageAndFocus(pageId: PageId, selector: string): void {
  renderSourceState();
  renderStage(pageId);
  window.requestAnimationFrame(() => {
    els.stagePage.querySelector<HTMLElement>(selector)?.focus();
  });
}

function renderPageTabs(pageId: PageId): string {
  const tabs = pageTabs[pageId] || [];
  if (!tabs.length) return "";
  const active = getActivePageTab(pageId);
  return `
    <nav class="window-tab-strip" aria-label="${t("当前系统内部标签")}" role="tablist">
      ${tabs.map((tab) => `
        <button id="page-tab-${escapeHtml(pageId)}-${escapeHtml(tab.id)}" class="page-tab ${tab.id === active ? "active" : ""}" data-page-tab="${escapeHtml(tab.id)}" role="tab" aria-controls="systemWindowPanel" aria-selected="${tab.id === active ? "true" : "false"}" tabindex="${tab.id === active ? "0" : "-1"}">
          <span>${escapeHtml(t(tab.label))}</span>
          ${state.locale === "zh-CN" ? "" : `<em>${escapeHtml(tab.code)}</em>`}
        </button>
      `).join("")}
    </nav>
  `;
}

function tabPanel(label: string, title: string, desc: string, rows: LedgerRow[] = [], hot = false): string {
  return `
    <section class="ledger-panel ${hot ? "hot" : ""}">
      <header class="ledger-title">
        <span class="hud-label">${escapeHtml(label)}</span>
        <h2>${escapeHtml(title)}</h2>
        <p>${escapeHtml(desc)}</p>
      </header>
      <div class="ledger-rows">
        ${rows.map(([rowTitle, rowDesc, status, tone]) => receiptRow(rowTitle, rowDesc, status, tone)).join("")}
      </div>
    </section>
  `;
}

function renderRuntimeEmpty(label: string, title: string, desc: string, retryTarget = ""): string {
  return `
    <section class="runtime-empty">
      <span class="hud-label">${escapeHtml(label)}</span>
      <h2>${escapeHtml(title)}</h2>
      <p>${escapeHtml(desc)}</p>
      ${retryTarget ? `<button class="runtime-retry" type="button" data-retry-projection="${escapeHtml(retryTarget)}">${retryTarget === "runtime" ? t("重新连接") : t("重新读取")}</button>` : ""}
    </section>
  `;
}

function renderDepositionUnavailable(label: string): string {
  if (depositionProjection.loading && !depositionProjection.index) {
    return renderRuntimeEmpty(label, "正在读取沉淀真源", "从本地宿主读取记忆条目、已登记容器与活动关系卡。不存在静态回退数据。");
  }
  if (depositionProjection.error || !depositionProjection.index) {
    return renderRuntimeEmpty(label, "沉淀真源不可用", depositionProjection.error || "本地宿主没有返回可用的沉淀索引。", "deposition");
  }
  return "";
}

function depositionItems(kind: DepositionKind): DepositionItem[] {
  const key = kind === "memory" ? "memory" : kind === "container" ? "containers" : "relations";
  return depositionProjection.index?.[key] || [];
}

export function selectDepositionItem(kind: DepositionKind, items: DepositionItem[]): DepositionItem | null {
  const stateKey: keyof Pick<UiState, "selectedMemoryId" | "selectedContainerId" | "selectedRelationId"> = kind === "memory" ? "selectedMemoryId" : kind === "container" ? "selectedContainerId" : "selectedRelationId";
  const selected = items.find((item) => item.id === state[stateKey]) || items[0] || null;
  state[stateKey] = selected?.id || "";
  return selected;
}

function depositionDetail(kind: DepositionKind, itemId: string): DepositionDetailItem | null {
  return depositionProjection.details[kind]?.[itemId]?.item || null;
}

function depositionDetailStatus(kind: DepositionKind, itemId: string, loadingText: string): string {
  const error = depositionProjection.detailErrors[`${kind}:${itemId}`];
  return error
    ? `<p class="runtime-empty-copy warn">${escapeHtml(error)}</p>`
    : `<p class="runtime-empty-copy">${escapeHtml(loadingText)}</p>`;
}

function depositionRow(kind: DepositionKind, item: DepositionItem, selected: boolean, description: unknown, status: unknown): string {
  const layerLabel = kind === "memory"
    ? (item.memory_layers || []).join(" + ") || item.memory_layer || "MEM"
    : kind === "container" ? item.prefix || "WB" : item.category || "REL";
  return `
    <button class="deposition-row ${selected ? "active" : ""}" data-deposition-kind="${kind}" data-deposition-id="${escapeHtml(item.id)}" aria-current="${selected ? "true" : "false"}" ${kind === "memory" ? 'aria-haspopup="dialog"' : ""}>
      <span>${escapeHtml(layerLabel)}</span>
      <div><b>${escapeHtml(item.title || item.name || item.id)}</b><small>${escapeHtml(description || item.id)}</small></div>
      <em>${escapeHtml(status || item.status || "READ")}</em>
    </button>
  `;
}

function depositionJump(kind: DepositionKind, itemId: string, label = ""): string {
  return `<button class="deposition-link" data-deposition-jump-kind="${kind}" data-deposition-id="${escapeHtml(itemId)}">${escapeHtml(label || itemId)}</button>`;
}

function renderDepositionEmpty(label: string, title: string, desc: string): string {
  return `<section class="deposition-empty"><span class="hud-label">${escapeHtml(label)}</span><h2>${escapeHtml(title)}</h2><p>${escapeHtml(desc)}</p><button class="empty-state-action" type="button" data-close-system-window>${t("继续交互")}</button></section>`;
}

function renderRuntimeFrames(frames: CallFrame[]): string {
  if (!frames.length) return '<p class="runtime-empty-copy">尚无 Frame 投影。</p>';
  return `<div class="runtime-frame-list">${frames.map((frame) => `
    <div class="runtime-frame-row">
      <b>${escapeHtml(frame.label || frame.frame_id)}</b>
      <span>${escapeHtml(runtimeTerm(frame.call_channel || frame.phase || "unknown"))}</span>
      <code>#${escapeHtml(frame.event_start_index)}–#${escapeHtml(frame.event_end_index)}</code>
    </div>
  `).join("")}</div>`;
}

export function relayRuntimeState(): RelayRuntimeState {
  const status = runtimeProjection.status;
  const cli = status?.cli?.data || {};
  const activeFlags = Array.isArray(cli.active_flags) ? cli.active_flags : [];
  return {
    ready: cli.round_type === "relay" && activeFlags.includes("continue_requested"),
    inFlight: Boolean(taskProjection.relayPending || status?.relay_in_flight),
    mutationInFlight: Boolean(status?.mutation_in_flight),
    roundType: cli.round_type || "none",
    activeFlags,
  };
}

function taskRecordRows(records: TaskRecord[], emptyText: string): string {
  if (!records.length) return `<p class="runtime-empty-copy">${escapeHtml(emptyText)}</p>`;
  return `<div class="task-record-list">${records.map((record) => `
    <article class="task-record ${record.status === "blocked" ? "warn" : ""}">
      <header><b>${escapeHtml(record.id)}</b><em>${escapeHtml(record.status || "unknown")}</em></header>
      <p>${escapeHtml(record.title || "无描述")}</p>
      <small>${record.required ? "必需" : "可选"}${record.reason ? ` · ${escapeHtml(record.reason)}` : ""}</small>
      ${(record.evidence_refs || []).length ? `<div class="task-evidence-refs">${(record.evidence_refs || []).map((ref) => `<code>${escapeHtml(ref)}</code>`).join("")}</div>` : ""}
    </article>
  `).join("")}</div>`;
}

function taskEvidenceSelection(): {
  round: number;
  live: NonNullable<typeof runtimeProjection.live>;
  frame: CallFrame | null;
} | null {
  const rounds = runtimeProjection.conversationRoundOrder;
  if (state.selectedTaskRound !== null && !runtimeProjection.conversationRounds.has(state.selectedTaskRound)) {
    state.selectedTaskRound = null;
    state.selectedTaskFrame = null;
  }
  const round = state.selectedTaskRound ?? runtimeProjection.round ?? rounds.at(-1) ?? null;
  if (round === null) return null;
  const live = round === runtimeProjection.round
    ? runtimeProjection.live
    : runtimeProjection.conversationRounds.get(round) || null;
  if (!live) return null;
  const frames = live.frame_catalog || [];
  if (state.selectedTaskFrame !== null && !frames.some((frame) => frame.frame_id === state.selectedTaskFrame)) {
    state.selectedTaskFrame = null;
  }
  const frameEntry = state.selectedTaskFrame === null
    ? frames.at(-1) || null
    : frames.find((item) => item.frame_id === state.selectedTaskFrame) || null;
  const detail = runtimeProjection.frameDetail;
  const frame = frameEntry && detail?.round === round && detail.frameId === frameEntry.frame_id
    ? detail.frame
    : frameEntry;
  return { round, live, frame };
}

function taskRoundSelector(selectedRound: number): string {
  const rounds = runtimeProjection.conversationRoundOrder;
  const latest = runtimeProjection.round ?? rounds.at(-1) ?? selectedRound;
  const historical = rounds.filter((round) => round !== latest).reverse();
  return `<label class="protocol-round-select"><span>${t("选择轮次")}</span><select data-task-round>
    <option value="latest" ${state.selectedTaskRound === null ? "selected" : ""}>${t("最新")} · R${escapeHtml(latest)}</option>
    ${historical.map((round) => `<option value="${escapeHtml(round)}" ${state.selectedTaskRound === round ? "selected" : ""}>R${escapeHtml(round)}</option>`).join("")}
  </select></label>`;
}

function taskFrameSelector(frames: CallFrame[], selectedFrame: CallFrame): string {
  const historical = frames.filter((frame) => frame.frame_id !== frames.at(-1)?.frame_id).reverse();
  const label = (frame: CallFrame): string => `${runtimeTerm(frame.phase || frame.call_channel || "frame")} · ${frame.iteration ?? 1}`;
  return `<label class="protocol-round-select"><span>${t("选择帧次")}</span><select data-task-frame>
    <option value="latest" ${state.selectedTaskFrame === null ? "selected" : ""}>${t("最新")} · ${escapeHtml(label(frames.at(-1) || selectedFrame))}</option>
    ${historical.map((frame) => `<option value="${escapeHtml(frame.frame_id)}" ${state.selectedTaskFrame === frame.frame_id ? "selected" : ""}>${escapeHtml(label(frame))}</option>`).join("")}
  </select></label>`;
}

function taskFrameSnapshot(frame: CallFrame | null): string {
  const pane = frame?.context_panes?.find((item) => item.id === "40_high_freq");
  const taskBoard = pane?.content_blocks?.find((block) => block.block_id === "high_freq:task_board");
  return String(taskBoard?.content_md || taskBoard?.content_raw || "").trim();
}

function renderTaskEvidencePage(): string {
  if (runtimeProjection.host !== "connected") {
    return renderRuntimeEmpty("WORKBENCH", "本地宿主未连接", runtimeProjection.error || "无法读取任务真账。", "runtime");
  }
  if (taskProjection.loading && !taskProjection.data) {
    return renderRuntimeEmpty("WORKBENCH", "正在读取任务真账", "active task、guide 与 pending input 只从 Workbench 读取。");
  }
  if (taskProjection.error || !taskProjection.data) {
    return renderRuntimeEmpty("WORKBENCH", "任务投影不可用", taskProjection.error || "宿主没有返回结构化任务投影。", "task");
  }
  const projection = taskProjection.data;
  const task = projection.task;
  const summary = projection.summary || {};
  const relay = relayRuntimeState();
  const relayDisabled = !relay.ready || relay.inFlight || relay.mutationInFlight;
  const relayLabel = relay.inFlight ? "中继执行中" : relay.ready ? "可以继续" : "等待 continue_requested";
  const selectedEvidence = taskEvidenceSelection();
  const selectedFrame = selectedEvidence?.frame || null;
  const evidenceItems = (selectedEvidence ? runtimeProjection.ledgerItems.get(selectedEvidence.round) || [] : [])
    .filter((item) => (item.type === "tool"
      || /tool|receipt|settle/i.test(String(item.event_type || "")))
      && (!selectedFrame || item.frame_id === selectedFrame.frame_id))
    .slice(-8)
    .reverse();
  const reviewingHistory = state.selectedTaskRound !== null || state.selectedTaskFrame !== null;
  const taskSnapshot = reviewingHistory ? taskFrameSnapshot(selectedFrame) : "";
  const pendingInputs = task?.pending_inputs || [];
  const requirements = task?.source_requirements || [];
  const risks = task?.risk_notes || [];

  return `<section class="task-workspace">
    <header class="task-hero">
      <div><span class="hud-label">${t("任务工作台")} · ${escapeHtml(projection.active_guides?.work || t("当前没有活动任务"))}</span>
        <h2>${escapeHtml(task?.title || t("当前没有活动任务"))}</h2>
        <p>${escapeHtml(task?.goal || t("工作台未登记活动任务；不会从对话文案推断任务。"))}</p>
      </div>
      <dl class="task-metrics">
        <div><dt>${t("项目数")}</dt><dd>${escapeHtml(summary.open_items || 0)}</dd></div>
        <div><dt>${t("验收数")}</dt><dd>${escapeHtml(summary.pending_acceptance || 0)}</dd></div>
        <div><dt>${t("输入数")}</dt><dd>${escapeHtml(summary.open_pending_inputs || 0)}</dd></div>
        <div><dt>${t("任务状态")}</dt><dd>${escapeHtml(runtimeTerm(summary.state || "empty"))}</dd></div>
      </dl>
    </header>
    <section class="relay-console ${relay.ready ? "ready" : ""}">
      <div><span class="hud-label">${t("运行时中继")} · ${escapeHtml(relay.roundType)}</span><strong>${escapeHtml(relayLabel)}</strong>
        <p>${t("只在结构化状态同时给出中继轮型与继续请求时开放；执行权限继承下方通信带。")} <code>round_type=relay</code> · <code>continue_requested</code></p>
      </div>
      <button type="button" data-runtime-relay ${relayDisabled ? "disabled" : ""}>${t("执行下一中继轮")}</button>
      <span role="status">${escapeHtml(taskProjection.relayFeedback || (relay.ready ? t("未执行；等待用户操作。") : `flags: ${relay.activeFlags.join(", ") || "none"}`))}</span>
    </section>
    <div class="task-columns">
      <section class="task-pane"><header><span class="hud-label">${t(reviewingHistory ? "该帧任务快照" : "任务账本")}</span><strong>${escapeHtml(reviewingHistory ? selectedFrame?.frame_id || "none" : task?.id || "none")}</strong></header>
        <div class="task-pane-scroll ${reviewingHistory ? "task-frame-snapshot" : ""}" data-stage-scroll-key="${escapeHtml(reviewingHistory ? `run:tools:task:${selectedEvidence?.round || "none"}:${selectedFrame?.frame_id || "none"}` : `run:tools:task:${task?.id || "none"}`)}">
          ${reviewingHistory
    ? (taskSnapshot
      ? renderMarkdownDocument(`task-snapshot:${selectedEvidence?.round}:${selectedFrame?.frame_id}`, taskSnapshot)
      : `<p class="runtime-empty-copy">${t("该帧未装配活动任务清单。")}</p>`)
    : `${pendingInputs.length ? `<section class="task-subsection"><h3>${t("待整合输入")}</h3>${pendingInputs.map((item) => `<article class="task-pending"><b>${escapeHtml(item.id)}</b><em>${escapeHtml(item.status)}</em><p>${escapeHtml(item.summary || t("无摘要"))}</p>${(item.source_refs || []).map((ref) => `<code>${escapeHtml(ref)}</code>`).join("")}</article>`).join("")}</section>` : ""}
          <section class="task-subsection"><h3>${t("任务项")}</h3>${taskRecordRows(task?.items || [], t("当前没有任务项。"))}</section>
          <section class="task-subsection"><h3>${t("验收项")}</h3>${taskRecordRows(task?.acceptance || [], t("当前没有验收项。"))}</section>
          ${requirements.length ? `<section class="task-subsection"><h3>${t("来源要求")}</h3>${requirements.map((item) => `<p><b>${escapeHtml(item.id)}</b>${escapeHtml(item.summary || t("无摘要"))}</p>`).join("")}</section>` : ""}
          ${risks.length ? `<section class="task-subsection warn"><h3>${t("风险备注")}</h3>${risks.map((note) => `<p>${escapeHtml(note)}</p>`).join("")}</section>` : ""}`}
        </div>
      </section>
      <section class="task-pane evidence"><header class="task-evidence-header"><div><span class="hud-label">${t("轮次证据")}</span><strong>${escapeHtml(selectedEvidence ? `${t("本地宿主已连接")} · R${selectedEvidence.round} · ${selectedFrame?.frame_id || t("尚无帧次")}` : t("无轮次"))}</strong></div>
        ${selectedEvidence && selectedFrame ? `<div class="context-selectors">${taskRoundSelector(selectedEvidence.round)}${taskFrameSelector(selectedEvidence.live.frame_catalog || [], selectedFrame)}</div>` : ""}</header>
        ${selectedEvidence ? renderLedgerDirectory(selectedEvidence.round, evidenceItems, t("该帧尚无结构化工具调用或回执证据。"), `run:tools:evidence:${selectedEvidence.round}:${selectedFrame?.frame_id || "none"}`) : `<p class="runtime-empty-copy">${t("该帧尚无结构化工具调用或回执证据。")}</p>`}
      </section>
    </div>
  </section>`;
}

function renderRuntimeRunPage(): string {
  if (runtimeProjection.host !== "connected") {
    return renderRuntimeEmpty(t("本地宿主"), t("本地宿主未连接"), runtimeProjection.error || t("请从本地 GUI 宿主启动界面。"), "runtime");
  }
  const live = runtimeProjection.live;
  const tab = getActivePageTab("run");
  if (tab === "tools") {
    return renderTaskEvidencePage();
  }
  if (!live || runtimeProjection.round == null) {
    return renderRuntimeEmpty(t("轮次账本"), t("尚无轮次"), t("宿主已连接，等待真实事件"));
  }
  const ledgerItems = runtimeProjection.ledgerItems.get(runtimeProjection.round) || [];
  if (tab === "receipts") {
    const receipts = ledgerItems.filter((item) => item.type === "settlement" || item.event_type.includes("receipt") || item.event_type.includes("settled"));
    return renderLedgerDirectory(runtimeProjection.round, receipts, t("尚无回执或结算事件。"), `run:receipts:${runtimeProjection.round}`);
  }
  if (tab === "risks") {
    const lifecycle = live.round_lifecycle || {};
    const reasons: Array<[string, string]> = [
      ...(lifecycle.fatal_reasons || []).map((reason): [string, string] => ["FATAL", reason]),
      ...(lifecycle.degraded_reasons || []).map((reason): [string, string] => ["DEGRADED", reason]),
    ];
    return reasons.length ? `<div class="runtime-reasons">${reasons.map(([kind, reason]) => `<p><b>${escapeHtml(kind)}</b>${escapeHtml(reason)}</p>`).join("")}</div>` : renderRuntimeEmpty(t("风险警报"), t("没有投影到事故原因"), t("这只表示当前轮次账本未携带严重或降级原因。"));
  }
  const statusbar = live.statusbar_projection || {};
  const lifecycle = live.round_lifecycle || {};
  const indexes = lifecycle.event_indexes || {};
  return `
    <div class="runtime-run" data-stage-scroll-key="${escapeHtml(`run:round:${runtimeProjection.round}`)}">
      <section class="run-focus">
        <div class="focus-kicker"><span class="hud-label">${t("当前运行")}</span><em>${escapeHtml(runtimeTerm(lifecycle.state || "running"))}</em></div>
        <h2>${escapeHtml(statusbar.round?.id || `R${String(runtimeProjection.round).padStart(6, "0")}`)} · ${escapeHtml(runtimeTerm(statusbar.round?.type || t("轮型未投影")))}</h2>
        <p>${escapeHtml(statusbar.round?.progress || t("等待结构化状态栏"))} · ${escapeHtml(statusbar.workhood || t("工化未投影"))}</p>
        <div class="runtime-lifecycle" aria-label="${t("轮次生命周期事件索引")}">
          ${Object.entries(indexes).map(([eventType, index]) => `<span><b>${escapeHtml(runtimeTerm(eventType))}</b><code>#${escapeHtml(index)}</code></span>`).join("") || `<span>${t("尚无生命周期事件索引")}</span>`}
        </div>
      </section>
      <section class="status-ledger">
        <header><span class="hud-label">${t("状态概览")}</span></header>
        <div class="status-ledger-row"><b>${t("轮次")}</b><span>${escapeHtml(runtimeTerm(statusbar.round?.type || t("未投影")))}</span><em>${escapeHtml(statusbar.round?.id || "—")}</em></div>
        <div class="status-ledger-row"><b>${t("模式")}</b><span>${escapeHtml(statusbar.dynamic || t("结构化投影尚不可用"))}</span><em>${escapeHtml(runtimeTerm(statusbar.mode || t("未投影")))}</em></div>
        <div class="status-ledger-row"><b>${t("帧次")}</b><span>${escapeHtml(live.latest_frame_id || t("尚无帧次"))}</span><em>${escapeHtml((live.frame_catalog || []).length)}</em></div>
        <div class="status-ledger-row ${lifecycle.state === "unsettled" ? "warn" : ""}"><b>${t("结算")}</b><span>${escapeHtml(runtimeTerm(lifecycle.settlement_status || "pending"))}</span><em>${escapeHtml(runtimeTerm(lifecycle.state || "running"))}</em></div>
      </section>
      <section class="runtime-frames" data-stage-scroll-key="${escapeHtml(`run:round:${runtimeProjection.round}:frames`)}"><span class="hud-label">${t("帧次列表")}</span>${renderRuntimeFrames(live.frame_catalog || [])}</section>
    </div>
  `;
}

function contextPaneMarkdown(pane?: ContextPane): string {
  const source = pane?.content_md || pane?.content_raw || "";
  if (!pane || !["00_call_header", "01_tool_header", "02_generation_config"].includes(pane.id)) return source;
  const raw = pane.content_raw || source;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (pane.id === "01_tool_header" && parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      const { tools: _tools, ...metadata } = parsed as Record<string, unknown>;
      return `\`\`\`json\n${JSON.stringify(metadata, null, 2)}\n\`\`\``;
    }
    return `\`\`\`json\n${raw}\n\`\`\``;
  } catch {
    return `\`\`\`text\n${raw}\n\`\`\``;
  }
}

function contextToolAnnotations(pane?: ContextPane): Array<{ name: string; description: string; parameters: unknown }> {
  if (pane?.id !== "01_tool_header") return [];
  try {
    const parsed: unknown = JSON.parse(pane.content_raw || pane.content_md || "{}");
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return [];
    const tools = (parsed as Record<string, unknown>).tools;
    if (!Array.isArray(tools)) return [];
    return tools.flatMap((tool) => {
      if (tool === null || typeof tool !== "object" || Array.isArray(tool)) return [];
      const fn = (tool as Record<string, unknown>).function;
      if (fn === null || typeof fn !== "object" || Array.isArray(fn)) return [];
      const record = fn as Record<string, unknown>;
      if (typeof record.name !== "string" || !record.name.trim()) return [];
      return [{
        name: record.name.trim(),
        description: typeof record.description === "string" && record.description.trim()
          ? record.description.trim()
          : "未提供工具注释。",
        parameters: record.parameters ?? {},
      }];
    });
  } catch {
    return [];
  }
}

function renderContextToolIndex(pane?: ContextPane): string {
  const tools = contextToolAnnotations(pane);
  if (!tools.length) return "";
  return `<section class="context-tool-index" aria-label="${t("工具详情目录")}"><span class="hud-label">${t("工具详情目录")}</span>${tools.map((tool) => `<button type="button" aria-haspopup="dialog" data-context-tool="${escapeHtml(tool.name)}"><b>${escapeHtml(tool.name)}</b><span>${t("查看详情")}</span></button>`).join("")}</section>`;
}

function renderContextToolSummary(pane?: ContextPane): string {
  if (pane?.id !== "01_tool_header") return "";
  let data: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(pane.content_raw || pane.content_md || "{}");
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return "";
    data = parsed as Record<string, unknown>;
  } catch {
    return "";
  }
  const enabled = (value: unknown, yes: MessageKey, no: MessageKey): string => value === true ? t(yes) : value === false ? t(no) : t("未设置");
  const values: Array<[string, string]> = [
    [t("原生工具模式"), data.native_tool_mode == null ? t("未设置") : String(data.native_tool_mode)],
    [t("权限说明"), String(data.permission_label || t("未标记"))],
    [t("权限级别"), data.permission_level === "limited" ? t("只读") : data.permission_level === "guarded" ? t("受限") : data.permission_level === "unlimited" ? t("放行") : String(data.permission_level || t("未标记"))],
    [t("标准工具"), enabled(data.standard_tools_enabled, "已启用", "未启用")],
    [t("终端工具"), data.terminal_tool === "reaction_finalize" ? t("反应阶段收束") : String(data.terminal_tool || t("未设置"))],
    [t("工具模式"), data.tool_mode === "free" ? t("自由") : String(data.tool_mode || t("未标记"))],
    [t("工具数量"), `${Array.isArray(data.tool_names) ? data.tool_names.length : contextToolAnnotations(pane).length} ${t("个")}`],
    [t("工具传输"), enabled(data.tools_transmitted, "已传输", "未传输")],
  ];
  return `<section class="context-tool-summary" aria-label="${t("工具调用总览")}"><header><span class="hud-label">${t("工具调用总览")}</span><strong>${t("当前帧次")}</strong></header><dl>${values.map(([label, value]) => `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl></section>`;
}

function renderContextPaneDetail(round: number, frame: CallFrame, pane?: ContextPane): string {
  const diffTarget = contextPrefixDiffFor(round, frame)?.target;
  const paneTarget = diffTarget?.pane_id === pane?.id ? diffTarget : null;
  const toolSummary = renderContextToolSummary(pane);
  if (toolSummary) {
    return `${paneTarget ? renderContextDiffSeam(paneTarget) : ""}${toolSummary}${renderContextToolIndex(pane)}`;
  }
  if (!pane) return "";
  const blocks = pane.content_blocks || [];
  if (!blocks.length) {
    const content = renderContextBlock(
      `context:${round}:${frame.frame_id}:${pane.id}`,
      contextPaneLabel(pane.id),
      contextPaneMarkdown(pane),
      pane.chars ?? pane.raw_chars ?? 0,
      "",
      "",
      "",
      paneTarget && ["block_inside", "layer_inside"].includes(paneTarget.placement)
        ? paneTarget
        : null,
    );
    return paneTarget && !["block_inside", "layer_inside"].includes(paneTarget.placement)
      ? `${paneTarget.placement === "layer_start" || paneTarget.placement === "layer_boundary" ? renderContextDiffSeam(paneTarget) : ""}${content}${paneTarget.placement !== "layer_start" && paneTarget.placement !== "layer_boundary" ? renderContextDiffSeam(paneTarget) : ""}`
      : content;
  }
  const targetBlockPresent = Boolean(
    paneTarget?.block_id && blocks.some((block) => block.block_id === paneTarget.block_id),
  );
  const cards = blocks.map((block) => {
    const provenance = block.provenance || {};
    const meta = [
      block.source_block_id ? `${t("来源")} ${block.source_block_id}` : "",
      provenance.kind,
      provenance.round ? `${t("轮次")} ${provenance.round}` : "",
      provenance.step,
      provenance.iter != null ? `${t("帧次")} ${provenance.iter}` : "",
    ].filter(Boolean).map(escapeHtml).join(" · ");
    const timestamp = String(provenance.timestamp || "");
    const before = paneTarget
      && paneTarget.placement === "block_boundary"
      && block.block_id === paneTarget.block_id
      ? renderContextDiffSeam(paneTarget)
      : "";
    const card = renderContextBlock(
      `context:${round}:${frame.frame_id}:${pane.id}:${block.block_id}`,
      block.title || contextPaneLabel(pane.id),
      block.content_md || block.content_raw || "",
      block.chars ?? block.raw_chars ?? 0,
      block.tone,
      meta,
      timestamp,
      paneTarget
        && block.block_id === paneTarget.block_id
        && ["block_inside", "layer_inside"].includes(paneTarget.placement)
        ? paneTarget
        : null,
    );
    return `${before}${card}`;
  }).join("");
  const beforeStack = paneTarget
    && ["layer_start", "layer_boundary"].includes(paneTarget.placement)
    ? renderContextDiffSeam(paneTarget)
    : "";
  const afterStack = paneTarget
    && (
      ["layer_end", "request_end"].includes(paneTarget.placement)
      || (paneTarget.placement === "block_boundary" && !targetBlockPresent)
    )
    ? renderContextDiffSeam(paneTarget)
    : "";
  return `<div class="context-block-stack">${beforeStack}${cards}${afterStack}</div>`;
}

function renderContextDiffSeam(target: RequestPrefixDiffTarget): string {
  return `<div class="context-diff-seam" data-context-diff-anchor tabindex="-1"><span>${escapeHtml(contextDiffLocationLabel(target))}</span></div>`;
}

function contextDiffLocationLabel(target: RequestPrefixDiffTarget): string {
  if (target.source_mapping === "derived") {
    return t("变化发生在该块编译出的协议片段");
  }
  if (target.placement === "block_inside") {
    return t("变化从块内第 {offset} 字符开始", {
      offset: Number(target.block_offset ?? target.source_offset).toLocaleString(state.locale),
    });
  }
  return t("请求体变化后缀从这里开始");
}

function renderContextBlock(
  documentId: string,
  title: string,
  content: string,
  chars: number,
  tone = "",
  meta = "",
  timestamp = "",
  diffTarget: RequestPrefixDiffTarget | null = null,
): string {
  const time = renderFullTime(timestamp, "context-block-time");
  return `<section class="context-block-card${diffTarget ? " context-diff-block" : ""}" data-tone="${escapeHtml(tone)}"><header><b>${escapeHtml(title)}</b><span>${escapeHtml(chars.toLocaleString(state.locale))} ${t("字符")}${meta ? ` · ${meta}` : ""}${time ? ` · ${t("语料时间")} ${time}` : ""}</span>${diffTarget ? `<em class="context-diff-block-marker" data-context-diff-anchor tabindex="-1">${escapeHtml(contextDiffLocationLabel(diffTarget))}</em>` : ""}</header><div class="context-block-body">${renderMarkdownDocument(documentId, content || "—")}</div></section>`;
}

const contextInstrumentPaneIds = ["00_call_header", "01_tool_header", "02_generation_config"];
const contextPaneLabels: Record<string, MessageKey> = {
  "00_call_header": "A_调用头",
  "01_tool_header": "B_工具头",
  "02_generation_config": "C_生成参数",
  "10_permanent": "1_永固层",
  "20_periodic": "2_定期层",
  "30_lately": "3_最近缓存",
  "40_high_freq": "4_高频层",
  "50_now": "5_当前缓存",
  "60_statusbar": "6_状态栏",
  "99_popup": "7_弹窗层",
};

function contextPaneLabel(paneId: string): string {
  const label = contextPaneLabels[paneId];
  return label ? t(label) : paneId;
}

function contextPrefixDiffFor(round: number, frame: CallFrame): RequestPrefixDiffPayload | null {
  const payload = runtimeProjection.contextPrefixDiff;
  return runtimeProjection.contextPrefixDiffKey === `${round}:${frame.frame_id}`
    && payload?.state === "ready"
    && payload.current?.round === round
    && payload.current?.frame_id === frame.frame_id
    && payload.target
    ? payload
    : null;
}

function contextDiffTooltip(diff: RequestPrefixDiffPayload): string {
  const target = diff.target;
  const ratio = `${((Number(diff.prefix_ratio) || 0) * 100).toFixed(1)}%`;
  const change = target?.change_kind === "insert"
    ? t("新增")
    : target?.change_kind === "delete"
      ? t("删除")
      : t("替换");
  return t("与 {frame} 比较：公共前缀 {bytes} 字节（{ratio}）；目标 {pane}{block}；变化类型 {change}。请求体前缀不等同于 provider 实际缓存命中。", {
    frame: diff.previous?.frame_id || "—",
    bytes: Number(diff.common_prefix_bytes || 0).toLocaleString(state.locale),
    ratio,
    pane: target ? contextPaneLabel(target.pane_id) : "—",
    block: target?.block_id ? ` / ${target.block_id}` : "",
    change,
  });
}

function renderContextDiffRail(panes: ContextPane[], round: number, frame: CallFrame): string {
  const diff = contextPrefixDiffFor(round, frame);
  const target = diff?.target;
  const railLabel = runtimeProjection.contextPrefixDiffLoading
    ? t("正在计算请求体前缀差分")
    : runtimeProjection.contextPrefixDiff?.state === "identical"
      ? t("与最近兼容帧的请求体完全相同")
      : runtimeProjection.contextPrefixDiff?.state === "unavailable"
        || runtimeProjection.contextPrefixDiffError
        ? t("没有可用的请求体前缀差分")
        : t("请求体前缀差分轨道");
  const alignment = target && ["layer_start", "layer_boundary"].includes(target.placement)
    ? "at-start"
    : target && ["layer_end", "request_end"].includes(target.placement)
      ? "at-end"
      : "at-center";
  const tooltip = diff ? contextDiffTooltip(diff) : "";
  return `<div class="context-diff-rail" aria-label="${escapeHtml(railLabel)}" aria-busy="${runtimeProjection.contextPrefixDiffLoading ? "true" : "false"}">
    ${panes.map((pane) => `<div class="context-diff-slot">${target?.pane_id === pane.id ? `<button type="button" class="context-diff-cursor ${alignment}" data-context-diff-jump title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(tooltip)}"><span aria-hidden="true"></span></button>` : ""}</div>`).join("")}
  </div>`;
}

export function jumpToContextPrefixDiff(): void {
  const target = runtimeProjection.contextPrefixDiff?.target;
  if (!target || runtimeProjection.contextPrefixDiff?.state !== "ready") return;
  state.activeRuntimePane = target.pane_id;
  renderStage("context");
  window.requestAnimationFrame(() => {
    const article = els.stagePage.querySelector<HTMLElement>(".runtime-context-workspace article");
    const anchor = article?.querySelector<HTMLElement>("[data-context-diff-anchor]");
    if (!article || !anchor) return;
    const articleRect = article.getBoundingClientRect();
    const anchorRect = anchor.getBoundingClientRect();
    const targetTop = article.scrollTop + anchorRect.top - articleRect.top
      - Math.min(96, article.clientHeight * 0.2);
    article.scrollTop = Math.max(0, targetTop);
    anchor.focus({ preventScroll: true });
  });
}

function renderContextInstrument(pane: ContextPane): string {
  let data: Record<string, unknown> = {};
  try {
    const parsed: unknown = JSON.parse(pane.content_raw || pane.content_md || "{}");
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      data = parsed as Record<string, unknown>;
    }
  } catch {
    // Invalid source remains reachable through the ordinary content detail fallback.
  }
  const record = (value: unknown): Record<string, unknown> => (
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? value as Record<string, unknown>
      : {}
  );
  const text = (value: unknown, fallback = "—"): string => (
    typeof value === "string" && value.trim()
      ? value.trim()
      : typeof value === "number" ? String(value) : fallback
  );
  let kind = "call";
  let primary = pane.id;
  let secondary = t("结构化调用头");
  if (pane.id === "00_call_header") {
    const call = record(data.call);
    const endpoint = record(data.endpoint);
    primary = text(call.phase, text(call.channel, "unknown"));
    secondary = `${t("第 {attempt} 次", { attempt: text(call.attempt, "?") })} · ${text(endpoint.tier, t("端点未标记"))}`;
  } else if (pane.id === "01_tool_header") {
    kind = "tools";
    primary = text(data.permission_label, text(data.permission_level, t("未标记")));
    const toolCount = Array.isArray(data.tool_names) ? data.tool_names.length : 0;
    secondary = `${toolCount} ${t("个工具")} · ${text(data.tool_mode, t("模式未标记"))}`;
  } else if (pane.id === "02_generation_config") {
    kind = "generation";
    primary = text(data.reasoning_effort, t("未标记"));
    secondary = t("温度 {value}", { value: text(data.temperature, t("未标记")) });
  }
  const paneLabel = contextPaneLabel(pane.id);
  return `<button class="context-instrument ${kind}" data-runtime-pane="${escapeHtml(pane.id)}" aria-label="${escapeHtml(t("查看 {title} 层真实内容", { title: paneLabel }))}">
    <span class="context-instrument-top"><b>${escapeHtml(paneLabel)}</b></span>
    <span class="context-instrument-readout"><strong>${escapeHtml(primary)}</strong><small>${escapeHtml(secondary)}</small></span>
    <span class="context-instrument-foot"><em>${escapeHtml(pane.chars ?? pane.raw_chars ?? 0)} CH</em></span>
  </button>`;
}

function contextSelection(): { round: number; live: NonNullable<typeof runtimeProjection.live> } | null {
  const rounds = runtimeProjection.conversationRoundOrder;
  if (state.selectedContextRound !== null && !runtimeProjection.conversationRounds.has(state.selectedContextRound)) {
    state.selectedContextRound = null;
  }
  const round = state.selectedContextRound ?? runtimeProjection.round ?? rounds.at(-1) ?? null;
  if (round === null) return null;
  const live = round === runtimeProjection.round
    ? runtimeProjection.live
    : runtimeProjection.conversationRounds.get(round) || null;
  return live ? { round, live } : null;
}

function contextRoundSelector(selectedRound: number): string {
  const rounds = runtimeProjection.conversationRoundOrder;
  const latest = runtimeProjection.round ?? rounds.at(-1) ?? selectedRound;
  const historical = rounds.filter((round) => round !== latest).reverse();
  return `<label class="protocol-round-select context-round-select"><span>${t("选择轮次")}</span><select data-context-round>
    <option value="latest" ${state.selectedContextRound === null ? "selected" : ""}>${t("最新")} · R${escapeHtml(latest)}</option>
    ${historical.map((round) => `<option value="${escapeHtml(round)}" ${state.selectedContextRound === round ? "selected" : ""}>R${escapeHtml(round)}</option>`).join("")}
  </select></label>`;
}

function contextFrameSelection(live: NonNullable<typeof runtimeProjection.live>): CallFrame | null {
  const frames = live.frame_catalog || [];
  if (state.selectedContextFrame !== null && !frames.some((frame) => frame.frame_id === state.selectedContextFrame)) {
    state.selectedContextFrame = null;
  }
  return state.selectedContextFrame === null
    ? frames.at(-1) || null
    : frames.find((frame) => frame.frame_id === state.selectedContextFrame) || null;
}

function contextFrameSelector(frames: CallFrame[], selectedFrame: CallFrame): string {
  const historical = frames.filter((frame) => frame.frame_id !== frames.at(-1)?.frame_id).reverse();
  const label = (frame: CallFrame): string => `${runtimeTerm(frame.phase || frame.call_channel || "frame")} · ${frame.iteration ?? 1}`;
  return `<label class="protocol-round-select context-frame-select"><span>${t("选择帧次")}</span><select data-context-frame>
    <option value="latest" ${state.selectedContextFrame === null ? "selected" : ""}>${t("最新")} · ${escapeHtml(label(frames.at(-1) || selectedFrame))}</option>
    ${historical.map((frame) => `<option value="${escapeHtml(frame.frame_id)}" ${state.selectedContextFrame === frame.frame_id ? "selected" : ""}>${escapeHtml(label(frame))}</option>`).join("")}
  </select></label>`;
}

function contextSelectors(round: number, live: NonNullable<typeof runtimeProjection.live>, frame: CallFrame): string {
  const frameTime = renderFullTime(frame.created_at, "context-frame-time");
  return `<div class="context-selectors">${contextRoundSelector(round)}${contextFrameSelector(live.frame_catalog || [], frame)}${frameTime ? `<p class="context-frame-created">${t("调用编译时间")} · ${frameTime}</p>` : ""}</div>`;
}

function frameContextUsage(frame: CallFrame | null | undefined, ring = false): string {
  const usage = frame?.context_usage;
  const chars = usage ? formattedInteger(usage.chars) : "—";
  const inputTokens = usage?.state === "pending"
    ? t("等待模型回执")
    : usage?.state === "reported"
      ? formattedInteger(usage.input_tokens)
      : "—";
  const windowTokens = formattedInteger(usage?.window_tokens);
  const ratio = usage?.state === "reported"
    && Number(usage.input_tokens) >= 0
    && Number(usage.window_tokens) > 0
    ? Math.min(1, Number(usage.input_tokens) / Number(usage.window_tokens))
    : 0;
  const stateClass = usage?.state || "unavailable";
  const frameId = frame?.frame_id || t("尚无帧次");
  const models = settingsProjection.data?.model_catalog.models || [];
  const detectedModel = models.find((item) => item.id === frame?.model_profile_id);
  const detected = formattedInteger(
    detectedModel?.detected_context_window,
  );
  const accessible = t("帧 {frame}：字符 {chars}；输入 Tokens {tokens} / {window}；识别容量 {detected}；Runtime 运行上限 {window}", {
    frame: frameId,
    chars,
    tokens: inputTokens,
    window: windowTokens,
    detected,
  });
  const values = ring ? "" : `
    <span class="context-usage-value"><b>${t("字符")}</b> ${escapeHtml(chars)}</span>
    <span class="context-usage-value"><b>Tokens</b> ${escapeHtml(inputTokens)} / ${escapeHtml(windowTokens)}</span>`;
  return `<span class="frame-context-usage ${escapeHtml(stateClass)}" aria-label="${escapeHtml(accessible)}" title="${escapeHtml(accessible)}">
    ${ring ? `<span class="context-usage-ring" style="--context-usage-angle:${escapeHtml((ratio * 360).toFixed(2))}deg" aria-hidden="true"></span>` : ""}${values}
  </span>`;
}

function renderRuntimeContextPage(): string {
  if (runtimeProjection.host !== "connected") return renderRuntimeEmpty(t("上下文"), t("本地宿主未连接"), runtimeProjection.error || t("无法读取运行时投影。"), "runtime");
  const selectedRound = contextSelection();
  if (!selectedRound) return renderRuntimeEmpty(t("上下文十层"), t("尚无上下文投影"), t("十层只从真实轮次账本与实时分层投影读取。"));
  const { round, live } = selectedRound;
  const frameEntry = contextFrameSelection(live);
  if (!frameEntry) return renderRuntimeEmpty(t("帧次"), t("该轮尚无调用帧"), t("上下文十层只从真实帧次输入快照读取。"));
  const detail = runtimeProjection.frameDetail;
  const detailKey = `${round}:${frameEntry.frame_id}`;
  const frame = detail?.round === round && detail.frameId === frameEntry.frame_id ? detail.frame : null;
  if (!frame) {
    const message = runtimeProjection.frameDetailError
      ? `${t("上下文详情读取失败")}：${runtimeProjection.frameDetailError}`
      : runtimeProjection.frameDetailLoading === detailKey ? t("正在读取所选 Frame") : t("请选择 Frame 读取上下文详情");
    return `<section class="runtime-empty"><span class="hud-label">${t("上下文十层")}</span><h2>${escapeHtml(frameEntry.frame_id)}</h2><p>${escapeHtml(message)}</p><button type="button" data-load-frame-detail data-detail-round="${escapeHtml(round)}" data-detail-frame="${escapeHtml(frameEntry.frame_id)}">${t("重试")}</button></section>`;
  }
  const panes = frame.context_panes || [];
  const tab = getActivePageTab("context");
  if (tab === "guide") {
    return `<div class="runtime-context-guide">
      <header class="ledger-title compact"><div><span class="hud-label">${t("上下文十层")} · ${t("轮次")} ${escapeHtml(round)}</span><h2>${escapeHtml(frame.frame_id)}</h2>${frameContextUsage(frame)}</div><div class="context-head-actions"><p>${escapeHtml(panes.length)} ${t("层")} · ${escapeHtml(t("来源 {source}", { source: frame.layer_source || "none" }))}</p>${contextSelectors(round, live, frame)}</div></header>
      <div class="context-guide-scroll" data-stage-scroll-key="${escapeHtml(`context:guide:${round}:${frame.frame_id}`)}">
        <section class="context-instrument-cluster" aria-label="${t("调用头仪表")}">${panes.slice(0, 3).map(renderContextInstrument).join("")}</section>
        <div class="layer-ledger">${panes.slice(3).map((pane) => `
          <button class="layer-ledger-row" data-runtime-pane="${escapeHtml(pane.id)}" aria-label="${escapeHtml(t("查看 {title} 层真实内容", { title: contextPaneLabel(pane.id) }))}">
            <div><b>${escapeHtml(contextPaneLabel(pane.id))}</b><small>${escapeHtml((pane.content_md || pane.content_raw || "").slice(0, 96) || t("空层"))}</small></div><em>${escapeHtml(pane.chars ?? pane.raw_chars ?? 0)}</em>
          </button>
        `).join("")}</div>
      </div>
      <details class="runtime-assembly context-assembly-details"><summary>${t("装配详情")}</summary>${renderRuntimeFrames([frame])}<pre>${escapeHtml(JSON.stringify(frame.manifest || {}, null, 2))}</pre></details>
    </div>`;
  }
  const selected = panes.find((pane) => pane.id === state.activeRuntimePane) || panes[0];
  return `<div class="runtime-context-workspace">
    <nav aria-label="${t("上下文十层")}" data-stage-scroll-key="${escapeHtml(`context:content:${round}:${frame.frame_id}:nav`)}">${panes.map((pane) => `<button class="${pane.id === selected?.id ? "active" : ""}" data-runtime-pane="${escapeHtml(pane.id)}"><b>${escapeHtml(contextPaneLabel(pane.id))}</b><span>${escapeHtml(pane.chars ?? pane.raw_chars ?? 0)}</span></button>`).join("")}</nav>
    ${renderContextDiffRail(panes, round, frame)}
    <article data-stage-scroll-key="${escapeHtml(`context:content:${round}:${frame.frame_id}:${selected?.id || "none"}`)}"><header class="context-detail-head"><div><span class="hud-label">${t("内容详情")} · ${t("轮次")} ${escapeHtml(round)} · ${t("帧次")} ${escapeHtml(frame.frame_id)}</span><h2>${escapeHtml(selected ? contextPaneLabel(selected.id) : t("空层"))}</h2></div>${contextSelectors(round, live, frame)}</header>${renderContextPaneDetail(round, frame, selected)}</article>
  </div>`;
}

const protocolCategoryLabels: Record<string, MessageKey> = {
  permanent: "全文常驻",
  passive_read: "被动只读",
  step_level: "步级",
  periodic: "周期",
  on_demand: "按需",
};

function ledgerCardId(card: ConversationCard | LedgerItem, position: number): string {
  if ("detail_ref" in card && card.detail_ref) return card.detail_ref;
  return String((card as ConversationCard).card_id || card.event_index || `event-${position}`);
}

const providerErrorCopy: Record<ProviderErrorKind, { title: MessageKey; what: MessageKey; action: MessageKey }> = {
  cancelled: { title: "模型调用已停止", what: "本次模型调用已被取消，没有继续等待上游响应。", action: "如需继续，请重新发送；若并非主动停止，请检查宿主是否仍在运行。" },
  connection_refused: { title: "上游节点拒绝连接", what: "UPSP 已到达目标地址，但该地址没有接受连接。", action: "请确认中转站或模型服务已启动，并核对基础地址与端口。" },
  dns_error: { title: "模型服务地址无法解析", what: "系统无法把模型服务域名解析为可连接的地址。", action: "请检查基础地址拼写、DNS 和当前网络连接。" },
  tls_error: { title: "模型服务安全连接失败", what: "与模型服务建立 HTTPS 安全连接时校验证书或握手失败。", action: "请检查系统时间、证书链和服务地址，避免改用不受信任的连接。" },
  timeout: { title: "模型服务响应超时", what: "模型服务未在允许时间内建立响应、返回首字或继续流式输出。", action: "请检查网络和上游负载后重试；持续发生时再调整对应超时配置。" },
  connection_interrupted: { title: "模型服务连接中断", what: "连接已经建立，但在响应完成前被远端或网络中断。", action: "请检查网络与中转站日志后重试。" },
  authentication: { title: "模型服务鉴权失败", what: "模型服务没有接受当前凭据。", action: "请重新保存正确的 API Key，并确认它属于当前服务。" },
  permission_denied: { title: "模型服务拒绝访问", what: "凭据已被识别，但没有执行这次请求所需的权限。", action: "请检查账号、项目和模型访问权限。" },
  model_unavailable: { title: "当前模型不可用", what: "配置的模型不存在、未开放，或当前没有可用上游通道。", action: "请核对模型标识和中转站的可用模型列表。" },
  rate_limit_or_quota: { title: "模型服务限流或额度不足", what: "模型服务因请求频率、额度或余额限制拒绝了调用。", action: "请稍后重试，或检查账号额度、余额和限流策略。" },
  context_too_long: { title: "请求上下文过长", what: "本次请求超过了模型或上游允许的上下文大小。", action: "请缩短输入或历史上下文，或改用支持更长上下文的模型。" },
  endpoint_not_found: { title: "模型服务接口不存在", what: "请求到达了服务，但当前接口路径不存在。", action: "请核对基础地址和协议类型；基础地址不要重复填写请求后缀。" },
  request_rejected: { title: "模型服务拒绝了请求", what: "上游认为本次请求的参数或格式不合法。", action: "请核对模型配置；技术详情中通常会指出具体字段。" },
  upstream_unavailable: { title: "上游模型服务暂不可用", what: "中转站或网关无法从上游模型服务取得有效响应。", action: "请稍后重试，并检查中转站和上游服务状态。" },
  invalid_response: { title: "模型服务响应格式异常", what: "上游返回了无法解析或不完整的 JSON、SSE 或工具调用结果。", action: "请检查中转站兼容性和上游原始响应。" },
  service_error: { title: "模型服务内部错误", what: "模型服务在处理请求时发生了内部错误。", action: "请稍后重试；持续发生时携带技术详情排查上游日志。" },
  unknown: { title: "未识别的模型服务错误", what: "现有信息不足以可靠判断错误类别。", action: "请保留技术详情，并从上游日志和连接配置继续排查。" },
};

function ledgerCardTitle(card: ConversationCard | LedgerItem): string {
  const hint = "provider_error_hint" in card ? card.provider_error_hint : undefined;
  const copy = hint && providerErrorCopy[hint.kind];
  return copy ? t(copy.title) : String(card.title || card.type || card.event_type || "");
}

function ledgerCardMarkdown(card: ConversationCard): string {
  const raw = typeof card.content_md === "string" && card.content_md.trim()
    ? card.content_md
    : `_${t("无可展示的结构化内容。")}_`;
  const hint = card.provider_error_hint;
  const copy = hint && providerErrorCopy[hint.kind];
  if (!hint || !copy) return raw;
  const lines = [
    `**${t("发生了什么")}：** ${t(copy.what)}`,
    `**${t("处理建议")}：** ${t(copy.action)}`,
  ];
  const statuses = (hint.http_statuses || []).filter((status) => Number.isInteger(status));
  if (statuses.length) lines.push(`**${t("错误链")}：** ${statuses.map((status) => `HTTP ${status}`).join(" → ")}`);
  if (hint.target) lines.push(`**${t("目标地址")}：** ${hint.target}`);
  lines.push(`### ${t("技术详情")}`, raw.replace(/\r\n?/g, "\n").split("\n").map((line) => `    ${line}`).join("\n"));
  return lines.join("\n\n");
}

function ledgerBlocking(card: ConversationCard | LedgerItem): boolean {
  const eventType = String(card.event_type || "").toLowerCase();
  return card.severity === "error"
    || card.type === "warning-error"
    || eventType.includes("blocked")
    || eventType.includes("fatal");
}

function ledgerSelection(): { round: number; live: NonNullable<typeof runtimeProjection.live> } | null {
  const rounds = runtimeProjection.conversationRoundOrder;
  if (state.selectedLedgerRound !== null && !runtimeProjection.conversationRounds.has(state.selectedLedgerRound)) {
    state.selectedLedgerRound = null;
  }
  const round = state.selectedLedgerRound ?? runtimeProjection.round ?? rounds.at(-1) ?? null;
  if (round === null) return null;
  const live = round === runtimeProjection.round
    ? runtimeProjection.live
    : runtimeProjection.conversationRounds.get(round) || null;
  return live ? { round, live } : null;
}

function ledgerRoundSelector(selectedRound: number): string {
  const rounds = runtimeProjection.conversationRoundOrder;
  const latest = runtimeProjection.round ?? rounds.at(-1) ?? selectedRound;
  const historical = rounds.filter((round) => round !== latest).reverse();
  return `<label class="protocol-round-select"><span>${t("选择轮次")}</span><select data-ledger-round>
    <option value="latest" ${state.selectedLedgerRound === null ? "selected" : ""}>${t("最新")} · R${escapeHtml(latest)}</option>
    ${historical.map((round) => `<option value="${escapeHtml(round)}" ${state.selectedLedgerRound === round ? "selected" : ""}>R${escapeHtml(round)}</option>`).join("")}
  </select></label>`;
}

function renderProtocolLedger(): string {
  if (runtimeProjection.host !== "connected") {
    return renderRuntimeEmpty(t("运行时"), t("本地宿主未连接"), runtimeProjection.error || t("无法读取轮次账本。"), "runtime");
  }
  const selected = ledgerSelection();
  if (!selected) return renderRuntimeEmpty(t("运行时"), t("尚无动态账本"), t("事件、工具与结算只在真实轮次账本出现后展示。"));
  const { round, live } = selected;
  const items = runtimeProjection.ledgerItems.get(round) || [];
  const lifecycle = live.round_lifecycle || {};
  const blockingCount = items.filter(ledgerBlocking).length
    + (lifecycle.fatal_reasons || []).length
    + (lifecycle.degraded_reasons || []).length;
  return `<section class="protocol-center protocol-ledger" data-stage-scroll-key="${escapeHtml(`audit:ledger:${round}`)}">
    <header class="protocol-center-head">
      <div><span class="hud-label">${t("运行时")} · ${t("轮次")} ${escapeHtml(round)}</span><h2>${t("动态账本")}</h2><p>${t("按运行时原投影顺序列出 {count} 个结构化事件；正文仅在详情弹窗中读取。", { count: items.length })}</p></div>
      <div class="protocol-head-actions">${ledgerRoundSelector(round)}<button class="evidence-export" type="button" data-export-evidence>${t("导出当前证据")}</button></div>
    </header>
    <dl class="protocol-summary">
      <div><dt>${t("轮次")}</dt><dd>R${escapeHtml(round)}</dd></div>
      <div><dt>${t("生命周期")}</dt><dd>${escapeHtml(runtimeTerm(lifecycle.state || "running"))}</dd></div>
      <div><dt>${t("结算")}</dt><dd>${escapeHtml(runtimeTerm(lifecycle.settlement_status || "pending"))}</dd></div>
      <div class="${blockingCount ? "warn" : ""}"><dt>${t("阻塞项")}</dt><dd>${escapeHtml(blockingCount)}</dd></div>
    </dl>
    <p class="export-feedback" role="status">${escapeHtml(runtimeProjection.exportFeedback)}</p>
    <div class="protocol-index" role="list" aria-label="${escapeHtml(t("轮次 {round} 事件目录", { round }))}">
      ${renderLedgerDirectory(round, items, t("该轮没有结构化事件。"), `audit:ledger:${round}:items`)}
    </div>
  </section>`;
}

function protocolEntryRow(entry: ProtocolCatalogEntry, detail: string, tags: string[]): string {
  return `<button class="protocol-index-row" type="button" data-protocol-document data-protocol-kind="${entry.kind}" data-protocol-id="${escapeHtml(entry.id)}">
    <span class="protocol-index-main"><b>${escapeHtml(entry.description || entry.file)}</b><small>${escapeHtml(detail || entry.source_ref)}</small></span>
    <span class="protocol-index-tags">${tags.map((tag) => `<em>${escapeHtml(tag)}</em>`).join("")}</span>
  </button>`;
}

function renderProtocolCatalog(kind: "rules" | "docs"): string {
  if (protocolProjection.loading && !protocolProjection.catalog) {
    return renderRuntimeEmpty(kind.toUpperCase(), t("正在读取协议目录"), t("目录只从位格登记表生成。"));
  }
  if (protocolProjection.error || !protocolProjection.catalog) {
    return `<section class="runtime-empty"><span class="hud-label">${kind.toUpperCase()}</span><h2>${t("协议目录不可用")}</h2><p>${escapeHtml(protocolProjection.error || t("宿主没有返回协议目录。"))}</p><button type="button" data-retry-protocol-catalog>${t("只读重试")}</button></section>`;
  }
  const catalog = protocolProjection.catalog;
  if (kind === "rules") {
    return `<section class="protocol-center" data-stage-scroll-key="audit:rules">
      <header class="protocol-center-head"><div><span class="hud-label">${t("规则")} · ${t("登记表")}</span><h2>${t("规则")}</h2><p>${t("{count} 项注册规则；分类与装载口径按登记表原序展示。", { count: catalog.rules.total })}</p></div><p class="registry-history">${t("登记表历史版本")} · ${escapeHtml(catalog.rules.registry_version || t("未标记"))}</p></header>
      <div class="protocol-index grouped">${catalog.rules.categories.map((category) => `<section class="protocol-index-group"><header><b>${escapeHtml(protocolCategoryLabels[category.id] ? t(protocolCategoryLabels[category.id]) : category.id)}</b><span>${escapeHtml(category.id)} · ${escapeHtml(category.count)} ${t("项")}</span></header>${category.entries.length ? category.entries.map((entry) => protocolEntryRow(entry, entry.trigger || entry.load || entry.source_ref, [entry.layer || category.id])).join("") : `<p class="protocol-index-empty">${t("当前分类为空。")}</p>`}</section>`).join("")}</div>
    </section>`;
  }
  return `<section class="protocol-center" data-stage-scroll-key="audit:docs">
    <header class="protocol-center-head"><div><span class="hud-label">${t("文档")} · ${t("登记表")}</span><h2>${t("文档")}</h2><p>${t("{registrations} 条用途登记，按安全相对路径合并为 {count} 份正文。", { registrations: catalog.docs.registrations, count: catalog.docs.total })}</p></div><p class="registry-history">${t("登记表历史版本")} · ${escapeHtml(catalog.docs.registry_version || t("未标记"))}</p></header>
    <div class="protocol-index">${catalog.docs.entries.map((entry) => protocolEntryRow(entry, entry.source_ref, entry.categories || [])).join("")}</div>
  </section>`;
}

function renderRuntimeAuditPage(): string {
  const tab = getActivePageTab("audit");
  if (tab === "rules") return renderProtocolCatalog("rules");
  if (tab === "docs") return renderProtocolCatalog("docs");
  return renderProtocolLedger();
}

function renderRunPage(): string {
  return renderRuntimeRunPage();
}

function renderContextPage(): string {
  return renderRuntimeContextPage();
}

function personaValue(path: string): unknown {
  return personaProjection.state?.fields.find((field) => field.path === path)?.value;
}

function personaNumber(path: string): number | null {
  const value = personaValue(path);
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function personaGroupLabel(group: string): string {
  const key = personaGroupLabels[group];
  return key ? t(key) : t("未知");
}

function personaFieldLabel(field: PersonaStateField): string {
  const parts = field.path.split(".").slice(1);
  const group = parts[0] || "";
  let detail = parts.slice(1);
  if (group === "dynamic_axes" && detail.at(-1) === "value") detail = detail.slice(0, -1);
  const labels = detail.map((part) => {
    const key = personaFieldLabels[part];
    return key ? t(key) : t("未知");
  });
  return [personaGroupLabel(group), ...labels].join(" · ");
}

function personaRawValue(value: unknown): string {
  if (value !== null && typeof value === "object") return renderStructuredJson(value);
  const source = value === undefined ? "undefined" : JSON.stringify(value);
  return `<code class="persona-state-raw">${escapeHtml(source)}</code>`;
}

function personaObservedAt(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(state.locale, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

function personaVital(label: MessageKey, path: string, description: string): string {
  const value = personaNumber(path);
  const position = value === null ? 0 : Math.max(0, Math.min(100, (value + 100) / 2));
  const display = value === null
    ? "—"
    : `${value > 0 ? "+" : ""}${new Intl.NumberFormat(state.locale, { maximumFractionDigits: 1 }).format(value)}`;
  return `<div class="persona-vital">
    <span>${t(label)}：${escapeHtml(description)}</span>
    <strong>${escapeHtml(display)}</strong>
    <div class="persona-vital-track" role="img" aria-label="${escapeHtml(`${t(label)} ${description} ${display}`)}"><i style="--value:${position}%"></i></div>
  </div>`;
}

function renderLedgerDirectory(round: number, items: LedgerItem[], emptyText: string, key: string): string {
  if (!items.length) return `<p class="runtime-empty-copy">${escapeHtml(emptyText)}</p>`;
  return `<div class="protocol-index" data-stage-scroll-key="${escapeHtml(key)}" role="list">${items.map((item, position) => {
    const ref = ledgerCardId(item, position);
    const warn = ledgerBlocking(item);
    const time = String(item.recorded_at || "").replace("T", " ").slice(0, 19) || t("未记录");
    return `<button class="protocol-index-row ledger-event ${warn ? "warn" : ""}" type="button" role="listitem" data-ledger-event data-ledger-round="${escapeHtml(round)}" data-ledger-card-id="${escapeHtml(ref)}">
      <span class="protocol-index-seq">#${escapeHtml(item.event_index ?? position)}</span>
      <span class="protocol-index-main"><b>${escapeHtml(ledgerCardTitle(item))}</b><small>${escapeHtml(time)} · ${escapeHtml(runtimeTerm(item.phase || "round"))} · ${escapeHtml(item.frame_id || "round")}</small></span>
      <span class="protocol-index-tags"><em>${escapeHtml(item.type || "event")}</em>${item.severity ? `<em>${escapeHtml(item.severity)}</em>` : ""}</span>
    </button>`;
  }).join("")}</div>`;
}

function personaMeter(label: MessageKey, value: number | null, display: string): string {
  const position = value === null ? 0 : Math.max(0, Math.min(100, value));
  return `<div class="persona-meter">
    <span>${t(label)}</span>
    <strong>${escapeHtml(display)}</strong>
    <div class="persona-meter-track"><i style="--value:${position}%"></i></div>
  </div>`;
}

function renderPersonaStateGroup(group: string, fields: PersonaStateField[]): string {
  return `<details class="persona-state-group" data-persona-state-group="${escapeHtml(group)}">
    <summary><span>${escapeHtml(personaGroupLabel(group))}</span><code>base.${escapeHtml(group)}</code><em>${fields.length}</em></summary>
    <div class="persona-state-table-scroll"><table class="persona-state-table">
      <thead><tr><th scope="col">${t("字段")}</th><th scope="col">${t("值")}</th></tr></thead>
      <tbody>${fields.map((field) => `<tr><th scope="row"><span>${escapeHtml(personaFieldLabel(field))}</span><code>${escapeHtml(field.path)}</code></th><td>${personaRawValue(field.value)}</td></tr>`).join("")}</tbody>
    </table></div>
  </details>`;
}

function personaCoreSection(markdown: string, section: number): string {
  const lines = markdown.replaceAll("\r\n", "\n").split("\n");
  const start = lines.findIndex((line) => new RegExp(`^##\\s+${section}\\.`).test(line));
  if (start < 0) return "";
  const next = lines.findIndex((line, index) => index > start && /^##\s+/.test(line));
  return lines.slice(start + 1, next < 0 ? undefined : next).join("\n").trim();
}

function personaCorePairs(section: string): Array<[string, string]> {
  return section.split("\n").flatMap((line) => {
    const match = line.trim().match(/^([^：:\n]+)[：:]\s*(.+)$/);
    return match ? [[match[1].trim(), match[2].trim()] as [string, string]] : [];
  });
}

function personaCorePair(pairs: Array<[string, string]>, label: string): string {
  return pairs.find(([key]) => key.replaceAll(" ", "").includes(label))?.[1] || "—";
}

function personaRoleLabel(value: string): string {
  const match = value.match(/^(.+?)\s*[（(]([^）)]+)[）)]\s*$/);
  return match ? (state.locale === "en-US" ? match[2] : match[1]).trim() : value;
}

function personaCoreAxes(section: string): Array<{
  title: string;
  leftCode: string;
  leftValue: number;
  rightCode: string;
  rightValue: number;
}> {
  return section.split(/^###\s+/m).slice(1).flatMap((block) => {
    const lines = block.trim().split("\n");
    const match = lines.find((line) => /^定位[：:]/.test(line.trim()))?.match(
      /^定位[：:]\s*([A-Z])\s+(\d+(?:\.\d+)?)%\s*\/\s*([A-Z])\s+(\d+(?:\.\d+)?)%/,
    );
    if (!match) return [];
    return [{
      title: (lines[0] || "").replace(/^[①②③④⑤⑥]\s*/, "").replace(/（.*$/, "").trim(),
      leftCode: match[1],
      leftValue: Number(match[2]),
      rightCode: match[3],
      rightValue: Number(match[4]),
    }];
  });
}

function renderPersonaCore(): string {
  const projection = personaProjection;
  if (projection.coreLoading && !projection.core) {
    return `<section class="persona-source-empty" data-persona-view="core"><p>${t("正在读取位格核心档案")}</p></section>`;
  }
  if (!projection.core) {
    return `<section class="persona-source-empty" data-persona-view="core">
      <h2>${t("核心档案暂不可用")}</h2>
      <p class="runtime-error">${escapeHtml(projection.coreError || t("核心档案读取失败"))}</p>
      <button type="button" data-retry-persona-core>${t("只读重试")}</button>
    </section>`;
  }
  const content = projection.core.content_md;
  const identity = personaCorePairs(personaCoreSection(content, 1));
  const roles = personaCorePairs(personaCoreSection(content, 2))
    .filter(([label]) => /^角色\d+$/.test(label))
    .map(([, value]) => personaRoleLabel(value));
  const axes = personaCoreAxes(personaCoreSection(content, 3));
  const codeLine = personaCoreSection(content, 4).split("\n").find(Boolean)?.trim() || "—";
  const code = codeLine.match(/（([^）]+)）/)?.[1] || codeLine;
  const declaration = personaCoreSection(content, 6);
  const displayName = personaCorePair(identity, "中文名");
  const englishName = personaCorePair(identity, "英文名");
  const shortName = personaCorePair(identity, "缩写");

  return `<section class="persona-core-page" data-persona-view="core">
    <header class="persona-state-head persona-core-head">
      <div><span class="hud-label">${t("核心档案")}</span><h2>${t("核心档案")}</h2></div>
      <div><code>${escapeHtml(projection.core.source_ref)}</code><span>${t("真源")} · ${t("只读")}</span></div>
    </header>
    ${projection.coreError ? `<p class="runtime-error">${escapeHtml(projection.coreError)}</p>` : ""}
    <section class="persona-id-card" aria-label="${t("核心档案")}">
      <div class="persona-emblem" aria-hidden="true"><span>${escapeHtml(shortName)}</span></div>
      <dl class="persona-id-fields">
        <div><dt>${t("中文名")}</dt><dd>${escapeHtml(displayName)}</dd></div>
        <div><dt>${t("英文名")}</dt><dd>${escapeHtml(englishName)}</dd></div>
        <div><dt>${t("缩写")}</dt><dd>${escapeHtml(shortName)}</dd></div>
        <div><dt>PID</dt><dd>${escapeHtml(personaCorePair(identity, "PID"))}</dd></div>
        <div class="persona-id-field-wide"><dt>${t("位格编码")}</dt><dd><strong>${escapeHtml(code)}</strong></dd></div>
        <div class="persona-id-field-wide"><dt>${t("社会定位")}</dt><dd>${roles.length ? `<ol class="persona-role-list">${roles.map((role) => `<li>${escapeHtml(role)}</li>`).join("")}</ol>` : "—"}</dd></div>
      </dl>
    </section>
    ${axes.length ? `<section class="persona-core-axes" aria-label="${t("核心六轴")}">${axes.map((axis) => `
      <article class="persona-core-axis">
        <header><b>${escapeHtml(axis.title)}</b><code>${escapeHtml(axis.leftCode)}↔${escapeHtml(axis.rightCode)}</code></header>
        <div class="persona-core-axis-readout"><strong>${escapeHtml(axis.leftValue)}%</strong><span>/</span><strong>${escapeHtml(axis.rightValue)}%</strong></div>
        <div class="persona-core-axis-track" style="--left:${Math.max(0, Math.min(100, axis.leftValue))}%" role="img" aria-label="${escapeHtml(`${axis.title} ${axis.leftCode} ${axis.leftValue}% / ${axis.rightCode} ${axis.rightValue}%`)}"><i></i><i></i></div>
        <footer><span>${escapeHtml(axis.leftCode)}</span><span>${escapeHtml(axis.rightCode)}</span></footer>
      </article>`).join("")}</section>` : `<p class="runtime-error">${t("核心六轴未能从真源识别；请核对完整档案。")}</p>`}
    <section class="persona-core-declaration"><h3>${t("主体自述")}</h3>${declaration ? renderMarkdownDocument("persona:core:declaration", declaration) : `<p>—</p>`}</section>
    <details class="persona-state-all persona-core-source">
      <summary><span>${t("完整核心档案")}</span><em>${t("Markdown 真源")}</em></summary>
      <article>${renderMarkdownDocument("persona:core:source", content)}</article>
    </details>
  </section>`;
}

function renderPersonaState(): string {
  const projection = personaProjection;
  if (projection.stateLoading && !projection.state) {
    return `<section class="persona-source-empty" data-persona-view="state"><p>${t("正在读取生命状态")}</p></section>`;
  }
  if (!projection.state) {
    return `<section class="persona-source-empty" data-persona-view="state">
      <h2>${t("生命状态暂不可用")}</h2>
      <p class="runtime-error">${escapeHtml(projection.stateError || t("生命状态读取失败"))}</p>
      <button type="button" data-retry-persona-state>${t("只读重试")}</button>
    </section>`;
  }

  const fields = projection.state.fields;
  const groups = new Map<string, PersonaStateField[]>();
  fields.forEach((field) => {
    const group = field.path.split(".")[1] || "unknown";
    groups.set(group, [...(groups.get(group) || []), field]);
  });
  const totalRound = personaValue("base.meta.total_round");
  const phase = runtimeTerm(personaValue("base.runtime.phase"), t("未知"));
  const activity = runtimeTerm(personaValue("base.activity_mode"), t("未知"));
  const sleep = runtimeTerm(personaValue("base.sleep_state.level"), t("未知"));
  const identityConfirmed = personaValue("base.identity.confirmed") === true;
  const identityAnchor = personaValue("base.identity.current_relation_id")
    || personaValue("base.identity.current_declared_name")
    || personaValue("base.identity.local_default_relation_id")
    || t("未绑定");
  const identitySource = runtimeTerm(personaValue("base.identity.current_source"), t("未知"));
  const activeFlags = fields.filter((field) => (
    field.path.startsWith("base.heartbeat_flags.") && field.value === true
  ));
  const workhood = personaNumber("base.workhood_index.value");
  const fatigue = personaNumber("base.fatigue.value");
  const usageRatio = personaNumber("base.token_usage.usage_ratio");
  const percent = usageRatio === null ? null : usageRatio * 100;
  const dynamicDescription = (axis: string) => projection.state?.dynamic_descriptions[axis] || t("未投影");
  const number = (value: number | null, suffix = "") => value === null
    ? "—"
    : `${new Intl.NumberFormat(state.locale, { maximumFractionDigits: 1 }).format(value)}${suffix}`;

  return `<section class="persona-state-page" data-persona-view="state">
    <header class="persona-state-head">
      <div><span class="hud-label">${t("生命状态")}</span><h2>${t("生命状态")}</h2><p>${t("状态真源只读投影；不进行补缺或写回。")}</p></div>
      <div><code>${escapeHtml(projection.state.source_ref)}</code><span>${t("读取于 {time}", { time: personaObservedAt(projection.state.observed_at) })}</span></div>
    </header>
    ${projection.stateStale && projection.stateError ? `<p class="runtime-error persona-stale-notice">${escapeHtml(projection.stateError)} · ${t("显示的是上次成功读取的状态")}</p>` : ""}
    <div class="persona-state-quick">
      ${metric(t("总轮次"), totalRound ?? "—", t("轮次"), "hot")}
      ${metric(t("运行阶段"), phase, t("运行状态"), "")}
      ${metric(t("活动模式"), activity, t("当前"), "")}
      ${metric(t("睡眠状态"), sleep, t("当前"), "")}
    </div>
    <section class="persona-vitals" aria-label="${t("动态轴")}">
      ${personaVital("效价", "base.dynamic_axes.valence.value", dynamicDescription("valence"))}
      ${personaVital("唤醒度", "base.dynamic_axes.arousal.value", dynamicDescription("arousal"))}
      ${personaVital("聚焦", "base.dynamic_axes.focus.value", dynamicDescription("focus"))}
      ${personaVital("心境", "base.dynamic_axes.mood.value", dynamicDescription("mood"))}
      ${personaVital("幽默", "base.dynamic_axes.humor.value", dynamicDescription("humor"))}
      ${personaVital("安全", "base.dynamic_axes.safety.value", dynamicDescription("safety"))}
    </section>
    <section class="persona-meters">
      ${personaMeter("工化指数", workhood, number(workhood))}
      ${personaMeter("疲劳", fatigue, number(fatigue))}
      ${personaMeter("令牌用量", percent, number(percent, "%"))}
    </section>
    <dl class="persona-identity-strip">
      <div><dt>${t("身份锚点")}</dt><dd>${escapeHtml(identityAnchor)}</dd></div>
      <div><dt>${t("确认状态")}</dt><dd>${t(identityConfirmed ? "已确认" : "未确认")}</dd></div>
      <div><dt>${t("当前来源")}</dt><dd>${escapeHtml(identitySource)}</dd></div>
      <div><dt>${t("上次状态结算标识")}</dt><dd>${escapeHtml(personaValue("base.meta.last_state_settlement_id") ?? t("无"))}</dd></div>
    </dl>
    <section class="persona-active-flags">
      <h3>${t("活动旗标")}</h3>
      ${activeFlags.length
        ? `<div>${activeFlags.map((field) => `<span>${escapeHtml(personaFieldLabel(field))}</span>`).join("")}</div>`
        : `<p>${t("当前没有置位的心跳旗标。")}</p>`}
    </section>
    <details class="persona-state-all">
      <summary><span>${t("完整状态")}</span><em>${t("共 {count} 个登记字段", { count: fields.length })}</em></summary>
      <div class="persona-state-groups">${[...groups.entries()].map(([group, entries]) => renderPersonaStateGroup(group, entries)).join("")}</div>
    </details>
  </section>`;
}

function renderPersonaPage(): string {
  return getActivePageTab("persona") === "state"
    ? renderPersonaState()
    : renderPersonaCore();
}

function renderMemoryPage(): string {
  const unavailable = renderDepositionUnavailable("MEMORY / READONLY");
  if (unavailable) return unavailable;
  const tab = getActivePageTab("mem");
  let items = depositionItems("memory");
  if (tab === "stm") items = items.filter((item) => item.stm_present === true || item.memory_layer === "STM");
  if (tab === "ltm") items = items.filter((item) => Boolean(item.ltm_layer) || (item.memory_layer || "").startsWith("LTM/"));
  if (tab === "mounts") items = items.filter((item) => (item.linked_containers || []).length);
  if (tab === "search") {
    const query = state.memoryQuery.trim().toLocaleLowerCase("zh-CN");
    if (query) {
      items = items.filter((item) => [
        item.id,
        item.title,
        item.subject,
        item.current_overview,
        ...(item.tags || []),
        ...(item.linked_containers || []),
      ].some((value) => String(value || "").toLocaleLowerCase("zh-CN").includes(query)));
    }
  }
  const emptyLabel = tab === "stm" ? "STM" : tab === "ltm" ? "LTM" : tab === "mounts" ? "MOUNT" : "MEMORY";
  const search = tab === "search" ? `
    <label class="deposition-search"><span>${t("搜索记忆条目")}</span><input data-memory-search type="search" value="${escapeHtml(state.memoryQuery)}" placeholder="${t("ID、标题、标签或容器")}" autocomplete="off"></label>
  ` : "";
  return `
    <div class="deposition-workspace memory-index-only" aria-busy="${depositionProjection.loading ? "true" : "false"}">
      <nav class="deposition-master" aria-label="${t("记忆条目列表")}">
        <header><span class="hud-label">${escapeHtml(emptyLabel)}</span><strong>${items.length} ${t("条")}</strong></header>
        ${search}
        <div class="deposition-list" data-stage-scroll-key="${escapeHtml(`mem:${tab}:${state.memoryQuery.trim()}`)}">${items.length ? items.map((item) => depositionRow(
          "memory",
          item,
          false,
          item.current_overview || (item.tags || []).join(" / ") || item.id,
          `W${item.weight ?? "?"}`,
        )).join("") : renderDepositionEmpty(emptyLabel, t("没有记忆条目"), tab === "search" && state.memoryQuery ? t("当前查询没有匹配条目。") : t("该层当前没有可投影的记忆条目；隐私条目不会显示。"))}</div>
      </nav>
    </div>
  `;
}

function renderRelationsPage(): string {
  const unavailable = renderDepositionUnavailable("RELATION / READONLY");
  if (unavailable) return unavailable;
  const items = depositionItems("relation");
  const selected = selectDepositionItem("relation", items);
  const detail = selected ? depositionDetail("relation", selected.id) : null;
  const axes = detail?.axes || {};
  const axisLabels: Record<string, MessageKey> = { trust: "信任", safety: "安心", value: "重视", investment: "投入", honesty: "坦诚", resonance: "共振" };
  return `
    <div class="deposition-workspace">
      <nav class="deposition-master" aria-label="${t("活动关系卡列表")}">
        <header><span class="hud-label">${t("关系域")}</span><strong>${items.length} ${t("张")}</strong></header>
        <div class="deposition-list" data-stage-scroll-key="relations:list">${items.map((item) => depositionRow("relation", item, item.id === selected?.id, item.id, item.status)).join("")}</div>
      </nav>
      <section class="deposition-detail" aria-live="polite" data-stage-scroll-key="${escapeHtml(`relations:detail:${selected?.id || "none"}`)}">
        ${!selected ? renderDepositionEmpty(t("关系域"), t("没有活动关系卡"), t("当前关系登记表没有活动卡片。")) : `
          <header class="ledger-title compact"><div><span class="hud-label">${escapeHtml(selected.category || "REL")} / ACTIVE CARD</span><h2>${escapeHtml(detail?.name || selected.name || selected.id)}</h2></div><p>${escapeHtml(selected.id)}</p></header>
          ${detail ? `
            <div class="relation-axis-grid">${Object.entries(axisLabels).map(([axis, label]) => `<div><span>${escapeHtml(t(label))}</span><strong>${Number(axes[axis] || 0) >= 0 ? "+" : ""}${escapeHtml(axes[axis] || 0)}</strong></div>`).join("")}</div>
            <section class="deposition-notes"><span class="hud-label">${t("关系笔记")}</span>${(detail.notes || []).length ? (detail.notes || []).map((note) => `<article><time>${escapeHtml(note.date || t("未标日期"))}</time><p>${escapeHtml(note.content)}</p></article>`).join("") : `<p class="runtime-empty-copy">${t("没有关系笔记。")}</p>`}</section>
          ` : depositionDetailStatus("relation", selected.id, t("正在读取关系卡…"))}
        `}
      </section>
    </div>
  `;
}

function renderContainersPage(): string {
  const unavailable = renderDepositionUnavailable("CONTAINER / CONTROLLED");
  if (unavailable) return unavailable;
  const tab = getActivePageTab("containers");
  const prefix = { project: "PRJ", dc: "DC", ec: "EC", skl: "SKL" }[tab];
  const items = prefix ? depositionItems("container").filter((item) => item.prefix === prefix) : [];
  const selected = selectDepositionItem("container", items);
  const detail = selected ? depositionDetail("container", selected.id) : null;
  return `
    <div class="deposition-workspace">
      <nav class="deposition-master" aria-label="${t("已登记容器列表")}">
        <header><span class="hud-label">${escapeHtml(prefix || "CAND")}</span><strong>${items.length} ${t("个")}</strong></header>
        <div class="deposition-list" data-stage-scroll-key="${escapeHtml(`containers:${tab}:list`)}">${items.map((item) => depositionRow("container", item, item.id === selected?.id, item.id, item.status)).join("")}</div>
      </nav>
      <section class="deposition-detail" aria-live="polite" data-stage-scroll-key="${escapeHtml(`containers:${tab}:detail:${selected?.id || "none"}`)}">
        ${!selected ? renderDepositionEmpty(prefix || "CANDIDATES", tab === "candidates" ? t("候选容器未接入") : t("没有 {prefix} 容器", { prefix: prefix || "" }), tab === "candidates" ? t("只读取正式登记表实例；候选集合仍保持延期。") : t("当前登记表没有该类型实例，这是正常空态。")) : `
          <header class="ledger-title compact"><div><span class="hud-label">${escapeHtml(selected.prefix)} / REGISTERED</span><h2>${escapeHtml(selected.title || selected.id)}</h2></div><p>${escapeHtml(selected.id)}</p></header>
          <dl class="container-facts">
            <div><dt>${t("状态")}</dt><dd>${escapeHtml(runtimeTerm(selected.status || t("未知")))}</dd></div>
            <div><dt>${t("目标")}</dt><dd>${escapeHtml(detail?.default_target || t("读取中"))}</dd></div>
          </dl>
          ${detail ? `<pre class="deposition-content">${escapeHtml(detail.content || t("容器正文为空。"))}</pre>${detail.content_truncated ? `<p class="deposition-source-note">${t("正文超过 64 KiB，当前只读投影已明确截断。")}</p>` : ""}` : depositionDetailStatus("container", selected.id, t("正在读取容器正文…"))}
          <section class="deposition-refs"><span class="hud-label">${t("记忆引用")}</span>${selected.entries?.length ? selected.entries.map((entry) => `<div>${entry.mem_id ? depositionJump("memory", entry.mem_id) : `<em>${t("无 MEM 引用")}</em>`}<span>${escapeHtml(entry.title || entry.target_file || t("已登记条目"))}</span><code>${entry.round == null ? "" : `R${escapeHtml(entry.round)}`}</code></div>`).join("") : `<p class="runtime-empty-copy">${t("没有已登记的记忆引用。")}</p>`}</section>
        `}
      </section>
    </div>
  `;
}

function renderAuditPage(): string {
  return renderRuntimeAuditPage();
}

type SettingKind = "string" | "int" | "float" | "bool" | "enum";
interface SettingFieldSpec {
  key: string;
  label: MessageKey;
  kind: SettingKind;
  description?: MessageKey;
  wide?: boolean;
  min?: number;
  max?: number;
  step?: number;
  options?: ReadonlyArray<readonly [string, MessageKey | null]>;
}

const runtimeSettingFields: SettingFieldSpec[] = [
  {
    key: "response_anchor.prompt",
    label: "回答锚点",
    kind: "string",
    max: 512,
    wide: true,
    description: "示例：使用用户当前语言；先给结论，再给必要证据。",
  },
  { key: "heartbeat.interval", label: "心跳间隔", kind: "int", min: 1, max: 3600 },
  { key: "round.reminder_seconds", label: "反应提醒时间（秒）", kind: "int", min: 60, max: 86400 },
  { key: "round.warning_seconds", label: "反应警告时间（秒）", kind: "int", min: 60, max: 172800 },
  { key: "round.auto_relay_seconds", label: "自动中继时间（秒）", kind: "int", min: 60, max: 259200 },
  { key: "rhythm.period", label: "节律周期", kind: "int", min: 1, max: 100000 },
  { key: "standby.idle_threshold_min", label: "待机阈值", kind: "int", min: 1, max: 10080 },
  { key: "audit.round_snapshot_retention", label: "轮次快照保留量", kind: "int", min: 1, max: 64 },
  { key: "audit.round_snapshot_max_mib", label: "轮次快照总上限（MiB）", kind: "int", min: 1, max: 4096 },
  { key: "audit.state_backup_retention", label: "状态备份保留量", kind: "int", min: 1, max: 4096 },
  { key: "general_tools.file_read_window_chars", label: "文件读取窗口", kind: "int", min: 1, max: 16777216 },
  { key: "general_tools.web_fetch_window_chars", label: "网页读取窗口", kind: "int", min: 1, max: 16777216 },
  { key: "general_tools.web_search_window_results", label: "搜索结果窗口", kind: "int", min: 1, max: 1000 },
];

type ContextSettingsFileId = "memory" | "lately" | "periodic" | "high_freq" | "relation";

const contextSettingFields: Record<ContextSettingsFileId, SettingFieldSpec[]> = {
  memory: [
    { key: "heat.zone_thresholds.significant", label: "显著区阈值", kind: "int", min: 1, max: 100 },
    { key: "heat.zone_thresholds.uncertain", label: "不确定区阈值", kind: "int", min: 0, max: 99 },
    { key: "heat.decay_rates.significant", label: "显著区每轮衰减", kind: "int", min: -100, max: 0 },
    { key: "heat.decay_rates.uncertain", label: "不确定区每轮衰减", kind: "int", min: -100, max: 0 },
    { key: "heat.decay_rates.decay", label: "衰减区每轮衰减", kind: "int", min: -100, max: 0 },
    { key: "heat.initial_by_weight.1", label: "权重一初始热度", kind: "int", min: 0, max: 100 },
    { key: "heat.initial_by_weight.2", label: "权重二初始热度", kind: "int", min: 0, max: 100 },
    { key: "heat.initial_by_weight.3", label: "权重三初始热度", kind: "int", min: 0, max: 100 },
    { key: "heat.initial_by_weight.4", label: "权重四初始热度", kind: "int", min: 0, max: 100 },
    { key: "heat.initial_by_weight.5", label: "权重五初始热度", kind: "int", min: 0, max: 100 },
    { key: "heat.recall_boost", label: "召回增益", kind: "int", min: 0, max: 100 },
    { key: "heat.upgrade_high_rounds", label: "高热升格轮数", kind: "int", min: 1, max: 100000 },
    { key: "heat.locked_value", label: "热度锁定值", kind: "int", min: 0, max: 100 },
  ],
  lately: [
    { key: "pressure_ratio", label: "压缩触发比例", kind: "float", min: 0.50, max: 0.99, step: 0.01 },
    { key: "protected_interaction_count", label: "保护最近交互数", kind: "int", min: 0, max: 128 },
    { key: "semantic_summary_ratio", label: "分片摘要上限比例", kind: "float", min: 0.01, max: 0.50, step: 0.005 },
    { key: "cycle_target_ratio", label: "单周期目标比例", kind: "float", min: 0.05, max: 0.80, step: 0.01 },
    { key: "batch_source_chars", label: "单帧处理字符上限", kind: "int", min: 1024, max: 262144 },
  ],
  periodic: [
    { key: "limits.periodic_memory_items_chars", label: "定期记忆条目上限", kind: "int", min: 1, max: 16777216 },
  ],
  high_freq: [
    { key: "content_limits.reference_window_chars", label: "高频引用窗口", kind: "int", min: 1, max: 16777216 },
    { key: "index_display_limits.container_index", label: "容器索引显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.ltm_heat_index", label: "长期记忆热度显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.stm_heat_index", label: "短期记忆热度显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.skills_inverted", label: "技能倒排显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.relation_inverted", label: "关系倒排显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.relation_domain", label: "关系域显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.ltm_inverted", label: "长期记忆倒排显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.stm_inverted", label: "短期记忆倒排显示量", kind: "int", min: 1, max: 1000 },
    { key: "index_display_limits.association_index", label: "联想索引显示量", kind: "int", min: 1, max: 1000 },
  ],
  relation: [
    { key: "relation_context.max_slots", label: "关系在场槽位", kind: "int", min: 1, max: 32 },
  ],
};

function renderSettingField(field: SettingFieldSpec, value: SettingValue): string {
  const common = `data-setting-key="${escapeHtml(field.key)}" data-setting-kind="${field.kind}"`;
  let control = "";
  if (field.kind === "bool") {
    control = `<label class="settings-switch"><input type="checkbox" ${common} ${value ? "checked" : ""}><span>${t("启用")}</span></label>`;
  } else if (field.kind === "enum") {
    control = `<select ${common}>${(field.options || []).map(([option, label]) => `<option value="${escapeHtml(option)}" ${String(value) === option ? "selected" : ""}>${escapeHtml(label ? t(label) : option || t("系统默认"))}</option>`).join("")}</select>`;
  } else {
    const numeric = field.kind === "int" || field.kind === "float";
    const attributes = numeric
      ? `type="number" ${field.min == null ? "" : `min="${field.min}"`} ${field.max == null ? "" : `max="${field.max}"`} step="${field.step ?? (field.kind === "int" ? 1 : "any")}"`
      : `type="text" ${field.max == null ? "" : `maxlength="${field.max}"`}`;
    control = `<input ${attributes} ${common} value="${escapeHtml(value)}">`;
  }
  return `<label class="settings-field${field.wide ? " settings-source" : ""}"><span><b>${t(field.label)}</b></span>${control}${field.description ? `<small>${t(field.description)}</small>` : ""}</label>`;
}

function settingsFeedback(): string {
  const message = settingsProjection.error || settingsProjection.feedback;
  return `<p class="settings-feedback ${settingsProjection.error ? "warn" : ""}" data-settings-feedback role="status" ${message ? "" : "hidden"}>${escapeHtml(message)}</p>`;
}

function settingsActions(): string {
  const pending = settingsProjection.pending ? "disabled" : "";
  return `<footer class="settings-actions"><span>${t("保存到现有配置")}</span><button type="submit" class="primary-action" ${pending}>${settingsProjection.pending ? t("保存中") : t("保存设置")}</button></footer>`;
}

function settingsForm(fileId: SettingsFileId, title: MessageKey, description: MessageKey, body: string): string {
  return `<section class="ledger-panel settings-panel">
    <header class="ledger-title"><h2>${t(title)}</h2><p>${t(description)}</p></header>
    <form data-settings-form="${fileId}">${body}${settingsActions()}</form>
  </section>`;
}

function renderRuntimeSettings(values: Record<string, SettingValue>): string {
  const responseAnchor = runtimeSettingFields[0];
  const primary = runtimeSettingFields.slice(1, 7);
  const advanced = runtimeSettingFields.slice(7);
  return `${settingsForm("system", "运行设置", "控制 Seed 串行运行、节律、阈值与宿主保留量。修改在下一次相关读取时生效。", `
    <div class="settings-grid">${renderSettingField(responseAnchor, values[responseAnchor.key])}</div>
    <div class="settings-grid">${primary.map((field) => renderSettingField(field, values[field.key])).join("")}</div>
    <details class="settings-advanced"><summary>${t("高级运行设置")}</summary><div class="settings-grid">${advanced.map((field) => renderSettingField(field, values[field.key])).join("")}</div></details>
  `)}${settingsFeedback()}`;
}

function keySourceLabel(source: string): string {
  if (source === "env") return t("进程环境密钥已就绪");
  if (source === "config") return t("配置文件中已保存");
  return t("尚未配置");
}

function renderContextSettings(files: SettingsPayload["files"]): string {
  const groups: Array<[ContextSettingsFileId, MessageKey, MessageKey]> = [
    ["lately", "最近缓存", "近期语料的容量、裁剪与压缩边界。"],
    ["periodic", "定期层", "定期记忆投影的内容上限。"],
    ["high_freq", "高频层", "高频索引与引用窗口的显示边界。"],
    ["relation", "关系在场投影", "每轮可装配的关系在场槽位。"],
  ];
  return `<form class="settings-context-form" data-context-settings-form>
    ${groups.map(([fileId, title, description]) => `<section class="ledger-panel settings-panel settings-context-panel" data-settings-file="${fileId}">
      <header class="ledger-title"><h2>${t(title)}</h2><p>${t(description)}</p></header>
      <div class="settings-grid">${contextSettingFields[fileId].map((field) => renderSettingField(field, files[fileId].values[field.key])).join("")}</div>
    </section>`).join("")}
    <details class="settings-advanced ledger-panel settings-panel settings-context-panel" data-settings-file="memory">
      <summary>${t("记忆代谢")}</summary>
      <p>${t("记忆热度、召回、衰减与升格边界。")}</p>
      <div class="settings-grid">${contextSettingFields.memory.map((field) => renderSettingField(field, files.memory.values[field.key])).join("")}</div>
    </details>
    ${settingsActions()}
  </form>${settingsFeedback()}`;
}

const routePhases = ["setup", "reaction", "cleanup"] as const;
const routeSlots = ["primary", "backup_1", "backup_2"] as const;

function routePhaseLabel(phase: typeof routePhases[number]): MessageKey {
  return ({ setup: "起手", reaction: "反应", cleanup: "善后" } as const)[phase];
}

function routeSlotLabel(slot: typeof routeSlots[number]): MessageKey {
  return ({ primary: "主模型", backup_1: "备用一", backup_2: "备用二" } as const)[slot];
}

function explicitRouteSlot(
  phase: typeof routePhases[number],
  slot: typeof routeSlots[number],
): ModelRouteSlot | null {
  const row = settingsProjection.data?.persona.model_routing.values.routes[phase];
  if (!row) return null;
  if (slot === "primary") return row.primary;
  return row.backups[slot === "backup_1" ? 0 : 1];
}

function modelById(modelId: string): ModelProfile | undefined {
  return settingsProjection.data?.model_catalog.models.find((item) => item.id === modelId);
}

function effortOptions(model: ModelProfile | undefined, selected: string): string {
  const supported = model?.reasoning?.supported?.length
    ? model.reasoning.supported
    : ["", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"];
  const values = [...new Set([selected, ...supported])];
  return values.map((value) => `<option value="${escapeHtml(value)}" ${value === selected ? "selected" : ""}>${escapeHtml(value || t("系统默认"))}</option>`).join("");
}

function renderRouteCell(phase: typeof routePhases[number], slot: typeof routeSlots[number]): string {
  const data = settingsProjection.data;
  if (!data) return "";
  const explicit = explicitRouteSlot(phase, slot);
  const effective = slot === "primary"
    ? data.persona.effective_routes.effective_primaries[phase]
    : null;
  const shown = explicit || effective;
  const model = shown ? modelById(shown.model_id) : undefined;
  const inheritedFrom = !explicit && effective
    ? data.persona.effective_routes.primary_sources[phase]
    : null;
  const blankLabel = slot === "primary" && phase !== "setup"
    ? t("自动继承")
    : t("未配置");
  return `<div class="route-cell" data-route-cell="${phase}:${slot}">
    <select data-route-model data-route-phase="${phase}" data-route-slot="${slot}" aria-label="${t(routePhaseLabel(phase))} ${t(routeSlotLabel(slot))}">
      <option value="">${blankLabel}</option>
      ${data.model_catalog.models.map((item) => `<option value="${escapeHtml(item.id)}" ${explicit?.model_id === item.id ? "selected" : ""}>${escapeHtml(item.alias)} · ${escapeHtml(item.model)}</option>`).join("")}
    </select>
    <select data-route-effort data-route-phase="${phase}" data-route-slot="${slot}" ${explicit ? "" : "disabled"} aria-label="${t(routePhaseLabel(phase))} ${t(routeSlotLabel(slot))} ${t("推理强度")}">
      ${effortOptions(model, shown?.reasoning_effort || model?.reasoning.default || "")}
    </select>
    <small>${inheritedFrom ? t("继承自{phase}", { phase: t(routePhaseLabel(inheritedFrom as typeof routePhases[number])) }) : explicit ? t("显式配置") : t("空槽位")}</small>
  </div>`;
}

function renderModelRouting(): string {
  const data = settingsProjection.data;
  if (!data) return "";
  const resolved = data.persona.effective_routes.phases;
  return `<section class="ledger-panel settings-panel routing-settings">
    <header class="ledger-title"><h2>${t("模型路由")}</h2><p>${t("分别为起手、反应和善后选择主模型与两个备用模型；空白主模型按阶段向下继承。")}</p></header>
    <form data-routing-settings-form>
      <div class="route-matrix" role="group" aria-label="${t("三阶段模型路由")}">
        <div class="route-corner"></div>${routeSlots.map((slot) => `<b>${t(routeSlotLabel(slot))}</b>`).join("")}
        ${routePhases.map((phase) => `<strong>${t(routePhaseLabel(phase))}</strong>${routeSlots.map((slot) => renderRouteCell(phase, slot)).join("")}`).join("")}
      </div>
      <div class="settings-switch cross-phase-switch"><span>${t("允许跨阶段模型容灾")}</span><input type="checkbox" data-cross-phase-failover aria-label="${t("允许跨阶段模型容灾")}" ${data.persona.model_routing.values.cross_phase_failover_enabled ? "checked" : ""}></div>
      <div class="resolved-routes">
        ${routePhases.map((phase) => `<div><b>${t(routePhaseLabel(phase))}</b><span>${resolved[phase].length ? resolved[phase].map((item, index) => `${index + 1}. ${escapeHtml(item.model_alias)} · ${escapeHtml(item.reasoning_effort || t("系统默认"))}`).join(" → ") : t("未配置可用模型")}</span></div>`).join("")}
      </div>
      ${settingsActions()}
    </form>
    ${settingsFeedback()}
  </section>`;
}

function renderSettingsPage(): string {
  const tab = getActivePageTab("settings");
  if (settingsProjection.loading && !settingsProjection.data) {
    return `<section class="ledger-panel settings-panel"><p class="runtime-empty-copy">${t("正在读取本机设置")}</p></section>`;
  }
  if (!settingsProjection.data) {
    return `<section class="ledger-panel settings-panel"><p class="runtime-error">${escapeHtml(settingsProjection.error || t("设置暂不可用"))}</p><button type="button" class="ghost-action" data-reload-settings>${t("重新载入")}</button></section>`;
  }
  if (tab === "routing") return renderModelRouting();
  if (tab === "context") return renderContextSettings(settingsProjection.data.files);
  return renderRuntimeSettings(settingsProjection.data.files.system.values);
}

function renderInterfaceSettings(): string {
  const configured = settingsProjection.data?.interface.values.locale || "system";
  return `<section class="global-setting-section">
    <header><h3>${t("界面与语言")}</h3><p>${t("语言是 UPSP 全局设置；跟随系统时，每次按浏览器语言裁决。")}</p></header>
    <form data-interface-settings-form>
      <label class="locale-setting"><span><b>${t("界面语言")}</b><small>${t("保存后立即应用到当前界面。")}</small></span>
        <select data-interface-locale aria-label="${t("界面语言")}">
          <option value="system" ${configured === "system" ? "selected" : ""}>${t("跟随系统")}</option>
          <option value="zh-CN" ${configured === "zh-CN" ? "selected" : ""}>简体中文</option>
          <option value="en-US" ${configured === "en-US" ? "selected" : ""}>English</option>
        </select>
      </label>
      ${settingsActions()}
    </form>
  </section>`;
}

const providerUrlSuffixes: Record<ModelConnection["protocol"], string> = {
  openai_chat: "/v1/chat/completions",
  openai_responses: "/v1/responses",
  anthropic_messages: "/v1/messages",
};

export function providerUrlSuffix(protocol: string): string {
  return providerUrlSuffixes[protocol as ModelConnection["protocol"]] || "";
}

function providerUrlParts(url: string): [string, string] {
  const normalized = url.trim().replace(/\/+$/, "");
  const paths = [...new Set([...Object.values(providerUrlSuffixes), "/chat/completions", "/responses", "/messages"])]
    .sort((left, right) => right.length - left.length);
  const path = paths.find((candidate) => normalized.endsWith(candidate)) || "";
  return path ? [normalized.slice(0, -path.length), path] : [normalized, ""];
}

export function providerBaseUrl(url: string): string {
  return providerUrlParts(url)[0];
}

export function providerRequestPath(url: string, protocol: string): string {
  return url ? providerUrlParts(url)[1] : providerUrlSuffix(protocol);
}

export function providerRequestUrl(baseUrl: string, requestPath: string): string {
  const base = baseUrl.trim().replace(/\/+$/, "");
  const path = requestPath.trim();
  return path ? `${base}/${path.replace(/^\/+/, "")}` : base;
}

function connectionEditor(connection?: ModelConnection): string {
  const id = connection?.id || "";
  const protocol = connection?.protocol || "openai_chat";
  return `<form class="catalog-editor" data-model-catalog-form="connection" data-model-catalog-id="${escapeHtml(id)}" autocomplete="off">
    <label><span>${t("备注名")}</span><input name="alias" value="${escapeHtml(connection?.alias || "")}" required maxlength="80"></label>
    <label><span>${t("协议")}</span><select name="protocol" data-provider-protocol><option value="openai_chat" ${protocol === "openai_chat" ? "selected" : ""}>OpenAI Chat</option><option value="openai_responses" ${protocol === "openai_responses" ? "selected" : ""}>OpenAI Responses</option><option value="anthropic_messages" ${protocol === "anthropic_messages" ? "selected" : ""}>Anthropic Messages</option></select></label>
    <label class="wide"><span>${t("基础地址")}</span><div class="provider-url-editor"><input name="url_base" type="url" value="${escapeHtml(providerBaseUrl(connection?.url || ""))}" required aria-label="${t("基础地址")}"><input name="url_path" value="${escapeHtml(providerRequestPath(connection?.url || "", protocol))}" data-provider-url-path aria-label="${t("请求路径")}"></div><small>${t("请求路径会按协议给出默认值；如服务商端点不同，可直接修改覆盖。")}</small></label>
    <label class="wide"><span>${t("密钥环境变量")}</span><input name="api_key_env" value="${escapeHtml(connection?.api_key_env || "")}" pattern="[A-Za-z_][A-Za-z0-9_]*"></label>
    <footer><button type="button" class="ghost-action" data-cancel-catalog-edit>${t("取消")}</button><button type="submit" class="primary-action">${t("保存")}</button></footer>
  </form>`;
}

function formattedInteger(value: number | null | undefined): string {
  return Number.isInteger(value) && Number(value) >= 0
    ? Number(value).toLocaleString(state.locale)
    : "—";
}

function modelContextSourceLabel(model?: ModelProfile): string {
  const source = model?.context_window_source || (model?.context_window ? "legacy_manual" : "unknown");
  if (source === "provider") return t("模型服务元数据");
  if (source === "registry") return t("内置官方模型表");
  if (source === "legacy_manual") return t("旧配置手动值");
  return t("尚未识别");
}

function modelEditor(model?: ModelProfile): string {
  const data = settingsProjection.data;
  if (!data) return "";
  const supported = (model?.reasoning.supported || []).join(", ");
  const detected = Number(model?.detected_context_window || 0);
  const source = model?.context_window_source || (model?.context_window ? "legacy_manual" : "unknown");
  return `<form class="catalog-editor" data-model-catalog-form="model" data-model-catalog-id="${escapeHtml(model?.id || "")}" autocomplete="off">
    <label><span>${t("备注名")}</span><input name="alias" value="${escapeHtml(model?.alias || "")}" required maxlength="80"></label>
    <label><span>${t("服务连接")}</span><select name="connection_id" data-model-context-input required><option value="">${t("请选择")}</option>${data.model_catalog.connections.map((item) => `<option value="${escapeHtml(item.id)}" ${model?.connection_id === item.id ? "selected" : ""}>${escapeHtml(item.alias)}</option>`).join("")}</select></label>
    <label><span>${t("模型 ID")}</span><input name="model" data-model-context-input value="${escapeHtml(model?.model || "")}" required></label>
    <label><span>${t("识别容量")}</span><div class="model-context-detection"><output data-model-context-detected>${escapeHtml(detected > 0 ? formattedInteger(detected) : modelContextSourceLabel(model))}</output><button type="button" data-resolve-model-context>${t("重新识别")}</button></div></label>
    <label><span>${t("运行上限")}</span><input name="context_window" type="number" min="1" max="${escapeHtml(detected > 0 ? detected : 100000000)}" value="${escapeHtml(model?.context_window || "")}" required></label>
    <label><span>${t("输出 Token 上限")}</span><input name="output_token_limit" type="number" min="0" max="1000000" value="${escapeHtml(model?.output_token_limit || 0)}" required><small>${t("0 表示自动：OpenAI 使用服务默认值，原生 Anthropic 使用 32000。")}</small></label>
    <input name="detected_context_window" type="hidden" value="${escapeHtml(detected)}">
    <input name="context_window_source" type="hidden" value="${escapeHtml(source)}">
    <p class="model-context-feedback wide" data-model-context-feedback>${escapeHtml(modelContextSourceLabel(model))}</p>
    <label><span>${t("支持的推理强度")}</span><input name="reasoning_supported" value="${escapeHtml(supported)}" placeholder="low, medium, high"></label>
    <label><span>${t("默认推理强度")}</span><input name="reasoning_default" value="${escapeHtml(model?.reasoning.default || "")}"></label>
    <div class="settings-switch"><span>${t("流式输出")}</span><input name="streaming_enabled" type="checkbox" aria-label="${t("流式输出")}" ${model?.streaming.enabled !== false ? "checked" : ""}></div>
    <div class="settings-switch"><span>${t("返回用量")}</span><input name="streaming_include_usage" type="checkbox" aria-label="${t("返回用量")}" ${model?.streaming.include_usage !== false ? "checked" : ""}></div>
    <details class="wide"><summary>${t("兼容请求参数")}</summary><label><span>JSON</span><textarea name="request_overrides" rows="6">${escapeHtml(JSON.stringify(model?.request_overrides || {}, null, 2))}</textarea></label></details>
    <footer><button type="button" class="ghost-action" data-cancel-catalog-edit>${t("取消")}</button><button type="submit" class="primary-action">${t("保存")}</button></footer>
  </form>`;
}

function renderConnectionCard(connection: ModelConnection): string {
  const disabled = settingsProjection.pending ? "disabled" : "";
  const keyActionLabel = connection.key_present ? t("更换密钥") : t("保存密钥");
  const keyInputPlaceholder = connection.key_present ? "••••••••" : t("输入新密钥");
  const keyInputLabel = connection.key_present ? t("输入新密钥以更换") : t("输入新密钥");
  return `<article class="catalog-item">
    <header><div><strong>${escapeHtml(connection.alias)}</strong><span>${escapeHtml(connection.protocol)} · ${escapeHtml(connection.url)}</span></div><div><button type="button" data-edit-catalog="connection" data-catalog-id="${escapeHtml(connection.id)}">${t("编辑")}</button><button type="button" data-delete-catalog="connection" data-catalog-id="${escapeHtml(connection.id)}">${t("删除")}</button></div></header>
    <div class="connection-key"><span>${t("密钥状态：{status}", { status: keySourceLabel(connection.key_source) })}</span><input type="password" data-provider-key-input="${escapeHtml(connection.id)}" autocomplete="new-password" placeholder="${keyInputPlaceholder}" aria-label="${keyInputLabel}" ${disabled}><button type="button" data-provider-key-action="set" data-provider-key-connection="${escapeHtml(connection.id)}" ${disabled}>${keyActionLabel}</button><button type="button" data-provider-key-action="delete" data-provider-key-connection="${escapeHtml(connection.id)}" ${disabled}>${t("删除密钥")}</button></div>
    ${state.editingConnectionId === connection.id ? connectionEditor(connection) : ""}
  </article>`;
}

function renderModelCard(model: ModelProfile): string {
  const connection = settingsProjection.data?.model_catalog.connections.find((item) => item.id === model.connection_id);
  return `<article class="catalog-item">
    <header><div><strong>${escapeHtml(model.alias)}</strong><span>${escapeHtml(model.model)} · ${escapeHtml(connection?.alias || t("连接缺失"))}</span></div><div><button type="button" data-edit-catalog="model" data-catalog-id="${escapeHtml(model.id)}">${t("编辑")}</button><button type="button" data-delete-catalog="model" data-catalog-id="${escapeHtml(model.id)}">${t("删除")}</button></div></header>
    <p>${t("识别容量：{detected}；运行上限：{limit}；输出上限：{output}；默认推理强度：{effort}", { detected: formattedInteger(model.detected_context_window), limit: formattedInteger(model.context_window), output: model.output_token_limit > 0 ? formattedInteger(model.output_token_limit) : t("自动"), effort: model.reasoning.default || t("系统默认") })}</p>
    ${state.editingModelId === model.id ? modelEditor(model) : ""}
  </article>`;
}

function objectValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function renderTransportSettings(): string {
  const transport = objectValue(settingsProjection.data?.model_catalog.transport);
  const handshake = objectValue(transport.handshake);
  const breaker = objectValue(transport.circuit_breaker);
  const field = (name: string, label: MessageKey, value: unknown, min: number, max: number) => `<label><span>${t(label)}</span><input type="number" name="${name}" value="${escapeHtml(value)}" min="${min}" max="${max}" required></label>`;
  return `<details class="transport-settings"><summary>${t("传输与容灾")}</summary><form data-transport-settings-form>
    ${field("timeout_seconds", "连接握手等待", handshake.timeout_seconds, 1, 3600)}
    ${field("retry", "暂态重试次数", handshake.retry, 0, 2)}
    ${field("request_timeout_seconds", "普通响应等待", handshake.request_timeout_seconds, 1, 3600)}
    ${field("stream_first_chunk_timeout_seconds", "流式首块等待", handshake.stream_first_chunk_timeout_seconds, 1, 3600)}
    ${field("stream_idle_timeout_seconds", "流式空闲等待", handshake.stream_idle_timeout_seconds, 1, 3600)}
    ${field("stream_content_overrun_chars", "流式内容超限窗口", handshake.stream_content_overrun_chars, 0, 16777216)}
    ${field("max_failures", "熔断失败上限", breaker.max_failures, 1, 100)}
    ${field("cooldown_seconds", "熔断冷却时间", breaker.cooldown_seconds, 0, 86400)}
    ${settingsActions()}
  </form></details>`;
}

function renderModelSettings(): string {
  const data = settingsProjection.data;
  if (!data) return "";
  const override = data.environment_override ? `<p class="settings-source-note warn">${t("当前宿主使用进程环境覆盖；模型库仍可管理，但当前进程以覆盖链为准。")}</p>` : "";
  return `<section class="global-setting-section model-catalog">
    <header><h3>${t("模型服务")}</h3><p>${t("服务连接保存地址与共享密钥；模型配置保存模型能力。同一连接可供多个模型复用。")}</p></header>
    ${override}${settingsFeedback()}
    <section class="catalog-group"><header><h4>${t("服务连接")}</h4><button type="button" data-new-catalog="connection">${t("添加连接")}</button></header>
      ${state.editingConnectionId === "new" ? connectionEditor() : ""}
      <div class="catalog-list">${data.model_catalog.connections.map(renderConnectionCard).join("") || `<p>${t("尚无服务连接")}</p>`}</div>
    </section>
    <section class="catalog-group"><header><h4>${t("模型配置")}</h4><button type="button" data-new-catalog="model" ${data.model_catalog.connections.length ? "" : "disabled"}>${t("添加模型")}</button></header>
      ${state.editingModelId === "new" ? modelEditor() : ""}
      <div class="catalog-list">${data.model_catalog.models.map(renderModelCard).join("") || `<p>${t("尚无模型配置")}</p>`}</div>
    </section>
    ${renderTransportSettings()}
  </section>`;
}

function renderAboutSettings(): string {
  if (aboutProjection.loading && !aboutProjection.data) {
    return `<p class="runtime-empty-copy">${t("正在读取关于信息")}</p>`;
  }
  const data = aboutProjection.data;
  if (!data) {
    return `<p class="runtime-error">${escapeHtml(aboutProjection.error || t("关于信息暂不可用"))}</p><button type="button" data-reload-about>${t("重新载入")}</button>`;
  }
  const author = data.product.author[state.locale];
  const signature = data.build.signature_status === "unsigned"
    ? t("未签名 Alpha")
    : data.build.signature_status;
  return `<section class="global-setting-section about-section">
    <header class="about-identity">
      <img src="./assets/upsp-logo.png" alt="UPSP">
      <div><h3>${escapeHtml(data.product.name)}</h3><strong>${escapeHtml(data.product.version)}</strong><p>${escapeHtml(author)}</p></div>
    </header>
    <dl class="about-facts">
      <div><dt>${t("发行渠道")}</dt><dd>${escapeHtml(data.product.channel)}</dd></div>
      <div><dt>${t("构建号")}</dt><dd>${escapeHtml(data.product.build_number)}</dd></div>
      <div><dt>${t("许可")}</dt><dd>${escapeHtml(data.product.license)}</dd></div>
      <div><dt>${t("签名状态")}</dt><dd>${escapeHtml(signature)}</dd></div>
      <div><dt>${t("架构")}</dt><dd>${escapeHtml(data.build.architecture)}</dd></div>
      <div><dt>Git HEAD</dt><dd>${escapeHtml(data.build.git_head)}${data.build.source_dirty ? ` · ${t("源码有未提交改动")}` : ""}</dd></div>
    </dl>
    <div class="about-actions">
      <a href="${escapeHtml(data.links.repository)}" target="_blank" rel="noopener noreferrer">${t("打开 GitHub 仓库")}</a>
      <a href="${escapeHtml(data.links.releases)}" target="_blank" rel="noopener noreferrer">${t("获取新版本")}</a>
      <button type="button" data-copy-about-diagnostics>${t("复制诊断信息")}</button>
    </div>
    <section class="about-data-policy">
      <h4>${t("用户数据与密钥")}</h4>
      <p>${t("位格、记忆和轮次保存在“文档\\UPSP”。")}</p>
      <p>${t("模型设置、密钥和缓存在“LocalAppData\\UPSP”。")}</p>
      <p>${t("卸载和覆盖升级不会删除这些内容。")}</p>
    </section>
    <p class="about-copyright">${escapeHtml(data.product.copyright)}</p>
  </section>`;
}

export function aboutDiagnosticText(): string {
  const data = aboutProjection.data;
  if (!data) return "";
  return [
    `${data.product.name} ${data.product.version}`,
    `${t("发行渠道")}: ${data.product.channel}`,
    `${t("构建号")}: ${data.product.build_number}`,
    `Git HEAD: ${data.build.git_head}`,
    `${t("源码状态")}: ${data.build.source_dirty ? t("有未提交改动") : t("干净")}`,
    `${t("架构")}: ${data.build.architecture}`,
    `${t("签名状态")}: ${data.build.signature_status === "unsigned" ? t("未签名 Alpha") : data.build.signature_status}`,
  ].join("\n");
}

export function renderGlobalSettings(): void {
  const open = state.globalSettingsOpen;
  els.globalSettingsOverlay.hidden = !open;
  els.globalSettingsOverlay.toggleAttribute("inert", !open);
  els.globalSettingsToggle.setAttribute("aria-expanded", String(open));
  if (!open) return;
  els.globalSettingsOverlay.querySelectorAll<HTMLElement>("[data-global-settings-tab]").forEach((button) => {
    const active = button.dataset.globalSettingsTab === state.globalSettingsTab;
    button.classList.toggle("active", active);
    button.setAttribute("aria-current", active ? "page" : "false");
  });
  if (state.globalSettingsTab === "about") {
    els.globalSettingsContent.innerHTML = renderAboutSettings();
  } else if (settingsProjection.loading && !settingsProjection.data) {
    els.globalSettingsContent.innerHTML = `<p class="runtime-empty-copy">${t("正在读取本机设置")}</p>`;
  } else if (!settingsProjection.data) {
    els.globalSettingsContent.innerHTML = `<p class="runtime-error">${escapeHtml(settingsProjection.error || t("设置暂不可用"))}</p><button type="button" data-reload-settings>${t("重新载入")}</button>`;
  } else {
    els.globalSettingsContent.innerHTML = state.globalSettingsTab === "models"
      ? renderModelSettings()
      : `${renderInterfaceSettings()}${settingsFeedback()}`;
  }
}

function metric(label: string, value: unknown, caption: string, tone: string): string {
  return `
    <div class="metric-card ${tone === "hot" ? "hot" : ""}">
      <b>${escapeHtml(label)}</b>
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(caption)}</span>
    </div>
  `;
}

function receiptRow(title: unknown, desc: unknown, status: unknown, tone: string): string {
  return `
    <div class="receipt-row ${tone === "warn" ? "warn" : ""}">
      <i class="status-dot ${tone === "warn" ? "warn" : ""}"></i>
      <span><b>${escapeHtml(title)}</b><span>${escapeHtml(desc)}</span></span>
      <em>${escapeHtml(status)}</em>
    </div>
  `;
}

const manualTitles: Record<string, readonly [MessageKey, MessageKey]> = {
  "intro.md": ["什么是 UPSP", "UPSP 基本解释"],
  "step-wheel.md": ["三步轮", "起手 / 反应 / 善后"],
  "context-layers.md": ["上下文十层", "常驻层至弹窗层"],
  "content-window.md": ["内容窗口两种挂载", "常驻 / 即时"],
  "memory-bus.md": ["记忆总线", "MEM 不是普通容器"],
  "work-containers.md": ["工作容器", "辩证链 / 事件链 / 项目 / 技能"],
  "audit-tools.md": ["协议中心", "动态账本 / 规则 / 文档"],
  "base-serial.md": ["Base 串行模型路由", "逐帧串行，多模型容灾"],
};

function manualTopicRows(files: string[]): string {
  return files.map((file) => {
    const entry = manualTitles[file];
    const title = entry ? t(entry[0]) : file;
    const summary = entry ? t(entry[1]) : "";
    return `
      <button class="manual-topic" data-manual="${escapeHtml(file)}">
        <span><b>${escapeHtml(title)}</b><small>${escapeHtml(summary)}</small></span>
        <em class="hud-label">${t("打开")}</em>
      </button>
    `;
  }).join("");
}

function rememberDetailFocus(): void {
  if (!els.manualOverlay.hidden) return;
  manualReturnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
}

function focusDetailClose(): void {
  window.requestAnimationFrame(() => {
    els.manualOverlay.querySelector<HTMLElement>("button[data-close-manual]")?.focus();
  });
}

function detailSourceLabel(sourceType: "RUNTIME" | "RULES" | "DOCS" | "MANUAL" | "MEMORY" | "TOOL"): string {
  if (state.locale === "en-US") return sourceType;
  return ({ RUNTIME: "运行时", RULES: "规则", DOCS: "文档", MANUAL: "手册", MEMORY: "记忆", TOOL: "工具" } as const)[sourceType];
}

function showDetail({
  sourceType,
  title,
  summary,
  sourceRef,
  documentId,
  contentMd,
  actionsHtml = "",
  ledgerJson = false,
}: {
  sourceType: "RUNTIME" | "RULES" | "DOCS" | "MANUAL" | "MEMORY" | "TOOL";
  title: string;
  summary: string;
  sourceRef: string;
  documentId: string;
  contentMd: string;
  actionsHtml?: string;
  ledgerJson?: boolean;
}): void {
  els.manualTitle.textContent = title;
  els.manualSummary.textContent = summary;
  els.manualPageLabel.textContent = detailSourceLabel(sourceType);
  els.manualSources.textContent = sourceRef;
  els.manualBody.innerHTML = `${actionsHtml}${renderMarkdownDocument(documentId, contentMd)}`;
  els.manualOverlay.hidden = false;
  hydrateMarkdownDocuments(els.manualBody, els.manualBody);
  if (ledgerJson) hydrateLedgerJsonTables(els.manualBody);
}

export function openContextToolAnnotation(toolName: string): void {
  const selectedRound = contextSelection();
  const selected = selectedRound ? contextFrameSelection(selectedRound.live) : null;
  const detail = runtimeProjection.frameDetail;
  const frame = selectedRound && selected && detail?.round === selectedRound.round && detail.frameId === selected.frame_id
    ? detail.frame
    : null;
  const pane = frame?.context_panes?.find((entry) => entry.id === "01_tool_header");
  const tool = contextToolAnnotations(pane).find((entry) => entry.name === toolName);
  if (!selectedRound || !frame || !tool) return;
  rememberDetailFocus();
  showDetail({
    sourceType: "TOOL",
    title: tool.name,
    summary: t("当前帧次工具注释与参数"),
    sourceRef: `${t("当前轮")} R${selectedRound.round} · ${frame.frame_id} · 01_tool_header`,
    documentId: `context-tool:${selectedRound.round}:${frame.frame_id}:${tool.name}`,
    contentMd: `${tool.description}\n\n## ${t("参数")}\n\n\`\`\`json\n${JSON.stringify(tool.parameters, null, 2)}\n\`\`\``,
    ledgerJson: true,
  });
  focusDetailClose();
}

function showDetailError({
  sourceType,
  title,
  sourceRef,
  message,
  retryAttributes,
}: {
  sourceType: "RULES" | "DOCS" | "MANUAL" | "MEMORY";
  title: string;
  sourceRef: string;
  message: string;
  retryAttributes: string;
}): void {
  els.manualTitle.textContent = title;
  els.manualSummary.textContent = t("正文未载入；没有使用回退伪正文。");
  els.manualPageLabel.textContent = detailSourceLabel(sourceType);
  els.manualSources.textContent = sourceRef;
  els.manualBody.innerHTML = `<section class="detail-load-error"><h3>${t("读取失败")}</h3><p>${escapeHtml(message)}</p><button type="button" ${retryAttributes}>${t("只读重试")}</button></section>`;
  els.manualOverlay.hidden = false;
}

export function openMemoryDetail(itemId: string, { retry = false }: { retry?: boolean } = {}): void {
  const item = depositionItems("memory").find((entry) => entry.id === itemId);
  if (!item) return;
  if (!retry) rememberDetailFocus();
  const detail = depositionDetail("memory", itemId);
  const sourceRound = detail?.created_round ?? item.created_round;
  const sourceInstance = detail?.created_instance_id || item.created_instance_id || "meta";
  const recallRound = detail?.last_recalled_round ?? item.last_recalled_round;
  const recallInstance = detail?.last_recalled_instance_id || item.last_recalled_instance_id || "meta";
  const created = fullLocalTime(detail?.created_at ?? item.created_at)?.label || t("未记录");
  const stored = fullLocalTime(detail?.stored_at ?? item.stored_at)?.label || t("未入库");
  const recalled = fullLocalTime(detail?.last_recalled_at ?? item.last_recalled_at)?.label || t("未记录");
  const sourceRef = `${itemId} · ${sourceRound == null ? t("轮次未记录") : `${sourceInstance}/R${sourceRound}`} · ${recallRound == null ? t("轮次未记录") : `${recallInstance}/R${recallRound}`} · ${t("创建时间")} ${created} · ${t("入库时间")} ${stored} · ${t("最近调用时间")} ${recalled} · ${(detail?.linked_containers || item.linked_containers || []).length} ${t("个挂接")}`;
  if (detail) {
    const stmPresent = detail.stm_present === true || detail.memory_layer === "STM";
    const ltmLayer = detail.ltm_layer || ((detail.memory_layer || "").startsWith("LTM/") ? detail.memory_layer : "");
    const mounted = Boolean(detail.periodic_mounted);
    const mountStatus = detail.periodic_mount_status || (mounted ? "mounted" : "unmounted");
    const mountPending = mountStatus === "awaiting_completion" || mountStatus === "mount_blocked";
    const mutation = depositionProjection.periodicMutation;
    const pending = mutation.pending && mutation.memId === itemId;
    const layerStates = [
      ...(stmPresent ? [t("STM 衰减中")] : []),
      ...(ltmLayer ? [ltmLayer] : []),
    ];
    const mountState = mounted ? t("定期层已挂载") : mountPending ? t("等待回忆重整或挂载重试") : t("未挂载");
    const action = mounted || mountPending ? "unmount" : "mount";
    const actionLabel = mounted ? t("从定期层卸载") : mountPending ? t("取消等待挂载") : t("挂载到定期层");
    const actionsHtml = `
      <div class="periodic-memory-control" aria-live="polite">
        <span class="periodic-memory-status${mounted ? " is-mounted" : mountPending ? " is-pending" : ""}" role="img" aria-label="${escapeHtml(mountState)}" title="${escapeHtml(mountState)}"></span>
        <button type="button" aria-pressed="${mounted}" data-periodic-memory-action="${action}" data-memory-id="${escapeHtml(itemId)}" ${pending ? "disabled" : ""}>${pending ? t("正在处理") : actionLabel}</button>
      </div>`;
    showDetail({
      sourceType: "MEMORY",
      title: detail.title || item.title || itemId,
      summary: `${layerStates.join(" · ") || detail.memory_layer || item.memory_layer || "MEM"} · ${t("公开")} · W${detail.weight ?? item.weight ?? "?"} · ${t("主题")} ${detail.subject || item.subject || t("未记录")}`,
      sourceRef,
      documentId: `memory:${itemId}`,
      contentMd: memoryBodyMarkdown(detail.body),
      actionsHtml,
    });
  } else {
    const error = depositionProjection.detailErrors[`memory:${itemId}`];
    if (error) {
      showDetailError({
        sourceType: "MEMORY",
        title: item.title || itemId,
        sourceRef,
        message: error,
        retryAttributes: `data-retry-memory-detail data-memory-id="${escapeHtml(itemId)}"`,
      });
    } else {
      els.manualPageLabel.textContent = "MEMORY";
      els.manualTitle.textContent = item.title || itemId;
      els.manualSummary.textContent = t("正在读取记忆条目正文…");
      els.manualSources.textContent = sourceRef;
      els.manualBody.innerHTML = `<p class="runtime-empty-copy">${t("正在读取正文…")}</p>`;
      els.manualOverlay.hidden = false;
    }
  }
  if (!retry) focusDetailClose();
}

export async function openManual(file = state.manualFile, { retry = false }: { retry?: boolean } = {}): Promise<void> {
  if (!retry) rememberDetailFocus();
  state.manualFile = file;
  els.manualOverlay.hidden = false;
  els.manualPageLabel.textContent = detailSourceLabel("MANUAL");
  els.manualTitle.textContent = manualTitles[file] ? t(manualTitles[file][0]) : "UPSP Manual";
  els.manualSummary.textContent = t("正在读取本地同源手册…");
  const localizedFile = state.locale === "en-US" ? file.replace(/\.md$/, ".en-US.md") : file;
  els.manualSources.textContent = `manual/${localizedFile}`;
  els.manualBody.innerHTML = `<p class="runtime-empty-copy">${t("正在读取正文…")}</p>`;
  if (!retry) focusDetailClose();
  try {
    const parsed = await loadManual(localizedFile);
    showDetail({
      sourceType: "MANUAL",
      title: parsed.meta.title || "UPSP Manual",
      summary: parsed.meta.summary || "",
      sourceRef: parsed.meta.sourceRefs ? `source refs: ${parsed.meta.sourceRefs}` : `manual/${localizedFile}`,
      documentId: `manual:${state.locale}:${file}`,
      contentMd: parsed.body,
    });
  } catch (error: unknown) {
    showDetailError({
      sourceType: "MANUAL",
      title: manualTitles[file] ? t(manualTitles[file][0]) : "UPSP Manual",
      sourceRef: `manual/${localizedFile}`,
      message: error instanceof Error ? error.message : "manual_read_failed",
      retryAttributes: `data-retry-manual="${escapeHtml(file)}"`,
    });
  }
}

async function loadManual(file: string): Promise<{ meta: Record<string, string>; body: string }> {
  const response = await fetch(`./manual/${file}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`manual_read_failed:${response.status}`);
  return parseFrontmatter(await response.text());
}

export function openLedgerEvent(round: number, cardId: string, card: ConversationCard): void {
  rememberDetailFocus();
  showDetail({
    sourceType: "RUNTIME",
    title: ledgerCardTitle(card) || `${t("运行时")} ${state.locale === "zh-CN" ? "事件" : "event"}`,
    summary: `${card.type || "event"} · #${card.event_index ?? cardId}`,
    sourceRef: `${t("当前轮")} R${round} · ${card.phase || "round"} · ${card.frame_id || "round"} · ${card.event_type || "event"}`,
    documentId: `ledger:${round}:${cardId}`,
    contentMd: ledgerCardMarkdown(card),
    ledgerJson: true,
  });
  focusDetailClose();
}

export async function openProtocolDocument(
  kind: "rule" | "doc",
  itemId: string,
  { retry = false }: { retry?: boolean } = {},
): Promise<void> {
  if (!retry) rememberDetailFocus();
  const sourceType = kind === "rule" ? "RULES" : "DOCS";
  els.manualOverlay.hidden = false;
  els.manualPageLabel.textContent = detailSourceLabel(sourceType);
  els.manualTitle.textContent = itemId.split("/").at(-1) || itemId;
  els.manualSummary.textContent = t("正在读取登记正文…");
  els.manualSources.textContent = `${kind}:${itemId}`;
  els.manualBody.innerHTML = `<p class="runtime-empty-copy">${t("正在读取正文…")}</p>`;
  if (!retry) focusDetailClose();
  try {
    const response = await fetch(`./api/protocol/document?kind=${kind}&id=${encodeURIComponent(itemId)}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`protocol_document_read_failed:${response.status}`);
    const payload = await response.json() as ProtocolDocumentPayload;
    if (
      payload.schema_version !== "seed_gui_protocol_document.v1"
      || payload.kind !== kind
      || payload.id !== itemId
      || typeof payload.content_md !== "string"
    ) {
      throw new Error("protocol_document_schema_mismatch");
    }
    showDetail({
      sourceType,
      title: payload.title || itemId,
      summary: payload.description || (payload.categories || []).join(" / "),
      sourceRef: payload.source_ref,
      documentId: `protocol:${kind}:${itemId}`,
      contentMd: payload.content_md,
    });
  } catch (error: unknown) {
    showDetailError({
      sourceType,
      title: itemId.split("/").at(-1) || itemId,
      sourceRef: `${kind}:${itemId}`,
      message: error instanceof Error ? error.message : "protocol_document_read_failed",
      retryAttributes: `data-retry-protocol-document data-protocol-kind="${kind}" data-protocol-id="${escapeHtml(itemId)}"`,
    });
  }
}

function parseFrontmatter(markdown: string): { meta: Record<string, string>; body: string } {
  const match = /^---\n([\s\S]*?)\n---\n?([\s\S]*)$/m.exec(markdown);
  if (!match) return { meta: {}, body: markdown };
  const meta: Record<string, string> = {};
  for (const line of match[1].split("\n")) {
    const index = line.indexOf(":");
    if (index > -1) {
      meta[line.slice(0, index).trim()] = line.slice(index + 1).trim();
    }
  }
  return { meta, body: match[2] };
}

export function closeManual(): void {
  if (els.manualOverlay.hidden) return;
  els.manualOverlay.hidden = true;
  const target = manualReturnFocus;
  manualReturnFocus = null;
  if (target?.isConnected) target.focus();
}

function clearNavCollapseTimer(): void {
  if (!navCollapseTimer) return;
  window.clearTimeout(navCollapseTimer);
  navCollapseTimer = 0;
}

function clearSystemCloseTimer(): void {
  if (!systemCloseTimer) return;
  window.clearTimeout(systemCloseTimer);
  systemCloseTimer = 0;
}

export function openNav(): void {
  clearNavCollapseTimer();
  if (state.navCollapseLocked || els.app.classList.contains("nav-force-collapsed")) return;
  els.leftRail.classList.add("nav-expanded");
}

export function scheduleNavCollapse(): void {
  clearNavCollapseTimer();
  navCollapseTimer = window.setTimeout(() => {
    collapseNavNow();
  }, 500);
}

export function collapseNavNow({ force = false }: { force?: boolean } = {}): void {
  clearNavCollapseTimer();
  els.leftRail.classList.remove("nav-expanded");
  if (force) els.app.classList.add("nav-force-collapsed");
}

export function rememberSystemReturnFocus(target: SystemReturnFocus): void {
  systemReturnFocus = target;
}

export function closeSystemWindow(): void {
  const panel = els.stagePage.querySelector<HTMLElement>(".system-window");
  const duration = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 140;
  panel?.classList.add("closing");
  clearSystemCloseTimer();
  const returnFocus = systemReturnFocus;
  systemCloseTimer = window.setTimeout(() => {
    systemCloseTimer = 0;
    state.systemWindowOpen = false;
    syncShellState();
    renderNavigation();
    renderStage(state.activePage);
    if (returnFocus instanceof HTMLElement && returnFocus.isConnected) {
      returnFocus.focus();
      return;
    }
    const descriptor: FocusReturnDescriptor | null = returnFocus instanceof HTMLElement ? null : returnFocus;
    const pageId = descriptor?.pageId || state.activePage;
    const tabId = descriptor?.tabId || "";
    const selector = tabId
      ? `[data-page="${CSS.escape(pageId)}"][data-tab="${CSS.escape(tabId)}"]`
      : `[data-page="${CSS.escape(pageId)}"]:not([data-tab])`;
    els.surfaceNav.querySelector<HTMLElement>(selector)?.focus();
  }, duration);
}

export function renderComposerState(): void {
  const inFlight = runtimeProjection.sending || runtimeProjection.status?.send_in_flight;
  const relayInFlight = taskProjection.relayPending || runtimeProjection.status?.relay_in_flight;
  const stage = runtimeProjection.status?.stage || "";
  const stopRequested = runtimeProjection.status?.stop_requested === true;
  const canStop = runtimeProjection.status?.can_stop === true;
  const localSettlement = stage === "cleanup_local";
  const stopAvailable = canStop || Boolean(
    inFlight && runtimeProjection.awaitingProjection && !localSettlement,
  );
  const roundActive = Boolean(
    inFlight
    || relayInFlight
    || canStop
    || localSettlement
    || (stopRequested && runtimeProjection.status?.current_round != null)
  );
  const projectedPermission = runtimeProjection.status?.execution_permission?.pending_level
    || runtimeProjection.status?.execution_permission?.permission_level;
  if (roundActive && !runtimeProjection.permissionChanging && projectedPermission) {
    els.permissionLevel.value = projectedPermission;
  }
  const connected = runtimeProjection.host === "connected";
  const pending = inFlight || relayInFlight || runtimeProjection.awaitingProjection;
  const modelReady = settingsProjection.data?.persona.setup_model_ready === true;
  const frames = runtimeProjection.live?.frame_catalog || [];
  const latestFrame = frames.find((frame) => frame.frame_id === runtimeProjection.live?.latest_frame_id) || frames.at(-1) || null;
  const reportedFrame = latestFrame?.context_usage?.state === "reported"
    ? latestFrame
    : [...frames].reverse().find((frame) => frame.context_usage?.state === "reported")
      || [...runtimeProjection.conversationRoundOrder].reverse().flatMap((round) => [
        ...(runtimeProjection.conversationRounds.get(round)?.frame_catalog || []),
      ].reverse()).find((frame) => frame.context_usage?.state === "reported")
      || latestFrame;
  const contextUsageHtml = frameContextUsage(reportedFrame, true);
  if (els.contextUsage.innerHTML !== contextUsageHtml) {
    els.contextUsage.innerHTML = contextUsageHtml;
  }
  els.configureModelButton.hidden = modelReady;
  els.sendButton.hidden = !modelReady || roundActive;
  els.stopButton.hidden = !roundActive || (!stopAvailable && !localSettlement);
  els.stopButton.disabled = localSettlement || runtimeProjection.stopping || !stopAvailable;
  els.stopButton.title = localSettlement ? t("本地结算不可中断") : "";
  els.sendButton.disabled = !connected || pending || !modelReady;
  els.permissionLevel.disabled = runtimeProjection.permissionChanging || stage.startsWith("cleanup");
  els.messageInput.readOnly = pending;
  els.runtimeComposer.setAttribute("aria-busy", String(pending));
  if (!modelReady) {
    els.sendFeedback.textContent = t("尚未配置可用模型，完成模型服务与起手路由后即可发送。");
  } else if (localSettlement) {
    els.sendFeedback.textContent = t("正在本地善后");
  } else if (stage === "tool_approval") {
    els.sendFeedback.textContent = t("等待工具执行审批");
  } else if (runtimeProjection.stopping || (stopRequested && roundActive)) {
    els.sendFeedback.textContent = t("正在停止生成");
  } else if (relayInFlight) {
    els.sendFeedback.textContent = t("中继执行中");
  } else if (inFlight || runtimeProjection.awaitingProjection) {
    els.sendFeedback.textContent = t("提交中");
  } else if (runtimeProjection.sendFeedback) {
    els.sendFeedback.textContent = runtimeProjection.sendFeedback;
  } else if (connected) {
    els.sendFeedback.textContent = t("默认受限；副作用工具会在当前对话中逐次请求审批。");
  } else {
    els.sendFeedback.textContent = t("等待本地宿主");
  }
}
