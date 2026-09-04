import type {
  AboutPayload,
  CallFrame,
  DepositionDetailPayload,
  DepositionIndexPayload,
  DepositionItem,
  DepositionKind,
  DialogueNode,
  JsonObject,
  LiveEventsPayload,
  LiveDetailKind,
  LiveDetailPayload,
  LivePayload,
  LiveState,
  LedgerItem,
  ConversationCard,
  ModelContextResolution,
  PageId,
  PermissionLevel,
  PeriodicMemoryMountReceipt,
  PersonaCatalogPayload,
  PersonaCorePayload,
  PersonaStatePayload,
  ProtocolCatalogPayload,
  RequestPrefixDiffPayload,
  RoundListPayload,
  RuntimeStatus,
  SettingValue,
  SettingsFileId,
  SettingsPayload,
  TaskProjectionPayload,
} from "./contracts";
import {
  aboutProjection,
  depositionProjection,
  els,
  polling,
  personaProjection,
  personaCatalogProjection,
  protocolProjection,
  runtimePages,
  runtimeProjection,
  settingsProjection,
  state,
  taskProjection,
} from "./state";
import { configuredLocale, t } from "./i18n";
import {
  getActivePageTab,
  changeLocale,
  relayRuntimeState,
  renderChat,
  renderComposerState,
  renderIdentity,
  renderOverview,
  renderSourceState,
  renderStage,
  renderGlobalSettings,
  openGlobalSettings,
  openMemoryDetail,
  renderStageAndFocus,
  selectDepositionItem,
} from "./view";

let contextPrefixDiffController: AbortController | null = null;
const depositionDetailRequests = new Map<string, Promise<boolean>>();
const LIVE_PROJECTION_RETRY_DELAY_MS = 5_000;

class RuntimeRequestError extends Error {
  status: number;
  code: string;
  payload: JsonObject;

  constructor(message: string, status = 0, code = "request_failed", payload: JsonObject = {}) {
    super(message);
    this.name = "RuntimeRequestError";
    this.status = status;
    this.code = code;
    this.payload = payload;
  }
}

function errorView(error: unknown): RuntimeRequestError {
  if (error instanceof RuntimeRequestError) return error;
  if (error instanceof Error) return new RuntimeRequestError(error.message);
  return new RuntimeRequestError(String(error || "unknown"));
}

function jsonObject(value: unknown): JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonObject
    : {};
}

async function fetchRuntimeJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, { cache: "no-store", ...options });
  let payload: unknown = {};
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok) {
    const object = jsonObject(payload);
    const code = typeof object.error === "string" ? object.error : "request_failed";
    throw new RuntimeRequestError(
      typeof object.error === "string" ? object.error : `HTTP ${response.status}`,
      response.status,
      code,
      object,
    );
  }
  return payload as T;
}

function runtimeProjectionAdvanced(): boolean {
  const baseline = runtimeProjection.submitBaseline;
  if (!baseline || runtimeProjection.round == null) return false;
  return runtimeProjection.round !== baseline.round
    || Number(runtimeProjection.live?.last_event_index || 0) > baseline.eventIndex;
}

export function refreshRuntimeUi(): void {
  const key = JSON.stringify([
    runtimeProjection.host,
    runtimeProjection.hostSession,
    runtimeProjection.round,
    runtimeProjection.live?.last_event_index || 0,
    runtimeProjection.live?.round_lifecycle?.state || "",
    runtimeProjection.status?.send_in_flight || false,
    runtimeProjection.status?.relay_in_flight || false,
    runtimeProjection.status?.stage || "",
    runtimeProjection.status?.stop_requested || false,
    runtimeProjection.status?.can_stop || false,
    runtimeProjection.status?.pending_tool_approval?.approval_id || "",
    runtimeProjection.status?.interrupted_recovery?.pending || false,
    runtimeProjection.status?.interrupted_recovery?.round || 0,
    runtimeProjection.status?.interrupted_recovery?.applied_unregistered || 0,
    runtimeProjection.status?.interrupted_recovery?.applied_registered || 0,
    runtimeProjection.status?.interrupted_recovery?.not_applied || 0,
    runtimeProjection.status?.interrupted_recovery?.known_result || 0,
    runtimeProjection.status?.interrupted_recovery?.conflict || 0,
    runtimeProjection.status?.interrupted_recovery?.outcome_unknown || 0,
    runtimeProjection.status?.cli?.data?.round_type || "",
    (runtimeProjection.status?.cli?.data?.active_flags || []).join(","),
    runtimeProjection.sending,
    runtimeProjection.stopping,
    taskProjection.relayPending,
    runtimeProjection.awaitingProjection,
    runtimeProjection.error,
    runtimeProjection.liveError,
    runtimeProjection.liveErrorEventIndex,
    runtimeProjection.sendFeedback,
    runtimeProjection.exportFeedback,
    runtimeProjection.conversationHistoryVersion,
    runtimeProjection.conversationHistoryError,
    runtimeProjection.conversationHistoryHasMore,
    runtimeProjection.conversationHistoryLoading,
  ]);
  if (key === runtimeProjection.renderKey) {
    renderComposerState();
    return;
  }
  runtimeProjection.renderKey = key;
  renderSourceState();
  renderIdentity();
  renderOverview();
  renderChat();
  if (runtimePages.has(state.activePage)) renderStage(state.activePage);
  if (state.activePage === "context" && getActivePageTab("context") === "content") {
    void pollContextPrefixDiffForSelection();
  }
}

function validateLiveState(statePayload: LiveState | null): void {
  if (statePayload !== null && statePayload?.schema_version !== "round_live_state.v3") {
    throw new Error("round_live_state_schema_mismatch");
  }
}

function selectedContextDiffRef(): { round: number; frameId: string } | null {
  const rounds = runtimeProjection.conversationRoundOrder;
  const selectedRound = state.selectedContextRound;
  const round = selectedRound !== null && runtimeProjection.conversationRounds.has(selectedRound)
    ? selectedRound
    : runtimeProjection.round ?? rounds.at(-1) ?? null;
  if (round === null) return null;
  const live = round === runtimeProjection.round
    ? runtimeProjection.live
    : runtimeProjection.conversationRounds.get(round) || null;
  const frames = live?.frame_catalog || [];
  const frame = state.selectedContextFrame === null
    ? frames.at(-1)
    : frames.find((item) => item.frame_id === state.selectedContextFrame);
  return frame ? { round, frameId: frame.frame_id } : null;
}

function validContextPrefixDiff(
  payload: RequestPrefixDiffPayload,
  ref: { round: number; frameId: string },
): boolean {
  if (
    payload.schema_version !== "seed_gui_request_prefix_diff.v1"
    || !["ready", "identical", "unavailable"].includes(payload.state)
  ) return false;
  if (payload.state === "unavailable") return true;
  return payload.current?.round === ref.round
    && payload.current?.frame_id === ref.frameId
    && typeof payload.previous?.frame_id === "string"
    && Number.isInteger(payload.common_prefix_bytes)
    && Number.isInteger(payload.current_wire_bytes)
    && (payload.state === "identical" || Boolean(payload.target?.pane_id));
}

export async function pollContextPrefixDiffForSelection(
  { force = false }: { force?: boolean } = {},
): Promise<void> {
  const ref = selectedContextDiffRef();
  if (!ref) {
    contextPrefixDiffController?.abort();
    runtimeProjection.contextPrefixDiffKey = "";
    runtimeProjection.contextPrefixDiff = null;
    runtimeProjection.contextPrefixDiffLoading = false;
    runtimeProjection.contextPrefixDiffError = "";
    return;
  }
  const key = `${ref.round}:${ref.frameId}`;
  if (
    !force
    && runtimeProjection.contextPrefixDiffKey === key
    && (
      runtimeProjection.contextPrefixDiffLoading
      || runtimeProjection.contextPrefixDiff
      || runtimeProjection.contextPrefixDiffError
    )
  ) return;
  contextPrefixDiffController?.abort();
  const controller = new AbortController();
  contextPrefixDiffController = controller;
  runtimeProjection.contextPrefixDiffKey = key;
  runtimeProjection.contextPrefixDiff = null;
  runtimeProjection.contextPrefixDiffLoading = true;
  runtimeProjection.contextPrefixDiffError = "";
  try {
    const payload = await fetchRuntimeJson<RequestPrefixDiffPayload>(
      `./api/context/request-prefix-diff?round=${ref.round}&frame_id=${encodeURIComponent(ref.frameId)}`,
      { signal: controller.signal },
    );
    if (!validContextPrefixDiff(payload, ref)) {
      throw new Error("context_prefix_diff_schema_mismatch");
    }
    if (runtimeProjection.contextPrefixDiffKey === key) {
      runtimeProjection.contextPrefixDiff = payload;
    }
  } catch (error: unknown) {
    if (!(error instanceof DOMException && error.name === "AbortError")
        && runtimeProjection.contextPrefixDiffKey === key) {
      const failure = errorView(error);
      runtimeProjection.contextPrefixDiffError = failure.code || failure.message || "unknown";
    }
  } finally {
    if (runtimeProjection.contextPrefixDiffKey === key) {
      runtimeProjection.contextPrefixDiffLoading = false;
      if (state.activePage === "context" && getActivePageTab("context") === "content") {
        renderStage("context");
      }
    }
    if (contextPrefixDiffController === controller) contextPrefixDiffController = null;
  }
}

function liveForRound(round: number): LiveState | null {
  return round === runtimeProjection.round
    ? runtimeProjection.live
    : runtimeProjection.conversationRounds.get(round) || null;
}

function detailKey(round: number, ref: string): string {
  return `${round}:${ref}`;
}

async function fetchLiveDetail(kind: LiveDetailKind, round: number, ref = ""): Promise<LiveDetailPayload> {
  const query = new URLSearchParams({ kind, round: String(round) });
  if (ref) query.set("ref", ref);
  const payload = await fetchRuntimeJson<LiveDetailPayload>(`./api/live/detail?${query.toString()}`);
  if (payload.schema_version !== "round_live_detail.v1" || payload.kind !== kind || payload.round !== round) {
    throw new Error("round_live_detail_schema_mismatch");
  }
  if (ref && payload.ref !== ref) throw new Error("round_live_detail_ref_mismatch");
  return payload;
}

export async function loadLegacyCards(round: number): Promise<void> {
  if (runtimeProjection.legacyCards.has(round) || runtimeProjection.legacyCardsLoading.has(round)) return;
  const generation = runtimeProjection.detailGeneration;
  runtimeProjection.legacyCardsLoading.add(round);
  runtimeProjection.legacyCardsErrors.delete(round);
  refreshRuntimeUi();
  try {
    const detail = await fetchLiveDetail("legacy_conversation", round);
    if (generation !== runtimeProjection.detailGeneration || !liveForRound(round)) return;
    const body = jsonObject(detail.payload);
    if (!Array.isArray(body.conversation)) throw new Error("legacy_conversation_detail_invalid");
    runtimeProjection.legacyCards.set(round, body.conversation as ConversationCard[]);
  } catch (error: unknown) {
    if (generation === runtimeProjection.detailGeneration) {
      const failure = errorView(error);
      runtimeProjection.legacyCardsErrors.set(round, failure.code || failure.message);
    }
  } finally {
    if (generation === runtimeProjection.detailGeneration) {
      runtimeProjection.legacyCardsLoading.delete(round);
      runtimeProjection.conversationHistoryVersion += 1;
      refreshRuntimeUi();
    }
  }
}

export async function loadLedgerItems(round: number): Promise<void> {
  if (runtimeProjection.ledgerItems.has(round) || runtimeProjection.ledgerItemsLoading.has(round)) return;
  const generation = runtimeProjection.detailGeneration;
  runtimeProjection.ledgerItemsLoading.add(round);
  runtimeProjection.ledgerItemsErrors.delete(round);
  refreshRuntimeUi();
  try {
    const detail = await fetchLiveDetail("ledger", round);
    if (generation !== runtimeProjection.detailGeneration || !liveForRound(round)) return;
    const body = jsonObject(detail.payload);
    if (!Array.isArray(body.items)) throw new Error("ledger_detail_invalid");
    runtimeProjection.ledgerItems.set(round, body.items as LedgerItem[]);
  } catch (error: unknown) {
    if (generation === runtimeProjection.detailGeneration) {
      const failure = errorView(error);
      runtimeProjection.ledgerItemsErrors.set(round, failure.code || failure.message);
    }
  } finally {
    if (generation === runtimeProjection.detailGeneration) {
      runtimeProjection.ledgerItemsLoading.delete(round);
      runtimeProjection.conversationHistoryVersion += 1;
      refreshRuntimeUi();
    }
  }
}

export async function fetchLedgerEventCard(round: number, eventRef: string): Promise<ConversationCard | null> {
  const generation = runtimeProjection.detailGeneration;
  const detail = await fetchLiveDetail("event", round, eventRef);
  if (generation !== runtimeProjection.detailGeneration || !liveForRound(round)) return null;
  const body = jsonObject(detail.payload);
  const cards = Array.isArray(body.cards) ? body.cards as ConversationCard[] : [];
  const event = jsonObject(body.event);
  if (cards.length) {
    const sections = cards.map((card) => {
      const title = String(card.title || card.type || "event");
      const content = String(card.content_md || card.content_raw || card.content || "");
      return `## ${title}\n\n${content}`;
    });
    sections.push(`## ${t("原始 JSON")}\n\n\`\`\`json\n${JSON.stringify(event, null, 2)}\n\`\`\``);
    return {
      ...cards[0],
      title: String(event.event_type || cards[0].title || "event"),
      content_md: sections.join("\n\n"),
      content_raw: JSON.stringify(event, null, 2),
    };
  }
  return {
    event_index: Number(event.event_index || eventRef),
    event_type: String(event.event_type || "event"),
    phase: String(event.phase || "round"),
    frame_id: String(event.frame_id || ""),
    recorded_at: String(event.recorded_at || ""),
    title: String(event.event_type || "event"),
    type: "event",
    content_md: `\`\`\`json\n${JSON.stringify(event, null, 2)}\n\`\`\``,
  };
}

export async function loadFrameDetail(round: number, frameId: string, { force = false } = {}): Promise<void> {
  const key = detailKey(round, frameId);
  if (!force && runtimeProjection.frameDetail?.round === round && runtimeProjection.frameDetail.frameId === frameId) return;
  if (runtimeProjection.frameDetailLoading === key) return;
  const generation = runtimeProjection.detailGeneration;
  runtimeProjection.frameDetail = null;
  runtimeProjection.frameDetailLoading = key;
  runtimeProjection.frameDetailError = "";
  refreshRuntimeUi();
  try {
    const detail = await fetchLiveDetail("frame", round, frameId);
    if (
      generation !== runtimeProjection.detailGeneration
      || runtimeProjection.frameDetailLoading !== key
      || !liveForRound(round)
    ) return;
    const frame = detail.payload as CallFrame;
    if (!frame || frame.frame_id !== frameId) throw new Error("frame_detail_invalid");
    runtimeProjection.frameDetail = { round, frameId, frame };
  } catch (error: unknown) {
    if (
      generation === runtimeProjection.detailGeneration
      && runtimeProjection.frameDetailLoading === key
    ) {
      const failure = errorView(error);
      runtimeProjection.frameDetailError = failure.code || failure.message;
    }
  } finally {
    if (generation === runtimeProjection.detailGeneration && runtimeProjection.frameDetailLoading === key) {
      runtimeProjection.frameDetailLoading = "";
      runtimeProjection.conversationHistoryVersion += 1;
      refreshRuntimeUi();
    }
  }
}

export async function loadTimelineNodeDetail(round: number, nodeId: string): Promise<void> {
  const key = detailKey(round, nodeId);
  const current = liveForRound(round)?.dialogue_timeline?.nodes.find((node) => node.node_id === nodeId);
  const cached = runtimeProjection.timelineNodeDetails.get(key);
  if (
    cached
    && cached.status === current?.status
    && cached.ended_at === current?.ended_at
    && cached.approval_decision === current?.approval_decision
  ) return;
  if (runtimeProjection.timelineNodeLoading.has(key)) return;
  const generation = runtimeProjection.detailGeneration;
  runtimeProjection.timelineNodeLoading.add(key);
  runtimeProjection.timelineNodeErrors.delete(key);
  refreshRuntimeUi();
  try {
    const detail = await fetchLiveDetail("timeline_node", round, nodeId);
    if (generation !== runtimeProjection.detailGeneration || !liveForRound(round)) return;
    const node = detail.payload as DialogueNode;
    if (!node || node.node_id !== nodeId) throw new Error("timeline_node_detail_invalid");
    runtimeProjection.timelineNodeDetails.set(key, node);
  } catch (error: unknown) {
    if (generation === runtimeProjection.detailGeneration) {
      const failure = errorView(error);
      runtimeProjection.timelineNodeErrors.set(key, failure.code || failure.message);
    }
  } finally {
    if (generation === runtimeProjection.detailGeneration) {
      runtimeProjection.timelineNodeLoading.delete(key);
      runtimeProjection.conversationHistoryVersion += 1;
      refreshRuntimeUi();
    }
  }
}

function refreshAdvancedRoundDetails(round: number, live: LiveState | null): void {
  const reloadLedger = runtimeProjection.ledgerItems.delete(round);
  runtimeProjection.ledgerItemsErrors.delete(round);
  const currentNodes = new Map(
    (live?.dialogue_timeline?.nodes || []).map((node) => [node.node_id, node]),
  );
  const reloadNodeIds: string[] = [];
  for (const [key, cached] of runtimeProjection.timelineNodeDetails) {
    if (!key.startsWith(`${round}:`)) continue;
    const current = currentNodes.get(cached.node_id);
    if (
      current
      && current.status === cached.status
      && current.ended_at === cached.ended_at
      && current.approval_decision === cached.approval_decision
    ) continue;
    runtimeProjection.timelineNodeDetails.delete(key);
    runtimeProjection.timelineNodeErrors.delete(key);
    if (current?.detail_ref && state.conversationDisclosure.get(current.node_id) === true) {
      reloadNodeIds.push(current.node_id);
    }
  }
  const ledgerVisible = state.activePage === "audit"
    || (state.activePage === "run" && ["tools", "receipts"].includes(getActivePageTab("run")));
  if (reloadLedger && ledgerVisible) void loadLedgerItems(round);
  for (const nodeId of reloadNodeIds) void loadTimelineNodeDetail(round, nodeId);
}

export function loadSelectedRuntimeDetails(pageId: PageId, tabId = ""): void {
  const selectedRound = pageId === "audit"
    ? state.selectedLedgerRound
    : pageId === "context"
      ? state.selectedContextRound
      : state.selectedTaskRound;
  const round = selectedRound ?? runtimeProjection.round ?? runtimeProjection.conversationRoundOrder.at(-1) ?? null;
  if (round === null) return;
  const live = liveForRound(round);
  if (!live) return;
  if (pageId === "audit" || (pageId === "run" && ["tools", "receipts"].includes(tabId))) {
    void loadLedgerItems(round);
  }
  if (pageId !== "context" && !(pageId === "run" && tabId === "tools")) return;
  const selectedFrameId = pageId === "context" ? state.selectedContextFrame : state.selectedTaskFrame;
  const frames = live.frame_catalog || [];
  const frame = selectedFrameId === null
    ? frames.at(-1)
    : frames.find((item) => item.frame_id === selectedFrameId);
  if (frame) void loadFrameDetail(round, frame.frame_id);
}

function clearLiveDetails(): void {
  runtimeProjection.detailGeneration += 1;
  runtimeProjection.legacyCards.clear();
  runtimeProjection.legacyCardsLoading.clear();
  runtimeProjection.legacyCardsErrors.clear();
  runtimeProjection.ledgerItems.clear();
  runtimeProjection.ledgerItemsLoading.clear();
  runtimeProjection.ledgerItemsErrors.clear();
  runtimeProjection.frameDetail = null;
  runtimeProjection.frameDetailLoading = "";
  runtimeProjection.frameDetailError = "";
  runtimeProjection.timelineNodeDetails.clear();
  runtimeProjection.timelineNodeLoading.clear();
  runtimeProjection.timelineNodeErrors.clear();
}

function clearRoundProjectionForIdentityMutation(): void {
  clearLiveDetails();
  state.conversationStickToBottom = true;
  runtimeProjection.hostSession = "";
  runtimeProjection.live = null;
  runtimeProjection.round = null;
  runtimeProjection.conversationRounds.clear();
  runtimeProjection.conversationRoundOrder = [];
  runtimeProjection.conversationHistoryInitialized = false;
  runtimeProjection.conversationHistoryLatest = null;
  runtimeProjection.conversationHistoryHasMore = false;
  runtimeProjection.conversationHistoryLoading = false;
  runtimeProjection.conversationHistoryError = "";
  runtimeProjection.liveError = "";
  runtimeProjection.liveErrorEventIndex = 0;
  runtimeProjection.liveRetryAfter = 0;
  runtimeProjection.fullRefreshNeeded = true;
  state.selectedTaskRound = null;
  state.selectedTaskFrame = null;
  state.selectedContextRound = null;
  state.selectedContextFrame = null;
  state.selectedLedgerRound = null;
}

function requestDesktopBackendRestart(): void {
  const webview = (window as Window & {
    chrome?: { webview?: { postMessage: (message: unknown) => void } };
  }).chrome?.webview;
  if (!webview) return;
  window.requestAnimationFrame(() => webview.postMessage({
    schema_version: "upsp_desktop_message.v1",
    command: "restart_backend",
  }));
}

function cacheLatestConversation(round: number | null, liveState: LiveState | null): void {
  if (round === null || !Number.isInteger(round) || !liveState) return;
  runtimeProjection.conversationRounds.set(round, liveState);
  if (!runtimeProjection.conversationRoundOrder.includes(round)) {
    runtimeProjection.conversationRoundOrder = [
      ...runtimeProjection.conversationRoundOrder,
      round,
    ].sort((left, right) => left - right);
  }
}

async function syncConversationHistory({ force = false }: { force?: boolean } = {}): Promise<void> {
  if (runtimeProjection.conversationHistoryLoading) return;
  const generation = runtimeProjection.detailGeneration;
  const latestRound = runtimeProjection.round;
  cacheLatestConversation(latestRound, runtimeProjection.live);
  if (
    !force
    && runtimeProjection.conversationHistoryInitialized
    && runtimeProjection.conversationHistoryLatest === latestRound
  ) return;

  runtimeProjection.conversationHistoryInitialized = true;
  runtimeProjection.conversationHistoryLatest = latestRound;
  runtimeProjection.conversationHistoryLoading = true;
  refreshRuntimeUi();
  try {
    const payload = await fetchRuntimeJson<RoundListPayload>("./api/rounds");
    if (generation !== runtimeProjection.detailGeneration) return;
    if (!Array.isArray(payload.rounds)) throw new Error("round_list_schema_mismatch");
    const roundIds = [...new Set(payload.rounds
      .map((item) => Number(item.round))
      .filter((round) => Number.isInteger(round)))]
      .sort((left, right) => left - right);
    if (latestRound !== null && Number.isInteger(latestRound) && !roundIds.includes(latestRound)) {
      roundIds.push(latestRound);
      roundIds.sort((left, right) => left - right);
    }

    const retained = new Set(roundIds);
    [...runtimeProjection.conversationRounds.keys()].forEach((round) => {
      if (!retained.has(round)) runtimeProjection.conversationRounds.delete(round);
    });
    const missing = roundIds
      .filter((round) => !runtimeProjection.conversationRounds.has(round))
      .sort((left, right) => right - left);
    const failed: Array<{ round: number; error: unknown }> = [];
    for (const round of force ? missing.slice(0, 2) : []) {
      try {
        const roundPayload = await fetchRuntimeJson<LivePayload>(`./api/live/state?round=${round}`);
        if (generation !== runtimeProjection.detailGeneration) return;
        validateLiveState(roundPayload.state || null);
        if (Number(roundPayload.round) !== round || !roundPayload.state) {
          throw new Error("round_history_projection_mismatch");
        }
        runtimeProjection.conversationRounds.set(round, roundPayload.state);
        if (roundPayload.state.display_mode === "legacy") void loadLegacyCards(round);
        runtimeProjection.conversationRoundOrder = roundIds.filter(
          (item) => runtimeProjection.conversationRounds.has(item),
        );
        runtimeProjection.conversationHistoryVersion += 1;
        refreshRuntimeUi();
      } catch (error) {
        failed.push({ round, error });
      }
    }
    runtimeProjection.conversationRoundOrder = roundIds.filter(
      (round) => runtimeProjection.conversationRounds.has(round),
    );
    runtimeProjection.conversationHistoryHasMore = roundIds.some(
      (round) => !runtimeProjection.conversationRounds.has(round),
    );
    if (state.selectedLedgerRound !== null && !retained.has(state.selectedLedgerRound)) {
      state.selectedLedgerRound = null;
    }
    if (state.selectedContextRound !== null && !retained.has(state.selectedContextRound)) {
      state.selectedContextRound = null;
    }
    if (state.selectedTaskRound !== null && !retained.has(state.selectedTaskRound)) {
      state.selectedTaskRound = null;
      state.selectedTaskFrame = null;
    }
    runtimeProjection.conversationHistoryError = failed.length
      ? t("较早对话未完全载入")
      : "";
  } catch (error) {
    if (generation === runtimeProjection.detailGeneration) {
      runtimeProjection.conversationHistoryError = t("较早对话未完全载入");
    }
  } finally {
    if (generation === runtimeProjection.detailGeneration) {
      runtimeProjection.conversationHistoryLoading = false;
    }
  }
  if (generation === runtimeProjection.detailGeneration) {
    runtimeProjection.conversationHistoryVersion += 1;
  }
}

async function fetchFullLiveProjection(): Promise<{ round: number | null; state: LiveState | null }> {
  const payload = await fetchRuntimeJson<LivePayload>("./api/live/state?round=latest");
  validateLiveState(payload.state || null);
  return { round: payload.round ?? null, state: payload.state || null };
}

async function fetchLiveProjection(forceFull = false): Promise<{ round: number | null; state: LiveState | null }> {
  if (forceFull || runtimeProjection.fullRefreshNeeded) return fetchFullLiveProjection();
  const after = Number(runtimeProjection.live?.last_event_index || 0);
  const payload = await fetchRuntimeJson<LiveEventsPayload>(`./api/live/events?round=latest&after=${after}`);
  if (payload.schema_version !== "round_live_events.v2") {
    throw new Error("round_live_events_schema_mismatch");
  }
  const nextRound = payload.round ?? null;
  if (nextRound !== runtimeProjection.round) return fetchFullLiveProjection();
  if (payload.state != null) {
    validateLiveState(payload.state);
    return { round: nextRound, state: payload.state };
  }
  return { round: runtimeProjection.round, state: runtimeProjection.live };
}

export function pollRuntime({ forceFull = false, ignoreVisibility = false }: { forceFull?: boolean; ignoreVisibility?: boolean } = {}): Promise<boolean> {
  if (document.hidden && !ignoreVisibility) return Promise.resolve(false);
  if (polling.runtime) {
    if (forceFull) polling.runtimeForceQueued = true;
    return polling.runtime;
  }
  const request = (async () => {
    let requestGeneration = runtimeProjection.detailGeneration;
    let status: RuntimeStatus;
    try {
      status = await fetchRuntimeJson<RuntimeStatus>("./api/runtime/status");
      if (status.schema_version !== "seed_gui_runtime_status.v3") {
        throw new Error("runtime_status_schema_mismatch");
      }
    } catch (error: unknown) {
      if (requestGeneration !== runtimeProjection.detailGeneration) return false;
      const failure = errorView(error);
      runtimeProjection.host = "error";
      runtimeProjection.status = null;
      runtimeProjection.liveError = "";
      runtimeProjection.liveErrorEventIndex = 0;
      runtimeProjection.liveRetryAfter = 0;
      runtimeProjection.fullRefreshNeeded = true;
      runtimeProjection.error = `${t("宿主状态读取失败")}：${failure.code || failure.message || "unknown"}`;
      refreshRuntimeUi();
      return true;
    }
    if (requestGeneration !== runtimeProjection.detailGeneration) return false;

    const nextHostSession = status.host_session || "";
    const identityChanged = Boolean(
      runtimeProjection.hostSession
      && runtimeProjection.hostSession !== nextHostSession,
    );
    if (identityChanged) {
      clearRoundProjectionForIdentityMutation();
      requestGeneration = runtimeProjection.detailGeneration;
    }
    runtimeProjection.hostSession = nextHostSession;
    const activeRound = status.current_round !== null
      && status.current_round !== undefined
      && Number.isInteger(Number(status.current_round))
      ? Number(status.current_round)
      : null;
    if (
      !identityChanged
      && activeRound !== null
      && runtimeProjection.round !== null
      && activeRound !== runtimeProjection.round
    ) {
      cacheLatestConversation(runtimeProjection.round, runtimeProjection.live);
      clearLiveDetails();
      requestGeneration = runtimeProjection.detailGeneration;
      runtimeProjection.live = null;
      runtimeProjection.round = activeRound;
      runtimeProjection.liveError = "";
      runtimeProjection.liveErrorEventIndex = 0;
      runtimeProjection.liveRetryAfter = 0;
      runtimeProjection.fullRefreshNeeded = true;
    }
    runtimeProjection.host = "connected";
    runtimeProjection.status = status;
    runtimeProjection.error = "";

    let liveAdvanced = false;
    let liveSucceeded = false;
    const retryDue = Date.now() >= runtimeProjection.liveRetryAfter;
    if (forceFull || !runtimeProjection.liveError || retryDue) {
      const previousRound = runtimeProjection.round;
      const previousEventIndex = Number(
        runtimeProjection.live?.last_event_index || 0,
      );
      try {
        const livePayload = await fetchLiveProjection(
          forceFull || Boolean(runtimeProjection.liveError),
        );
        if (requestGeneration !== runtimeProjection.detailGeneration) return false;
        if (activeRound !== null && livePayload.round !== activeRound) {
          throw new Error("round_live_status_projection_mismatch");
        }
        runtimeProjection.live = livePayload.state;
        runtimeProjection.round = livePayload.round;
        runtimeProjection.liveError = "";
        runtimeProjection.liveErrorEventIndex = 0;
        runtimeProjection.liveRetryAfter = 0;
        runtimeProjection.fullRefreshNeeded = false;
        liveAdvanced = previousRound !== livePayload.round
          || previousEventIndex !== Number(livePayload.state?.last_event_index || 0);
        liveSucceeded = true;
      } catch (error: unknown) {
        if (requestGeneration !== runtimeProjection.detailGeneration) return false;
        const failure = errorView(error);
        runtimeProjection.liveError = `${t("实时投影读取失败")}：${failure.code || failure.message || "unknown"}`;
        runtimeProjection.liveErrorEventIndex = Number(
          runtimeProjection.live?.last_event_index || 0,
        );
        runtimeProjection.liveRetryAfter = Date.now() + LIVE_PROJECTION_RETRY_DELAY_MS;
        runtimeProjection.fullRefreshNeeded = true;
      }
    }
    if (liveSucceeded && liveAdvanced && runtimeProjection.round !== null) {
      refreshAdvancedRoundDetails(runtimeProjection.round, runtimeProjection.live);
    }
    if (liveSucceeded) {
      void syncConversationHistory().then(() => refreshRuntimeUi());
      if (liveAdvanced) {
        await pollTaskProjection({ force: true, ignoreVisibility: true });
      }
      if (runtimeProjection.awaitingProjection && runtimeProjectionAdvanced()) {
        runtimeProjection.awaitingProjection = false;
        runtimeProjection.submitBaseline = null;
      }
    }
    refreshRuntimeUi();
    const visibleRound = runtimeProjection.round;
    if (
      visibleRound !== null
      && runtimeProjection.live?.display_mode === "legacy"
      && !runtimeProjection.legacyCards.has(visibleRound)
      && !runtimeProjection.legacyCardsLoading.has(visibleRound)
    ) {
      window.requestAnimationFrame(() => window.requestAnimationFrame(
        () => void loadLegacyCards(visibleRound),
      ));
    }
    return true;
  })();
  polling.runtime = request.finally(() => {
    polling.runtime = null;
    if (polling.runtimeForceQueued) {
      polling.runtimeForceQueued = false;
      pollRuntime({ forceFull: true, ignoreVisibility: true });
    }
  });
  return polling.runtime;
}

export function runtimePollingActive(): boolean {
  const status = runtimeProjection.status;
  const lifecycle = runtimeProjection.live?.round_lifecycle?.state;
  return Boolean(
    status?.send_in_flight
    || status?.pending_tool_approval
    || status?.relay_in_flight
    || status?.mutation_in_flight
    || status?.can_stop
    || status?.stop_requested
    || (lifecycle && !["closed", "settled", "unsettled"].includes(lifecycle)),
  );
}

export function pollTaskProjection({ force = false, ignoreVisibility = false }: { force?: boolean; ignoreVisibility?: boolean } = {}): Promise<boolean> {
  if (document.hidden && !ignoreVisibility) return Promise.resolve(false);
  if (polling.task) {
    if (force) polling.taskForceQueued = true;
    return polling.task;
  }
  if (!taskProjection.data) taskProjection.loading = true;
  let changed = force;
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<TaskProjectionPayload>("./api/workbench/task");
      if (payload.schema_version !== "seed_gui_task_projection.v1"
          || (payload.task !== null && typeof payload.task !== "object")) {
        throw new Error("task_projection_schema_mismatch");
      }
      const nextKey = JSON.stringify(payload);
      changed = changed || nextKey !== taskProjection.renderKey || Boolean(taskProjection.error);
      taskProjection.data = payload;
      taskProjection.renderKey = nextKey;
      taskProjection.error = "";
    } catch (error: unknown) {
      const failure = errorView(error);
      const nextError = `${t("任务真账读取失败")}：${failure.code || failure.message || "unknown"}`;
      changed = changed || nextError !== taskProjection.error;
      taskProjection.error = nextError;
    } finally {
      taskProjection.loading = false;
    }
    if (changed) {
      renderSourceState();
      renderOverview();
      if (state.activePage === "run" && getActivePageTab("run") === "tools") {
        renderStage("run");
      }
    }
    return true;
  })();
  polling.task = request.finally(() => {
    polling.task = null;
    if (polling.taskForceQueued) {
      polling.taskForceQueued = false;
      pollTaskProjection({ force: true, ignoreVisibility: true });
    }
  });
  return polling.task;
}

export async function pollProtocolCatalog({ force = false }: { force?: boolean } = {}): Promise<boolean> {
  if (!force && protocolProjection.catalog) return false;
  protocolProjection.loading = true;
  try {
    const payload = await fetchRuntimeJson<ProtocolCatalogPayload>("./api/protocol/catalog");
    if (
      payload.schema_version !== "seed_gui_protocol_catalog.v1"
      || !Array.isArray(payload.rules?.categories)
      || !Array.isArray(payload.docs?.entries)
    ) {
      throw new Error("protocol_catalog_schema_mismatch");
    }
    protocolProjection.catalog = payload;
    protocolProjection.renderKey = JSON.stringify(payload);
    protocolProjection.error = "";
  } catch (error: unknown) {
    const failure = errorView(error);
    protocolProjection.error = `${t("协议目录读取失败")}：${failure.code || failure.message || "unknown"}`;
  } finally {
    protocolProjection.loading = false;
  }
  if (state.activePage === "audit") renderStage("audit");
  return true;
}

function validateSettingsPayload(payload: SettingsPayload): void {
  const fileIds: SettingsFileId[] = ["system", "now", "lately", "periodic", "high_freq", "relation"];
  if (
    payload.schema_version !== "seed_gui_settings.v3"
    || !payload.files
    || !fileIds.every((fileId) => (
      typeof payload.files[fileId]?.revision === "string"
      && payload.files[fileId]?.values !== null
      && typeof payload.files[fileId]?.values === "object"
    ))
    || typeof payload.interface?.revision !== "string"
    || !["system", "zh-CN", "en-US"].includes(payload.interface?.values?.locale)
    || typeof payload.model_catalog?.revision !== "string"
    || !Array.isArray(payload.model_catalog?.connections)
    || !Array.isArray(payload.model_catalog?.models)
    || typeof payload.persona?.model_routing?.revision !== "string"
    || typeof payload.persona?.setup_model_ready !== "boolean"
  ) {
    throw new Error("settings_projection_schema_mismatch");
  }
}

function acceptSettingsPayload(payload: SettingsPayload): void {
  validateSettingsPayload(payload);
  settingsProjection.data = payload;
  settingsProjection.renderKey = JSON.stringify(payload);
  settingsProjection.error = "";
  const nextLocale = configuredLocale(payload.interface.values.locale);
  if (nextLocale !== state.locale) changeLocale(nextLocale);
}

function refreshSettingsUi(): void {
  if (state.activePage === "settings") renderStage("settings");
  if (state.globalSettingsOpen) renderGlobalSettings();
  renderComposerState();
}

function refreshModelCatalogMutationUi(
  entity: "connection" | "model",
  id: string | null,
): void {
  const selector = `[data-model-catalog-form="${entity}"][data-model-catalog-id="${CSS.escape(id || "")}"]`;
  const submit = els.globalSettingsContent.querySelector<HTMLButtonElement>(
    `${selector} button[type="submit"]`,
  );
  if (submit) {
    submit.disabled = settingsProjection.pending;
    submit.textContent = settingsProjection.pending ? t("保存中") : t("保存");
  }
  const feedback = els.globalSettingsContent.querySelector<HTMLElement>(
    "[data-settings-feedback]",
  );
  if (feedback) {
    const message = settingsProjection.error || settingsProjection.feedback;
    feedback.textContent = message;
    feedback.hidden = !message;
    feedback.classList.toggle("warn", Boolean(settingsProjection.error));
  }
}

function refreshPersonaUi(): void {
  if (state.systemWindowOpen && state.activePage === "persona") renderStage("persona");
}

export function pollPersonaCore({ force = false }: { force?: boolean } = {}): Promise<boolean> {
  if (polling.personaCore) return polling.personaCore;
  if (!force && personaProjection.core) return Promise.resolve(false);
  personaProjection.coreLoading = true;
  refreshPersonaUi();
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<PersonaCorePayload>("./api/persona/core");
      if (
        payload.schema_version !== "seed_gui_persona_core.v1"
        || typeof payload.source_ref !== "string"
        || typeof payload.content_md !== "string"
        || !payload.content_md.trim()
      ) {
        throw new Error("persona_core_schema_mismatch");
      }
      personaProjection.core = payload;
      personaProjection.coreRenderKey = JSON.stringify(payload);
      personaProjection.coreError = "";
    } catch (error: unknown) {
      const failure = errorView(error);
      personaProjection.coreError = `${t("核心档案读取失败")}：${failure.code || failure.message || "unknown"}`;
    } finally {
      personaProjection.coreLoading = false;
    }
    refreshPersonaUi();
    return true;
  })();
  polling.personaCore = request.finally(() => { polling.personaCore = null; });
  return polling.personaCore;
}

export function pollPersonaState({
  force = false,
  ignoreVisibility = false,
}: { force?: boolean; ignoreVisibility?: boolean } = {}): Promise<boolean> {
  if (!ignoreVisibility && (
    document.hidden
    || !state.systemWindowOpen
    || state.activePage !== "persona"
  )) return Promise.resolve(false);
  if (polling.personaState) return polling.personaState;
  personaProjection.stateLoading = true;
  if (!personaProjection.state) refreshPersonaUi();
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<PersonaStatePayload>("./api/persona/state");
      if (
        payload.schema_version !== "seed_gui_persona_state.v1"
        || typeof payload.observed_at !== "string"
        || typeof payload.source_ref !== "string"
        || !Array.isArray(payload.fields)
        || payload.fields.some((field) => typeof field?.path !== "string")
      ) {
        throw new Error("persona_state_schema_mismatch");
      }
      const nextKey = JSON.stringify(payload);
      const changed = force
        || nextKey !== personaProjection.stateRenderKey
        || Boolean(personaProjection.stateError);
      personaProjection.state = payload;
      personaProjection.stateRenderKey = nextKey;
      personaProjection.stateError = "";
      personaProjection.stateStale = false;
      if (changed) refreshPersonaUi();
    } catch (error: unknown) {
      const failure = errorView(error);
      personaProjection.stateError = `${t("生命状态读取失败")}：${failure.code || failure.message || "unknown"}`;
      personaProjection.stateStale = Boolean(personaProjection.state);
      refreshPersonaUi();
    } finally {
      personaProjection.stateLoading = false;
    }
    return true;
  })();
  polling.personaState = request.finally(() => { polling.personaState = null; });
  return polling.personaState;
}

export function pollPersonaProjection({ force = false }: { force?: boolean } = {}): Promise<boolean[]> {
  return Promise.all([
    pollPersonaCore({ force }),
    pollPersonaState({ force, ignoreVisibility: true }),
  ]);
}

export function pollSettings({ force = false }: { force?: boolean } = {}): Promise<boolean> {
  if (polling.settings) {
    if (force) polling.settingsForceQueued = true;
    return polling.settings;
  }
  if (!force && settingsProjection.data) return Promise.resolve(false);
  settingsProjection.loading = true;
  const request = (async () => {
    try {
      acceptSettingsPayload(await fetchRuntimeJson<SettingsPayload>("./api/settings"));
      settingsProjection.feedback = "";
    } catch (error: unknown) {
      const failure = errorView(error);
      settingsProjection.error = failure.status === 404
        ? t("本地宿主版本过旧，请重启 GUI 服务")
        : `${t("设置读取失败")}：${failure.code || failure.message || "unknown"}`;
    } finally {
      settingsProjection.loading = false;
    }
    refreshSettingsUi();
    return true;
  })();
  polling.settings = request.finally(() => {
    polling.settings = null;
    if (polling.settingsForceQueued) {
      polling.settingsForceQueued = false;
      void pollSettings({ force: true });
    }
  });
  return polling.settings;
}

export async function submitSettings(
  updates: Array<[SettingsFileId, Record<string, SettingValue>]>,
): Promise<void> {
  if (settingsProjection.pending || !settingsProjection.data || !updates.length) return;
  settingsProjection.pending = true;
  settingsProjection.feedback = t("正在保存设置");
  settingsProjection.error = "";
  refreshSettingsUi();
  try {
    for (const [fileId, changes] of updates) {
      const payload = await fetchRuntimeJson<SettingsPayload>("./api/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          revision: fileId === "interface"
            ? settingsProjection.data.interface.revision
            : fileId === "models"
              ? settingsProjection.data.model_catalog.revision
            : fileId === "model_routing"
              ? settingsProjection.data.persona.model_routing.revision
              : settingsProjection.data.files[fileId]?.revision,
          file: fileId,
          changes,
        }),
      });
      acceptSettingsPayload(payload);
    }
    settingsProjection.feedback = t("设置已保存");
  } catch (error: unknown) {
    const failure = errorView(error);
    settingsProjection.error = failure.status === 409
      ? t("设置已被其他操作更新，请重新载入后再试")
      : `${t("设置保存失败")}：${failure.code || failure.message || "unknown"}`;
    settingsProjection.feedback = "";
  } finally {
    settingsProjection.pending = false;
  }
  refreshSettingsUi();
}

export async function submitProviderKey(
  connectionId: string,
  action: "set" | "delete",
  key: string,
): Promise<void> {
  if (settingsProjection.pending || !settingsProjection.data) return;
  settingsProjection.pending = true;
  settingsProjection.error = "";
  settingsProjection.feedback = action === "set" ? t("正在保存密钥") : t("正在删除密钥");
  refreshSettingsUi();
  try {
    const payload = await fetchRuntimeJson<SettingsPayload>("./api/settings/provider-key", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        connection_id: connectionId,
        action,
        key,
        revision: settingsProjection.data.model_catalog.revision,
      }),
    });
    acceptSettingsPayload(payload);
    settingsProjection.feedback = action === "set" ? t("密钥已保存") : t("密钥已删除");
  } catch (error: unknown) {
    const failure = errorView(error);
    settingsProjection.error = failure.status === 409
      ? t("当前有其他写入正在进行，请稍后再试")
      : `${action === "set" ? t("密钥保存失败") : t("密钥删除失败")}：${failure.code || failure.message || "unknown"}`;
    settingsProjection.feedback = "";
  } finally {
    settingsProjection.pending = false;
  }
  refreshSettingsUi();
}

export async function submitModelCatalog(
  entity: "connection" | "model",
  action: "create" | "update" | "delete",
  id: string | null,
  values: JsonObject,
): Promise<void> {
  if (settingsProjection.pending || !settingsProjection.data) return;
  settingsProjection.pending = true;
  settingsProjection.error = "";
  settingsProjection.feedback = t("正在保存设置");
  refreshModelCatalogMutationUi(entity, id);
  let saved = false;
  try {
    const payload = await fetchRuntimeJson<SettingsPayload>("./api/settings/model-catalog", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        revision: settingsProjection.data.model_catalog.revision,
        entity,
        action,
        id,
        values,
      }),
    });
    acceptSettingsPayload(payload);
    settingsProjection.feedback = t("设置已保存");
    state.editingConnectionId = null;
    state.editingModelId = null;
    saved = true;
  } catch (error: unknown) {
    const failure = errorView(error);
    settingsProjection.error = failure.status === 409
      ? t("设置已被其他操作更新，请重新载入后再试")
      : `${t("设置保存失败")}：${failure.code || failure.message || "unknown"}`;
    settingsProjection.feedback = "";
  } finally {
    settingsProjection.pending = false;
  }
  if (saved) refreshSettingsUi();
  else refreshModelCatalogMutationUi(entity, id);
}

export function depositionPage(kind: DepositionKind): PageId {
  return kind === "memory" ? "mem" : kind === "container" ? "containers" : "relations";
}

export function loadActiveDepositionDetail(pageId: PageId): void {
  const selection = pageId === "mem"
    ? ["memory", state.selectedMemoryId] as const
    : pageId === "containers"
      ? ["container", state.selectedContainerId] as const
      : pageId === "relations"
        ? ["relation", state.selectedRelationId] as const
        : null;
  if (selection?.[1]) void loadDepositionDetail(selection[0], selection[1]);
}

export function loadDepositionDetail(kind: DepositionKind, itemId: string, { force = false, render = true }: { force?: boolean; render?: boolean } = {}): Promise<boolean> {
  if (!itemId) return Promise.resolve(false);
  const key = `${kind}:${itemId}`;
  const pending = depositionDetailRequests.get(key);
  if (pending) {
    return force
      ? pending.then(() => loadDepositionDetail(kind, itemId, { force: true, render }))
      : pending;
  }
  if (!force && depositionProjection.details[kind]?.[itemId]) return Promise.resolve(true);
  depositionProjection.pendingDetails.add(key);
  delete depositionProjection.detailErrors[key];
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<DepositionDetailPayload>(`./api/deposition/${kind}?id=${encodeURIComponent(itemId)}`);
      if (payload.schema_version !== "seed_gui_deposition_detail.v1" || payload.kind !== kind || payload.item?.id !== itemId) {
        throw new Error("deposition_detail_schema_mismatch");
      }
      depositionProjection.details[kind][itemId] = payload;
      return true;
    } catch (error: unknown) {
      const failure = errorView(error);
      depositionProjection.detailErrors[key] = `${t("详情读取失败")}：${failure.code || failure.message || "unknown"}`;
      return false;
    }
  })();
  const tracked = request.finally(() => {
    depositionDetailRequests.delete(key);
    depositionProjection.pendingDetails.delete(key);
    if (render && state.activePage === depositionPage(kind)) renderStage(state.activePage);
  });
  depositionDetailRequests.set(key, tracked);
  return tracked;
}

export function pollDeposition({ force = false, ignoreVisibility = false }: { force?: boolean; ignoreVisibility?: boolean } = {}): Promise<boolean> {
  if (document.hidden && !ignoreVisibility) return Promise.resolve(false);
  if (polling.deposition) {
    if (force) {
      polling.depositionForceQueued = true;
      return polling.deposition.then(() => (
        polling.deposition || pollDeposition({ force: true, ignoreVisibility })
      ));
    }
    return polling.deposition;
  }
  if (!depositionProjection.index) depositionProjection.loading = true;
  let changed = force;
  let loaded = false;
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<DepositionIndexPayload>("./api/deposition");
      if (payload.schema_version !== "seed_gui_deposition_index.v1"
          || !Array.isArray(payload.memory)
          || !Array.isArray(payload.containers)
          || !Array.isArray(payload.relations)) {
        throw new Error("deposition_index_schema_mismatch");
      }
      const nextKey = JSON.stringify(payload);
      changed = changed || nextKey !== depositionProjection.renderKey || Boolean(depositionProjection.error);
      depositionProjection.index = payload;
      depositionProjection.renderKey = nextKey;
      depositionProjection.error = "";
      loaded = true;
      if (changed) {
        const selected: Array<[DepositionKind, DepositionItem | null]> = [
          ["memory", selectDepositionItem("memory", payload.memory)],
          ["container", selectDepositionItem("container", payload.containers)],
          ["relation", selectDepositionItem("relation", payload.relations)],
        ];
        await Promise.all(selected.map(([kind, item]) => (
          item ? loadDepositionDetail(kind, item.id, { force: true }) : Promise.resolve()
        )));
      }
    } catch (error: unknown) {
      const failure = errorView(error);
      const nextError = `${t("沉淀索引读取失败")}：${failure.code || failure.message || "unknown"}`;
      changed = changed || nextError !== depositionProjection.error;
      depositionProjection.error = nextError;
    } finally {
      depositionProjection.loading = false;
    }
    if (changed) {
      renderSourceState();
      if (["mem", "relations", "containers"].includes(state.activePage)) {
        renderStage(state.activePage);
        loadActiveDepositionDetail(state.activePage);
      }
    }
    return loaded;
  })();
  polling.deposition = request.finally(() => {
    polling.deposition = null;
    if (polling.depositionForceQueued) {
      polling.depositionForceQueued = false;
      pollDeposition({ force: true, ignoreVisibility: true });
    }
  });
  return polling.deposition;
}

function confirmUnlimitedPermission(permissionLevel: PermissionLevel): boolean {
  if (permissionLevel !== "unlimited" || runtimeProjection.unlimitedConfirmed) return true;
  const confirmed = window.confirm(t("放行权限会允许副作用工具直接执行。仅确认当前页面会话使用放行权限？"));
  if (!confirmed) {
    els.permissionLevel.value = "guarded";
    runtimeProjection.unlimitedConfirmed = false;
    return false;
  }
  runtimeProjection.unlimitedConfirmed = true;
  return true;
}

export async function submitRuntimeMessage(event: SubmitEvent): Promise<void> {
  event.preventDefault();
  const message = els.messageInput.value;
  const permissionLevel = els.permissionLevel.value as PermissionLevel;
  runtimeProjection.sendFeedback = "";
  if (!settingsProjection.data?.persona.setup_model_ready) {
    runtimeProjection.sendFeedback = t("尚未配置可用模型，完成模型服务与起手路由后即可发送。");
    openGlobalSettings("models");
    refreshRuntimeUi();
    return;
  }
  if (!message.trim()) {
    runtimeProjection.sendFeedback = t("请输入非空消息。");
    refreshRuntimeUi();
    return;
  }
  if (new TextEncoder().encode(message).length > 1024 * 1024) {
    runtimeProjection.sendFeedback = t("消息超过 1 MiB。");
    refreshRuntimeUi();
    return;
  }
  if (!confirmUnlimitedPermission(permissionLevel)) {
    runtimeProjection.sendFeedback = t("已保持受限权限。");
    refreshRuntimeUi();
    return;
  }
  runtimeProjection.submitBaseline = {
    round: runtimeProjection.round,
    eventIndex: Number(runtimeProjection.live?.last_event_index || 0),
  };
  els.messageInput.value = "";
  runtimeProjection.sending = true;
  runtimeProjection.awaitingProjection = true;
  refreshRuntimeUi();
  try {
    await fetchRuntimeJson<JsonObject>("./api/runtime/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message,
        permission_level: permissionLevel,
        unlimited_confirmed: permissionLevel === "unlimited" && runtimeProjection.unlimitedConfirmed,
      }),
    });
  } catch (error: unknown) {
    if (!els.messageInput.value) els.messageInput.value = message;
    const failure = errorView(error);
    runtimeProjection.awaitingProjection = false;
    runtimeProjection.submitBaseline = null;
    const labels = {
      400: t("输入参数无效"),
      403: t("来源或放行权限确认被拒绝"),
      409: t("已有运行时写操作正在执行"),
      502: t("运行时执行失败"),
      503: t("本地运行时宿主不可用"),
    };
    runtimeProjection.sendFeedback = `${labels[failure.status as keyof typeof labels] || t("提交失败")}：${failure.code || failure.message}`;
  } finally {
    runtimeProjection.sending = false;
    await pollRuntime({ forceFull: true, ignoreVisibility: true });
  }
}

export async function submitPeriodicMemory(
  action: "mount" | "unmount",
  memId: string,
): Promise<void> {
  const mutation = depositionProjection.periodicMutation;
  if (mutation.pending) return;
  mutation.pending = true;
  mutation.memId = memId;
  mutation.feedback = "";
  mutation.receipt = null;
  let mutationAccepted = false;
  let truthReloaded = false;
  const detailKey = `memory:${memId}`;
  openMemoryDetail(memId, { retry: true });
  try {
    const payload = await fetchRuntimeJson<{
      schema_version: string;
      submission_source: string;
      receipt?: PeriodicMemoryMountReceipt;
    }>("./api/deposition/memory/periodic", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, mem_id: memId }),
    });
    if (payload.schema_version !== "seed_gui_periodic_memory_result.v1"
        || payload.submission_source !== "seed_gui"
        || payload.receipt?.schema_version !== "periodic_memory_mount_receipt.v2"
        || payload.receipt.tool_id !== "periodic_memory_mount"
        || payload.receipt.mem_id !== memId
        || !["applied", "noop"].includes(payload.receipt.status)) {
      throw new Error("periodic_memory_receipt_mismatch");
    }
    mutation.receipt = payload.receipt;
    mutationAccepted = true;
    const indexReloaded = await pollDeposition({ force: true, ignoreVisibility: true });
    const detailReloaded = await loadDepositionDetail("memory", memId, { force: true, render: false });
    const truth = depositionProjection.details.memory[memId]?.item;
    const mounted = truth?.periodic_mounted;
    const mountStatus = truth?.periodic_mount_status || (mounted ? "mounted" : "unmounted");
    truthReloaded = indexReloaded
      && detailReloaded
      && typeof mounted === "boolean"
      && mountStatus === (payload.receipt.mount_status || (action === "mount" ? "mounted" : "unmounted"));
    if (!truthReloaded) {
      delete depositionProjection.details.memory[memId];
      depositionProjection.detailErrors[detailKey] = t("操作已提交，但记忆真源重读失败；为避免显示旧状态，请重新读取详情。");
      throw new Error("periodic_memory_truth_reload_failed");
    }
  } catch (error: unknown) {
    const failure = errorView(error);
    const labels = {
      400: t("定期层挂载参数无效"),
      403: t("请求来源被拒绝"),
      404: t("记忆条目不存在"),
      409: t("Runtime 正忙或挂载状态冲突"),
      503: t("本地定期层处理器不可用"),
    };
    mutation.feedback = mutationAccepted && !truthReloaded
      ? t("操作已提交，但记忆真源重读失败；为避免显示旧状态，请重新读取详情。")
      : failure.code === "periodic_memory_budget_exceeded"
        ? t("定期层已达到当前配置上限，请先取消其他挂载或调整上限后再试。")
        : `${labels[failure.status as keyof typeof labels] || t("定期层挂载变更失败")}：${failure.code || failure.message}`;
    window.alert(mutation.feedback);
  } finally {
    mutation.pending = false;
    openMemoryDetail(memId, { retry: true });
  }
}

export async function submitRuntimePermissionChange(): Promise<void> {
  const status = runtimeProjection.status;
  if (!status?.current_round || !status.can_stop || runtimeProjection.permissionChanging) return;
  const projected = status.execution_permission?.pending_level
    || status.execution_permission?.permission_level
    || "guarded";
  const permissionLevel = els.permissionLevel.value as PermissionLevel;
  if (!confirmUnlimitedPermission(permissionLevel)) {
    els.permissionLevel.value = projected;
    runtimeProjection.sendFeedback = t("已保持当前执行权限。");
    refreshRuntimeUi();
    return;
  }
  runtimeProjection.permissionChanging = true;
  runtimeProjection.sendFeedback = t("执行权限将在下一帧边界生效。");
  refreshRuntimeUi();
  try {
    await fetchRuntimeJson<JsonObject>("./api/runtime/execution-permission", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        permission_level: permissionLevel,
        unlimited_confirmed: permissionLevel === "unlimited" && runtimeProjection.unlimitedConfirmed,
      }),
    });
  } catch (error: unknown) {
    els.permissionLevel.value = projected;
    const failure = errorView(error);
    runtimeProjection.sendFeedback = `${t("执行权限切换失败")}：${failure.code || failure.message}`;
  } finally {
    runtimeProjection.permissionChanging = false;
    await pollRuntime({ forceFull: true, ignoreVisibility: true });
  }
}

export async function resolveModelContextWindow(
  connectionId: string,
  model: string,
): Promise<ModelContextResolution> {
  const payload = await fetchRuntimeJson<ModelContextResolution>(
    "./api/settings/model-context-window/resolve",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connection_id: connectionId, model }),
    },
  );
  if (
    payload.schema_version !== "seed_gui_model_context_resolution.v1"
    || payload.model !== model
    || !["provider", "registry", "unknown"].includes(payload.source)
  ) {
    throw new Error("model_context_resolution_schema_mismatch");
  }
  return payload;
}

export async function submitToolApproval(
  approvalId: string,
  decision: "allow_once" | "skip",
): Promise<void> {
  if (!approvalId || runtimeProjection.approvalSubmitting) return;
  runtimeProjection.approvalSubmitting = approvalId;
  runtimeProjection.approvalFeedback = "";
  renderChat();
  try {
    await fetchRuntimeJson<JsonObject>("./api/runtime/tool-approval", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ approval_id: approvalId, decision }),
    });
  } catch (error: unknown) {
    const failure = errorView(error);
    runtimeProjection.approvalFeedback = `${t("审批失败")}：${failure.code || failure.message}`;
  } finally {
    runtimeProjection.approvalSubmitting = "";
    await pollRuntime({ forceFull: true, ignoreVisibility: true });
  }
}

export function pollAbout({ force = false }: { force?: boolean } = {}): Promise<boolean> {
  if (polling.about) return polling.about;
  if (!force && aboutProjection.data) return Promise.resolve(false);
  aboutProjection.loading = true;
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<AboutPayload>("./api/about");
      if (
        payload.schema_version !== "seed_gui_about.v1"
        || typeof payload.product?.version !== "string"
        || typeof payload.links?.repository !== "string"
        || typeof payload.build?.git_head !== "string"
      ) throw new Error("about_projection_schema_mismatch");
      aboutProjection.data = payload;
      aboutProjection.error = "";
    } catch (error: unknown) {
      const failure = errorView(error);
      aboutProjection.error = `${t("关于信息读取失败")}：${failure.code || failure.message || "unknown"}`;
    } finally {
      aboutProjection.loading = false;
    }
    renderIdentity();
    if (state.globalSettingsOpen && state.globalSettingsTab === "about") renderGlobalSettings();
    return true;
  })();
  polling.about = request.finally(() => { polling.about = null; });
  return polling.about;
}

export function pollPersonaCatalog({ force = false }: { force?: boolean } = {}): Promise<boolean> {
  if (polling.personas) return polling.personas;
  if (!force && personaCatalogProjection.data) return Promise.resolve(false);
  personaCatalogProjection.loading = true;
  const request = (async () => {
    try {
      const payload = await fetchRuntimeJson<PersonaCatalogPayload>("./api/personas");
      if (payload.schema_version !== "seed_gui_persona_catalog.v1") {
        throw new Error("persona_catalog_schema_mismatch");
      }
      personaCatalogProjection.data = payload;
      personaCatalogProjection.error = "";
    } catch (error: unknown) {
      const failure = errorView(error);
      personaCatalogProjection.error = failure.code || failure.message;
    } finally {
      personaCatalogProjection.loading = false;
    }
    renderIdentity();
    return true;
  })();
  polling.personas = request.finally(() => { polling.personas = null; });
  return polling.personas;
}

export async function submitInstanceMutation(
  path: string,
  body: JsonObject,
): Promise<void> {
  if (personaCatalogProjection.pending) return;
  personaCatalogProjection.pending = true;
  personaCatalogProjection.error = "";
  renderIdentity();
  try {
    const receipt = await fetchRuntimeJson<JsonObject>(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (receipt.schema_version !== "seed_gui_instance_mutation_receipt.v1") {
      throw new Error("instance_mutation_receipt_mismatch");
    }
    clearRoundProjectionForIdentityMutation();
    if (receipt.restart_required === true) {
      runtimeProjection.sendFeedback = t("正在切换位格或分身");
      refreshRuntimeUi();
      requestDesktopBackendRestart();
    } else {
      refreshRuntimeUi();
      await pollPersonaCatalog({ force: true });
    }
  } catch (error: unknown) {
    const failure = errorView(error);
    personaCatalogProjection.error = failure.code === "instance_restart_host_required"
      ? t("此操作需要桌面客户端安全重启后端。")
      : failure.code === "instance_mutation_failed"
        ? t("位格或分身变更失败，请重试。")
      : failure.code || failure.message;
  } finally {
    personaCatalogProjection.pending = false;
    renderIdentity();
  }
}

export async function submitRuntimeStop(): Promise<void> {
  if (runtimeProjection.stopping) return;
  runtimeProjection.stopping = true;
  runtimeProjection.sendFeedback = t("正在请求停止");
  refreshRuntimeUi();
  try {
    const receipt = await fetchRuntimeJson<{
      schema_version?: string;
      reason?: string;
      stage?: string;
    }>("./api/runtime/stop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (receipt.schema_version !== "seed_gui_runtime_stop_receipt.v1") {
      throw new Error("runtime_stop_receipt_mismatch");
    }
    runtimeProjection.sendFeedback = receipt.reason === "local_cleanup_in_progress"
      ? t("正在本地善后")
      : t("已请求停止；将完成本地保存与结算");
  } catch (error: unknown) {
    const failure = errorView(error);
    const labels = {
      403: t("请求来源被拒绝"),
      409: t("当前没有正在执行的轮次"),
      503: t("本地运行时宿主不可用"),
    };
    runtimeProjection.sendFeedback = `${labels[failure.status as keyof typeof labels] || t("停止失败")}：${failure.code || failure.message}`;
  } finally {
    runtimeProjection.stopping = false;
    await pollRuntime({ forceFull: true, ignoreVisibility: true });
  }
}

export async function submitRuntimeRelay(): Promise<void> {
  const relay = relayRuntimeState();
  if (taskProjection.relayPending) return;
  taskProjection.relayFeedback = "";
  if (!relay.ready) {
    taskProjection.relayFeedback = t("当前结构化状态不是可执行中继。");
    renderStage("run");
    return;
  }
  if (relay.mutationInFlight || relay.inFlight) {
    taskProjection.relayFeedback = t("已有运行时写操作正在执行。");
    renderStage("run");
    return;
  }
  const permissionLevel = els.permissionLevel.value as PermissionLevel;
  if (!confirmUnlimitedPermission(permissionLevel)) {
    taskProjection.relayFeedback = t("已保持受限权限；未执行中继。");
    renderStage("run");
    return;
  }
  taskProjection.relayPending = true;
  renderSourceState();
  renderComposerState();
  renderStage("run");
  try {
    const payload = await fetchRuntimeJson<{ ok?: boolean; command?: string }>("./api/runtime/relay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        permission_level: permissionLevel,
        unlimited_confirmed: permissionLevel === "unlimited" && runtimeProjection.unlimitedConfirmed,
      }),
    });
    if (payload.ok !== true || payload.command !== "relay") {
      throw new Error("runtime_relay_response_mismatch");
    }
    taskProjection.relayFeedback = t("中继已完成；界面只显示随后重读到的真实轮次账本。");
  } catch (error: unknown) {
    const failure = errorView(error);
    const labels = {
      400: t("中继参数无效"),
      403: t("来源或放行权限确认被拒绝"),
      409: t("中继状态已变化或已有写操作在途"),
      502: t("运行时中继执行失败"),
      503: t("本地运行时宿主不可用"),
    };
    taskProjection.relayFeedback = `${labels[failure.status as keyof typeof labels] || t("中继失败")}：${failure.code || failure.message}`;
  } finally {
    taskProjection.relayPending = false;
    await Promise.all([
      pollRuntime({ forceFull: true, ignoreVisibility: true }),
      pollTaskProjection({ force: true, ignoreVisibility: true }),
    ]);
  }
}

export async function retryProjection(target: string): Promise<void> {
  if (target === "history") {
    await syncConversationHistory({ force: true });
    refreshRuntimeUi();
    return;
  }
  if (target === "live") {
    runtimeProjection.liveRetryAfter = 0;
    await pollRuntime({ forceFull: true, ignoreVisibility: true });
    return;
  }
  if (target === "runtime") {
    await Promise.all([
      pollRuntime({ forceFull: true, ignoreVisibility: true }),
      pollTaskProjection({ force: true, ignoreVisibility: true }),
      pollDeposition({ force: true, ignoreVisibility: true }),
    ]);
    return;
  }
  if (target === "task") {
    await pollTaskProjection({ force: true, ignoreVisibility: true });
    return;
  }
  if (target === "deposition") {
    await pollDeposition({ force: true, ignoreVisibility: true });
  }
}

function evidenceExportRound(): number | null {
  const selectedRound = state.selectedLedgerRound ?? runtimeProjection.round;
  const live = selectedRound === runtimeProjection.round
    ? runtimeProjection.live
    : selectedRound == null ? null : runtimeProjection.conversationRounds.get(selectedRound) || null;
  return live && selectedRound != null ? selectedRound : null;
}

export async function exportCurrentEvidence(): Promise<void> {
  const round = evidenceExportRound();
  if (round === null) {
    runtimeProjection.exportFeedback = t("没有可导出的真实轮次投影。");
    renderStageAndFocus("audit", "[data-export-evidence]");
    return;
  }
  try {
    const detail = await fetchLiveDetail("evidence", round);
    const payload = jsonObject(detail.payload);
    payload.exported_at = new Date().toISOString();
    payload.task_projection = taskProjection.data;
    payload.deposition_index = depositionProjection.index;
    const body = `${JSON.stringify(payload, null, 2)}\n`;
    const url = URL.createObjectURL(new Blob([body], { type: "application/json;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `upsp-seed-round-${String(round).padStart(6, "0")}-evidence.json`;
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
    runtimeProjection.exportFeedback = t("已从当前结构化投影生成本地 JSON。");
  } catch (error: unknown) {
    runtimeProjection.exportFeedback = `${t("导出失败")}：${errorView(error).message || "unknown"}`;
  }
  renderStageAndFocus("audit", "[data-export-evidence]");
}
