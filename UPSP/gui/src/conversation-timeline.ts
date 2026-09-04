import type { ConversationCard, DialogueActivity, DialogueNode, LiveState } from "./contracts";
import { scrollConversationToBottomIfSticky as scrollToBottomIfSticky } from "./conversation-scroll";
import { hydrateMarkdownDocuments, renderMarkdownDocument } from "./markdown";
import { bootstrapProjection, els, runtimeProjection, state } from "./state";
import { runtimeTerm, t } from "./i18n";

interface SmoothState {
  element: HTMLElement;
  displayed: string;
  target: string;
  deadline: number;
}

const smoothStates = new Map<string, SmoothState>();
let smoothTimer = 0;
let elapsedTimer = 0;
let visibilityBound = false;
let fontReadyBound = false;

function personaAbbreviation(): string {
  return bootstrapProjection.data?.identity?.abbreviation || "UPSP";
}

function text(value: unknown): string {
  return String(value ?? "");
}

function formatJson(value: unknown): string {
  if (value === undefined || value === null || value === "") return t("无");
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function phaseLabel(value: string | undefined): string {
  if (value === "setup") return t("起手");
  if (value === "cleanup") return t("善后");
  if (value === "reaction" || value === "final_reply") return t("反应");
  return value ? runtimeTerm(value) : t("轮次");
}

function statusLabel(value: string): string {
  const labels: Record<string, string> = {
    active: t("进行中"),
    running: t("执行中"),
    pending_approval: t("等待审批"),
    completed: t("已完成"),
    success: t("成功"),
    failed: t("失败"),
    rejected: t("已拒绝"),
    not_adopted: t("输出未采用"),
    interrupted: t("输出中断"),
    stopped: t("已停止"),
    skipped: t("已跳过"),
    unmatched_result: t("未匹配结果"),
  };
  return labels[value] || value || t("已记录");
}

function eventTime(value: string | undefined): string {
  const date = new Date(value || "");
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat(state.locale === "en-US" ? "en-US" : "zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).format(date);
}

function eventDuration(node: DialogueNode): string {
  const started = Date.parse(node.started_at || "");
  const ended = Date.parse(node.ended_at || "");
  if (!Number.isFinite(started) || !Number.isFinite(ended) || ended < started) return "";
  return `${((ended - started) / 1000).toFixed(1)}s`;
}

function activityLabel(activity: DialogueActivity | undefined): string {
  const labels: Record<string, string> = {
    connecting: t("连接"),
    reasoning: t("思考"),
    output: t("输出"),
    tool: t("工具"),
    approval: t("审批"),
    waiting: t("等待"),
    completed: t("已完成"),
    stopped: t("已停止"),
    failed: t("失败"),
    local_settlement: t("本地结算"),
  };
  return labels[text(activity?.activity)] || text(activity?.activity) || t("等待");
}

function projectedActivity(
  round: number,
  activity: DialogueActivity | undefined,
  activeRound: boolean,
): DialogueActivity | undefined {
  const status = runtimeProjection.status;
  if (
    !activeRound
    || activity?.terminal !== true
    || status?.stage !== "cleanup_local"
    || Number(status.current_round) !== round
  ) return activity;
  return {
    ...activity,
    phase: "cleanup",
    activity: "local_settlement",
    terminal: false,
    round_ended_at: "",
  };
}

function orderedNodes(live: LiveState): DialogueNode[] {
  const timeline = live.dialogue_timeline;
  if (timeline?.schema_version !== "round_dialogue_timeline.v1") return [];
  const byId = new Map(timeline.nodes.map((node) => [node.node_id, node]));
  return timeline.order.map((nodeId) => byId.get(nodeId)).filter((node): node is DialogueNode => Boolean(node));
}

function reconcileChildren(parent: HTMLElement, order: string[], selector: string): void {
  const retained = new Set(order);
  parent.querySelectorAll<HTMLElement>(`:scope > ${selector}`).forEach((element) => {
    const key = element.dataset.timelineKey || "";
    if (!retained.has(key)) {
      smoothStates.delete(key);
      element.remove();
    }
  });
  order.forEach((key) => {
    const child = parent.querySelector<HTMLElement>(`:scope > ${selector}[data-timeline-key="${CSS.escape(key)}"]`);
    if (child) parent.appendChild(child);
  });
}

function nodeRoot(rail: HTMLElement, node: DialogueNode): HTMLElement {
  const existing = rail.querySelector<HTMLElement>(`:scope > [data-timeline-key="${CSS.escape(node.node_id)}"]`);
  if (existing) return existing;
  const element = document.createElement(node.type === "reasoning" || node.type === "tool" ? "details" : "article");
  element.dataset.timelineKey = node.node_id;
  element.dataset.dialogueNodeId = node.node_id;
  element.className = `dialogue-node dialogue-${node.type}`;
  rail.appendChild(element);
  return element;
}

function metadata(node: DialogueNode): string {
  const parts = [phaseLabel(node.phase)];
  if (node.iteration !== undefined && node.iteration !== null) parts.push(`${t("迭代")} ${node.iteration}`);
  parts.push(statusLabel(node.status));
  const recordedAt = eventTime(node.started_at);
  const duration = eventDuration(node);
  if (recordedAt) parts.push(recordedAt);
  if (duration) parts.push(duration);
  return parts.join(" · ");
}

function setPlainBody(root: HTMLElement, source: string, className = "dialogue-text"): HTMLElement {
  let body = root.querySelector<HTMLElement>(`:scope > .${className}`);
  if (!body) {
    body = document.createElement("div");
    body.className = className;
    root.appendChild(body);
  }
  if (body.dataset.renderMode !== "plain") {
    body.replaceChildren();
    body.dataset.renderMode = "plain";
  }
  return body;
}

function settleMarkdown(root: HTMLElement, node: DialogueNode, source: string): void {
  let body = root.querySelector<HTMLElement>(":scope > .dialogue-text");
  if (!body) {
    body = document.createElement("div");
    body.className = "dialogue-text";
    root.appendChild(body);
  }
  if (body.dataset.renderMode === "markdown" && body.dataset.source === source) return;
  smoothStates.delete(node.node_id);
  body.dataset.renderMode = "markdown";
  body.dataset.source = source;
  body.innerHTML = renderMarkdownDocument(`dialogue-${node.node_id}`, source);
  hydrateMarkdownDocuments(body, els.chatThread, () => state.conversationStickToBottom);
}

function prefersReducedMotion(): boolean {
  return window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
}

function smoothText(root: HTMLElement, node: DialogueNode, source: string): void {
  const body = setPlainBody(root, source);
  let smooth = smoothStates.get(node.node_id);
  if (!smooth || smooth.element !== body || !source.startsWith(smooth.displayed)) {
    smooth = {
      element: body,
      displayed: source.startsWith(body.textContent || "") ? body.textContent || "" : source,
      target: source,
      deadline: performance.now() + 1500,
    };
    smoothStates.set(node.node_id, smooth);
  }
  const targetChanged = smooth.target !== source;
  smooth.target = source;
  if (targetChanged) smooth.deadline = performance.now() + 1500;
  body.dataset.source = source;
  body.textContent = smooth.displayed;
  ensureSmoothTimer();
}

function scrollConversationToBottomIfSticky(): void {
  scrollToBottomIfSticky(els.chatThread, state);
}

function flushSmoothStates(): void {
  let changed = false;
  smoothStates.forEach((smooth) => {
    changed = changed || smooth.displayed !== smooth.target;
    smooth.displayed = smooth.target;
    smooth.element.textContent = smooth.target;
  });
  if (smoothTimer) window.clearInterval(smoothTimer);
  smoothTimer = 0;
  if (changed) scrollConversationToBottomIfSticky();
}

function ensureSmoothTimer(): void {
  if (smoothTimer || document.hidden || prefersReducedMotion()) {
    if (document.hidden || prefersReducedMotion()) flushSmoothStates();
    return;
  }
  smoothTimer = window.setInterval(() => {
    let pending = false;
    let changed = false;
    smoothStates.forEach((smooth, key) => {
      if (!smooth.element.isConnected) {
        smoothStates.delete(key);
        return;
      }
      const backlog = smooth.target.length - smooth.displayed.length;
      if (backlog <= 0) return;
      const remainingTicks = Math.max(1, Math.ceil((smooth.deadline - performance.now()) / 40));
      const take = Math.max(1, Math.ceil(backlog / remainingTicks));
      smooth.displayed = smooth.target.slice(0, smooth.displayed.length + take);
      smooth.element.textContent = smooth.displayed;
      changed = true;
      pending = pending || smooth.displayed !== smooth.target;
    });
    if (changed) scrollConversationToBottomIfSticky();
    if (!pending) {
      window.clearInterval(smoothTimer);
      smoothTimer = 0;
    }
  }, 40);
}

function updateUser(root: HTMLElement, node: DialogueNode): void {
  root.className = "dialogue-node dialogue-user chat-bubble user";
  let who = root.querySelector<HTMLElement>(":scope > b");
  if (!who) {
    who = document.createElement("b");
    root.appendChild(who);
  }
  who.textContent = t("你");
  const body = setPlainBody(root, node.content_raw || "");
  body.textContent = node.content_raw || "";
}

function updateReasoning(root: HTMLDetailsElement, node: DialogueNode, isLatest: boolean, ordinal: number): void {
  root.className = `dialogue-node dialogue-reasoning status-${node.status}`;
  let summary = root.querySelector<HTMLElement>(":scope > summary");
  if (!summary) {
    summary = document.createElement("summary");
    root.appendChild(summary);
  }
  summary.textContent = `${t("思考片段")} ${ordinal} · ${metadata(node)}`;
  const disclosure = state.conversationDisclosure.get(node.node_id);
  root.open = disclosure === undefined ? node.status === "active" && isLatest : disclosure;
  const body = setPlainBody(root, node.content_raw || "", "dialogue-reasoning-body");
  body.textContent = node.content_raw || "";
}

function updateMessage(root: HTMLElement, node: DialogueNode, smooth: boolean): void {
  root.className = `dialogue-node dialogue-${node.type} chat-bubble system status-${node.status}`;
  let head = root.querySelector<HTMLElement>(":scope > .dialogue-node-head");
  if (!head) {
    head = document.createElement("div");
    head.className = "dialogue-node-head";
    root.prepend(head);
  }
  head.textContent = `${node.type === "final" ? t("最终回复") : t("轮中进展")} · ${metadata(node)}`;
  if (node.type === "final") {
    const copy = document.createElement("button");
    copy.type = "button";
    copy.className = "chat-item-copy";
    copy.dataset.markdownDocumentCopy = "true";
    copy.textContent = t("复制最终回复");
    head.append(" · ", copy);
  }
  const source = node.content_raw || node.message || "";
  if (smooth) smoothText(root, node, source);
  else settleMarkdown(root, node, source);
}

function updateFailure(root: HTMLElement, node: DialogueNode): void {
  root.className = `dialogue-node dialogue-failure status-${node.status}`;
  root.textContent = "";
  const head = document.createElement("b");
  head.textContent = `${t("失败状态")} · ${metadata(node)}`;
  const body = document.createElement("p");
  body.textContent = node.message || statusLabel(node.status);
  root.append(head, body);
}

function updateTool(root: HTMLDetailsElement, node: DialogueNode, round: number): void {
  root.className = `dialogue-node dialogue-tool status-${node.status}`;
  let summary = root.querySelector<HTMLElement>(":scope > summary");
  if (!summary) {
    summary = document.createElement("summary");
    root.appendChild(summary);
  }
  summary.textContent = `${node.tool_id || t("工具")} · ${metadata(node)}`;
  const disclosure = state.conversationDisclosure.get(node.node_id);
  if (disclosure !== undefined) root.open = disclosure;
  let body = root.querySelector<HTMLElement>(":scope > .dialogue-tool-body");
  if (!body) {
    body = document.createElement("div");
    body.className = "dialogue-tool-body";
    root.appendChild(body);
  }
  const detailKey = `${round}:${node.detail_ref || node.node_id}`;
  const detail = runtimeProjection.timelineNodeDetails.get(detailKey);
  const argumentsValue = detail?.arguments;
  const resultValue = detail?.result;
  const signature = JSON.stringify([
    node.status, argumentsValue, resultValue, node.approval_id, node.approval_decision,
    runtimeProjection.approvalSubmitting, runtimeProjection.approvalFeedback,
    runtimeProjection.timelineNodeLoading.has(detailKey), runtimeProjection.timelineNodeErrors.get(detailKey),
  ]);
  if (body.dataset.signature === signature) return;
  body.dataset.signature = signature;
  body.replaceChildren();
  const argumentsTitle = document.createElement("b");
  argumentsTitle.textContent = t("调用参数");
  const argumentsBody = document.createElement("pre");
  if (!detail && node.detail_ref) {
    const loading = runtimeProjection.timelineNodeLoading.has(detailKey);
    const error = runtimeProjection.timelineNodeErrors.get(detailKey);
    argumentsBody.textContent = error ? `${t("详情读取失败")}：${error}` : t(loading ? "正在读取工具详情" : "展开后读取工具详情");
    if (!loading) {
      argumentsBody.dataset.timelineDetailRound = String(round);
      argumentsBody.dataset.timelineDetailRef = node.detail_ref;
    }
  } else argumentsBody.textContent = formatJson(argumentsValue);
  const resultTitle = document.createElement("b");
  resultTitle.textContent = t("执行结果");
  const resultBody = document.createElement("pre");
  resultBody.textContent = node.status === "unmatched_result"
    ? t("未匹配结果")
    : detail ? formatJson(resultValue) : "—";
  body.append(argumentsTitle, argumentsBody, resultTitle, resultBody);
  const pending = runtimeProjection.status?.pending_tool_approval;
  if (node.status === "pending_approval" && node.approval_id && pending?.approval_id === node.approval_id) {
    const actions = document.createElement("div");
    actions.className = "tool-approval-actions";
    actions.setAttribute("role", "group");
    actions.setAttribute("aria-label", t("工具执行审批"));
    const submitting = runtimeProjection.approvalSubmitting === node.approval_id;
    for (const [decision, label] of [["skip", t("跳过")], ["allow_once", t("本次允许")]] as const) {
      const button = document.createElement("button");
      button.type = "button";
      button.dataset.toolApprovalId = node.approval_id;
      button.dataset.toolApprovalDecision = decision;
      button.disabled = submitting;
      button.textContent = submitting ? t("正在处理") : label;
      actions.appendChild(button);
    }
    body.appendChild(actions);
    if (runtimeProjection.approvalFeedback) {
      const feedback = document.createElement("small");
      feedback.setAttribute("role", "status");
      feedback.textContent = runtimeProjection.approvalFeedback;
      body.appendChild(feedback);
    }
  }
}

function updateActivity(
  root: HTMLElement,
  activity: DialogueActivity | undefined,
  projectionStale = false,
): void {
  root.className = `dialogue-activity ${activity?.terminal ? "terminal" : "active"}${projectionStale ? " projection-stale" : ""}`;
  root.dataset.startedAt = activity?.round_started_at || "";
  root.dataset.endedAt = activity?.round_ended_at || "";
  root.dataset.terminal = String(Boolean(activity?.terminal));
  root.dataset.projectionStale = String(projectionStale);
  let ring = root.querySelector<HTMLElement>(":scope > .dialogue-breath-ring");
  let label = root.querySelector<HTMLElement>(":scope > .dialogue-activity-label");
  let elapsed = root.querySelector<HTMLElement>(":scope > .dialogue-activity-elapsed");
  if (!ring) {
    ring = document.createElement("span");
    ring.className = "dialogue-breath-ring";
    ring.setAttribute("aria-hidden", "true");
    root.appendChild(ring);
  }
  if (!label) {
    label = document.createElement("span");
    label.className = "dialogue-activity-label";
    root.appendChild(label);
  }
  if (!elapsed) {
    elapsed = document.createElement("span");
    elapsed.className = "dialogue-activity-elapsed";
    root.appendChild(elapsed);
  }
  label.textContent = `${phaseLabel(activity?.phase)} · ${activityLabel(activity)}`;
  root.setAttribute("role", "status");
  root.setAttribute("aria-label", label.textContent);
}

function updateElapsed(): void {
  els.chatThread.querySelectorAll<HTMLElement>(".dialogue-activity").forEach((root) => {
    const target = root.querySelector<HTMLElement>(".dialogue-activity-elapsed");
    if (!target) return;
    const started = Date.parse(root.dataset.startedAt || "");
    if (!Number.isFinite(started)) {
      target.textContent = "";
      return;
    }
    const terminal = root.dataset.terminal === "true";
    const previous = Number(root.dataset.elapsedSeconds || 0);
    if (root.dataset.projectionStale === "true" && root.dataset.elapsedSeconds !== undefined) {
      target.textContent = `${previous}${t("秒")}`;
      return;
    }
    const ended = Date.parse(root.dataset.endedAt || "");
    const current = terminal && Number.isFinite(ended) ? ended : Date.now();
    const seconds = Math.max(previous, Math.floor((current - started) / 1000));
    root.dataset.elapsedSeconds = String(seconds);
    target.textContent = `${seconds}${t("秒")}`;
  });
}

function ensureTimers(): void {
  if (!elapsedTimer) elapsedTimer = window.setInterval(updateElapsed, 1000);
  if (!fontReadyBound) {
    void document.fonts?.ready.then(scrollConversationToBottomIfSticky);
    fontReadyBound = true;
  }
  if (!visibilityBound) {
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) flushSmoothStates();
    });
    visibilityBound = true;
  }
  updateElapsed();
}

function reconcileRound(round: number, live: LiveState, activeRound: boolean): HTMLElement {
  const key = `round:${round}`;
  let root = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${CSS.escape(key)}"]`);
  if (!root) {
    root = document.createElement("section");
    root.className = "chat-round-timeline";
    root.dataset.timelineKey = key;
    root.dataset.chatAnchor = key;
    const rail = document.createElement("div");
    rail.className = "dialogue-event-rail";
    root.appendChild(rail);
    els.chatThread.appendChild(root);
  }
  const rail = root.querySelector<HTMLElement>(":scope > .dialogue-event-rail")!;
  if (live.display_mode === "legacy") {
    const cards = runtimeProjection.legacyCards.get(round);
    rail.replaceChildren();
    if (!cards) {
      const notice = document.createElement("p");
      notice.className = "chat-history-warning";
      const loading = runtimeProjection.legacyCardsLoading.has(round);
      const error = runtimeProjection.legacyCardsErrors.get(round);
      notice.textContent = error ? `${t("较早对话未完全载入")}：${error}` : t(loading ? "正在读取最近对话" : "正在读取最近对话");
      rail.appendChild(notice);
    } else {
      cards.forEach((card, position) => {
        const article = document.createElement("article");
        article.className = `runtime-card legacy-conversation-card ${card.severity === "error" ? "warn" : ""}`;
        article.dataset.timelineKey = `legacy:${round}:${card.card_id || card.event_index || position}`;
        const header = document.createElement("header");
        const title = document.createElement("strong");
        title.textContent = card.title || card.type;
        const summary = document.createElement("span");
        summary.textContent = card.summary || `#${card.event_index || 0}`;
        header.append(title, summary);
        const body = document.createElement("pre");
        body.textContent = text(card.content_raw || card.content_md || card.content);
        article.append(header, body);
        rail.appendChild(article);
      });
    }
    return root;
  }
  const nodes = orderedNodes(live);
  const activeText = [...nodes].reverse().find((node) => ["progress", "final"].includes(node.type) && node.status === "active");
  let reasoningOrdinal = 0;
  nodes.forEach((node, position) => {
    const element = nodeRoot(rail, node);
    if (node.type === "user") updateUser(element, node);
    else if (node.type === "reasoning") {
      reasoningOrdinal += 1;
      updateReasoning(element as HTMLDetailsElement, node, position === nodes.length - 1, reasoningOrdinal);
    } else if (node.type === "tool") updateTool(element as HTMLDetailsElement, node, round);
    else if (node.type === "failure") updateFailure(element, node);
    else updateMessage(
      element,
      node,
      activeRound && live.dialogue_activity?.terminal !== true && activeText?.node_id === node.node_id && !prefersReducedMotion(),
    );
    if (node.type === "tool" && node.detail_ref) {
      element.dataset.timelineDetailRound = String(round);
      element.dataset.timelineDetailRef = node.detail_ref;
    }
  });
  const activityKey = `${key}:activity`;
  let activity = rail.querySelector<HTMLElement>(`:scope > [data-timeline-key="${CSS.escape(activityKey)}"]`);
  if (!activity) {
    activity = document.createElement("div");
    activity.dataset.timelineKey = activityKey;
    rail.appendChild(activity);
  }
  updateActivity(
    activity,
    projectedActivity(round, live.dialogue_activity, activeRound),
    activeRound && Boolean(runtimeProjection.liveError),
  );
  reconcileChildren(rail, [...nodes.map((node) => node.node_id), activityKey], "[data-timeline-key]");
  return root;
}

export function rememberTimelineDisclosure(details: HTMLDetailsElement, open: boolean): void {
  const key = details.dataset.dialogueNodeId;
  if (key) state.conversationDisclosure.set(key, open);
}

export function renderConversationTimeline(): void {
  if (runtimeProjection.liveError) flushSmoothStates();
  const hadMessages = els.chatThread.childElementCount > 0;
  if (!hadMessages) state.conversationStickToBottom = true;
  const stickToBottom = state.conversationStickToBottom;
  const previousScrollTop = els.chatThread.scrollTop;
  const previousAnchor = [...els.chatThread.children].find((item): item is HTMLElement => (
    item instanceof HTMLElement && Boolean(item.dataset.chatAnchor) && item.offsetTop + item.offsetHeight > previousScrollTop
  ));
  const previousAnchorKey = previousAnchor?.dataset.chatAnchor || "";
  const previousAnchorOffset = previousAnchor ? previousAnchor.offsetTop - previousScrollTop : 0;
  const order: string[] = [];
  const interrupted = runtimeProjection.status?.interrupted_recovery;
  if (interrupted?.pending) {
    const key = "interrupted-recovery";
    let notice = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${key}"]`);
    if (!notice) {
      notice = document.createElement("div");
      notice.className = "chat-projection-warning";
      notice.dataset.timelineKey = key;
      notice.setAttribute("role", "status");
      const copy = document.createElement("span");
      copy.className = "chat-projection-warning-copy";
      copy.append(document.createElement("strong"), document.createElement("small"));
      notice.appendChild(copy);
      els.chatThread.appendChild(notice);
    }
    const known = Number(interrupted.applied_unregistered || 0)
      + Number(interrupted.applied_registered || 0)
      + Number(interrupted.not_applied || 0)
      + Number(interrupted.known_result || 0);
    const uncertain = Number(interrupted.conflict || 0)
      + Number(interrupted.outcome_unknown || 0);
    notice.querySelector("strong")!.textContent = t("上次任务意外中断，下一条消息将先核对现场");
    notice.querySelector("small")!.textContent = `${t("已确定动作")} ${known} · ${t("受阻或结果不确定动作")} ${uncertain}`;
    order.push(key);
  }
  if (runtimeProjection.liveError) {
    const key = "live-warning";
    let warning = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${key}"]`);
    if (!warning) {
      warning = document.createElement("div");
      warning.className = "chat-projection-warning";
      warning.dataset.timelineKey = key;
      warning.setAttribute("role", "status");
      const message = document.createElement("span");
      message.className = "chat-projection-warning-copy";
      const title = document.createElement("strong");
      const detail = document.createElement("small");
      const retry = document.createElement("button");
      retry.type = "button";
      retry.dataset.retryProjection = "live";
      retry.textContent = t("重试");
      message.append(title, detail);
      warning.append(message, retry);
      els.chatThread.appendChild(warning);
    }
    warning.querySelector("strong")!.textContent = t("对话投影暂不可用，Runtime 仍在运行");
    const stoppedAt = runtimeProjection.liveErrorEventIndex > 0
      ? `${t("更新停在事件")} #${runtimeProjection.liveErrorEventIndex} · `
      : "";
    warning.querySelector("small")!.textContent = `${stoppedAt}${runtimeProjection.liveError}`;
    warning.querySelector(":scope > .chat-projection-approval")?.remove();
    const pendingApproval = runtimeProjection.status?.pending_tool_approval;
    const approvalAlreadyProjected = Boolean(
      pendingApproval?.approval_id
      && runtimeProjection.live?.dialogue_timeline?.nodes.some(
        (node) => node.status === "pending_approval"
          && node.approval_id === pendingApproval.approval_id,
      ),
    );
    if (pendingApproval?.approval_id && !approvalAlreadyProjected) {
      const approval = document.createElement("div");
      approval.className = "chat-projection-approval";
      const label = document.createElement("b");
      label.textContent = `${t("等待工具执行审批")} · ${pendingApproval.tool_label || pendingApproval.tool_id}`;
      const actions = document.createElement("div");
      actions.className = "tool-approval-actions";
      actions.setAttribute("role", "group");
      actions.setAttribute("aria-label", t("工具执行审批"));
      const submitting = runtimeProjection.approvalSubmitting === pendingApproval.approval_id;
      for (const [decision, actionLabel] of [["skip", t("跳过")], ["allow_once", t("本次允许")]] as const) {
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.toolApprovalId = pendingApproval.approval_id;
        button.dataset.toolApprovalDecision = decision;
        button.disabled = submitting;
        button.textContent = submitting ? t("正在处理") : actionLabel;
        actions.appendChild(button);
      }
      approval.append(label, actions);
      if (runtimeProjection.approvalFeedback) {
        const feedback = document.createElement("small");
        feedback.setAttribute("role", "status");
        feedback.textContent = runtimeProjection.approvalFeedback;
        approval.appendChild(feedback);
      }
      warning.appendChild(approval);
    }
    order.push(key);
  }
  if (runtimeProjection.conversationHistoryHasMore) {
    const key = "history-more";
    let loader = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${key}"]`);
    if (!loader) {
      loader = document.createElement("p");
      loader.className = "chat-history-warning";
      loader.dataset.timelineKey = key;
      els.chatThread.appendChild(loader);
    }
    loader.innerHTML = `<button type="button" data-retry-projection="history" ${runtimeProjection.conversationHistoryLoading ? "disabled" : ""}>${t(runtimeProjection.conversationHistoryLoading ? "正在载入较早对话" : "加载较早对话")}</button>`;
    order.push(key);
  }
  if (runtimeProjection.conversationHistoryError) {
    const key = "history-warning";
    let warning = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${key}"]`);
    if (!warning) {
      warning = document.createElement("p");
      warning.className = "chat-history-warning";
      warning.dataset.timelineKey = key;
      warning.innerHTML = `<span>${t("较早对话未完全载入")}</span><button type="button" data-retry-projection="history">${t("重试")}</button>`;
      els.chatThread.appendChild(warning);
    }
    order.push(key);
  }
  runtimeProjection.conversationRoundOrder.forEach((round) => {
    const live = runtimeProjection.conversationRounds.get(round);
    if (!live) return;
    const root = reconcileRound(round, live, round === runtimeProjection.round);
    order.push(root.dataset.timelineKey || `round:${round}`);
  });
  if (!order.length) {
    const key = "empty";
    let empty = els.chatThread.querySelector<HTMLElement>(`:scope > [data-timeline-key="${key}"]`);
    if (!empty) {
      empty = document.createElement("div");
      empty.className = "conversation-empty";
      empty.dataset.timelineKey = key;
      els.chatThread.appendChild(empty);
    }
    const connected = runtimeProjection.host === "connected";
    empty.innerHTML = `
      <span class="hud-label">${connected ? "ROUND / READY" : "RUNTIME / OFFLINE"}</span>
      <strong>${t(connected ? "尚无真实对话事件。" : "本地宿主未连接。")}</strong>
      <p>${t(connected
        ? "输入内容后，真实事件会按发生顺序显示在这里。"
        : "恢复本地宿主后，这里会继续读取当前分身的真实对话。")}</p>
    `;
    order.push(key);
  }
  reconcileChildren(els.chatThread, order, "[data-timeline-key]");
  ensureTimers();
  if (stickToBottom) els.chatThread.scrollTop = els.chatThread.scrollHeight;
  else {
    const nextAnchor = previousAnchorKey
      ? els.chatThread.querySelector<HTMLElement>(`[data-chat-anchor="${CSS.escape(previousAnchorKey)}"]`)
      : null;
    els.chatThread.scrollTop = nextAnchor ? nextAnchor.offsetTop - previousAnchorOffset : previousScrollTop;
  }
}
