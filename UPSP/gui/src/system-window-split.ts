import { t } from "./i18n";
import { els, state } from "./state";
import {
  DEFAULT_SYSTEM_WINDOW_RATIO,
  SYSTEM_WINDOW_RATIO_STORAGE_KEY,
  computeSystemWindowSplitGeometry,
  parseStoredSystemWindowRatio,
  systemWindowRatioFromKey,
  systemWindowRatioFromPointer,
} from "./system-window-split-model";
import type { SystemWindowSplitGeometry } from "./system-window-split-model";

const DESKTOP_QUERY = "(min-width: 761px)";

interface SplitDragState {
  pointerId: number;
  initialRatio: number;
  initialSystemWidth: number;
  startClientX: number;
}

let dragState: SplitDragState | null = null;
let deferredRender: (() => void) | null = null;
let initialized = false;
let resizeObserver: ResizeObserver | null = null;

function isDesktopSplitAvailable(): boolean {
  return window.matchMedia(DESKTOP_QUERY).matches;
}

function persistRatio(): void {
  try {
    localStorage.setItem(SYSTEM_WINDOW_RATIO_STORAGE_KEY, String(state.systemWindowRatio));
  } catch {
    // Storage is only a local convenience; resizing remains functional without it.
  }
}

function setDragging(active: boolean): void {
  els.app.classList.toggle("system-split-dragging", active);
  els.systemWindowSplitter.classList.toggle("dragging", active);
}

function updateAccessibility(geometry: SystemWindowSplitGeometry): void {
  const splitter = els.systemWindowSplitter;
  const minimum = Math.round(geometry.minRatio * 100);
  const maximum = Math.round(geometry.maxRatio * 100);
  const current = Math.round(geometry.effectiveRatio * 100);
  splitter.setAttribute("aria-valuemin", String(minimum));
  splitter.setAttribute("aria-valuemax", String(maximum));
  splitter.setAttribute("aria-valuenow", String(current));
  splitter.setAttribute("aria-valuetext", t("系统窗宽度：{value}%", { value: current }));
  splitter.title = t("拖拽调整宽度，双击恢复一半");
}

export function applySystemWindowSplit(): SystemWindowSplitGeometry {
  const geometry = computeSystemWindowSplitGeometry(
    els.mainStage.getBoundingClientRect().width,
    state.systemWindowRatio,
  );
  els.app.style.setProperty("--system-window-width", `${geometry.systemWidth}px`);
  const visible = state.systemWindowOpen && isDesktopSplitAvailable() && geometry.availableWidth > 0;
  els.systemWindowSplitter.hidden = !visible;
  els.systemWindowSplitter.tabIndex = visible ? 0 : -1;
  updateAccessibility(geometry);
  if (!visible && dragState) cancelDrag();
  return geometry;
}

function ratioFromDrag(clientX: number): number {
  if (!dragState) return state.systemWindowRatio;
  const stage = els.mainStage.getBoundingClientRect();
  const systemWidth = dragState.initialSystemWidth + clientX - dragState.startClientX;
  return systemWindowRatioFromPointer(stage.width, systemWidth);
}

function applyPointerRatio(clientX: number): void {
  state.systemWindowRatio = ratioFromDrag(clientX);
  applySystemWindowSplit();
}

export function deferSystemWindowRender(render: () => void): boolean {
  if (!dragState) return false;
  deferredRender = render;
  return true;
}

function flushDeferredRender(): void {
  const render = deferredRender;
  deferredRender = null;
  render?.();
}

function finishDrag(pointerId: number, persist: boolean): void {
  if (!dragState || dragState.pointerId !== pointerId) return;
  const initialRatio = dragState.initialRatio;
  dragState = null;
  if (els.systemWindowSplitter.hasPointerCapture(pointerId)) {
    els.systemWindowSplitter.releasePointerCapture(pointerId);
  }
  if (!persist) state.systemWindowRatio = initialRatio;
  setDragging(false);
  applySystemWindowSplit();
  if (persist) persistRatio();
  flushDeferredRender();
}

function cancelDrag(): void {
  if (!dragState) return;
  finishDrag(dragState.pointerId, false);
}

function handlePointerDown(event: PointerEvent): void {
  if (event.button !== 0 || els.systemWindowSplitter.hidden) return;
  event.preventDefault();
  const geometry = applySystemWindowSplit();
  dragState = {
    pointerId: event.pointerId,
    initialRatio: state.systemWindowRatio,
    initialSystemWidth: geometry.systemWidth,
    startClientX: event.clientX,
  };
  els.systemWindowSplitter.setPointerCapture(event.pointerId);
  setDragging(true);
}

function handlePointerMove(event: PointerEvent): void {
  if (!dragState || dragState.pointerId !== event.pointerId) return;
  event.preventDefault();
  applyPointerRatio(event.clientX);
}

function resetSystemWindowRatio(): void {
  state.systemWindowRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
  try {
    localStorage.removeItem(SYSTEM_WINDOW_RATIO_STORAGE_KEY);
  } catch {
    // The in-memory reset still applies when local storage is unavailable.
  }
  applySystemWindowSplit();
}

function handleKeyDown(event: KeyboardEvent): void {
  const geometry = applySystemWindowSplit();
  const next = systemWindowRatioFromKey(geometry, event.key, event.shiftKey);
  if (next == null) return;
  event.preventDefault();
  state.systemWindowRatio = next;
  persistRatio();
  applySystemWindowSplit();
}

export function initSystemWindowSplit(): void {
  if (initialized) return;
  initialized = true;
  try {
    state.systemWindowRatio = parseStoredSystemWindowRatio(
      localStorage.getItem(SYSTEM_WINDOW_RATIO_STORAGE_KEY),
    );
  } catch {
    state.systemWindowRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
  }
  const splitter = els.systemWindowSplitter;
  splitter.addEventListener("pointerdown", handlePointerDown);
  splitter.addEventListener("pointermove", handlePointerMove);
  splitter.addEventListener("pointerup", (event) => finishDrag(event.pointerId, true));
  splitter.addEventListener("pointercancel", (event) => finishDrag(event.pointerId, false));
  splitter.addEventListener("lostpointercapture", (event) => finishDrag(event.pointerId, false));
  splitter.addEventListener("dblclick", resetSystemWindowRatio);
  splitter.addEventListener("keydown", handleKeyDown);
  resizeObserver = new ResizeObserver(() => window.requestAnimationFrame(applySystemWindowSplit));
  resizeObserver.observe(els.mainStage);
  applySystemWindowSplit();
}
