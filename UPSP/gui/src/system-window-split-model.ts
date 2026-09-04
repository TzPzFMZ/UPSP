export const SYSTEM_WINDOW_RATIO_STORAGE_KEY = "upsp.v5.systemWindowRatio";
export const DEFAULT_SYSTEM_WINDOW_RATIO = 0.5;
export const MIN_SYSTEM_WINDOW_RATIO = 0.3;
export const MAX_SYSTEM_WINDOW_RATIO = 0.7;

const MIN_SYSTEM_WINDOW_PX = 480;
const MIN_DIALOGUE_PX = 520;
export const STAGE_RESERVED_PX = 24;

export interface SystemWindowSplitGeometry {
  availableWidth: number;
  effectiveRatio: number;
  minRatio: number;
  maxRatio: number;
  systemWidth: number;
}

export function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}

export function parseStoredSystemWindowRatio(value: string | null): number {
  if (value == null || value.trim() === "") return DEFAULT_SYSTEM_WINDOW_RATIO;
  const parsed = Number(value);
  return Number.isFinite(parsed)
    && parsed >= MIN_SYSTEM_WINDOW_RATIO
    && parsed <= MAX_SYSTEM_WINDOW_RATIO
    ? parsed
    : DEFAULT_SYSTEM_WINDOW_RATIO;
}

export function computeSystemWindowSplitGeometry(
  stageWidth: number,
  desiredRatio: number,
): SystemWindowSplitGeometry {
  const availableWidth = Math.max(0, stageWidth - STAGE_RESERVED_PX);
  let minRatio = MIN_SYSTEM_WINDOW_RATIO;
  let maxRatio = MAX_SYSTEM_WINDOW_RATIO;
  if (availableWidth < MIN_SYSTEM_WINDOW_PX + MIN_DIALOGUE_PX) {
    minRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
    maxRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
  } else {
    minRatio = Math.max(minRatio, MIN_SYSTEM_WINDOW_PX / availableWidth);
    maxRatio = Math.min(maxRatio, 1 - MIN_DIALOGUE_PX / availableWidth);
    if (minRatio > maxRatio) {
      minRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
      maxRatio = DEFAULT_SYSTEM_WINDOW_RATIO;
    }
  }
  const normalizedDesired = clamp(
    Number.isFinite(desiredRatio) ? desiredRatio : DEFAULT_SYSTEM_WINDOW_RATIO,
    MIN_SYSTEM_WINDOW_RATIO,
    MAX_SYSTEM_WINDOW_RATIO,
  );
  const effectiveRatio = clamp(normalizedDesired, minRatio, maxRatio);
  return {
    availableWidth,
    effectiveRatio,
    minRatio,
    maxRatio,
    systemWidth: Math.round(availableWidth * effectiveRatio),
  };
}

export function systemWindowRatioFromPointer(stageWidth: number, systemWidth: number): number {
  const geometry = computeSystemWindowSplitGeometry(stageWidth, DEFAULT_SYSTEM_WINDOW_RATIO);
  const availableWidth = Math.max(1, geometry.availableWidth);
  return clamp(systemWidth / availableWidth, geometry.minRatio, geometry.maxRatio);
}

export function systemWindowRatioFromKey(
  geometry: SystemWindowSplitGeometry,
  key: string,
  shiftKey: boolean,
): number | null {
  const step = shiftKey ? 0.1 : 0.02;
  let next: number | null = null;
  if (key === "ArrowLeft") next = geometry.effectiveRatio - step;
  if (key === "ArrowRight") next = geometry.effectiveRatio + step;
  if (key === "Home") next = geometry.minRatio;
  if (key === "End") next = geometry.maxRatio;
  return next == null ? null : clamp(next, geometry.minRatio, geometry.maxRatio);
}
