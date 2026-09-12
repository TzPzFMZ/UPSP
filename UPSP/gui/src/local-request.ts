// Browsers may retry a POST when a reused connection closes before reply headers.
// The server rejects reused IDs, including after a restart via the session fence.
let session = "";

export class LocalRequestError extends Error {
  constructor(message: string, public status = 0, public code = "request_failed", public payload: Record<string, unknown> = {}) {
    super(message);
  }
}

export function submissionUnconfirmed(error: unknown): boolean {
  return !(error instanceof LocalRequestError) || error.status < 400 || error.status >= 500
    || error.code === "submission_outcome_unconfirmed";
}

export async function fetchLocalJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetchLocal(path, options);
  let payload: unknown;
  try { payload = await response.json(); }
  catch {
    if (response.ok) throw new LocalRequestError("invalid_json_response", response.status, "invalid_json_response");
  }
  const object = payload !== null && typeof payload === "object" && !Array.isArray(payload)
    ? payload as Record<string, unknown> : null;
  if (!response.ok) {
    const code = typeof object?.error === "string" ? object.error : "request_failed";
    throw new LocalRequestError(code, response.status, code, object || {});
  }
  if (!object) throw new LocalRequestError("invalid_json_object", response.status, "invalid_json_object");
  return object as T;
}

export async function fetchLocal(path: string, options: RequestInit = {}): Promise<Response> {
  const headers = new Headers(options.headers);
  if (options.method?.toUpperCase() === "POST") {
    if (!session) {
      const status = await fetch("./api/runtime/status", { cache: "no-store" });
      session = status.headers.get("X-UPSP-Request-Session") || "";
      await status.arrayBuffer();
    }
    if (!session) throw new Error("request_session_unavailable");
    headers.set("X-UPSP-Request-Session", session);
    headers.set("X-UPSP-Request-Id", crypto.randomUUID());
  }
  const response = await fetch(path, { cache: "no-store", ...options, headers });
  const observed = response.headers.get("X-UPSP-Request-Session");
  if (observed) session = observed;
  return response;
}
