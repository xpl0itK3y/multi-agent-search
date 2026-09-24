// Agent-trace bookkeeping shared by the research views.
import type { TraceEntry } from "./stream";
import type { GraphTrailEntry } from "./types";

// The research SSE replays the whole trail on every (re)connect and the views
// also pre-fill it from GET /graph, so one step can arrive several times. An
// entry is identified by timestamp|step|action|detail (agreed with the backend).
export function traceKey(entry: TraceEntry): string {
  return [entry.timestamp ?? "", entry.step, entry.action ?? "", entry.detail].join("|");
}

// Remembers which entries a view already shows; accept() is true only the first
// time an entry is seen. reset() when the trace is cleared (e.g. on retry).
export function createTraceDeduper() {
  const seen = new Set<string>();
  return {
    accept(entry: TraceEntry): boolean {
      const key = traceKey(entry);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    },
    reset(): void {
      seen.clear();
    },
  };
}

export function traceFromGraph(trail: GraphTrailEntry[]): TraceEntry[] {
  return trail.map((entry) => ({
    step: entry.step ?? "",
    detail: entry.detail ?? "",
    sources: entry.sources ?? [],
    agent: entry.agent,
    phase: entry.phase,
    action: entry.action,
    metrics: entry.metrics,
    timestamp: entry.timestamp,
  }));
}

// Delay before the n-th consecutive stream reconnect (0-based): 3 s, doubling,
// capped at 60 s — a failing API must not cause a tight reconnect loop.
const RECONNECT_BASE_MS = 3000;
const RECONNECT_MAX_MS = 60000;

export function reconnectDelayMs(attempt: number): number {
  return Math.min(RECONNECT_BASE_MS * 2 ** Math.max(0, attempt), RECONNECT_MAX_MS);
}
