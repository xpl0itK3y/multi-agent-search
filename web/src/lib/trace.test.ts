import { describe, expect, it } from "vitest";

import { createTraceDeduper, reconnectDelayMs, traceFromGraph, traceKey } from "./trace";
import type { TraceEntry } from "./stream";

const step = (over: Partial<TraceEntry> = {}): TraceEntry => ({
  step: "search",
  detail: "Searching web",
  action: "query",
  timestamp: "2026-09-24T10:00:00+00:00",
  ...over,
});

describe("trace de-duplication", () => {
  it("keys entries by timestamp|step|action|detail", () => {
    expect(traceKey(step())).toBe("2026-09-24T10:00:00+00:00|search|query|Searching web");
    expect(traceKey({ step: "analyze", detail: "" })).toBe("|analyze||");
  });

  it("accepts a replayed trail only once (GET /graph pre-fill + SSE replay + reconnect)", () => {
    const trail = [step(), step({ step: "analyze", action: undefined, detail: "Writing" })];
    const dedupe = createTraceDeduper();
    const shown: TraceEntry[] = [];
    const add = (e: TraceEntry) => dedupe.accept(e) && shown.push(e);

    traceFromGraph(trail).forEach(add); // pre-fill from /graph
    trail.forEach(add); // SSE replays the whole trail on connect
    trail.forEach(add); // …and again on every reconnect
    add(step({ detail: "Searching web", timestamp: "2026-09-24T10:00:05+00:00" })); // a real new step

    expect(shown).toHaveLength(3);
  });

  it("reset() forgets seen entries so a retried run shows its trail again", () => {
    const dedupe = createTraceDeduper();
    expect(dedupe.accept(step())).toBe(true);
    expect(dedupe.accept(step())).toBe(false);
    dedupe.reset();
    expect(dedupe.accept(step())).toBe(true);
  });
});

describe("reconnectDelayMs", () => {
  it("backs off exponentially from 3 s and caps at 60 s", () => {
    expect([0, 1, 2, 3, 4, 5, 10].map(reconnectDelayMs)).toEqual([
      3000, 6000, 12000, 24000, 48000, 60000, 60000,
    ]);
  });
});
