// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, shallowMount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import type { StreamHandlers, TraceEntry } from "@/lib/stream";

const mocks = vi.hoisted(() => ({
  api: {
    getStatus: vi.fn(),
    getGraph: vi.fn(),
    getReport: vi.fn(),
    retryResearch: vi.fn(),
  },
  openResearchStream: vi.fn(),
}));

vi.mock("@/lib/api", () => ({ api: mocks.api, apiErrorMessage: () => "error" }));
vi.mock("@/lib/stream", () => ({ openResearchStream: mocks.openResearchStream }));

import ResearchTurn from "./ResearchTurn.vue";

const trail: TraceEntry[] = [
  { step: "plan_start", detail: "Planning", action: "start", timestamp: "2026-09-24T10:00:00+00:00" },
  { step: "search", detail: "Searching", action: "query", timestamp: "2026-09-24T10:00:02+00:00" },
];

// Handlers passed to the n-th openResearchStream call.
const handlers = (n: number): StreamHandlers => mocks.openResearchStream.mock.calls[n][1];

async function mountRunning() {
  const wrapper = shallowMount(ResearchTurn, { props: { id: "r-1" }, global: { plugins: [i18n] } });
  await flushPromises();
  return wrapper;
}

describe("ResearchTurn live stream", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mocks.api.getStatus.mockResolvedValue({ status: "processing", prompt: "Topic", llm_token_usage: null });
    mocks.api.getGraph.mockResolvedValue({ graph_trail: trail });
    mocks.openResearchStream.mockImplementation(() => vi.fn());
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("shows each trail step once although /graph and the SSE replay both deliver it", async () => {
    const wrapper = await mountRunning();

    trail.forEach((e) => handlers(0).onTrace?.({ ...e }));
    await flushPromises();

    const entries = wrapper.findComponent({ name: "AgentActivityConsole" }).props("entries") as TraceEntry[];
    expect(entries.map((e) => e.step)).toEqual(["plan_start", "search"]);
  });

  it("does not reconnect after unmount", async () => {
    const wrapper = await mountRunning();
    expect(mocks.openResearchStream).toHaveBeenCalledTimes(1);

    handlers(0).onError?.("stream error");
    wrapper.unmount();
    await vi.advanceTimersByTimeAsync(120_000);

    expect(mocks.openResearchStream).toHaveBeenCalledTimes(1);
    expect(mocks.api.getStatus).toHaveBeenCalledTimes(1); // only the mount-time sync
  });

  it("backs off between consecutive reconnects and keeps one timer pending", async () => {
    await mountRunning();

    // Two errors on one connection (named stream_error, then CLOSED) → one reconnect.
    handlers(0).onError?.("stream error");
    handlers(0).onError?.("stream error");
    await vi.advanceTimersByTimeAsync(6_000);
    expect(mocks.openResearchStream).toHaveBeenCalledTimes(2);

    // The new connection fails straight away: the next attempt waits longer.
    handlers(1).onError?.("stream error");
    await vi.advanceTimersByTimeAsync(11_000);
    expect(mocks.openResearchStream).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(mocks.openResearchStream).toHaveBeenCalledTimes(3);
  });

  it("clears the trace and reasoning when the run is retried", async () => {
    mocks.api.getStatus.mockResolvedValue({ status: "failed", prompt: "Topic", llm_token_usage: null });
    mocks.api.retryResearch.mockResolvedValue({ id: "r-1", status: "queued" });
    const wrapper = await mountRunning();
    expect(mocks.openResearchStream).not.toHaveBeenCalled();

    await wrapper.find("button").trigger("click"); // the failed card's retry button
    await flushPromises();

    expect(mocks.api.retryResearch).toHaveBeenCalledWith("r-1");
    const console = wrapper.findComponent({ name: "AgentActivityConsole" });
    expect(console.props("entries")).toEqual([]);
    expect(console.props("reasoning")).toBe("");
    // The retried run's replay is shown again, not swallowed as "already seen".
    handlers(0).onTrace?.({ ...trail[0] });
    await flushPromises();
    expect(wrapper.findComponent({ name: "AgentActivityConsole" }).props("entries")).toHaveLength(1);
  });
});
