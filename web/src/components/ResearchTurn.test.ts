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

// The real ApiError (ResearchTurn tells a deleted research from a transient failure by it).
vi.mock("@/lib/api", async (importOriginal) => ({
  ApiError: (await importOriginal<typeof import("@/lib/api")>()).ApiError,
  api: mocks.api,
  apiErrorMessage: () => "error",
}));
vi.mock("@/lib/stream", () => ({ openResearchStream: mocks.openResearchStream }));

import { ApiError } from "@/lib/api";
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

  describe("a research that no longer exists", () => {
    const gone = () => new ApiError(404, "Research not found");

    it("stops reconnecting once /status says 404 and releases the thread", async () => {
      const wrapper = await mountRunning();
      mocks.api.getStatus.mockRejectedValue(gone());

      handlers(0).onError?.("connection lost"); // /events answered 404: EventSource closed
      await vi.advanceTimersByTimeAsync(10 * 60_000);

      expect(mocks.openResearchStream).toHaveBeenCalledTimes(1);
      expect(mocks.api.getStatus).toHaveBeenCalledTimes(2); // mount + the one catch-up
      expect(wrapper.emitted("done")).toEqual([["not_found"]]);
      expect(wrapper.text()).toContain(i18n.global.t("research.notFound"));
      expect(wrapper.text()).not.toContain(i18n.global.t("research.resume"));
    });

    it("never opens a stream when it is already gone on mount", async () => {
      mocks.api.getStatus.mockRejectedValue(new ApiError(403, "Forbidden"));
      const wrapper = await mountRunning();
      await vi.advanceTimersByTimeAsync(60_000);

      expect(mocks.openResearchStream).not.toHaveBeenCalled();
      expect(wrapper.emitted("done")).toEqual([["not_found"]]);
    });

    it("drops the reconnect the server's not-found stream error scheduled", async () => {
      await mountRunning();
      mocks.api.getStatus.mockRejectedValue(gone());

      // Auth-disabled servers stream an error, then done:"failed", for a missing research.
      handlers(0).onError?.("not found");
      await handlers(0).onDone?.("failed");
      await vi.advanceTimersByTimeAsync(10 * 60_000);

      expect(mocks.openResearchStream).toHaveBeenCalledTimes(1);
      expect(mocks.api.getStatus).toHaveBeenCalledTimes(2);
    });

    it("stops polling a queued research that was deleted", async () => {
      mocks.api.getStatus.mockResolvedValueOnce({ status: "queued", prompt: "Topic", llm_token_usage: null });
      await mountRunning();
      mocks.api.getStatus.mockRejectedValue(gone());

      await vi.advanceTimersByTimeAsync(60_000);

      expect(mocks.api.getStatus).toHaveBeenCalledTimes(2); // mount + one poll
    });

    it("keeps retrying through transient server and network errors", async () => {
      await mountRunning();
      mocks.api.getStatus.mockRejectedValueOnce(new ApiError(500, "boom")).mockRejectedValueOnce(new TypeError("fetch failed"));

      handlers(0).onError?.("connection lost");
      await vi.advanceTimersByTimeAsync(3_000);
      expect(mocks.openResearchStream).toHaveBeenCalledTimes(2);
      handlers(1).onError?.("connection lost");
      await vi.advanceTimersByTimeAsync(6_000);
      expect(mocks.openResearchStream).toHaveBeenCalledTimes(3);
    });
  });

  // A revoked cookie session (a logout elsewhere signs out every device) answers 401 to
  // /status, and the EventSource just closes: reconnecting can never succeed.
  describe("a session that is gone", () => {
    const signedOut = () => new ApiError(401, "Not authenticated");

    it("stops reconnecting once /status says 401, keeps the research and offers a resume", async () => {
      const wrapper = await mountRunning();
      mocks.api.getStatus.mockRejectedValue(signedOut());

      handlers(0).onError?.("connection lost"); // /events answered 401: EventSource closed
      await vi.advanceTimersByTimeAsync(10 * 60_000);

      expect(mocks.openResearchStream).toHaveBeenCalledTimes(1);
      expect(mocks.api.getStatus).toHaveBeenCalledTimes(2); // mount + the one catch-up
      expect(wrapper.emitted("done")).toBeUndefined();
      expect(wrapper.text()).toContain(i18n.global.t("research.resume"));

      // Signed in again (e.g. in another tab): the manual resume picks the stream up.
      mocks.api.getStatus.mockResolvedValue({ status: "processing", prompt: "Topic", llm_token_usage: null });
      await wrapper.findAll("button").find((b) => b.text() === i18n.global.t("research.resume"))!.trigger("click");
      await flushPromises();
      expect(mocks.openResearchStream).toHaveBeenCalledTimes(2);
    });

    it("never opens a stream when the session is gone on mount", async () => {
      mocks.api.getStatus.mockRejectedValue(signedOut());
      const wrapper = await mountRunning();
      await vi.advanceTimersByTimeAsync(10 * 60_000);

      expect(mocks.openResearchStream).not.toHaveBeenCalled();
      expect(mocks.api.getStatus).toHaveBeenCalledTimes(1);
      expect(wrapper.emitted("done")).toBeUndefined();
    });

    it("stops polling a queued research", async () => {
      mocks.api.getStatus.mockResolvedValueOnce({ status: "queued", prompt: "Topic", llm_token_usage: null });
      await mountRunning();
      mocks.api.getStatus.mockRejectedValue(signedOut());

      await vi.advanceTimersByTimeAsync(60_000);

      expect(mocks.api.getStatus).toHaveBeenCalledTimes(2); // mount + one poll
    });
  });
});
