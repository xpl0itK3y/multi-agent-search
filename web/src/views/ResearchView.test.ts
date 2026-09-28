// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, shallowMount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import type { StreamHandlers } from "@/lib/stream";

const mocks = vi.hoisted(() => ({
  api: {
    getStatus: vi.fn(),
    getGraph: vi.fn(),
    getMessages: vi.fn(),
  },
  openResearchStream: vi.fn(),
}));

vi.mock("@/lib/api", () => ({ api: mocks.api, apiErrorMessage: () => "error" }));
vi.mock("@/lib/stream", () => ({ openResearchStream: mocks.openResearchStream, streamChatAnswer: vi.fn() }));
vi.mock("vue-router", () => ({ useRouter: () => ({ push: vi.fn() }) }));

import ResearchView from "./ResearchView.vue";

const handlers = (): StreamHandlers => mocks.openResearchStream.mock.calls[0][1];

async function mountView(status: string) {
  mocks.api.getStatus.mockResolvedValue({ status, prompt: "Topic", llm_token_usage: null });
  const wrapper = shallowMount(ResearchView, { props: { id: "r-1" }, global: { plugins: [i18n] } });
  await flushPromises();
  return wrapper;
}

describe("ResearchView", () => {
  beforeEach(() => {
    mocks.api.getGraph.mockResolvedValue({ graph_trail: [] });
    mocks.api.getMessages.mockResolvedValue([]);
    mocks.openResearchStream.mockImplementation(() => vi.fn());
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("breathes in one place while running: the status dot until the live console shows", async () => {
    const wrapper = await mountView("processing");
    const dot = () => wrapper.get("[data-status-dot]").classes();

    expect(dot()).toEqual(expect.arrayContaining(["bg-accent", "live-dot"]));

    handlers().onTrace?.({ step: "search", detail: "Searching" });
    await flushPromises();
    expect(wrapper.findComponent({ name: "AgentActivityConsole" }).props("live")).toBe(true);
    expect(dot()).toContain("bg-accent");
    expect(dot()).not.toContain("live-dot");

    await handlers().onDone?.("completed");
    await flushPromises();
    expect(dot()).toContain("bg-success");
    expect(dot()).not.toContain("live-dot");
  });

  it("names the follow-up send button for what it does, not with the field's placeholder", async () => {
    const wrapper = await mountView("completed");

    const send = wrapper.get("textarea + button");
    expect(send.attributes("aria-label")).toBe(i18n.global.t("chat.send"));
    expect(send.attributes("aria-label")).not.toBe(i18n.global.t("chat.placeholder"));
  });
});
