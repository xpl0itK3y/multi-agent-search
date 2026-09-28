// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, shallowMount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";

import { i18n } from "@/i18n";
import type { ChatStreamHandlers } from "@/lib/stream";

const mocks = vi.hoisted(() => ({
  api: {
    getThread: vi.fn(),
    getMessages: vi.fn(),
  },
  streamChatAnswer: vi.fn(),
}));

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: mocks.api,
  apiErrorMessage: () => "error",
}));
vi.mock("@/lib/stream", () => ({ streamChatAnswer: mocks.streamChatAnswer }));

import ThreadView from "./ThreadView.vue";

function mountThread() {
  return shallowMount(ThreadView, { props: { threadId: "t-1" }, global: { plugins: [i18n] } });
}

describe("ThreadView states", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    mocks.api.getMessages.mockResolvedValue([]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    document.title = "";
  });

  it("shows a loading skeleton, not the empty sentence, while the thread loads", async () => {
    mocks.api.getThread.mockReturnValue(new Promise(() => {}));
    const wrapper = mountThread();
    await flushPromises();

    expect(wrapper.find('[aria-busy="true"]').exists()).toBe(true);
    expect(wrapper.text()).not.toContain(i18n.global.t("thread.empty"));
  });

  it("says the thread is empty only once it has loaded empty", async () => {
    mocks.api.getThread.mockResolvedValue([]);
    const wrapper = mountThread();
    await flushPromises();

    expect(wrapper.find('[aria-busy="true"]').exists()).toBe(false);
    expect(wrapper.text()).toContain(i18n.global.t("thread.empty"));
  });

  it("names the tab after the thread's first question", async () => {
    mocks.api.getThread.mockResolvedValue([{ id: "r-1", prompt: "Sodium-ion  vs\nLFP batteries for home storage in cold climates, 2026" }]);
    mountThread();
    await flushPromises();

    const title = document.title;
    expect(title.endsWith(" — Veris")).toBe(true);
    const shown = title.slice(0, -" — Veris".length);
    expect(Array.from(shown).length).toBeLessThanOrEqual(60);
    expect(shown.startsWith("Sodium-ion vs LFP batteries")).toBe(true);
  });

  it("leaves the tab's name alone when the thread arrives after the reader moved on", async () => {
    let resolve!: (list: { id: string; prompt: string }[]) => void;
    mocks.api.getThread.mockReturnValue(new Promise((r) => (resolve = r)));
    const wrapper = mountThread();
    await flushPromises();

    wrapper.unmount();
    document.title = "Settings — Veris"; // the router's afterEach, for the new page
    resolve([{ id: "r-1", prompt: "Old thread question" }]);
    await flushPromises();

    expect(document.title).toBe("Settings — Veris");
  });

  it("shows a failed follow-up answer under its question and retries it there", async () => {
    mocks.api.getThread.mockResolvedValue([{ id: "r-1", prompt: "Topic" }]);
    mocks.streamChatAnswer.mockImplementation(async (_id: string, _q: string, h: ChatStreamHandlers) => {
      h.onError?.("The answer stream broke");
    });
    const wrapper = mountThread();
    await flushPromises();

    wrapper.findComponent({ name: "Composer" }).vm.$emit("ask", "Why sodium?");
    await flushPromises();

    const alert = wrapper.get('[role="alert"]');
    expect(alert.text()).toContain("The answer stream broke");
    // Attached to its question: the alert sits in the same turn as the question bubble.
    expect(alert.element.parentElement!.textContent).toContain("Why sodium?");
    const retry = alert.get("button");
    expect(retry.text()).toBe(i18n.global.t("research.retry"));

    mocks.streamChatAnswer.mockImplementation(async (_id: string, _q: string, h: ChatStreamHandlers) => {
      h.onDone?.("Because it is cheaper.", []);
    });
    await retry.trigger("click");
    await flushPromises();

    expect(mocks.streamChatAnswer).toHaveBeenCalledTimes(2);
    expect(mocks.streamChatAnswer.mock.calls[1].slice(0, 2)).toEqual(["r-1", "Why sodium?"]);
    expect(wrapper.find('[role="alert"]').exists()).toBe(false);
  });
});
