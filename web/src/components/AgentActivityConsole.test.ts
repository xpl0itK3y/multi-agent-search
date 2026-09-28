// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { nextTick } from "vue";

import { i18n } from "@/i18n";
import type { TraceEntry } from "@/lib/stream";
import AgentActivityConsole from "./AgentActivityConsole.vue";

const FAKE = ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "requestAnimationFrame", "cancelAnimationFrame", "performance", "Date"] as const;

function consoleBodyVisible(status: string): boolean {
  const wrapper = mount(AgentActivityConsole, {
    props: { entries: [{ step: "search", detail: "Searching" }], status, live: status !== "completed" },
    global: { plugins: [i18n] },
  });
  const body = wrapper.findAll("div").find((d) => d.classes().includes("max-h-[60vh]"));
  const visible = Boolean(body?.isVisible());
  wrapper.unmount(); // stops the live elapsed-time interval
  return visible;
}

describe("AgentActivityConsole localization", () => {
  afterEach(() => {
    i18n.global.locale.value = "ru";
  });

  it("renders agent badges, statuses and labels in the active locale", () => {
    i18n.global.locale.value = "en";
    for (const status of ["processing", "completed", "failed"]) {
      const wrapper = mount(AgentActivityConsole, {
        props: {
          entries: [{ step: "search", detail: "Searching the web", agent: "SearchAgent", action: "query" }],
          reasoning: "thinking",
          status,
          live: status === "processing",
        },
        global: { plugins: [i18n] },
      });
      expect(wrapper.text(), status).not.toMatch(/[А-Яа-яЁё]/);
      if (status === "processing") expect(wrapper.text()).toContain(i18n.global.t("console.agents.SearchAgent"));
      wrapper.unmount();
    }
  });
});

describe("AgentActivityConsole initial state", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("starts expanded when Settings' auto-expand preference is on", () => {
    localStorage.setItem("research.auto_expand_console", "true");
    localStorage.setItem("activity_console.open", "0"); // a remembered collapse loses to the preference

    expect(consoleBodyVisible("completed")).toBe(true);
    expect(consoleBodyVisible("processing")).toBe(true);
  });

  it("otherwise collapses finished runs and remembers the last state for live ones", () => {
    expect(consoleBodyVisible("completed")).toBe(false);
    expect(consoleBodyVisible("processing")).toBe(true);

    localStorage.setItem("activity_console.open", "0");
    expect(consoleBodyVisible("processing")).toBe(false);
  });
});

describe("AgentActivityConsole source chips", () => {
  it("link only http(s) URLs, falling back to the source's domain", () => {
    const wrapper = mount(AgentActivityConsole, {
      props: {
        entries: [
          {
            step: "search",
            detail: "Searching",
            sources: [
              { domain: "ok.example", url: "https://ok.example/a" },
              { domain: "evil.example", url: "javascript:alert(document.domain)" },
            ],
          },
        ],
        status: "processing",
        live: true,
      },
      global: { plugins: [i18n] },
    });

    const hrefs = wrapper.findAll("a").map((a) => a.attributes("href"));
    wrapper.unmount();
    expect(hrefs).toEqual(["https://ok.example/a", "https://evil.example/"]);
  });
});

describe("AgentActivityConsole live feed", () => {
  afterEach(() => {
    vi.useRealTimers();
    localStorage.clear();
  });

  it("keeps the newest live step in view, in one instant scroll", async () => {
    vi.useFakeTimers({ toFake: [...FAKE] });
    const first: TraceEntry = { step: "search", detail: "Searching", timestamp: "2026-09-24T10:00:00+00:00" };
    const wrapper = mount(AgentActivityConsole, {
      props: { entries: [first], status: "processing", live: true },
      global: { plugins: [i18n] },
    });
    // The feed's own scroller (not the TransitionGroup inside it) is what follows.
    const box = wrapper.findAll("div").find((d) => d.classes().includes("max-h-[380px]"))!.element as HTMLElement;
    Object.defineProperty(box, "scrollHeight", { configurable: true, get: () => 900 });
    Object.defineProperty(box, "scrollTop", { configurable: true, writable: true, value: 0 });

    await wrapper.setProps({ entries: [first, { step: "crawl", detail: "Reading", timestamp: "2026-09-24T10:00:05+00:00" }] });
    await nextTick();
    vi.advanceTimersByTime(20); // one animation frame

    expect(box.scrollTop).toBe(box.scrollHeight);
    wrapper.unmount();
  });
});
