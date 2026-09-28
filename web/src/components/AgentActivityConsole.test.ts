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

describe("AgentActivityConsole tells the truth", () => {
  const at = (hms: string) => `2026-09-24T${hms}+00:00`;
  const mountConsole = (props: Record<string, unknown>) =>
    mount(AgentActivityConsole, { props: { entries: [], ...props }, global: { plugins: [i18n] } });

  afterEach(() => {
    vi.useRealTimers();
    localStorage.clear();
  });

  it("shows a finished run's real duration from its step timestamps", () => {
    const wrapper = mountConsole({
      entries: [
        { step: "plan_start", detail: "Planning", timestamp: at("11:57:50") },
        { step: "analyze", detail: "Writing", timestamp: at("11:59:29") },
      ],
      status: "completed",
      live: false,
    });
    expect(wrapper.text()).toContain("01:39");
    wrapper.unmount();
  });

  it("hides the timer when the steps carry no timestamps", () => {
    const wrapper = mountConsole({ entries: [{ step: "search", detail: "Searching" }], status: "processing", live: true });
    expect(wrapper.text()).not.toContain("⏱");
    wrapper.unmount();
  });

  it("keeps showing the latest real step during a long synthesis, with the elapsed run time", async () => {
    vi.useFakeTimers({ toFake: [...FAKE] });
    vi.setSystemTime(new Date(at("10:01:00")));
    const detail = "Drafting the comparison of the three reactor designs";
    const wrapper = mountConsole({
      entries: [
        { step: "plan_start", detail: "Planning", timestamp: at("10:00:00") },
        { step: "analyze", phase: "synthesis", agent: "AnalyzerAgent", detail, timestamp: at("10:00:30") },
      ],
      status: "analyzing",
      live: true,
    });

    await vi.advanceTimersByTimeAsync(80_000);

    expect(wrapper.get(`[title="${detail}"]`).text()).toBe(detail);
    for (const key of ["synthesisEvidence", "synthesisWriting", "synthesisCitations", "synthesisFinishing"]) {
      expect(wrapper.text()).not.toContain(i18n.global.t(`console.${key}`));
    }
    expect(wrapper.text()).toContain("02:20"); // 10:00:00 → 10:02:20, not the 80 s since mount
    wrapper.unmount();
  });

  it("never marks the step a failed run ended on as done", () => {
    const wrapper = mountConsole({
      entries: [
        { step: "plan_start", detail: "Planning", timestamp: at("10:00:00") },
        { step: "search", detail: "Searching", timestamp: at("10:00:05") },
      ],
      status: "failed",
      live: false,
    });

    const last = wrapper.findAll("[data-entry-state]").at(-1)!;
    expect(last.attributes("data-entry-state")).toBe("error");
    expect(last.text()).not.toContain("✓");
    expect(last.text()).toContain("✕");
    expect(last.text()).toContain(i18n.global.t("console.entryStopped"));

    const phases = wrapper.findAll("[data-phase-state]");
    expect(phases.map((p) => p.attributes("data-phase-state"))).toEqual(["done", "error", "pending", "pending", "pending"]);
    expect(phases[1].text()).toContain("✕");
    for (const later of phases.slice(2)) expect(later.classes()).toContain("opacity-40");
    wrapper.unmount();
  });

  it("shows a cancelled run's last step with a quiet stop marker", () => {
    const wrapper = mountConsole({
      entries: [
        { step: "plan_start", detail: "Planning", timestamp: at("10:00:00") },
        { step: "search", detail: "Searching", timestamp: at("10:00:05") },
      ],
      status: "cancelled",
      live: false,
    });

    const last = wrapper.findAll("[data-entry-state]").at(-1)!;
    expect(last.attributes("data-entry-state")).toBe("stopped");
    expect(last.text()).toContain("–");
    expect(last.text()).not.toContain("✓");
    const marker = last.get(`[title="${i18n.global.t("console.entryStopped")}"]`);
    expect(marker.classes()).toContain("text-muted");
    expect(marker.classes()).not.toContain("text-danger");
    expect(wrapper.find('[data-phase-state="stopped"]').text()).toContain("–");
    expect(wrapper.text()).toContain(i18n.global.t("console.stoppedName"));
    wrapper.unmount();
  });
});

describe("AgentActivityConsole calm", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("says a finished run is done in one line, without the header or the stepper", () => {
    const entries: TraceEntry[] = [
      { step: "plan_start", detail: "Planning", timestamp: "2026-09-24T11:57:50+00:00" },
      { step: "analyze", detail: "Writing", timestamp: "2026-09-24T11:59:29+00:00" },
    ];
    const wrapper = mount(AgentActivityConsole, {
      props: { entries, status: "completed", live: false },
      global: { plugins: [i18n] },
    });

    expect(wrapper.text()).toContain(i18n.global.t("console.doneIn", { time: "01:39" }));
    expect(wrapper.text()).toContain(i18n.global.t("console.journal", { n: 2 }));
    expect(wrapper.text()).not.toContain(i18n.global.t("console.reportName"));
    expect(wrapper.find("[data-phase-state]").exists()).toBe(false);
    // A finished step keeps its ✓ but no longer repeats it as a badge.
    expect(wrapper.text()).not.toContain(i18n.global.t("console.entryDone"));
    wrapper.unmount();
  });
});

describe("AgentActivityConsole in a narrow column", () => {
  const running = (entries: TraceEntry[]) =>
    mount(AgentActivityConsole, {
      props: { entries, status: "analyzing", live: true },
      global: { plugins: [i18n] },
      attachTo: document.body,
    });
  const planStep: TraceEntry = { step: "plan_start", detail: "Planning", agent: "OrchestratorAgent" };
  const analyzeStep: TraceEntry = { step: "analyze", phase: "synthesis", detail: "Writing", agent: "AnalyzerAgent" };

  afterEach(() => {
    localStorage.clear();
    document.body.innerHTML = "";
  });

  it("wraps the controls under the running agent's name instead of squeezing the name", () => {
    const wrapper = running([analyzeStep]);
    const header = wrapper.get("[data-console-header]");
    const [identity, controls] = header.element.children as unknown as HTMLElement[];

    expect(header.classes()).toContain("flex-wrap");
    // The identity asks for room of its own before it shares the row, and the controls
    // keep to the right edge on the line below.
    expect(identity.className).toMatch(/flex-\[1_1_\d+rem\]/);
    expect(controls.classList).toContain("ml-auto");
    // The name never shrinks for the role badge; the badge truncates first.
    const name = wrapper.get("[data-agent-name]");
    expect(name.text()).toBe("AnalyzerAgent");
    expect(name.classes()).toEqual(expect.arrayContaining(["shrink-0", "max-w-full", "truncate"]));
    wrapper.unmount();
  });

  it("keeps the journal toggle one width open or closed, so toggling never rewraps the row", async () => {
    const wrapper = running([analyzeStep]);
    const labels = () =>
      wrapper.get("[data-console-toggle]").findAll(".grid > span").map((s) => ({ text: s.text(), hidden: s.classes().includes("invisible") }));
    const collapse = i18n.global.t("console.collapse");
    const journal = i18n.global.t("console.journal", { n: 1 });

    expect(labels()).toEqual([{ text: collapse, hidden: false }, { text: journal, hidden: true }]);
    await wrapper.get("[data-console-toggle]").trigger("click");
    expect(labels()).toEqual([{ text: collapse, hidden: true }, { text: journal, hidden: false }]);
    wrapper.unmount();
  });

  it("fades the stepper on each side where more phases wait", async () => {
    const wrapper = running([analyzeStep]);
    const strip = wrapper.get("[data-stepper]");
    const el = strip.element as HTMLElement;
    Object.defineProperty(el, "clientWidth", { configurable: true, value: 300 });
    Object.defineProperty(el, "scrollWidth", { configurable: true, value: 640 });
    const scrollTo = async (left: number) => {
      el.scrollLeft = left;
      await strip.trigger("scroll");
    };

    await scrollTo(0);
    expect(strip.classes()).toContain("edge-fade-x");
    await scrollTo(120);
    expect(strip.classes()).toContain("stepper-fade-both");
    await scrollTo(340);
    expect(strip.classes()).toContain("stepper-fade-start");
    expect(strip.classes()).not.toContain("edge-fade-x");
    wrapper.unmount();
  });

  it("scrolls the phase in progress into view, clear of the fade, when the run moves on", async () => {
    const wrapper = running([planStep]);
    await nextTick(); // the first placement, before any layout exists
    const el = wrapper.get("[data-stepper]").element as HTMLElement;
    const scrollBy = vi.fn();
    Object.defineProperty(el, "scrollBy", { configurable: true, value: scrollBy });
    const rect = (left: number, right: number) => () => ({ left, right, top: 0, bottom: 20, width: right - left, height: 20 }) as DOMRect;
    el.getBoundingClientRect = rect(0, 300);
    const phases = el.querySelectorAll<HTMLElement>("[data-phase-state]");
    phases[3].getBoundingClientRect = rect(420, 540); // synthesis, off to the right

    await wrapper.setProps({ entries: [planStep, analyzeStep] });
    await nextTick();

    expect(phases[3].getAttribute("data-phase-state")).toBe("active");
    expect(scrollBy).toHaveBeenCalledTimes(1);
    expect(scrollBy.mock.calls[0][0].left).toBe(540 - 300 + 28);
    wrapper.unmount();
  });
});
