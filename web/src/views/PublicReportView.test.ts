// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";

const getPublicReport = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, getPublicReport } };
});

import { i18n } from "@/i18n";
import PublicReportView from "./PublicReportView.vue";

function report(overrides: Record<string, unknown> = {}) {
  return {
    prompt: "Which vector database fits a small team?",
    final_report: "# Report",
    depth: "medium",
    model: "m",
    created_at: "2026-03-14T10:00:00Z",
    sources: [{ id: "S1" }, { id: "S2" }, { id: "S3" }],
    citations: { total: 0, supported: 0, integrity: 0, grounding: [] },
    confidence: { overall: 0, components: [] },
    source_independence: { total_sources: 0, independence_score: 0 },
    source_reputation: { flagged_count: 1 },
    numeric_check: { total: 0, supported: 0 },
    stance: { applicable: false },
    red_team: {},
    source_integrity: { flagged: [], retracted_count: 0 },
    cross_language: { languages: [] },
    ...overrides,
  };
}

async function mountView() {
  setActivePinia(createPinia());
  const wrapper = mount(PublicReportView, {
    props: { token: "tok" },
    global: { plugins: [i18n], stubs: { MarkdownView: true } },
  });
  await flushPromises();
  return wrapper;
}

describe("PublicReportView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
  });

  it("scrolls itself and pins its header as floating chrome", async () => {
    getPublicReport.mockResolvedValue(report());
    const wrapper = await mountView();

    expect(wrapper.classes()).toContain("overflow-y-auto");
    expect(wrapper.find("header").classes()).toEqual(expect.arrayContaining(["sticky", "material-bar"]));
  });

  it("takes focus on arrival, so the keys scroll the report without a click first", async () => {
    // The document cannot scroll (the frames clip at the viewport): with focus on <body>,
    // PageDown, Space, the arrows and End did nothing until the reader clicked in.
    getPublicReport.mockResolvedValue(report());
    setActivePinia(createPinia());
    const wrapper = mount(PublicReportView, {
      props: { token: "tok" },
      global: { plugins: [i18n], stubs: { MarkdownView: true } },
      attachTo: document.body,
    });
    await flushPromises();

    expect(document.activeElement).toBe(wrapper.element);
    expect(wrapper.attributes("tabindex")).toBe("-1"); // focusable from script, never a Tab stop
    expect(wrapper.attributes()).toHaveProperty("data-scroll-root"); // no ring (style.css), also in contrast themes
    wrapper.unmount();
  });

  it("says what was asked, when, how deep and on how many sources", async () => {
    getPublicReport.mockResolvedValue(report());
    const wrapper = await mountView();

    expect(wrapper.text()).toContain("Which vector database fits a small team?");
    expect(wrapper.text()).toContain("March 14, 2026");
    expect(wrapper.text()).toContain(i18n.global.t("depth.medium"));
    expect(wrapper.text()).toContain("3 sources");
  });

  it("leaves out the parts it doesn't know instead of printing junk", async () => {
    getPublicReport.mockResolvedValue(report({ created_at: "not a date", depth: "weird", sources: [] }));
    const wrapper = await mountView();

    expect(wrapper.text()).not.toContain("Invalid");
    expect(wrapper.text()).not.toContain("depth.");
    expect(wrapper.text()).not.toContain("·  ·");
  });

  it("shows flagged sources in the danger colour", async () => {
    getPublicReport.mockResolvedValue(report());
    const wrapper = await mountView();

    expect(wrapper.find(".text-danger").text()).toContain(i18n.global.t("reputation.flagged"));
  });
});
