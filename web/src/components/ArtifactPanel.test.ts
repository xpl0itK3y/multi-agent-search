// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { compileStyle, parse } from "vue/compiler-sfc";
import postcss, { type AtRule } from "postcss";

import { i18n } from "@/i18n";

const mocks = vi.hoisted(() => ({
  api: {
    getShare: vi.fn(),
    createShare: vi.fn(),
    revokeShare: vi.fn(),
    getComparison: vi.fn(),
    getCitations: vi.fn(),
    getSourceIndependence: vi.fn(),
    getSourceReputation: vi.fn(),
    getStance: vi.fn(),
    getSourceIntegrity: vi.fn(),
    getCrossLanguage: vi.fn(),
    getConfidence: vi.fn(),
    getNumericCheck: vi.fn(),
    getSources: vi.fn(),
    getConflicts: vi.fn(),
    getVerification: vi.fn(),
    getRedTeam: vi.fn(),
    getGraph: vi.fn(),
    exportReport: vi.fn(),
  },
  confirm: vi.fn(),
}));

vi.mock("@/lib/api", () => ({ api: mocks.api, apiErrorMessage: () => "request failed" }));
// Keep the real confirmState (useDismiss reads it); only the dialog answer is scripted.
vi.mock("@/lib/confirm", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/confirm")>()),
  confirm: mocks.confirm,
}));

import ArtifactPanel from "./ArtifactPanel.vue";
import artifactPanelSource from "./ArtifactPanel.vue?raw";
import markdownViewSource from "./MarkdownView.vue?raw";

const t = (key: string) => i18n.global.t(key) as string;

// A component's scoped CSS rules as the build emits them (scope id data-v-test), one per
// selector, with the media query they sit in (null at the top level).
function scopedRules(source: string, filename: string) {
  const { descriptor } = parse(source);
  const { code } = compileStyle({
    source: descriptor.styles.find((s) => s.scoped)!.content,
    id: "data-v-test",
    scoped: true,
    filename,
  });
  const rules: { media: string | null; selector: string; decls: Record<string, string> }[] = [];
  postcss.parse(code).walkRules((rule) => {
    const media = rule.parent?.type === "atrule" ? (rule.parent as AtRule).params : null;
    const decls: Record<string, string> = {};
    rule.walkDecls((d) => {
      decls[d.prop] = d.value;
    });
    for (const selector of rule.selectors) rules.push({ media, selector, decls });
  });
  return rules;
}

// Every optional trust fetch fails quietly unless a test says otherwise.
function quietOptionalFetches() {
  for (const fn of [
    mocks.api.getComparison,
    mocks.api.getCitations,
    mocks.api.getSourceIndependence,
    mocks.api.getSourceReputation,
    mocks.api.getStance,
    mocks.api.getSourceIntegrity,
    mocks.api.getCrossLanguage,
    mocks.api.getConfidence,
    mocks.api.getNumericCheck,
  ]) {
    fn.mockRejectedValue(new Error("404"));
  }
}

let wrapper: VueWrapper | null = null;

async function mountPanel(props: { isFinal?: boolean; report?: string } = {}) {
  wrapper = mount(ArtifactPanel, {
    props: { id: "r-1", report: props.report ?? "# Report\n\nBody [S1].", isFinal: props.isFinal ?? true },
    global: {
      plugins: [i18n],
      stubs: { MarkdownView: true, ResearchDashboard: true, ReportSkeletonCanvas: true, SourceCard: true },
    },
    attachTo: document.body,
  });
  await flushPromises();
  return wrapper;
}

function buttonWithText(w: VueWrapper, text: string) {
  const found = w.findAll("button").find((b) => b.text().includes(text));
  if (!found) throw new Error(`no button with text ${text}`);
  return found;
}

beforeEach(() => {
  quietOptionalFetches();
  mocks.api.getShare.mockResolvedValue({ shared: false, token: "" });
});

afterEach(() => {
  wrapper?.unmount();
  wrapper = null;
  vi.clearAllMocks();
  vi.useRealTimers();
});

describe("ArtifactPanel sharing", () => {
  it("loads the share state on a final report without an unhandled rejection", async () => {
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    try {
      mocks.api.getShare.mockResolvedValue({ shared: true, token: "tok" });
      const w = await mountPanel();
      await new Promise((r) => setTimeout(r, 0));

      expect(rejections).toEqual([]);
      expect(mocks.api.getShare).toHaveBeenCalledWith("r-1");
      expect(mocks.api.getComparison).toHaveBeenCalledWith("r-1");
      // An already shared report says so on its button.
      expect(w.text()).toContain(t("share.shared"));
    } finally {
      process.off("unhandledRejection", onRejection);
    }
  });

  it("opening the popover publishes nothing; Create does, once", async () => {
    mocks.api.createShare.mockResolvedValue({ shared: true, token: "tok" });
    const w = await mountPanel();

    await buttonWithText(w, t("share.share")).trigger("click");
    await flushPromises();
    expect(mocks.api.createShare).not.toHaveBeenCalled();
    expect(w.find("input[readonly]").exists()).toBe(false);

    await buttonWithText(w, t("share.create")).trigger("click");
    await flushPromises();
    expect(mocks.api.createShare).toHaveBeenCalledTimes(1);
    expect((w.find("input[readonly]").element as HTMLInputElement).value).toContain("/r/tok");
  });

  it("shows a failed Create inside the popover", async () => {
    mocks.api.createShare.mockRejectedValue(new Error("offline"));
    const w = await mountPanel();

    await buttonWithText(w, t("share.share")).trigger("click");
    await buttonWithText(w, t("share.create")).trigger("click");
    await flushPromises();

    expect(w.find("[role=alert]").text()).toBe("request failed");
  });

  it("revokes only after the confirmation, and keeps the popover open", async () => {
    mocks.api.getShare.mockResolvedValue({ shared: true, token: "tok" });
    mocks.api.revokeShare.mockResolvedValue({ shared: false, token: "" });
    const w = await mountPanel();
    await buttonWithText(w, t("share.shared")).trigger("click");

    mocks.confirm.mockResolvedValueOnce(false);
    await buttonWithText(w, t("share.revoke")).trigger("click");
    await flushPromises();
    expect(mocks.confirm).toHaveBeenCalledTimes(1);
    expect(mocks.confirm.mock.calls[0][0]).toMatchObject({ danger: true, message: t("share.revokeConfirm") });
    expect(mocks.api.revokeShare).not.toHaveBeenCalled();

    mocks.confirm.mockResolvedValueOnce(true);
    await buttonWithText(w, t("share.revoke")).trigger("click");
    await flushPromises();
    expect(mocks.api.revokeShare).toHaveBeenCalledTimes(1);
    expect(w.find("[role=status]").text()).toBe(t("share.revoked"));
    expect(buttonWithText(w, t("share.create")).exists()).toBe(true);
  });
});

function tabButton(w: VueWrapper, key: string) {
  const label = t(`artifact.${key}`);
  const found = w.findAll("button").find((b) => b.text() === label);
  if (!found) throw new Error(`no tab ${key}`);
  return found;
}

describe("ArtifactPanel tabs", () => {
  it("starts the next tab's request while another one is still in flight", async () => {
    let resolveSources!: (v: unknown) => void;
    mocks.api.getSources.mockReturnValue(new Promise((r) => (resolveSources = r)));
    mocks.api.getConflicts.mockResolvedValue([]);
    const w = await mountPanel();

    await tabButton(w, "sources").trigger("click");
    expect(w.find("[aria-busy=true]").exists()).toBe(true);
    await tabButton(w, "conflicts").trigger("click");
    await flushPromises();

    expect(mocks.api.getConflicts).toHaveBeenCalledTimes(1);
    // An empty result reads as empty, never as a blank tab.
    expect(w.text()).toContain(t("artifact.conflictsEmpty"));

    resolveSources([]);
    await flushPromises();
    await tabButton(w, "sources").trigger("click");
    expect(w.text()).toContain(t("artifact.sourcesEmpty"));
    expect(mocks.api.getSources).toHaveBeenCalledTimes(1);
  });

  it("shows a failed request, with Retry, only on its own tab", async () => {
    mocks.api.getRedTeam
      .mockRejectedValueOnce(new Error("500"))
      .mockRejectedValueOnce(new Error("500"))
      .mockResolvedValueOnce({ findings: [], challenged: 0, held: 0 });
    mocks.api.getConflicts.mockResolvedValue([]);
    const w = await mountPanel();

    await tabButton(w, "redteam").trigger("click");
    await flushPromises();
    expect(w.find("[role=alert]").text()).toContain("request failed");
    expect(w.text()).not.toContain(t("redteam.empty"));

    await tabButton(w, "conflicts").trigger("click");
    await flushPromises();
    expect(w.find("[role=alert]").exists()).toBe(false);

    // Coming back tries once more by itself; Retry is there when that fails too.
    await tabButton(w, "redteam").trigger("click");
    await flushPromises();
    expect(mocks.api.getRedTeam).toHaveBeenCalledTimes(2);
    await buttonWithText(w, t("common.retry")).trigger("click");
    await flushPromises();
    expect(mocks.api.getRedTeam).toHaveBeenCalledTimes(3);
    expect(w.text()).toContain(t("redteam.empty"));
  });
});

describe("ArtifactPanel trust row", () => {
  const independence = {
    independence_score: 0.88,
    independent_origins: 7,
    total_sources: 8,
    clusters: [{ kind: "syndicated", label: "Echo", size: 2, source_ids: ["S7", "S8"], domains: ["a.example"] }],
  };
  const citations = {
    research_id: "r-1",
    total: 25,
    supported: 23,
    integrity: 0.92,
    unverified: 0,
    unsupported_claims: ["An unsupported claim."],
    grounding: [],
  };

  it("appears once, after every trust signal has answered", async () => {
    let resolveCitations!: (v: unknown) => void;
    mocks.api.getCitations.mockReturnValue(new Promise((r) => (resolveCitations = r)));
    mocks.api.getSourceIndependence.mockResolvedValue(independence);
    const w = await mountPanel();

    // Independence has answered, citations have not: nothing moves above the report yet.
    expect(w.text()).not.toContain(t("independence.title"));

    resolveCitations(citations);
    await flushPromises();
    expect(w.text()).toContain(t("independence.title"));
    expect(w.text()).toContain("92%");
  });

  it("opens a chip's list or tab, and has no trail tab", async () => {
    mocks.api.getCitations.mockResolvedValue(citations);
    mocks.api.getSourceIndependence.mockResolvedValue(independence);
    mocks.api.getSources.mockResolvedValue([]);
    const w = await mountPanel();

    expect(w.findAll("button").some((b) => b.text() === t("artifact.trail"))).toBe(false);

    const citationsChip = buttonWithText(w, t("citations.integrity"));
    expect(citationsChip.attributes("aria-expanded")).toBe("false");
    await citationsChip.trigger("click");
    expect(citationsChip.attributes("aria-expanded")).toBe("true");
    expect(w.text()).toContain("An unsupported claim.");

    await buttonWithText(w, t("independence.title")).trigger("click");
    await flushPromises();
    expect(mocks.api.getSources).toHaveBeenCalledWith("r-1");
    expect(w.text()).toContain(t("artifact.sourcesEmpty"));
  });

  // Colour dots vanished in forced colours, and they did not look like the marks.
  it("shows the verification legend as the marks themselves", async () => {
    const w = await mountPanel();

    expect(w.find(".verify-sample-weak").text()).toBe(t("verify.weak"));
    expect(w.find(".verify-sample-contested").text()).toBe(t("verify.contested"));
    expect(w.text()).toContain(`✓ ${t("verify.strong")}`);
    expect(w.find(".rounded-full.bg-success, .rounded-full.bg-warning, .rounded-full.bg-danger").exists()).toBe(false);

    await buttonWithText(w, t("verify.on")).trigger("click");
    expect(w.find(".verify-sample-weak").exists()).toBe(false);
  });

  it("draws the legend's lines exactly like the report's claim marks", () => {
    const legend = scopedRules(artifactPanelSource, "ArtifactPanel.vue");
    const report = scopedRules(markdownViewSource, "MarkdownView.vue");
    const pick = (rules: typeof legend, selector: string, media: string | null = null) =>
      Object.assign({}, ...rules.filter((r) => r.selector === selector && r.media === media).map((r) => r.decls));
    const L = "[data-v-test]";

    // Same line, colour tokens, contrast and forced-colours forms.
    for (const media of [null, "(prefers-contrast: more)"]) {
      expect(pick(legend, `.verify-sample${L}`, media)).toEqual(pick(report, `${L} .md-claim-text`, media));
      expect(pick(legend, `.verify-sample-weak${L}`, media)).toEqual(pick(report, `${L} .md-claim-weak .md-claim-text`, media));
      expect(pick(legend, `.verify-sample-contested${L}`, media)).toEqual(
        pick(report, `${L} .md-claim-contested .md-claim-text`, media),
      );
    }
    expect(pick(legend, `.verify-sample-contested${L}`, "(forced-colors: active)")).toEqual(
      pick(report, `${L} .md-claim-contested .md-claim-text`, "(forced-colors: active)"),
    );
    expect(pick(legend, `.verify-sample${L}`)["text-decoration-style"]).toBe("dotted");
  });
});

describe("ArtifactPanel menus", () => {
  it("closes the share popover on Escape, with focus back on its trigger", async () => {
    const w = await mountPanel();
    const trigger = buttonWithText(w, t("share.share"));

    await trigger.trigger("click");
    expect(trigger.attributes("aria-expanded")).toBe("true");
    expect(w.find("[role=dialog]").exists()).toBe(true);

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    await flushPromises();
    expect(w.find("[role=dialog]").exists()).toBe(false);
    expect(document.activeElement).toBe(trigger.element);
  });

  it("closes a menu on an outside press, and opening one closes the other", async () => {
    const w = await mountPanel();

    await buttonWithText(w, t("artifact.download")).trigger("click");
    expect(w.find("[role=menu]").exists()).toBe(true);
    // The first item takes focus, so arrows work at once.
    expect(document.activeElement?.getAttribute("role")).toBe("menuitem");

    await buttonWithText(w, t("share.share")).trigger("click");
    expect(w.find("[role=menu]").exists()).toBe(false);
    expect(w.find("[role=dialog]").exists()).toBe(true);

    document.body.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true }));
    await flushPromises();
    expect(w.find("[role=dialog]").exists()).toBe(false);
  });
});

describe("ArtifactPanel tab strip", () => {
  // jsdom has no layout: give the strip a width, a wider content and a scroll position.
  function fakeOverflow(el: HTMLElement, clientWidth: number, scrollWidth: number) {
    let left = 0;
    Object.defineProperty(el, "clientWidth", { configurable: true, get: () => clientWidth });
    Object.defineProperty(el, "scrollWidth", { configurable: true, get: () => scrollWidth });
    Object.defineProperty(el, "scrollLeft", { configurable: true, get: () => left, set: (v: number) => (left = v) });
  }
  function wheel(el: Element, deltaY: number) {
    const e = new WheelEvent("wheel", { deltaY, bubbles: true, cancelable: true });
    el.dispatchEvent(e);
    return e;
  }

  it("scrolls sideways on a vertical wheel, and lets the page scroll at the end", async () => {
    const w = await mountPanel();
    const strip = w.find("[role=tablist]").element as HTMLElement;
    fakeOverflow(strip, 300, 600);

    expect(wheel(strip, 120).defaultPrevented).toBe(true);
    expect(strip.scrollLeft).toBe(120);
    strip.scrollLeft = 300;
    expect(wheel(strip, 120).defaultPrevented).toBe(false);
    expect(wheel(strip, -50).defaultPrevented).toBe(true);
    expect(strip.scrollLeft).toBe(250);
  });

  it("shows the overflow fade only while there is more to scroll", async () => {
    const w = await mountPanel();
    const strip = w.find("[role=tablist]");
    fakeOverflow(strip.element as HTMLElement, 300, 600);

    await strip.trigger("scroll");
    expect(strip.classes()).toContain("edge-fade-x");
    (strip.element as HTMLElement).scrollLeft = 300;
    await strip.trigger("scroll");
    expect(strip.classes()).not.toContain("edge-fade-x");
  });

  it("moves between tabs with the arrow keys", async () => {
    mocks.api.getSources.mockResolvedValue([]);
    const w = await mountPanel();
    const tabs = () => w.findAll("[role=tab]");

    expect(tabs()[0].attributes("aria-selected")).toBe("true");
    await tabs()[0].trigger("keydown", { key: "ArrowRight" });
    await flushPromises();
    expect(tabs()[1].attributes("aria-selected")).toBe("true");
    expect(document.activeElement).toBe(tabs()[1].element);
    await tabs()[1].trigger("keydown", { key: "End" });
    expect(tabs()[tabs().length - 1].attributes("aria-selected")).toBe("true");
  });
});
