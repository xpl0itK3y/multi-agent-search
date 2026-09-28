// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { defineComponent, h } from "vue";

import { i18n } from "@/i18n";
import type { AgentMetadataItem } from "@/lib/types";

import {
  boundsOf,
  clampPan,
  clampPanSoft,
  clampZoom,
  fitBounds,
  limitPan,
  panRange,
  pinch,
  toWorld,
  unclampPanSoft,
  wheelUnit,
  wheelZoomFactor,
  zoomAt,
  ZMAX,
  ZMIN,
  STAGE_TEXT,
  STAGE_TONE,
  type View,
} from "./graphView";

const close = (a: number, b: number, digits = 6) => expect(a).toBeCloseTo(b, digits);

describe("graphView: zoomAt", () => {
  it("keeps the world point under the anchor fixed", () => {
    const view: View = { zoom: 0.55, panX: 40, panY: 60 };
    const anchor = { x: 300, y: 200 };
    const before = toWorld(view, anchor);
    const next = zoomAt(view, 1.2, anchor);

    close(next.zoom, 0.66);
    const after = toWorld(next, anchor);
    close(after.x, before.x);
    close(after.y, before.y);
  });

  it("clamps to the zoom limits and still keeps the anchor fixed", () => {
    const view: View = { zoom: 1.4, panX: -100, panY: 20 };
    const anchor = { x: 500, y: 300 };
    const next = zoomAt(view, 3, anchor);
    expect(next.zoom).toBe(ZMAX);
    const w0 = toWorld(view, anchor);
    const w1 = toWorld(next, anchor);
    close(w1.x, w0.x);
    close(w1.y, w0.y);

    expect(zoomAt({ zoom: 0.2, panX: 0, panY: 0 }, 0.1, anchor).zoom).toBe(ZMIN);
  });

  it("is reversible: in then out returns to the same view", () => {
    const view: View = { zoom: 0.55, panX: 40, panY: 60 };
    const c = { x: 640, y: 320 };
    const back = zoomAt(zoomAt(view, 1.2, c), 1 / 1.2, c);
    close(back.zoom, view.zoom);
    close(back.panX, view.panX);
    close(back.panY, view.panY);
  });
});

describe("graphView: pinch", () => {
  const view0: View = { zoom: 0.5, panX: 10, panY: 20 };
  const a0 = { x: 100, y: 100 };
  const b0 = { x: 200, y: 100 };

  it("zooms by the change in finger spread", () => {
    const next = pinch(view0, a0, b0, { x: 50, y: 100 }, { x: 250, y: 100 });
    close(next.zoom, 1);
  });

  it("keeps the world point under the starting midpoint under the current midpoint", () => {
    const w = toWorld(view0, { x: 150, y: 100 });
    // Spread and move both fingers 40px right and 30px down.
    const next = pinch(view0, a0, b0, { x: 90, y: 130 }, { x: 290, y: 130 });
    const under = toWorld(next, { x: 190, y: 130 });
    close(under.x, w.x);
    close(under.y, w.y);
  });

  it("pans 1:1 when the fingers move together without spreading", () => {
    const next = pinch(view0, a0, b0, { x: 130, y: 90 }, { x: 230, y: 90 });
    close(next.zoom, view0.zoom);
    close(next.panX, view0.panX + 30);
    close(next.panY, view0.panY - 10);
  });

  it("respects the zoom limits", () => {
    expect(pinch(view0, a0, b0, { x: 0, y: 100 }, { x: 2000, y: 100 }).zoom).toBe(ZMAX);
    expect(pinch(view0, a0, b0, { x: 149, y: 100 }, { x: 151, y: 100 }).zoom).toBe(ZMIN);
  });

  it("does not divide by zero when both fingers start on one point", () => {
    const next = pinch(view0, a0, a0, { x: 120, y: 100 }, { x: 180, y: 100 });
    expect(Number.isFinite(next.zoom)).toBe(true);
    expect(next.zoom).toBe(view0.zoom);
  });
});

describe("graphView: bounds and fit", () => {
  it("boundsOf wraps every rectangle, and is null for none", () => {
    expect(boundsOf([])).toBeNull();
    expect(
      boundsOf([
        { x: 60, y: 300, width: 230, height: 76 },
        { x: 6660, y: 160, width: 250, height: 76 },
      ]),
    ).toEqual({ minX: 60, minY: 160, maxX: 6910, maxY: 376 });
  });

  it("fits the whole box inside the padded viewport, centred", () => {
    const bbox = { minX: 60, minY: 160, maxX: 6910, maxY: 700 };
    const viewport = { w: 1440, h: 640 };
    const v = fitBounds(bbox, viewport, 48);

    const left = bbox.minX * v.zoom + v.panX;
    const right = bbox.maxX * v.zoom + v.panX;
    const top = bbox.minY * v.zoom + v.panY;
    const bottom = bbox.maxY * v.zoom + v.panY;
    expect(left).toBeGreaterThanOrEqual(48 - 1e-6);
    expect(right).toBeLessThanOrEqual(1440 - 48 + 1e-6);
    expect(top).toBeGreaterThanOrEqual(0);
    expect(bottom).toBeLessThanOrEqual(640);
    close(left + right, 1440);
    close(top + bottom, 640);
  });

  it("never zooms a small graph past 100%", () => {
    const v = fitBounds({ minX: 0, minY: 0, maxX: 200, maxY: 100 }, { w: 1440, h: 900 });
    expect(v.zoom).toBe(1);
    close(v.panX, (1440 - 200) / 2);
  });

  it("stops at the minimum zoom on a tiny phone viewport", () => {
    const v = fitBounds({ minX: 60, minY: 160, maxX: 6910, maxY: 700 }, { w: 358, h: 600 });
    expect(v.zoom).toBe(clampZoom(0.01));
  });
});

// ── The graph component's gestures (jsdom: MouseEvent-based pointer events, no layout) ──

const adminApi = vi.hoisted(() => ({ getAgents: vi.fn() }));
vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  adminApi,
}));

import AgentsGraphTab from "./AgentsGraphTab.vue";

const POSITIONS_KEY = "multi-agent-search:admin-nodes-pos-v3";

function agent(id: string): AgentMetadataItem {
  return {
    id, name: id, stage: "planning", role: "r", trigger: "t", inputs: [], outputs: [],
    source_file: "src/x.py", line_number: 1, description: "d", dependencies: [],
  };
}

// The inspector is stubbed: these tests only care whether it is asked to open, and for whom.
const InspectorStub = defineComponent({
  name: "AgentInspectorDrawer",
  props: { open: Boolean, agent: { type: Object, default: null } },
  render() {
    return h("div", { "data-test": "inspector", "data-open": String(this.open), "data-agent": (this.agent as AgentMetadataItem | null)?.id ?? "" });
  },
});

let wrapper: VueWrapper | null = null;

async function mountGraph() {
  wrapper = mount(AgentsGraphTab, {
    attachTo: document.body,
    global: { plugins: [i18n], stubs: { AgentInspectorDrawer: InspectorStub, teleport: true } },
  });
  await flushPromises();
  return wrapper;
}

const viewport = () => wrapper!.find('[data-test="graph-viewport"]');
const node = (id: string) => wrapper!.find(`[data-node-id="${id}"]`);
const inspector = () => wrapper!.find('[data-test="inspector"]');
const worldTransform = () => (wrapper!.find(".origin-top-left").element as HTMLElement).style.transform;

function pointer(type: string, x: number, y: number, extra: Record<string, unknown> = {}) {
  return { clientX: x, clientY: y, pointerId: 1, pointerType: "mouse", button: 0, buttons: type === "pointerup" ? 0 : 1, ...extra };
}

describe("AgentsGraphTab gestures", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    localStorage.clear();
    adminApi.getAgents.mockResolvedValue([agent("clarifier"), agent("optimizer")]);
  });

  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.clearAllMocks();
  });

  it("opens the inspector on a click, even with a few px of hand jitter", async () => {
    await mountGraph();
    const n = node("clarifier");
    await n.trigger("pointerdown", pointer("pointerdown", 500, 300));
    await n.trigger("pointermove", pointer("pointermove", 503, 302));
    await n.trigger("pointerup", pointer("pointerup", 503, 302));
    await n.trigger("click");

    expect(inspector().attributes("data-open")).toBe("true");
    expect(inspector().attributes("data-agent")).toBe("clarifier");
    // A click is not a drag: the saved layout is untouched.
    expect(localStorage.getItem(POSITIONS_KEY)).toBeNull();
  });

  it("dips a pressed node and lifts it only once the press becomes a drag", async () => {
    await mountGraph();
    const n = node("clarifier");
    const card = () => node("clarifier").find("div");
    await n.trigger("pointerdown", pointer("pointerdown", 500, 300));
    expect(card().classes()).toContain("scale-[0.985]");

    await n.trigger("pointermove", pointer("pointermove", 520, 300));
    expect(card().classes()).toContain("scale-[1.03]");
    expect(card().classes()).not.toContain("scale-[0.985]");

    await n.trigger("pointerup", pointer("pointerup", 520, 300));
    expect(card().classes()).not.toContain("scale-[1.03]");
  });

  it("drags a node 1:1 from where it was grabbed, saves the layout and swallows the click", async () => {
    await mountGraph();
    const n = node("clarifier");
    const before = (n.element as HTMLElement).style.transform; // translate(420px, 300px)
    expect(before).toBe("translate(420px, 300px)");

    await n.trigger("pointerdown", pointer("pointerdown", 500, 300));
    await n.trigger("pointermove", pointer("pointermove", 555, 311));
    await n.trigger("pointerup", pointer("pointerup", 555, 311));
    await n.trigger("click");

    // 55 and 11 screen px at 55% zoom: 100 and 20 world px, the grab offset kept.
    expect((node("clarifier").element as HTMLElement).style.transform).toBe("translate(520px, 320px)");
    expect(JSON.parse(localStorage.getItem(POSITIONS_KEY)!).clarifier).toEqual({ x: 520, y: 320 });
    expect(inspector().attributes("data-open")).toBe("false");

    // The next press starts fresh: a click after it opens the inspector again.
    await n.trigger("pointerdown", pointer("pointerdown", 600, 300));
    await n.trigger("pointerup", pointer("pointerup", 600, 300));
    await n.trigger("click");
    expect(inspector().attributes("data-open")).toBe("true");
  });

  it("pans the canvas by the pointer delta and ends the pan on pointercancel", async () => {
    await mountGraph();
    const start = worldTransform();
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 100));
    expect(viewport().classes()).toContain("cursor-grabbing");
    await viewport().trigger("pointermove", pointer("pointermove", 160, 130));
    await viewport().trigger("pointercancel", pointer("pointercancel", 0, 0));

    expect(viewport().classes()).toContain("cursor-grab");
    expect(worldTransform()).not.toBe(start);
    expect(worldTransform()).toContain("translate(100px, 90px)");

    // Nothing is left tracking: a later hover move doesn't pan.
    await viewport().trigger("pointermove", pointer("pointermove", 400, 400, { buttons: 0 }));
    expect(worldTransform()).toContain("translate(100px, 90px)");
  });

  it("ends a pan when the window loses focus", async () => {
    await mountGraph();
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 100));
    window.dispatchEvent(new Event("blur"));
    await flushPromises();
    expect(viewport().classes()).toContain("cursor-grab");
  });

  it("leaves presses on the canvas's own buttons alone", async () => {
    await mountGraph();
    const start = worldTransform();
    const fullscreen = viewport().find("button");
    await fullscreen.trigger("pointerdown", pointer("pointerdown", 100, 100));
    await viewport().trigger("pointermove", pointer("pointermove", 200, 200));
    expect(viewport().classes()).toContain("cursor-grab");
    expect(worldTransform()).toBe(start);
  });

  it("ignores the right mouse button", async () => {
    await mountGraph();
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 100, { button: 2 }));
    expect(viewport().classes()).toContain("cursor-grab");
  });

  it("opens a node from the keyboard", async () => {
    await mountGraph();
    const n = node("optimizer");
    expect(n.attributes("role")).toBe("button");
    expect(n.attributes("tabindex")).toBe("0");
    await n.trigger("keydown", { key: "Enter" });
    expect(inspector().attributes("data-agent")).toBe("optimizer");
    expect(inspector().attributes("data-open")).toBe("true");
  });

  it("keeps a saved layout and restores defaults without mutating them", async () => {
    localStorage.setItem(POSITIONS_KEY, JSON.stringify({ clarifier: { x: 999, y: 111 } }));
    await mountGraph();
    expect((node("clarifier").element as HTMLElement).style.transform).toBe("translate(999px, 111px)");

    // Drag a node that still sits at its default, then reset: it goes back to the default.
    const n = node("optimizer");
    await n.trigger("pointerdown", pointer("pointerdown", 500, 300));
    await n.trigger("pointermove", pointer("pointermove", 555, 300));
    await n.trigger("pointerup", pointer("pointerup", 555, 300));
    const reset = wrapper!.findAll("button").find((b) => b.attributes("title") === i18n.global.t("admin.agents.resetLayoutTooltip"))!;
    await reset.trigger("click");
    expect((node("optimizer").element as HTMLElement).style.transform).toBe("translate(790px, 300px)");
    expect((node("clarifier").element as HTMLElement).style.transform).toBe("translate(420px, 300px)");
  });
});

describe("graphView: wheel and pan limits", () => {
  it("normalises wheel units: lines and pages become pixels", () => {
    expect(wheelUnit(0, 800)).toBe(1);
    expect(wheelUnit(1, 800)).toBe(16);
    expect(wheelUnit(2, 800)).toBe(800);
  });

  it("follows a trackpad pinch exactly and tames a mouse notch", () => {
    close(wheelZoomFactor(-4), Math.exp(0.04));
    close(wheelZoomFactor(10), Math.exp(-0.1));
    // Chrome's 100 px notch and Firefox's 3-line notch zoom by the same step.
    close(wheelZoomFactor(100), wheelZoomFactor(3, 16));
    expect(wheelZoomFactor(100)).toBeGreaterThan(0.75);
    expect(wheelZoomFactor(-100)).toBeLessThan(1.3);
  });

  it("keeps at least 120px of the content on screen", () => {
    const bbox = { minX: 60, minY: 160, maxX: 6910, maxY: 700 };
    const r = panRange(bbox, 0.5, { w: 1200, h: 640 });
    // Right edge of the content at the left limit: exactly 120px in from the left.
    close(bbox.maxX * 0.5 + r.minX, 120);
    close(bbox.minX * 0.5 + r.maxX, 1200 - 120);
    close(bbox.maxY * 0.5 + r.minY, 120);
    close(bbox.minY * 0.5 + r.maxY, 640 - 120);
  });

  it("never inverts the range for content smaller than the margin", () => {
    const r = panRange({ minX: 0, minY: 0, maxX: 100, maxY: 50 }, 0.5, { w: 400, h: 300 });
    expect(r.minX).toBeLessThanOrEqual(r.maxX);
    expect(r.minY).toBeLessThanOrEqual(r.maxY);
  });

  it("limitPan clamps into range and never jumps a pan that is already outside", () => {
    expect(limitPan(0, 50, -100, 100)).toBe(50);
    expect(limitPan(0, 150, -100, 100)).toBe(100);
    expect(limitPan(0, -150, -100, 100)).toBe(-100);
    // Already past the max: it may come back, not go further.
    expect(limitPan(180, 170, -100, 100)).toBe(170);
    expect(limitPan(180, 200, -100, 100)).toBe(180);
  });
});

describe("AgentsGraphTab wheel, zoom buttons and fit", () => {
  const W = 1200;
  const H = 600;

  beforeEach(() => {
    i18n.global.locale.value = "en";
    localStorage.clear();
    adminApi.getAgents.mockResolvedValue([agent("clarifier")]);
    // jsdom has no layout: give every element the viewport's size.
    vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(W);
    vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(H);
  });

  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  // The world's transform, as a view: "translate(Xpx, Ypx) scale(Z)".
  function shownView(): View {
    const m = /translate\((-?[\d.e-]+)px, (-?[\d.e-]+)px\) scale\(([\d.e-]+)\)/.exec(worldTransform());
    if (!m) throw new Error(`unexpected transform ${worldTransform()}`);
    return { panX: Number(m[1]), panY: Number(m[2]), zoom: Number(m[3]) };
  }

  function wheel(init: WheelEventInit) {
    const e = new WheelEvent("wheel", { bubbles: true, cancelable: true, clientX: 0, clientY: 0, ...init });
    viewport().element.dispatchEvent(e);
    return e;
  }

  it("starts readable at the trigger when the whole graph would only fit unreadably small", async () => {
    await mountGraph();
    const v = shownView();
    expect(v.zoom).toBe(0.55);
    // The trigger card (x = 60) starts 48 px in from the left edge.
    close(60 * v.zoom + v.panX, 48);
  });

  it("fits the whole graph on the % button", async () => {
    await mountGraph();
    await wrapper!.find('[data-test="graph-fit"]').trigger("click");
    const v = shownView();
    expect(v.zoom).toBeLessThan(0.55);
    // Everything fits between the side paddings, and the label shows the live zoom.
    expect(60 * v.zoom + v.panX).toBeGreaterThanOrEqual(47.9);
    expect(6910 * v.zoom + v.panX).toBeLessThanOrEqual(W - 47.9);
    expect(wrapper!.find('[data-test="graph-fit"]').text()).toBe(`${Math.round(v.zoom * 100)}%`);
  });

  it("zooms the + button around the viewport centre", async () => {
    await mountGraph();
    const centre = { x: W / 2, y: H / 2 };
    const before = toWorld(shownView(), centre);
    const plus = wrapper!.findAll("button").find((b) => b.attributes("title") === i18n.global.t("admin.agents.zoomIn"))!;
    for (let i = 0; i < 5; i++) await plus.trigger("click");

    const after = toWorld(shownView(), centre);
    close(after.x, before.x, 3);
    close(after.y, before.y, 3);
    // A button change glides; the glide class goes once it is over.
    expect(wrapper!.find(".origin-top-left").classes()).toContain("duration-200");
  });

  it("pans on a plain wheel, in pixels for Chrome and in lines for Firefox", async () => {
    await mountGraph();
    const v0 = shownView();
    const e = wheel({ deltaX: 30, deltaY: 20 });
    await flushPromises();
    expect(e.defaultPrevented).toBe(true);
    close(shownView().panX, v0.panX - 30);
    close(shownView().panY, v0.panY - 20);

    wheel({ deltaX: 2, deltaMode: 1 });
    await flushPromises();
    close(shownView().panX, v0.panX - 30 - 32);
    expect(shownView().zoom).toBe(v0.zoom);
  });

  it("zooms around the cursor on Ctrl + wheel (and a trackpad pinch)", async () => {
    await mountGraph();
    const cursor = { x: 300, y: 200 };
    const before = toWorld(shownView(), cursor);
    const e = wheel({ deltaY: -8, ctrlKey: true, clientX: cursor.x, clientY: cursor.y });
    await flushPromises();
    expect(e.defaultPrevented).toBe(true);
    const v = shownView();
    expect(v.zoom).toBeGreaterThan(0);
    const after = toWorld(v, cursor);
    close(after.x, before.x, 3);
    close(after.y, before.y, 3);
  });

  it("leaves a vertical wheel to the page once the canvas can't pan further", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    await mountGraph();
    // Pan the content up to its limit (at least 120 px must stay on screen).
    wheel({ deltaY: 100000 });
    await flushPromises();
    vi.advanceTimersByTime(250); // the wheel sequence ends
    const pinned = shownView();

    const e = wheel({ deltaY: 100 });
    await flushPromises();
    expect(e.defaultPrevented).toBe(false);
    expect(shownView()).toEqual(pinned);

    // Back the other way the canvas takes the wheel again.
    vi.advanceTimersByTime(250);
    const back = wheel({ deltaY: -100 });
    expect(back.defaultPrevented).toBe(true);
  });

  it("moves the dot grid with the content", async () => {
    await mountGraph();
    wheel({ deltaX: 10, deltaY: 10 });
    await flushPromises();
    const v = shownView();
    const style = (viewport().element as HTMLElement).style;
    expect(style.backgroundPosition).toBe(`${v.panX}px ${v.panY}px`);
    const g = 20 * v.zoom * (v.zoom < 0.5 ? 2 : 1);
    expect(style.backgroundSize).toBe(`${g}px ${g}px`);
  });
});

describe("graphView: soft pan limits", () => {
  it("is 1:1 inside the range", () => {
    expect(clampPanSoft(50, -100, 100, 600)).toBe(50);
    expect(clampPanSoft(-100, -100, 100, 600)).toBe(-100);
  });

  it("resists more the further past the limit, and never reaches the finger", () => {
    const a = clampPanSoft(150, -100, 100, 600) - 100;
    const b = clampPanSoft(300, -100, 100, 600) - 100;
    const c = clampPanSoft(1100, -100, 100, 600) - 100;
    expect(a).toBeGreaterThan(0);
    expect(a).toBeLessThan(50);
    expect(b).toBeGreaterThan(a);
    expect((b - a) / 150).toBeLessThan(a / 50); // each further px of drag moves it less
    expect(c).toBeLessThan(600); // bounded by the viewport dimension
    expect(clampPanSoft(-250, -100, 100, 600)).toBeLessThan(-100);
    expect(clampPanSoft(-250, -100, 100, 600)).toBeGreaterThan(-250);
  });

  it("follows at the rubber band's slope right at the edge", () => {
    const shown = clampPanSoft(100.01, -100, 100, 600) - 100;
    close(shown / 0.01, 0.55, 2);
  });

  it("inverts exactly, so a grab mid-band continues without a jump", () => {
    for (const raw of [-900, -250, -101, -100, 0, 100, 140, 480, 2000]) {
      const shown = clampPanSoft(raw, -100, 100, 600);
      close(unclampPanSoft(shown, -100, 100, 600), raw, 6);
    }
  });

  it("clampPan settles into the range", () => {
    expect(clampPan(140, -100, 100)).toBe(100);
    expect(clampPan(-140, -100, 100)).toBe(-100);
    expect(clampPan(10, -100, 100)).toBe(10);
  });
});

describe("AgentsGraphTab soft limits and glide", () => {
  const W = 1200;
  const H = 600;

  beforeEach(() => {
    i18n.global.locale.value = "en";
    localStorage.clear();
    adminApi.getAgents.mockResolvedValue([agent("clarifier")]);
    vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(W);
    vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(H);
  });

  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const panXShown = () => Number(/translate\((-?[\d.e-]+)px/.exec(worldTransform())![1]);

  // The pan limit on the right: the content's left edge (x = 60) may come to 120 px
  // before the viewport's right edge.
  const maxPanX = (zoom: number) => W - 120 - 60 * zoom;

  it("resists a drag past the limit and springs back into range on a mouse release", async () => {
    await mountGraph();
    const limit = maxPanX(0.55);
    const x0 = panXShown();
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 300));
    // Drag 3000 px to the right: far past the limit.
    await viewport().trigger("pointermove", pointer("pointermove", 3100, 300));
    await viewport().trigger("pointerup", pointer("pointerup", 3100, 300));
    // (the last move is applied on release)
    expect(x0 + 3000).toBeGreaterThan(limit);

    // After release it is back on the limit, via the short view transition.
    close(panXShown(), limit, 3);
    expect(wrapper!.find(".origin-top-left").classes()).toContain("duration-200");
  });

  it("shows the band while dragging: past the limit it follows less than the pointer", async () => {
    await mountGraph();
    const limit = maxPanX(0.55);
    const x0 = panXShown();
    const toLimit = limit - x0;
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 300));
    await viewport().trigger("pointermove", pointer("pointermove", 100 + toLimit + 400, 300));
    // Apply the frame without releasing.
    await new Promise((r) => requestAnimationFrame(() => r(null)));
    const shown = panXShown();
    expect(shown).toBeGreaterThan(limit);
    expect(shown - limit).toBeLessThan(400);
    await viewport().trigger("pointerup", pointer("pointerup", 100 + toLimit + 400, 300));
  });

  it("glides on after a touch flick, and a new touch stops it where it is", async () => {
    vi.useFakeTimers({ toFake: ["requestAnimationFrame", "cancelAnimationFrame", "performance", "setTimeout", "clearTimeout"] });
    await mountGraph();
    const touch = (type: string, x: number) =>
      viewport().trigger(type, pointer(type, x, 300, { pointerType: "touch", pointerId: 7 }));

    // Flick left at 2000 px/s (inside the range: the canvas starts at the trigger).
    await touch("pointerdown", 800);
    for (let i = 1; i <= 5; i++) {
      vi.advanceTimersByTime(10);
      await touch("pointermove", 800 - 20 * i);
    }
    await touch("pointerup", 700);
    const released = panXShown();

    vi.advanceTimersByTime(100);
    await flushPromises();
    const gliding = panXShown();
    expect(gliding).toBeLessThan(released - 20); // still moving the way it was thrown

    // Grab it: it stops at once, where it is on screen.
    await touch("pointerdown", 500);
    const held = panXShown();
    vi.advanceTimersByTime(500);
    await flushPromises();
    expect(panXShown()).toBe(held);
    await touch("pointerup", 500);
  });

  it("stops a glide that runs into a limit on the limit", async () => {
    vi.useFakeTimers({ toFake: ["requestAnimationFrame", "cancelAnimationFrame", "performance", "setTimeout", "clearTimeout"] });
    await mountGraph();
    const touch = (type: string, x: number) =>
      viewport().trigger(type, pointer(type, x, 300, { pointerType: "touch", pointerId: 8 }));

    // Flick right, hard: the canvas starts near its right limit already.
    await touch("pointerdown", 100);
    for (let i = 1; i <= 5; i++) {
      vi.advanceTimersByTime(10);
      await touch("pointermove", 100 + 40 * i);
    }
    await touch("pointerup", 300);
    vi.advanceTimersByTime(4000);
    await flushPromises();
    close(panXShown(), maxPanX(0.55), 0);
  });
});

describe("AgentsGraphTab frame cost and reduced motion", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    localStorage.clear();
    adminApi.getAgents.mockResolvedValue([agent("clarifier")]);
  });

  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.restoreAllMocks();
    Reflect.deleteProperty(window, "matchMedia");
  });

  function emulateReducedMotion(reduce: boolean) {
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: (query: string) => ({
        matches: reduce && query.includes("prefers-reduced-motion"),
        media: query,
        addEventListener() {},
        removeEventListener() {},
      }),
    });
  }

  async function startWalkthrough() {
    const play = wrapper!.findAll("button").find((b) => b.attributes("title") === i18n.global.t("admin.agents.startSim"))!;
    await play.trigger("click");
  }

  it("draws travelling particles on the walkthrough's active wires", async () => {
    emulateReducedMotion(false);
    await mountGraph();
    await startWalkthrough();
    expect(wrapper!.findAll("circle").length).toBeGreaterThan(0);
    expect(wrapper!.find(".animate-n8n-wire").exists()).toBe(true);
  });

  it("renders no SMIL particles and no wire animation under reduced motion", async () => {
    emulateReducedMotion(true);
    await mountGraph();
    await startWalkthrough();
    expect(wrapper!.findAll("circle")).toHaveLength(0);
    expect(wrapper!.find(".animate-n8n-wire").exists()).toBe(false);
  });

  it("promotes the world to its own layer only while it moves, and blurs no node", async () => {
    await mountGraph();
    const world = () => wrapper!.find(".origin-top-left");
    expect(world().classes()).not.toContain("will-change-transform");
    await viewport().trigger("pointerdown", pointer("pointerdown", 100, 100));
    expect(world().classes()).toContain("will-change-transform");
    await viewport().trigger("pointerup", pointer("pointerup", 100, 100));
    expect(world().classes()).not.toContain("will-change-transform");
    expect(wrapper!.find(".interactive-node .backdrop-blur").exists()).toBe(false);
  });
});

describe("graphView: stage tones", () => {
  it("pairs a light-theme -700 shade with a dark-theme shade for every stage", () => {
    for (const [stage, tone] of Object.entries(STAGE_TONE)) {
      expect(tone).toMatch(/\btext-[a-z]+-700\b/);
      expect(tone).toMatch(/\bdark:text-[a-z]+-(300|400)\b/);
      expect(tone.startsWith(STAGE_TEXT[stage as keyof typeof STAGE_TEXT])).toBe(true);
    }
  });
});

describe("AgentsGraphTab inspector panel", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    localStorage.clear();
    adminApi.getAgents.mockResolvedValue([
      { ...agent("clarifier"), name: "ClarifierAgent" },
      { ...agent("optimizer"), name: "PromptOptimizerAgent" },
    ]);
  });

  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    vi.restoreAllMocks();
    document.body.innerHTML = "";
  });

  async function mountWithInspector() {
    wrapper = mount(AgentsGraphTab, { attachTo: document.body, global: { plugins: [i18n], stubs: { teleport: true } } });
    await flushPromises();
    return wrapper;
  }

  const panel = () => wrapper!.find('[data-test="agent-inspector"]');
  const escape = () => document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
  async function click(id: string) {
    const n = node(id);
    await n.trigger("pointerdown", pointer("pointerdown", 500, 300));
    await n.trigger("pointerup", pointer("pointerup", 500, 300));
    await n.trigger("click");
    await flushPromises();
  }

  it("opens beside the graph without a scrim, and swaps agents without closing", async () => {
    await mountWithInspector();
    await click("clarifier");
    expect(panel().attributes("aria-hidden")).toBeUndefined();
    expect(panel().attributes("aria-modal")).toBeUndefined();
    expect(wrapper!.find('[data-test="slideover-scrim"]').exists()).toBe(false);
    expect(panel().find("h2").text()).toBe("ClarifierAgent");

    await click("optimizer");
    expect(panel().attributes("aria-hidden")).toBeUndefined();
    expect(panel().find("h2").text()).toBe("PromptOptimizerAgent");
  });

  it("closes on Escape first, and leaves fullscreen only on the next one", async () => {
    await mountWithInspector();
    const fullscreen = wrapper!.findAll("button").find((b) => b.attributes("title") === i18n.global.t("admin.agents.fullscreen"))!;
    await fullscreen.trigger("click");
    const root = () => wrapper!.find("div");
    expect(root().classes()).toContain("fixed");

    await click("clarifier");
    escape();
    await flushPromises();
    expect(panel().attributes("aria-hidden")).toBe("true");
    expect(root().classes()).toContain("fixed");

    escape();
    await flushPromises();
    expect(root().classes()).not.toContain("fixed");
  });

  it("gives an accessible name to its close button", async () => {
    await mountWithInspector();
    await click("clarifier");
    const close = panel().find(`button[aria-label="${i18n.global.t("admin.agents.close")}"]`);
    expect(close.exists()).toBe(true);
    await close.trigger("click");
    expect(panel().attributes("aria-hidden")).toBe("true");
  });
});
