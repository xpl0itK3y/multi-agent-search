// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { mount, type VueWrapper } from "@vue/test-utils";
import { nextTick } from "vue";
import { compileStyle, parse } from "vue/compiler-sfc";

import SlideOver from "./SlideOver.vue";

const sfcSource = Object.values(
  import.meta.glob<string>("./SlideOver.vue", { query: "?raw", import: "default", eager: true }),
)[0];

// jsdom has no PointerEvent.
class TestPointerEvent extends MouseEvent {
  pointerId: number;
  pointerType: string;
  isPrimary: boolean;
  constructor(type: string, init: PointerEventInit = {}) {
    super(type, init);
    this.pointerId = init.pointerId ?? 1;
    this.pointerType = init.pointerType ?? "mouse";
    this.isPrimary = init.isPrimary ?? true;
  }
}

let wrapper: VueWrapper | null = null;

function mountSheet(props: Record<string, unknown> = {}) {
  wrapper = mount(SlideOver, {
    props: { open: false, label: "Details", ...props },
    slots: { default: '<button id="first">First</button><input id="field" /><button id="last">Last</button>' },
    attachTo: document.body,
  });
  return wrapper;
}

const panel = () => wrapper!.find('[data-test="slideover-panel"]');
const scrim = () => wrapper!.find('[data-test="slideover-scrim"]');
const fire = (el: Element, type: string) => el.dispatchEvent(new Event(type, { bubbles: true, cancelable: true }));
const key = (k: string, opts: KeyboardEventInit = {}) => {
  const e = new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...opts });
  (document.activeElement ?? document.body).dispatchEvent(e);
  return e;
};

describe("SlideOver", () => {
  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
    document.body.innerHTML = "";
  });

  it("is inert and hidden from assistive tech while closed", () => {
    mountSheet();
    expect(panel().attributes("inert")).toBeDefined();
    expect(panel().attributes("aria-hidden")).toBe("true");
    expect(panel().attributes("role")).toBe("dialog");
    expect(panel().attributes("aria-label")).toBe("Details");
  });

  it("moves focus inside on open and gives it back on close", async () => {
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();
    mountSheet();

    await wrapper!.setProps({ open: true });
    await nextTick();
    expect(panel().attributes("inert")).toBeUndefined();
    expect(panel().attributes("aria-modal")).toBe("true");
    expect(document.activeElement?.id).toBe("first");

    await wrapper!.setProps({ open: false });
    await nextTick();
    expect(document.activeElement).toBe(opener);
  });

  it("focuses [data-autofocus] when there is one", async () => {
    wrapper = mount(SlideOver, {
      props: { open: false },
      slots: { default: '<button>One</button><input id="auto" data-autofocus />' },
      attachTo: document.body,
    });
    await wrapper.setProps({ open: true });
    await nextTick();
    expect(document.activeElement?.id).toBe("auto");
  });

  it("closes on Escape", async () => {
    mountSheet({ open: true });
    await nextTick();
    key("Escape");
    expect(wrapper!.emitted("close")).toHaveLength(1);
  });

  it("leaves an Escape that an inner popover already handled", async () => {
    mountSheet({ open: true });
    await nextTick();
    const e = new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true });
    e.preventDefault();
    document.body.dispatchEvent(e);
    expect(wrapper!.emitted("close")).toBeUndefined();
  });

  it("keeps Tab inside a modal sheet", async () => {
    mountSheet({ open: true });
    await nextTick();
    (document.getElementById("last") as HTMLElement).focus();
    expect(key("Tab").defaultPrevented).toBe(true);
    expect(document.activeElement?.id).toBe("first");

    expect(key("Tab", { shiftKey: true }).defaultPrevented).toBe(true);
    expect(document.activeElement?.id).toBe("last");
  });

  it("does not close when a press starts in the panel and ends on the scrim", async () => {
    mountSheet({ open: true });
    await nextTick();
    fire(panel().element, "pointerdown");
    fire(scrim().element, "click");
    expect(wrapper!.emitted("close")).toBeUndefined();
  });

  it("closes when the press starts and ends on the scrim", async () => {
    mountSheet({ open: true });
    await nextTick();
    fire(scrim().element, "pointerdown");
    fire(scrim().element, "click");
    expect(wrapper!.emitted("close")).toHaveLength(1);
  });

  it("renders no scrim and no focus trap when non-modal", async () => {
    mountSheet({ open: true, modal: false });
    await nextTick();
    expect(scrim().exists()).toBe(false);
    expect(panel().attributes("aria-modal")).toBeUndefined();
    (document.getElementById("last") as HTMLElement).focus();
    expect(key("Tab").defaultPrevented).toBe(false);
  });

  it("can be dragged away by a finger that lands on a row inside it", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame", "performance", "Date"] });
    try {
      wrapper = mount(SlideOver, {
        props: { open: true, side: "right" },
        slots: { default: '<div class="h-full overflow-y-auto"><button id="row">Row</button></div>' },
        attachTo: document.body,
      });
      await nextTick();
      const el = panel().element as HTMLElement;
      const row = document.getElementById("row")!;
      el.setPointerCapture = vi.fn();
      const at = (target: Element, type: string, x: number) =>
        target.dispatchEvent(
          new TestPointerEvent(type, { bubbles: true, cancelable: true, clientX: x, clientY: 100, pointerId: 7, pointerType: "touch" }),
        );

      // The touch starts implicitly captured by the row under the finger.
      at(row, "pointerdown", 100);
      at(row, "gotpointercapture", 100);
      vi.advanceTimersByTime(10);
      at(row, "pointermove", 115); // clearly sideways: the sheet claims the gesture
      expect(el.setPointerCapture).toHaveBeenCalledWith(7);
      // The capture moves to the panel: the row's lostpointercapture bubbles through it.
      at(row, "lostpointercapture", 115);
      at(el, "gotpointercapture", 115);
      vi.advanceTimersByTime(10);
      at(el, "pointermove", 175);
      expect(el.style.transform).toBe("translate3d(60px, 0, 0)");

      at(el, "pointerup", 175);
      at(el, "lostpointercapture", 175);
      vi.advanceTimersByTime(1000);
      expect(wrapper.emitted("close")).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("makes its vertical scrollers pan only vertically, so a sideways drag reaches the panel", () => {
    // The browser stops looking for touch-action at the nearest scroller: without pan-y
    // there, a sideways drag inside the sheet's scrolling body becomes a browser pan.
    wrapper = mount(SlideOver, {
      props: { open: true },
      slots: {
        default:
          '<div id="body" class="h-full overflow-y-auto"><div id="list" class="max-h-60 overflow-y-scroll"></div>' +
          '<pre id="code" class="overflow-x-auto"></pre><div id="both" class="overflow-x-auto overflow-y-auto"></div></div>',
      },
      attachTo: document.body,
    });
    // Compile the component's scoped style for its own scope id (vitest skips CSS).
    const scopeId = panel().element.getAttributeNames().find((n) => n.startsWith("data-v-"));
    expect(scopeId).toBeDefined();
    const css = parse(sfcSource)
      .descriptor.styles.map((s) => compileStyle({ source: s.content, filename: "SlideOver.vue", id: scopeId!, scoped: s.scoped }).code)
      .join("\n")
      .replace(/\/\*[\s\S]*?\*\//g, "");
    const panY = [...css.matchAll(/([^{}]+)\{([^}]*)\}/g)]
      .filter(([, , body]) => /touch-action:\s*pan-y\s*(;|$)/.test(body.trim()))
      .flatMap(([, selectors]) => selectors.split(",").map((sel) => sel.trim()));
    expect(panY.length).toBeGreaterThan(0);
    const pansY = (id: string) => panY.some((sel) => document.getElementById(id)!.matches(sel));

    expect(pansY("body")).toBe(true);
    expect(pansY("list")).toBe(true);
    // Horizontal scrollers keep their own sideways pan.
    expect(pansY("code")).toBe(false);
    expect(pansY("both")).toBe(false);
  });

  it("slides from its side and back the same way", async () => {
    mountSheet({ side: "left" });
    expect(panel().classes()).toContain("-translate-x-full");
    expect(panel().classes()).toContain("left-0");
    await wrapper!.setProps({ open: true });
    expect(panel().classes()).toContain("translate-x-0");
    await wrapper!.setProps({ open: false, side: "right" });
    expect(panel().classes()).toContain("translate-x-full");
  });
});
