// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, ref } from "vue";

import { useStickToBottom } from "./useStickToBottom";

const FAKE = ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame", "performance", "Date"] as const;

// A scroller 200px tall over 1000px of content, scrolled to the bottom.
function setup(threshold?: number) {
  const dims = { scrollHeight: 1000, clientHeight: 200 };
  const el = document.createElement("div");
  Object.defineProperty(el, "scrollHeight", { configurable: true, get: () => dims.scrollHeight });
  Object.defineProperty(el, "clientHeight", { configurable: true, get: () => dims.clientHeight });
  Object.defineProperty(el, "scrollTop", { configurable: true, writable: true, value: 800 });
  document.body.appendChild(el);
  const scope = effectScope();
  const stick = scope.run(() => useStickToBottom(ref(el), threshold === undefined ? {} : { threshold }))!;
  const scrollTo = (top: number) => {
    el.scrollTop = top;
    el.dispatchEvent(new Event("scroll"));
  };
  return { el, dims, stick, scope, scrollTo };
}

describe("useStickToBottom", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: [...FAKE] });
    vi.advanceTimersByTime(1000); // well past page start
  });
  afterEach(() => {
    vi.useRealTimers();
    document.body.innerHTML = "";
  });

  it("lets go as soon as the reader wheels up", () => {
    const { el, stick } = setup();
    el.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    expect(stick.pinned.value).toBe(false);
  });

  it("stays pinned through scrolls the code causes", () => {
    const { stick, scrollTo } = setup();
    scrollTo(0); // e.g. content replaced, no user gesture
    expect(stick.pinned.value).toBe(true);
  });

  it("unpins on a user scroll away from the bottom and re-pins when it ends near it", () => {
    const { el, stick, scrollTo } = setup();
    el.dispatchEvent(new Event("touchmove"));
    scrollTo(300);
    expect(stick.pinned.value).toBe(false);

    vi.advanceTimersByTime(1000);
    scrollTo(750); // 50px from the bottom, within the 80px threshold
    expect(stick.pinned.value).toBe(true);
  });

  it("follows at most once per frame, instantly, and only while pinned", () => {
    const { el, dims, stick } = setup();
    dims.scrollHeight = 1400;
    stick.follow();
    stick.follow();
    expect(el.scrollTop).toBe(800);
    vi.advanceTimersByTime(20);
    expect(el.scrollTop).toBe(1400);

    el.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    dims.scrollHeight = 2000;
    stick.follow();
    vi.advanceTimersByTime(20);
    expect(el.scrollTop).toBe(1400);
  });

  it("unpins on PageUp/ArrowUp/Home, but not while typing in a field", () => {
    const { el, stick } = setup();
    const field = document.createElement("textarea");
    el.appendChild(field);
    field.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowUp", bubbles: true }));
    expect(stick.pinned.value).toBe(true);

    el.dispatchEvent(new KeyboardEvent("keydown", { key: "PageUp", bubbles: true }));
    expect(stick.pinned.value).toBe(false);
  });

  describe("gestures meant for something inside the scroller", () => {
    // A nested scroller: 100px tall over 400px of content, scrolled to `top`.
    function nested(parent: HTMLElement, { top = 150, axis = "y" }: { top?: number; axis?: "x" | "y" } = {}) {
      const inner = document.createElement("div");
      inner.style.setProperty(axis === "y" ? "overflow-y" : "overflow-x", "auto");
      const size = axis === "y" ? ["scrollHeight", "clientHeight", "scrollTop"] : ["scrollWidth", "clientWidth", "scrollLeft"];
      Object.defineProperty(inner, size[0], { configurable: true, value: 400 });
      Object.defineProperty(inner, size[1], { configurable: true, value: 100 });
      Object.defineProperty(inner, size[2], { configurable: true, writable: true, value: top });
      const child = document.createElement("p");
      inner.appendChild(child);
      parent.appendChild(inner);
      return { inner, child };
    }
    const wheel = (target: HTMLElement, init: WheelEventInit) =>
      target.dispatchEvent(new WheelEvent("wheel", { bubbles: true, cancelable: true, ...init }));
    const key = (target: HTMLElement, k: string) =>
      target.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));

    it("keeps following while a nested scroller reads back up, and lets go once it reaches its top", () => {
      const { el, dims, stick } = setup();
      const { inner, child } = nested(el);

      wheel(child, { deltaY: -40 });
      expect(stick.pinned.value).toBe(true);
      dims.scrollHeight = 1400;
      stick.follow();
      vi.advanceTimersByTime(20);
      expect(el.scrollTop).toBe(1400); // still following the stream

      inner.scrollTop = 0; // at its top the browser hands the wheel on to the thread
      wheel(child, { deltaY: -40 });
      expect(stick.pinned.value).toBe(false);
    });

    it("keeps following when a widget turned the wheel into its own (a tab strip scrolling sideways)", () => {
      const { el, stick } = setup();
      const strip = document.createElement("div");
      strip.addEventListener("wheel", (e) => e.preventDefault());
      el.appendChild(strip);

      wheel(strip, { deltaY: -40 });

      expect(stick.pinned.value).toBe(true);
    });

    it("gives a mostly sideways wheel to a sideways strip, but not a vertical one that drifts", () => {
      const { el, stick } = setup();
      const { child } = nested(el, { axis: "x", top: 0 });

      wheel(child, { deltaX: 30, deltaY: -4 });
      expect(stick.pinned.value).toBe(true);

      wheel(child, { deltaX: 2, deltaY: -40 });
      expect(stick.pinned.value).toBe(false);
    });

    it("keeps following on a key a widget handled or a nested scroller takes, lets go on the others", () => {
      const { el, stick } = setup();
      const tab = document.createElement("button");
      tab.addEventListener("keydown", (e) => e.preventDefault()); // Home: first tab
      el.appendChild(tab);
      const { inner, child } = nested(el);
      const plain = document.createElement("button");
      el.appendChild(plain);

      key(tab, "Home");
      expect(stick.pinned.value).toBe(true);
      key(child, "ArrowUp");
      expect(stick.pinned.value).toBe(true);

      inner.scrollTop = 0;
      key(child, "PageUp");
      expect(stick.pinned.value).toBe(false);

      stick.jumpToLatest();
      key(plain, "Home");
      expect(stick.pinned.value).toBe(false);
    });

    it("takes only a press in its own scrollbar gutter as a scrollbar drag", () => {
      const { el, stick, scrollTo } = setup();
      Object.defineProperty(el, "clientWidth", { configurable: true, value: 300 });
      const { inner } = nested(el);
      const press = (target: HTMLElement, offsetX: number) => {
        const e = new MouseEvent("pointerdown", { bubbles: true });
        Object.defineProperty(e, "offsetX", { value: offsetX });
        target.dispatchEvent(e);
      };

      press(inner, 310); // a wide nested scroller's own gutter
      scrollTo(300);
      expect(stick.pinned.value).toBe(true);

      press(el, 310);
      scrollTo(300);
      expect(stick.pinned.value).toBe(false);
    });
  });

  it("jumpToLatest re-pins and scrolls to the bottom", () => {
    const { el, stick } = setup();
    const spy = vi.fn();
    Object.defineProperty(el, "scrollTo", { configurable: true, value: spy });
    el.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    expect(stick.pinned.value).toBe(false);

    stick.jumpToLatest();

    expect(stick.pinned.value).toBe(true);
    expect(spy).toHaveBeenCalledWith({ top: 1000, behavior: "smooth" });
  });

  it("falls back to scrollTop where scrollTo is missing", () => {
    const { el, stick } = setup();
    Object.defineProperty(el, "scrollTo", { configurable: true, value: undefined });
    el.scrollTop = 0;
    stick.jumpToLatest();
    expect(el.scrollTop).toBe(1000);
  });

  it("rebinds when the element changes and lets go of it when the scope ends", async () => {
    const dims = { scrollHeight: 1000, clientHeight: 200 };
    const make = () => {
      const node = document.createElement("div");
      Object.defineProperty(node, "scrollHeight", { configurable: true, get: () => dims.scrollHeight });
      Object.defineProperty(node, "clientHeight", { configurable: true, get: () => dims.clientHeight });
      return node;
    };
    const a = make();
    const b = make();
    const target = ref<HTMLElement | null>(a);
    const scope = effectScope();
    const stick = scope.run(() => useStickToBottom(target))!;

    target.value = b;
    await nextTick();
    a.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    expect(stick.pinned.value).toBe(true);
    b.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    expect(stick.pinned.value).toBe(false);

    stick.jumpToLatest();
    scope.stop();
    b.dispatchEvent(new WheelEvent("wheel", { deltaY: -40 }));
    expect(stick.pinned.value).toBe(true);
  });
});
