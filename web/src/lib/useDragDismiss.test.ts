// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, ref } from "vue";

import { useDragDismiss } from "./useDragDismiss";

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

// rAF and performance.now follow the fake clock, so velocities and springs are exact.
const FAKE = ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame", "performance", "Date"] as const;

let scopes: ReturnType<typeof effectScope>[] = [];

// A 300px drawer on the left edge (it closes towards negative x) over a scrim.
function setup(side: "left" | "right" = "left") {
  const panel = document.createElement("div");
  Object.defineProperty(panel, "offsetWidth", { configurable: true, value: 300 });
  const scrim = document.createElement("div");
  document.body.append(scrim, panel);
  const open = ref(true);
  const onDismiss = vi.fn(() => {
    open.value = false;
  });
  const scope = effectScope();
  scopes.push(scope);
  scope.run(() => useDragDismiss({ panel: ref(panel), side, enabled: () => open.value, onDismiss, scrim: ref(scrim) }));

  let x = 0;
  let y = 0;
  const send = (type: string, pointerType = "touch") =>
    panel.dispatchEvent(
      new TestPointerEvent(type, { bubbles: true, cancelable: true, clientX: x, clientY: y, pointerId: 1, pointerType, isPrimary: true }),
    );
  const down = (at: number, pointerType?: string, atY = 100) => {
    x = at;
    y = atY;
    send("pointerdown", pointerType);
  };
  // Move to (to, y) after `ms` milliseconds.
  const move = (to: number, ms: number, pointerType?: string, toY = y) => {
    vi.advanceTimersByTime(ms);
    x = to;
    y = toY;
    send("pointermove", pointerType);
  };
  const up = (pointerType?: string) => send("pointerup", pointerType);
  return { panel, scrim, open, onDismiss, down, move, up };
}

describe("useDragDismiss", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: [...FAKE] });
    vi.advanceTimersByTime(1000);
  });
  afterEach(() => {
    for (const scope of scopes) scope.stop();
    scopes = [];
    vi.useRealTimers();
    document.body.innerHTML = "";
  });

  it("follows the finger 1:1 and fades the scrim with it", () => {
    const { panel, scrim, down, move } = setup();
    down(200);
    move(185, 10); // past the 10px hysteresis: claimed, tracked from here
    move(125, 10);
    expect(panel.style.transform).toBe("translate3d(-60px, 0, 0)");
    expect(panel.style.transition).toBe("none");
    expect(Number(scrim.style.opacity)).toBeCloseTo(1 - 60 / 300);
  });

  it("closes on a quick flick, after the spring hands off the release velocity", async () => {
    const { panel, scrim, onDismiss, down, move, up } = setup();
    down(200);
    move(185, 10);
    move(160, 10);
    move(125, 20); // -60px in 40ms
    up();
    expect(onDismiss).not.toHaveBeenCalled(); // the spring runs first

    vi.advanceTimersByTime(1000);
    expect(onDismiss).toHaveBeenCalledTimes(1);
    expect(panel.style.transform).toBe("translate3d(-300px, 0, 0)");

    // Once the closed state is in, the inline styles go.
    await nextTick();
    vi.advanceTimersByTime(20);
    expect(panel.style.transform).toBe("");
    expect(panel.style.transition).toBe("");
    expect(scrim.style.opacity).toBe("");
  });

  it("springs back open after a slow, short drag", () => {
    const { panel, onDismiss, down, move, up } = setup();
    down(200);
    for (let i = 1; i <= 6; i++) move(200 - 5 * i, 100); // -30px over 600ms
    up();
    vi.advanceTimersByTime(1000);
    expect(onDismiss).not.toHaveBeenCalled();
    expect(panel.style.transform).toBe("");
  });

  it("lets the velocity sign decide: a flick back towards open stays open", () => {
    const { onDismiss, down, move, up } = setup();
    down(200);
    move(180, 10);
    move(40, 100); // dragged most of the way closed…
    move(70, 20); // …then flicked back
    up();
    vi.advanceTimersByTime(1000);
    expect(onDismiss).not.toHaveBeenCalled();
  });

  it("resists past the open edge instead of following 1:1", () => {
    const { panel, onDismiss, down, move, up } = setup();
    down(100);
    move(115, 10);
    move(215, 10); // 100px towards open
    const shown = Number(/translate3d\((-?[\d.]+)px/.exec(panel.style.transform)![1]);
    expect(shown).toBeGreaterThan(0);
    expect(shown).toBeLessThan(60);
    up();
    vi.advanceTimersByTime(17);
    // It springs straight back instead of flying further out on the release velocity.
    const next = Number(/translate3d\((-?[\d.]+)px/.exec(panel.style.transform)![1]);
    expect(next).toBeLessThan(shown);
    vi.advanceTimersByTime(1000);
    expect(onDismiss).not.toHaveBeenCalled();
    expect(panel.style.transform).toBe("");
  });

  it("can be grabbed mid-settle and continues from where it is on screen", () => {
    const { panel, down, move, up } = setup();
    down(200);
    for (let i = 1; i <= 6; i++) move(200 - 20 * i, 100); // slow: it will spring back
    up();
    vi.advanceTimersByTime(50);
    const midway = panel.style.transform;
    expect(midway).not.toBe("");
    expect(midway).not.toBe("translate3d(0px, 0, 0)");

    down(150);
    vi.advanceTimersByTime(100);
    expect(panel.style.transform).toBe(midway); // held under the finger, no jump
  });

  it("leaves a mostly vertical drag to the native scroll", () => {
    const { panel, onDismiss, down, move, up } = setup();
    down(200, "touch", 100);
    move(196, 10, "touch", 140);
    move(150, 10, "touch", 150);
    up();
    vi.advanceTimersByTime(1000);
    expect(panel.style.transform).toBe("");
    expect(onDismiss).not.toHaveBeenCalled();
  });

  it("ignores the mouse", () => {
    const { panel, onDismiss, down, move, up } = setup();
    down(200, "mouse");
    move(185, 10, "mouse");
    move(100, 10, "mouse");
    up("mouse");
    vi.advanceTimersByTime(1000);
    expect(panel.style.transform).toBe("");
    expect(onDismiss).not.toHaveBeenCalled();
  });

  it("swallows the click that ends a drag", () => {
    const { panel, down, move, up } = setup();
    down(200);
    move(185, 10);
    move(160, 10);
    up();
    const click = new MouseEvent("click", { bubbles: true, cancelable: true });
    panel.dispatchEvent(click);
    expect(click.defaultPrevented).toBe(true);

    vi.advanceTimersByTime(1000);
    const later = new MouseEvent("click", { bubbles: true, cancelable: true });
    panel.dispatchEvent(later);
    expect(later.defaultPrevented).toBe(false);
  });

  it("works mirrored for a right-hand sheet", () => {
    const { onDismiss, down, move, up } = setup("right");
    down(100);
    move(115, 10);
    move(140, 10);
    move(175, 20);
    up();
    vi.advanceTimersByTime(1000);
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it("hands the panel back to CSS when it is closed elsewhere mid-drag", async () => {
    const { panel, open, down, move } = setup();
    down(200);
    move(185, 10);
    move(120, 10);
    open.value = false; // e.g. Escape
    await nextTick();
    expect(panel.style.transform).toBe("");
    expect(panel.style.transition).toBe("");
  });
});
