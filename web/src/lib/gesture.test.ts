// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createVelocityTracker, decay, project, rubberband, springTo } from "./gesture";

// rAF and performance.now are faked too, so frames advance with the clock.
const FAKE = ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "requestAnimationFrame", "cancelAnimationFrame", "performance", "Date"] as const;

describe("rubberband", () => {
  it("does not move at the edge and follows less the further past it", () => {
    expect(rubberband(0, 300)).toBe(0);
    expect(rubberband(10, 300)).toBeCloseTo(5.4, 1);
    expect(rubberband(-10, 300)).toBeCloseTo(-5.4, 1);

    let prev = 0;
    for (const o of [1, 10, 50, 100, 300, 1000, 10000]) {
      const r = rubberband(o, 300);
      expect(r).toBeGreaterThan(prev);
      expect(r).toBeLessThan(300);
      prev = r;
    }
  });
});

describe("project", () => {
  it("projects a release the way scrolling decelerates", () => {
    expect(project(1000)).toBeCloseTo(499, 0);
    expect(project(-1000)).toBeCloseTo(-499, 0);
    expect(project(1000, 0.99)).toBeCloseTo(99, 0);
  });
});

describe("createVelocityTracker", () => {
  it("measures px/s over the recent samples", () => {
    const tracker = createVelocityTracker();
    tracker.add(0, 0, 1000);
    tracker.add(25, -10, 1025);
    tracker.add(50, -20, 1050);
    const { vx, vy } = tracker.velocity(1050);
    expect(vx).toBeCloseTo(1000);
    expect(vy).toBeCloseTo(-400);
  });

  it("carries no momentum once the pointer has paused", () => {
    const tracker = createVelocityTracker();
    tracker.add(0, 0, 0);
    tracker.add(50, 0, 50);
    expect(tracker.velocity(50 + 81)).toEqual({ vx: 0, vy: 0 });
  });

  it("forgets samples older than 100ms and needs two of them", () => {
    const tracker = createVelocityTracker();
    tracker.add(0, 0, 0);
    expect(tracker.velocity(0)).toEqual({ vx: 0, vy: 0 });
    tracker.add(500, 0, 10); // a fast start…
    tracker.add(500, 0, 300); // …long ago by now
    tracker.add(530, 0, 330);
    expect(tracker.velocity(330).vx).toBeCloseTo(1000);
    tracker.reset();
    expect(tracker.velocity(330)).toEqual({ vx: 0, vy: 0 });
  });
});

describe("springTo", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: [...FAKE] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("settles on the target without overshooting and then completes", () => {
    const values: number[] = [];
    const onComplete = vi.fn();
    springTo({ from: 0, to: 300, response: 0.3, onUpdate: (v) => values.push(v), onComplete });

    vi.advanceTimersByTime(2000);

    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(values.at(-1)).toBe(300);
    expect(values.length).toBeGreaterThan(5);
    for (const v of values) expect(v).toBeGreaterThanOrEqual(0);
    for (const v of values) expect(v).toBeLessThanOrEqual(300);
    for (let i = 1; i < values.length; i++) expect(values[i]).toBeGreaterThanOrEqual(values[i - 1]);
  });

  it("continues at the hand-off velocity: a release moving away first keeps going that way", () => {
    const values: number[] = [];
    springTo({ from: 0, to: 100, velocity: -2000, onUpdate: (v) => values.push(v) });

    vi.advanceTimersByTime(17);

    expect(values).toHaveLength(1);
    expect(values[0]).toBeLessThan(0);
  });

  it("stops at its on-screen value", () => {
    const values: number[] = [];
    const onComplete = vi.fn();
    const anim = springTo({ from: 0, to: 300, onUpdate: (v) => values.push(v), onComplete });

    vi.advanceTimersByTime(100);
    const at = anim.stop();
    vi.advanceTimersByTime(1000);

    expect(at).toBeGreaterThan(0);
    expect(at).toBeLessThan(300);
    expect(at).toBe(values.at(-1));
    expect(onComplete).not.toHaveBeenCalled();
    expect(anim.stop()).toBe(at);
  });
});

describe("decay", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: [...FAKE] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("glides to where project() predicts and completes", () => {
    const values: number[] = [];
    const onComplete = vi.fn();
    decay({ from: 100, velocity: 3000, onUpdate: (v) => values.push(v), onComplete });

    vi.advanceTimersByTime(10_000);

    expect(onComplete).toHaveBeenCalledTimes(1);
    const expected = 100 + project(3000);
    expect(Math.abs(values.at(-1)! - expected) / expected).toBeLessThan(0.02);
    for (let i = 1; i < values.length; i++) expect(values[i]).toBeGreaterThanOrEqual(values[i - 1]);
  });

  it("can be stopped mid-glide", () => {
    const values: number[] = [];
    const anim = decay({ from: 0, velocity: -2000, onUpdate: (v) => values.push(v) });
    vi.advanceTimersByTime(200);
    const at = anim.stop();
    expect(at).toBeLessThan(0);
    expect(at).toBeGreaterThan(project(-2000));
    expect(at).toBe(values.at(-1));
  });
});
