// Gesture physics for the few interactions a finger drives (apple-design §5, §6, §9):
// drawer/sheet drag-to-dismiss, the admin graph's pan glide and the sidebar resize
// settle. Everything a button drives stays a CSS transition (see style.css).
//
// No dependencies: a rubber band, momentum projection, a short velocity history, a
// closed-form critically damped spring that takes the release velocity exactly and can
// be stopped at its on-screen value, and an exponential decay glide.

/** Soft boundary (§9): the further past the edge, the less the element follows. */
export function rubberband(overshoot: number, dimension: number, constant = 0.55): number {
  if (!overshoot || dimension <= 0) return 0;
  const o = Math.abs(overshoot);
  return (Math.sign(overshoot) * (o * dimension * constant)) / (dimension + constant * o);
}

/**
 * Momentum projection (§6): how far a release at `velocity` px/s would travel with
 * scroll-like deceleration. 0.998 matches normal scrolling; 0.99 is snappier.
 */
export function project(velocity: number, decelerationRate = 0.998): number {
  return ((velocity / 1000) * decelerationRate) / (1 - decelerationRate);
}

export interface VelocityTracker {
  /** Record a position at time `t` (ms, performance.now()). */
  add(x: number, y: number, t: number): void;
  /** px/s over the recent history; 0 when the pointer has paused or history is short. */
  velocity(now: number): { vx: number; vy: number };
  reset(): void;
}

const HISTORY_MS = 100;
const HISTORY_MAX = 8;
const PAUSE_MS = 80;

/** A short position history (the last 100 ms, at most 8 samples) for release velocity. */
export function createVelocityTracker(): VelocityTracker {
  let samples: { x: number; y: number; t: number }[] = [];
  return {
    add(x, y, t) {
      samples.push({ x, y, t });
      samples = samples.filter((s) => t - s.t <= HISTORY_MS).slice(-HISTORY_MAX);
    },
    velocity(now) {
      if (samples.length < 2) return { vx: 0, vy: 0 };
      const first = samples[0];
      const last = samples[samples.length - 1];
      const dt = last.t - first.t;
      // A finger that stopped before lifting carries no momentum.
      if (dt < 1 || now - last.t > PAUSE_MS) return { vx: 0, vy: 0 };
      return { vx: ((last.x - first.x) / dt) * 1000, vy: ((last.y - first.y) / dt) * 1000 };
    },
    reset() {
      samples = [];
    },
  };
}

export interface Animation {
  /** Cancel and return the current (presentation) value, so a new grab resumes from it. */
  stop(): number;
}

// Frames are timed by their own rAF timestamps (one clock, even under fake timers). The
// first frame counts as one 60 Hz frame; a long gap (a background tab) is capped.
const FIRST_FRAME_MS = 1000 / 60;
const MAX_FRAME_MS = 100;

const requestFrame = (cb: (ts: number) => void): number =>
  typeof requestAnimationFrame === "function"
    ? requestAnimationFrame(cb)
    : (setTimeout(() => cb(performance.now()), FIRST_FRAME_MS) as unknown as number);
const cancelFrame = (id: number): void => {
  if (typeof cancelAnimationFrame === "function") cancelAnimationFrame(id);
  else clearTimeout(id);
};

function frameClock() {
  let prev: number | null = null;
  return (ts: number): number => {
    const dt = prev === null ? FIRST_FRAME_MS : Math.min(Math.max(ts - prev, 0), MAX_FRAME_MS);
    prev = ts;
    return dt;
  };
}

// A spring is at rest once it is within half a pixel of its target and slower than this.
const REST_DISTANCE = 0.5;
const REST_VELOCITY = 20;

export interface SpringOptions {
  from: number;
  to: number;
  /** Initial velocity in px/s (the gesture's release velocity, §5). */
  velocity?: number;
  /** Seconds to (nearly) reach the target; not a duration. Apple's drawer uses 0.3. */
  response?: number;
  onUpdate: (value: number) => void;
  onComplete?: () => void;
}

/**
 * A critically damped spring (damping 1.0: no bounce), in closed form with
 * ω = 2π / response:
 *   x(t) = to + (x0 + (v0 + ω·x0)·t)·e^(−ωt),  v(t) = (v0 − ω·(v0 + ω·x0)·t)·e^(−ωt)
 * with x0 = from − to and v0 the initial velocity. rAF-driven.
 */
export function springTo(opts: SpringOptions): Animation {
  const { from, to, onUpdate, onComplete } = opts;
  const omega = (2 * Math.PI) / Math.max(opts.response ?? 0.3, 0.01);
  const x0 = from - to;
  const v0 = opts.velocity ?? 0;
  const b = v0 + omega * x0;
  const tick = frameClock();
  let t = 0; // seconds
  let current = from;
  let done = false;
  let id = 0;

  const frame = (ts: number) => {
    if (done) return;
    t += tick(ts) / 1000;
    const k = Math.exp(-omega * t);
    const x = to + (x0 + b * t) * k;
    const v = (v0 - omega * b * t) * k;
    if (Math.abs(x - to) < REST_DISTANCE && Math.abs(v) < REST_VELOCITY) {
      done = true;
      current = to;
      onUpdate(to);
      onComplete?.();
      return;
    }
    current = x;
    onUpdate(x);
    id = requestFrame(frame);
  };
  id = requestFrame(frame);

  return {
    stop() {
      if (!done) {
        done = true;
        cancelFrame(id);
      }
      return current;
    },
  };
}

export interface DecayOptions {
  from: number;
  /** px/s */
  velocity: number;
  /** Per-millisecond deceleration, as in project(). */
  rate?: number;
  onUpdate: (value: number) => void;
  onComplete?: () => void;
}

/**
 * An exponential glide (scroll-like momentum):
 *   x(t) = from + (v/1000)·(rate^t − 1)/ln(rate),  v(t) = v·rate^t   (t in ms)
 * It comes to rest where project() predicts, and stops once slower than 20 px/s.
 */
export function decay(opts: DecayOptions): Animation {
  const { from, velocity, onUpdate, onComplete } = opts;
  const rate = opts.rate ?? 0.998;
  const lnRate = Math.log(rate);
  const tick = frameClock();
  let t = 0; // ms
  let current = from;
  let done = false;
  let id = 0;

  const frame = (ts: number) => {
    if (done) return;
    t += tick(ts);
    const k = Math.pow(rate, t);
    const x = from + ((velocity / 1000) * (k - 1)) / lnRate;
    current = x;
    onUpdate(x);
    if (Math.abs(velocity * k) < REST_VELOCITY) {
      done = true;
      onComplete?.();
      return;
    }
    id = requestFrame(frame);
  };
  id = requestFrame(frame);

  return {
    stop() {
      if (!done) {
        done = true;
        cancelFrame(id);
      }
      return current;
    },
  };
}
