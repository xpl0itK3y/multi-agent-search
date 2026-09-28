// Drag a side panel (the mobile drawer, SlideOver sheets) away with a finger
// (apple-design §2, §3, §5, §6, §9):
// - the panel follows the finger 1:1 once the gesture is clearly horizontal, with a
//   rubber band past the open edge;
// - on release while still moving, its direction decides (the velocity sign, not the
//   position); released at rest, it commits when its landing point is past half-way;
// - it settles on a critically damped spring that starts at the release velocity, so
//   there is no seam between dragging and animating, and never overshoots the open
//   edge (a gap would show);
// - grabbing it mid-settle stops the spring where it is on screen and carries on.
// Touch and pen only: with a mouse, the scrim, ✕ and Escape already do this.
// The panel gets touch-action: pan-y, but the browser stops looking for touch-action at
// the nearest scroller, so a scroller inside the panel needs pan-y too (SlideOver does
// this for its vertical scrollers) or a sideways drag there becomes a browser pan.
import { getCurrentScope, nextTick, onScopeDispose, watch, type Ref } from "vue";

import { createVelocityTracker, project, rubberband, springTo, type Animation } from "./gesture";
import { prefersReducedMotion } from "./motion";

export interface DragDismissOptions {
  panel: Ref<HTMLElement | null>;
  /** The edge the panel is attached to (and leaves through). */
  side: "left" | "right";
  enabled: () => boolean;
  onDismiss: () => void;
  /** Fades with the drag, from opaque (open) to clear (closed). */
  scrim?: Ref<HTMLElement | null>;
}

const LOCK_PX = 10; // hysteresis before a direction is chosen (§10)
const HORIZONTAL_BIAS = 1.2; // |dx| must beat |dy| by this much to claim the gesture
const FLICK_PX_S = 300; // a flick: its direction decides, even from the rubber band
const MOVING_PX_S = 50; // slower than this at release, the panel counts as at rest
const RESPONSE = 0.3; // Apple's drawer response, critically damped (no bounce)
const OMEGA = (2 * Math.PI) / RESPONSE;
const CLICK_GUARD_MS = 250;
const REDUCED_FADE_MS = 200;

// Presses that start another gesture of their own: text fields, and horizontal
// scrollers (a wide table), whose pan must not drag the panel too.
function startsOwnGesture(target: EventTarget | null, panel: HTMLElement): boolean {
  let el = target instanceof Element ? target : null;
  while (el && el !== panel) {
    if (el.matches("input, textarea, select, [contenteditable]:not([contenteditable='false']), [data-no-drag-dismiss]")) return true;
    if (el instanceof HTMLElement && el.scrollWidth > el.clientWidth + 2) {
      const { overflowX } = getComputedStyle(el);
      if (overflowX === "auto" || overflowX === "scroll") return true;
    }
    el = el.parentElement;
  }
  return false;
}

export function useDragDismiss(opts: DragDismissOptions): void {
  const s = opts.side === "left" ? -1 : 1; // the closing direction on the x axis
  const tracker = createVelocityTracker();
  let gesture: { id: number; x0: number; y0: number; claimed: boolean; width: number; base: number } | null = null;
  let offset = 0;
  let anim: Animation | null = null;
  let dismissing = false;
  let unbind: (() => void) | null = null;

  const widthOf = (el: HTMLElement) =>
    el.getBoundingClientRect().width || el.offsetWidth || (typeof window !== "undefined" ? window.innerWidth : 0) || 1;

  function render(el: HTMLElement, x: number, width: number) {
    el.style.transform = `translate3d(${x}px, 0, 0)`;
    const scrim = opts.scrim?.value;
    if (scrim) scrim.style.opacity = String(1 - Math.min(Math.max((x * s) / width, 0), 1));
  }

  function clearInline() {
    offset = 0;
    const el = opts.panel.value;
    if (el) {
      el.style.transform = "";
      el.style.transition = "";
    }
    const scrim = opts.scrim?.value;
    if (scrim) {
      scrim.style.opacity = "";
      scrim.style.transition = "";
    }
  }

  function stopAnim() {
    const at = anim ? anim.stop() : offset;
    anim = null;
    return at;
  }

  // Direct manipulation: CSS transitions off while the finger (or the spring) drives it.
  function claim(el: HTMLElement, pointerId: number) {
    try {
      el.setPointerCapture?.(pointerId);
    } catch {
      // the pointer is already gone
    }
    el.style.transition = "none";
    const scrim = opts.scrim?.value;
    if (scrim) scrim.style.transition = "none";
  }

  // After a real drag, the release must not also click whatever is under the finger.
  function swallowNextClick() {
    if (typeof window === "undefined") return;
    const release = () => {
      window.removeEventListener("click", swallow, true);
      clearTimeout(timer);
    };
    const swallow = (e: Event) => {
      e.preventDefault();
      e.stopPropagation();
      release();
    };
    window.addEventListener("click", swallow, true);
    const timer = setTimeout(release, CLICK_GUARD_MS);
  }

  function finishDismiss(el: HTMLElement) {
    dismissing = true;
    opts.onDismiss();
    // Clear the inline transform only once the closed classes are on, so nothing jumps.
    nextTick(() =>
      requestAnimationFrame(() => {
        dismissing = false;
        if (opts.panel.value === el) clearInline();
      }),
    );
  }

  // Released still moving: the direction it was moving decides (the velocity sign, not
  // the position), so a slow push back toward open stays open even past half-way.
  // Released at rest: where it would come to rest (§6). A panel pulled out into the
  // rubber band and let back hasn't started closing: only a flick closes it from there.
  function releaseCloses(v: number, width: number): boolean {
    if (Math.abs(v) > FLICK_PX_S) return v * s > 0;
    if (offset * s <= 0) return false;
    if (Math.abs(v) > MOVING_PX_S) return v * s > 0;
    return (offset + project(v)) * s > width / 2;
  }

  function settle(close: boolean, velocity: number, width: number) {
    const el = opts.panel.value;
    if (!el) return;
    if (prefersReducedMotion()) {
      // No slide: stay put and let the closed state's opacity fade run, then reset.
      if (!close) return clearInline();
      el.style.transition = "";
      const scrim = opts.scrim?.value;
      if (scrim) {
        scrim.style.transition = "";
        scrim.style.opacity = "";
      }
      dismissing = true;
      opts.onDismiss();
      setTimeout(() => {
        dismissing = false;
        if (opts.panel.value === el) clearInline();
      }, REDUCED_FADE_MS);
      return;
    }
    const target = close ? s * width : 0;
    let v = velocity;
    const x0 = offset - target;
    if (!close) {
      // Released in the rubber band past the open edge: it springs straight back rather
      // than carrying on outwards and opening a gap.
      if (x0 * s < 0 && v * x0 > 0) v = 0;
      // Toward the open edge, cap the hand-off so the critically damped spring can't
      // carry the panel past it: with |v| <= ω·|x0| it arrives without crossing.
      if (v * x0 < 0 && Math.abs(v) > OMEGA * Math.abs(x0)) v = -OMEGA * x0;
    }
    anim = springTo({
      from: offset,
      to: target,
      velocity: v,
      response: RESPONSE,
      onUpdate: (x) => {
        offset = x;
        render(el, x, width);
      },
      onComplete: () => {
        anim = null;
        if (close) finishDismiss(el);
        else clearInline();
      },
    });
  }

  function bind(el: HTMLElement): () => void {
    const onPointerDown = (e: PointerEvent) => {
      if (gesture || dismissing || !opts.enabled()) return;
      if (e.pointerType === "mouse" || !e.isPrimary) return;
      const moving = anim !== null;
      if (!moving && startsOwnGesture(e.target, el)) return;
      const base = stopAnim();
      const width = widthOf(el);
      gesture = { id: e.pointerId, x0: e.clientX, y0: e.clientY, claimed: false, width, base };
      tracker.reset();
      if (moving) {
        // Grabbed mid-settle: it stops under the finger and follows from there (§3).
        gesture.claimed = true;
        claim(el, e.pointerId);
        offset = base;
        render(el, base, width);
        tracker.add(base, 0, performance.now());
      }
    };

    const onPointerMove = (e: PointerEvent) => {
      const g = gesture;
      if (!g || e.pointerId !== g.id) return;
      let dx = e.clientX - g.x0;
      if (!g.claimed) {
        const dy = e.clientY - g.y0;
        if (Math.hypot(dx, dy) < LOCK_PX) return;
        if (Math.abs(dx) <= HORIZONTAL_BIAS * Math.abs(dy)) {
          gesture = null; // vertical: the native scroll wins
          return;
        }
        g.claimed = true;
        claim(el, g.id);
        // Track from here on, so the panel doesn't jump by the hysteresis.
        g.x0 = e.clientX;
        dx = 0;
      }
      const raw = g.base + dx;
      // Past the open edge: resist progressively (§9). Toward closed: 1:1 up to closed.
      offset = raw * s < 0 ? -s * rubberband(Math.abs(raw), g.width) : s * Math.min(raw * s, g.width);
      render(el, offset, g.width);
      tracker.add(offset, 0, performance.now());
    };

    const onPointerUp = (e: PointerEvent) => {
      const g = gesture;
      if (!g || e.pointerId !== g.id) return;
      gesture = null;
      if (!g.claimed) return;
      const v = tracker.velocity(performance.now()).vx;
      swallowNextClick();
      settle(releaseCloses(v, g.width), v, g.width);
    };

    // The browser took the pointer (a pan it owns, a pinch): nothing more will come.
    const onPointerCancel = (e: PointerEvent) => {
      const g = gesture;
      if (!g || e.pointerId !== g.id) return;
      gesture = null;
      if (g.claimed) settle(false, 0, g.width);
    };

    // Only the panel's own capture ending counts. A touch starts out implicitly captured
    // by the element under the finger; when claim() moves the capture to the panel, that
    // child's lostpointercapture bubbles up here and must not cancel the drag just claimed.
    const onLostCapture = (e: PointerEvent) => {
      if (e.target === el) onPointerCancel(e);
    };

    const previousTouchAction = el.style.touchAction;
    el.style.touchAction = "pan-y";
    el.addEventListener("pointerdown", onPointerDown);
    el.addEventListener("pointermove", onPointerMove);
    el.addEventListener("pointerup", onPointerUp);
    el.addEventListener("pointercancel", onPointerCancel);
    el.addEventListener("lostpointercapture", onLostCapture);
    return () => {
      el.style.touchAction = previousTouchAction;
      el.removeEventListener("pointerdown", onPointerDown);
      el.removeEventListener("pointermove", onPointerMove);
      el.removeEventListener("pointerup", onPointerUp);
      el.removeEventListener("pointercancel", onPointerCancel);
      el.removeEventListener("lostpointercapture", onLostCapture);
    };
  }

  watch(
    opts.panel,
    (el) => {
      unbind?.();
      unbind = null;
      gesture = null;
      if (el) unbind = bind(el);
    },
    { immediate: true, flush: "post" },
  );

  // Closed (or disabled) from elsewhere mid-drag — Escape, ✕, a route change: hand the
  // panel back to its CSS transition, which starts from where it is on screen.
  watch(
    () => opts.enabled(),
    (enabled) => {
      if (enabled || dismissing) return;
      stopAnim();
      gesture = null;
      clearInline();
    },
  );

  if (getCurrentScope()) {
    onScopeDispose(() => {
      stopAnim();
      unbind?.();
      unbind = null;
    });
  }
}
