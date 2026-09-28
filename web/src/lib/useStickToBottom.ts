// Streamed content that follows only while the reader is at the bottom (apple-design §3:
// never fight the user's hand). The scroller stays pinned to the latest content until
// the reader scrolls away; reaching the bottom again, or jumpToLatest(), re-pins it.
//
// Only a real user gesture (wheel, touch, keys, a scrollbar drag) can unpin. Scrolls the
// code causes never do, so a burst of streamed tokens can't knock the view loose.
//
// A gesture meant for something inside the scroller never unpins it either: a wheel or key
// that a nested scroller can still take, or that a widget (a tab strip, a menu) already
// handled, leaves the followed scroller where it is, so it must keep following.
import { getCurrentScope, onScopeDispose, ref, watch, type Ref } from "vue";

import { smoothOrAuto } from "./motion";

// A scroll this soon after a user gesture is the user's own.
const INTENT_MS = 250;
const UNPIN_KEYS = new Set(["PageUp", "ArrowUp", "Home"]);
const SCROLLABLE = new Set(["auto", "scroll", "overlay"]);

function isEditable(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || typeof el.closest !== "function") return false;
  return !!el.closest("input, textarea, select, [contenteditable]:not([contenteditable='false'])");
}

// Whether a scroller between the event's target and the followed node can still move by
// (dx, dy). The browser gives a wheel or a scroll key to that nearest scroller and chains
// to the next one only at its end, so while it can move the followed node stays put.
function innerScrollerTakes(node: HTMLElement, target: EventTarget | null, dx: number, dy: number): boolean {
  for (let el = target instanceof Element ? target : null; el && el !== node; el = el.parentElement) {
    if (!(el instanceof HTMLElement)) continue;
    const style = getComputedStyle(el);
    if (dy && SCROLLABLE.has(style.overflowY)) {
      if (dy < 0 ? el.scrollTop > 0 : el.scrollTop + el.clientHeight < el.scrollHeight - 1) return true;
    }
    // Sideways only when the gesture is mostly sideways: a vertical wheel with a little
    // drift must still reach the followed node.
    if (dx && Math.abs(dx) > Math.abs(dy) && SCROLLABLE.has(style.overflowX)) {
      if (dx < 0 ? el.scrollLeft > 0 : el.scrollLeft + el.clientWidth < el.scrollWidth - 1) return true;
    }
  }
  return false;
}

export function useStickToBottom(el: Ref<HTMLElement | null>, { threshold = 80 }: { threshold?: number } = {}) {
  const pinned = ref(true);
  let lastIntent = -Infinity;
  let frame = 0;
  let unbind: (() => void) | null = null;

  const now = () => performance.now();
  const fromBottom = (node: HTMLElement) => node.scrollHeight - node.scrollTop - node.clientHeight;

  function cancelFrame() {
    if (frame) cancelAnimationFrame(frame);
    frame = 0;
  }

  function bind(node: HTMLElement): () => void {
    const onWheel = (e: WheelEvent) => {
      // Still marked as intent: should this node move after all, onScroll lets go.
      lastIntent = now();
      if (e.deltaY >= 0 || node.scrollHeight <= node.clientHeight) return;
      // A tab strip that turned the wheel sideways, or an inner scroller reading back up.
      if (e.defaultPrevented || innerScrollerTakes(node, e.target, e.deltaX, e.deltaY)) return;
      // Reading back up: let go at once, before the next token can pull the view down.
      pinned.value = false;
    };
    const onTouchMove = () => {
      lastIntent = now();
    };
    const onKeyDown = (e: KeyboardEvent) => {
      // A handled key (Home in a tab strip, ArrowUp in a menu) scrolls nothing.
      if (!UNPIN_KEYS.has(e.key) || e.defaultPrevented || isEditable(e.target)) return;
      lastIntent = now();
      if (!innerScrollerTakes(node, e.target, 0, -1)) pinned.value = false;
    };
    const onPointerDown = (e: PointerEvent) => {
      // A press in this node's own scrollbar gutter starts a scrollbar drag (a press in a
      // nested scroller's gutter targets that scroller instead).
      if (e.target === node && e.offsetX > node.clientWidth) lastIntent = now();
    };
    const onScroll = () => {
      if (fromBottom(node) <= threshold) pinned.value = true;
      else if (now() - lastIntent < INTENT_MS) pinned.value = false;
    };

    const passive = { passive: true } as const;
    node.addEventListener("wheel", onWheel, passive);
    node.addEventListener("touchmove", onTouchMove, passive);
    node.addEventListener("keydown", onKeyDown);
    node.addEventListener("pointerdown", onPointerDown, passive);
    node.addEventListener("scroll", onScroll, passive);
    return () => {
      node.removeEventListener("wheel", onWheel);
      node.removeEventListener("touchmove", onTouchMove);
      node.removeEventListener("keydown", onKeyDown);
      node.removeEventListener("pointerdown", onPointerDown);
      node.removeEventListener("scroll", onScroll);
    };
  }

  watch(
    el,
    (node) => {
      unbind?.();
      unbind = null;
      cancelFrame();
      if (node) unbind = bind(node);
    },
    { immediate: true, flush: "post" },
  );

  if (getCurrentScope()) {
    onScopeDispose(() => {
      unbind?.();
      unbind = null;
      cancelFrame();
    });
  }

  /** Keep the latest content in view: at most one instant scroll per frame, only while pinned. */
  function follow() {
    if (!pinned.value || frame) return;
    frame = requestAnimationFrame(() => {
      frame = 0;
      const node = el.value;
      // Instant, never smooth: stacked smooth scrolls lag behind a stream.
      if (node && pinned.value) node.scrollTop = node.scrollHeight;
    });
  }

  /** Go to the latest content and follow it again (the "jump to latest" pill). */
  function jumpToLatest() {
    pinned.value = true;
    const node = el.value;
    if (!node) return;
    if (typeof node.scrollTo === "function") node.scrollTo({ top: node.scrollHeight, behavior: smoothOrAuto() });
    else node.scrollTop = node.scrollHeight;
  }

  return { pinned, follow, jumpToLatest };
}
