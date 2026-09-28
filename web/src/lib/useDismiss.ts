// Close a popover or menu on an outside press or Escape (apple-design §16 wayfinding:
// "how do I get out?" always has an answer). Listening to pointerdown, not click, means
// the same press that starts a scroll or a drag elsewhere also dismisses.
import { getCurrentScope, onScopeDispose, watch, type Ref } from "vue";

import { confirmState } from "./confirm";

export function useDismiss(
  root: Ref<HTMLElement | null>,
  open: Ref<boolean>,
  opts: { trigger?: Ref<HTMLElement | null> } = {},
): void {
  let bound = false;

  const inside = (target: EventTarget | null, el: HTMLElement | null | undefined) =>
    !!el && target instanceof Node && el.contains(target);

  const onPointerDown = (e: PointerEvent) => {
    // A confirmation opened from inside the popover (e.g. revoke) keeps it open.
    if (confirmState.open) return;
    // The trigger toggles itself; presses inside the popover are its own.
    if (inside(e.target, root.value) || inside(e.target, opts.trigger?.value)) return;
    open.value = false;
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key !== "Escape" || e.defaultPrevented || confirmState.open) return;
    // Innermost first: this listener runs in the capture phase, so a surrounding sheet or
    // dialog sees defaultPrevented and stays open.
    e.preventDefault();
    open.value = false;
    opts.trigger?.value?.focus();
  };

  function add() {
    if (bound || typeof document === "undefined") return;
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown, true);
    bound = true;
  }
  function remove() {
    if (!bound) return;
    document.removeEventListener("pointerdown", onPointerDown, true);
    document.removeEventListener("keydown", onKeyDown, true);
    bound = false;
  }

  watch(open, (isOpen) => (isOpen ? add() : remove()), { immediate: true, flush: "sync" });
  if (getCurrentScope()) onScopeDispose(remove);
}
