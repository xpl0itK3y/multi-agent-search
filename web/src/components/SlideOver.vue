<script setup lang="ts">
// A side sheet (apple-design §7, §12, §14): it slides in from its edge and leaves the
// same way, on the sheet curve; CSS retargets from the current position when toggled
// quickly. A finger can drag it away (useDragDismiss). Modal sheets dim the page with
// the shared scrim, trap Tab and restore focus; non-modal ones (modal=false) leave
// the page interactive beside them. Under reduced motion it cross-fades instead.
import { nextTick, onBeforeUnmount, ref, watch } from "vue";

import { confirmState } from "@/lib/confirm";
import { useReducedMotion } from "@/lib/motion";
import { useDragDismiss } from "@/lib/useDragDismiss";

const props = withDefaults(
  defineProps<{
    open: boolean;
    side?: "left" | "right";
    modal?: boolean;
    label?: string;
    panelClass?: string;
    zClass?: string;
  }>(),
  { side: "right", modal: true, label: undefined, panelClass: "w-full max-w-2xl", zClass: "z-[70]" },
);
const emit = defineEmits<{ (e: "close"): void }>();
// The root is a fragment (scrim + panel): extra attributes (data-*, aria-*) go to the panel.
defineOptions({ inheritAttrs: false });

const panel = ref<HTMLElement | null>(null);
const scrim = ref<HTMLElement | null>(null);
const reducedMotion = useReducedMotion();

useDragDismiss({ panel, side: props.side, enabled: () => props.open, onDismiss: () => emit("close"), scrim });

const FOCUSABLE = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
  "[contenteditable]:not([contenteditable='false'])",
].join(", ");

function focusables(): HTMLElement[] {
  if (!panel.value) return [];
  return [...panel.value.querySelectorAll<HTMLElement>(FOCUSABLE)].filter((el) => !el.closest("[hidden], [inert]"));
}

// Tab and Shift+Tab stay inside a modal sheet.
function trapTab(e: KeyboardEvent) {
  const items = focusables();
  const root = panel.value;
  if (!root) return;
  if (!items.length) {
    e.preventDefault();
    root.focus({ preventScroll: true });
    return;
  }
  const first = items[0];
  const last = items[items.length - 1];
  const active = document.activeElement;
  const inside = root.contains(active);
  if (e.shiftKey && (!inside || active === first || active === root)) {
    e.preventDefault();
    last.focus();
  } else if (!e.shiftKey && (!inside || active === last)) {
    e.preventDefault();
    first.focus();
  }
}

function onKeyDown(e: KeyboardEvent) {
  // A confirmation opened from the sheet handles its own keys.
  if (!props.open || confirmState.open) return;
  if (e.key === "Escape") {
    // An open popover inside the sheet closes first (it marks the event handled).
    if (e.defaultPrevented) return;
    e.preventDefault();
    emit("close");
  } else if (e.key === "Tab" && props.modal) {
    trapTab(e);
  }
}

let returnFocus: HTMLElement | null = null;

watch(
  () => props.open,
  async (open) => {
    if (typeof document === "undefined") return;
    if (open) {
      returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      document.addEventListener("keydown", onKeyDown);
      await nextTick();
      if (!props.open || !panel.value) return;
      const target = panel.value.querySelector<HTMLElement>("[data-autofocus]") ?? focusables()[0] ?? panel.value;
      target.focus({ preventScroll: true });
    } else {
      document.removeEventListener("keydown", onKeyDown);
      const back = returnFocus;
      returnFocus = null;
      // Back to where the reader was, unless they have already moved on elsewhere.
      const active = document.activeElement;
      const focusLeftBehind = !active || active === document.body || !!panel.value?.contains(active);
      if (back?.isConnected && focusLeftBehind) back.focus({ preventScroll: true });
    }
  },
  { immediate: true },
);

onBeforeUnmount(() => {
  if (typeof document !== "undefined") document.removeEventListener("keydown", onKeyDown);
});

// The scrim closes the sheet only when the press both starts and ends on it, so a text
// selection dragged out of the panel doesn't close it.
let pressedScrim = false;
function onScrimPointerDown(e: PointerEvent) {
  pressedScrim = e.target === e.currentTarget;
}
function onScrimClick(e: MouseEvent) {
  const close = pressedScrim && e.target === e.currentTarget;
  pressedScrim = false;
  if (close && props.open) emit("close");
}
function onPanelPointerDown() {
  pressedScrim = false;
}
</script>

<template>
  <div
    v-if="modal"
    ref="scrim"
    class="scrim fixed inset-0 z-[60] transition-opacity duration-200"
    :class="open ? 'opacity-100' : 'pointer-events-none opacity-0'"
    aria-hidden="true"
    data-test="slideover-scrim"
    @pointerdown="onScrimPointerDown"
    @click="onScrimClick"
  />
  <!-- inert: `undefined` (not false) when open, since Vue writes inert="false" where the
       DOM has no inert property, and that attribute alone makes the element inert. -->
  <div
    ref="panel"
    role="dialog"
    tabindex="-1"
    :aria-modal="modal ? 'true' : undefined"
    :aria-label="label"
    :aria-hidden="open ? undefined : 'true'"
    :inert="open ? undefined : true"
    class="fixed inset-y-0 flex flex-col border-bd bg-surface text-ink shadow-e3"
    :class="[
      side === 'left' ? 'left-0 border-r' : 'right-0 border-l',
      panelClass,
      zClass,
      reducedMotion
        ? ['transition-[opacity,visibility] duration-150', open ? 'opacity-100' : 'pointer-events-none invisible opacity-0']
        : [
            'transition-[transform,visibility] ease-sheet',
            open
              ? 'translate-x-0 duration-300'
              : ['invisible duration-[220ms]', side === 'left' ? '-translate-x-full' : 'translate-x-full'],
          ],
    ]"
    data-test="slideover-panel"
    v-bind="$attrs"
    @pointerdown="onPanelPointerDown"
  >
    <slot />
  </div>
</template>
