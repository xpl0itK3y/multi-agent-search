<script setup lang="ts">
// The one confirmation dialog (apple-design §16 agency: only for destructive or
// irreversible actions). It dims the page with the shared scrim and rises slightly on the
// modal transition, leaving the same way (§7, §12). Keyboard: Escape cancels; Enter
// confirms a harmless action but never a destructive one on its own, where focus starts
// on Cancel and Enter activates the focused button; Tab stays on the two buttons. Focus
// goes back to where it was when it closes (lib/confirm.ts).
import { nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from "vue";
import { answerConfirm, confirmState } from "@/lib/confirm";

const id = useId();
const titleId = `${id}-title`;
const messageId = `${id}-message`;

const cancelButton = ref<HTMLButtonElement | null>(null);
const confirmButton = ref<HTMLButtonElement | null>(null);

function buttons(): HTMLButtonElement[] {
  return [cancelButton.value, confirmButton.value].filter((b): b is HTMLButtonElement => !!b);
}

function onKey(e: KeyboardEvent) {
  if (!confirmState.open) return;
  if (e.key === "Escape") {
    e.preventDefault();
    answerConfirm(false);
  } else if (e.key === "Enter") {
    const active = document.activeElement;
    // Enter on a focused button answers for that button (the same as a click).
    if (active === cancelButton.value || active === confirmButton.value) {
      e.preventDefault();
      answerConfirm(active === confirmButton.value);
      return;
    }
    // Focus elsewhere: only a harmless action confirms on Enter.
    if (!confirmState.danger) {
      e.preventDefault();
      answerConfirm(true);
    }
  } else if (e.key === "Tab") {
    const items = buttons();
    if (!items.length) return;
    e.preventDefault();
    const at = items.indexOf(document.activeElement as HTMLButtonElement);
    const next = at < 0 ? (e.shiftKey ? items.length - 1 : 0) : (at + (e.shiftKey ? -1 : 1) + items.length) % items.length;
    items[next].focus();
  }
}
onMounted(() => window.addEventListener("keydown", onKey));
onBeforeUnmount(() => window.removeEventListener("keydown", onKey));

// Opened: focus Cancel for a destructive action, Confirm otherwise.
watch(
  () => confirmState.open,
  async (open) => {
    if (!open) return;
    await nextTick();
    if (!confirmState.open) return;
    (confirmState.danger ? cancelButton.value : confirmButton.value)?.focus({ preventScroll: true });
  },
);

// The backdrop cancels only when the press starts and ends on it, so a text selection
// dragged out of the panel doesn't cancel.
let pressedBackdrop = false;
function onBackdropPointerDown(e: PointerEvent) {
  pressedBackdrop = e.target === e.currentTarget;
}
function onBackdropClick(e: MouseEvent) {
  const cancel = pressedBackdrop && e.target === e.currentTarget;
  pressedBackdrop = false;
  if (cancel) answerConfirm(false);
}
</script>

<template>
  <transition name="modal">
    <div
      v-if="confirmState.open"
      class="scrim fixed inset-0 z-[100] flex items-center justify-center px-4"
      data-test="confirm-backdrop"
      @pointerdown="onBackdropPointerDown"
      @click="onBackdropClick"
    >
      <div
        role="alertdialog"
        aria-modal="true"
        :aria-labelledby="confirmState.title ? titleId : messageId"
        :aria-describedby="confirmState.title ? messageId : undefined"
        class="modal-panel w-full max-w-sm rounded-2xl border border-bd bg-surface p-5 shadow-e3"
        data-test="confirm-panel"
      >
        <h3 v-if="confirmState.title" :id="titleId" class="mb-1.5 text-base font-semibold text-ink">{{ confirmState.title }}</h3>
        <p :id="messageId" class="text-pretty text-sm leading-relaxed text-muted">{{ confirmState.message }}</p>
        <div class="mt-5 flex justify-end gap-2">
          <button
            ref="cancelButton"
            type="button"
            class="press rounded-lg border border-bd px-3.5 py-2 text-sm text-ink hover:bg-surfaceHover"
            @click="answerConfirm(false)"
          >
            {{ confirmState.cancelText }}
          </button>
          <button
            ref="confirmButton"
            type="button"
            class="press rounded-lg px-3.5 py-2 text-sm font-medium"
            :class="confirmState.danger ? 'bg-red-600 text-white hover:bg-red-700' : 'bg-accent text-onAccent hover:opacity-90'"
            @click="answerConfirm(true)"
          >
            {{ confirmState.confirmText }}
          </button>
        </div>
      </div>
    </div>
  </transition>
</template>
