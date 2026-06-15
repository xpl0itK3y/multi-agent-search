<script setup lang="ts">
import { onBeforeUnmount, onMounted } from "vue";
import { answerConfirm, confirmState } from "@/lib/confirm";

function onKey(e: KeyboardEvent) {
  if (!confirmState.open) return;
  if (e.key === "Escape") answerConfirm(false);
  else if (e.key === "Enter") answerConfirm(true);
}
onMounted(() => window.addEventListener("keydown", onKey));
onBeforeUnmount(() => window.removeEventListener("keydown", onKey));
</script>

<template>
  <transition name="fade">
    <div
      v-if="confirmState.open"
      class="fixed inset-0 z-[100] flex items-center justify-center bg-black/50 px-4 backdrop-blur-sm"
      @click.self="answerConfirm(false)"
    >
      <div class="animate-rise w-full max-w-sm rounded-2xl border border-bd bg-surface p-5 shadow-2xl">
        <h3 v-if="confirmState.title" class="mb-1.5 text-base font-semibold text-ink">{{ confirmState.title }}</h3>
        <p class="text-sm leading-relaxed text-muted">{{ confirmState.message }}</p>
        <div class="mt-5 flex justify-end gap-2">
          <button
            class="rounded-lg border border-bd px-3.5 py-2 text-sm text-muted transition hover:text-ink"
            @click="answerConfirm(false)"
          >
            {{ confirmState.cancelText }}
          </button>
          <button
            class="rounded-lg px-3.5 py-2 text-sm font-medium transition"
            :class="confirmState.danger ? 'bg-red-500/90 text-white hover:bg-red-500' : 'bg-accent text-bg hover:opacity-90'"
            @click="answerConfirm(true)"
          >
            {{ confirmState.confirmText }}
          </button>
        </div>
      </div>
    </div>
  </transition>
</template>
