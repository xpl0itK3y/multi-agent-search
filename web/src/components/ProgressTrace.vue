<script setup lang="ts">
import { ref } from "vue";

export interface TraceEntry {
  step: string;
  detail: string;
}

defineProps<{ entries: TraceEntry[]; live?: boolean }>();

const open = ref(true);

// Friendly RU labels for graph step names.
const STEP_LABELS: Record<string, string> = {
  collect_context: "Сбор источников",
  replan: "Дополнительный поиск",
  analyze: "Синтез отчёта",
  verify: "Проверка фактов",
  tie_break: "Разрешение противоречий",
  complete: "Готово",
  stale_recovered: "Восстановление",
};

function label(step: string): string {
  return STEP_LABELS[step] ?? step;
}
</script>

<template>
  <div class="rounded-card border border-bd bg-surface/50">
    <button
      class="flex w-full items-center justify-between px-4 py-3 text-sm text-ink"
      @click="open = !open"
    >
      <span class="flex items-center gap-2">
        <span
          v-if="live"
          class="h-1.5 w-1.5 animate-pulse rounded-full bg-accent"
        />
        Ход работы
        <span class="text-muted">· {{ entries.length }}</span>
      </span>
      <span class="text-muted">{{ open ? "▾" : "▸" }}</span>
    </button>

    <div v-if="open" class="border-t border-bd px-4 py-3">
      <ol class="space-y-3">
        <li
          v-for="(entry, i) in entries"
          :key="i"
          class="flex gap-3 text-sm"
        >
          <span class="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-accentSoft" />
          <div class="min-w-0">
            <div class="text-ink">{{ label(entry.step) }}</div>
            <div v-if="entry.detail" class="text-muted">{{ entry.detail }}</div>
          </div>
        </li>
      </ol>
    </div>
  </div>
</template>
