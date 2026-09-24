<script setup lang="ts">
import { ref, watch } from "vue";
import type { PlanItem } from "@/lib/types";

const props = defineProps<{ prompt: string; items: PlanItem[]; busy?: boolean }>();
const emit = defineEmits<{ approve: [PlanItem[]]; cancel: [] }>();

interface EditableRow {
  id: string;
  description: string;
  queriesText: string;
}

const rows = ref<EditableRow[]>([]);

watch(
  () => props.items,
  (items) => {
    rows.value = items.map((it) => ({
      id: it.id,
      description: it.description,
      queriesText: (it.queries || []).join("\n"),
    }));
  },
  { immediate: true },
);

function removeRow(i: number) {
  rows.value.splice(i, 1);
}

function addRow() {
  rows.value.push({ id: `plan-${crypto.randomUUID()}`, description: "", queriesText: "" });
}

function onQueryFocus(e: FocusEvent) {
  const el = e.target as HTMLTextAreaElement;
  el.rows = 2;
}

function onQueryBlur(e: FocusEvent, row: EditableRow) {
  const el = e.target as HTMLTextAreaElement;
  if (!row.queriesText.includes("\n")) {
    el.rows = 1;
  }
}

function approve() {
  const items: PlanItem[] = rows.value
    .map((r) => ({
      id: r.id,
      description: r.description.trim(),
      queries: r.queriesText
        .split("\n")
        .map((q) => q.trim())
        .filter(Boolean),
    }))
    .filter((it) => it.queries.length > 0);
  emit("approve", items);
}
</script>

<template>
  <div class="animate-rise mx-auto w-full max-w-2xl rounded-2xl border border-bd/80 bg-surface/90 backdrop-blur-md shadow-xl overflow-hidden flex flex-col transition-all duration-300">
    <!-- Header -->
    <div class="px-5 pt-4 pb-3 border-b border-bd/60 bg-surface/95 shrink-0">
      <div class="flex items-center justify-between gap-2 mb-1">
        <div class="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-accent">
          <span class="text-sm">✶</span> {{ $t("plan.tag") }}
        </div>
        <span class="text-[11px] font-medium text-muted bg-bg/60 border border-bd px-2 py-0.5 rounded-full">
          {{ $t("plan.items", rows.length) }}
        </span>
      </div>

      <h2 v-if="prompt" class="line-clamp-1 font-serif text-base font-semibold leading-snug text-ink" :title="prompt">
        {{ prompt }}
      </h2>
      <p class="text-xs text-muted/85 mt-0.5">
        {{ $t("plan.subtitle") }}
      </p>
    </div>

    <!-- Scrollable Items List -->
    <div class="max-h-[46vh] overflow-y-auto px-5 py-3 space-y-2.5 scrollbar-thin scrollbar-thumb-bd/60 scrollbar-track-transparent">
      <TransitionGroup name="plan-item">
        <div
          v-for="(row, i) in rows"
          :key="row.id"
          class="rounded-xl border border-bd/60 bg-bg/40 p-2.5 transition-all duration-200 hover:border-accent/40 hover:bg-bg/70 group"
        >
          <div class="flex items-center gap-2 mb-1.5">
            <span class="flex items-center justify-center h-5 w-5 rounded-md bg-accent/15 text-accent text-[11px] font-bold shrink-0">
              {{ i + 1 }}
            </span>
            <input
              v-model="row.description"
              :placeholder="$t('plan.subquestion')"
              class="flex-1 truncate bg-transparent text-xs sm:text-sm font-medium text-ink placeholder:text-muted/60 focus:text-accent focus:outline-none"
            />
            <button
              class="text-muted/50 hover:text-red-400 p-1 rounded transition opacity-60 group-hover:opacity-100 text-xs"
              :title="$t('plan.delete')"
              @click="removeRow(i)"
            >
              ✕
            </button>
          </div>
          <textarea
            v-model="row.queriesText"
            rows="1"
            :placeholder="$t('plan.queries')"
            class="block w-full resize-none rounded-lg border border-bd/60 bg-surface/50 px-2.5 py-1.5 text-[11px] font-mono text-muted/90 placeholder:text-muted/50 focus:text-ink focus:rows-2 focus:outline-none transition-all"
            @focus="onQueryFocus"
            @blur="onQueryBlur($event, row)"
          />
        </div>
      </TransitionGroup>

      <div v-if="!rows.length" class="py-6 text-center text-xs text-muted">
        {{ $t("plan.empty") }}
      </div>
    </div>

    <!-- Footer Actions -->
    <div class="px-5 py-3 bg-surface/95 border-t border-bd/70 flex flex-wrap items-center justify-between gap-2 shrink-0">
      <div class="flex items-center gap-2">
        <button
          type="button"
          class="inline-flex items-center gap-1 rounded-lg border border-bd/70 bg-bg/50 px-2.5 py-1.5 text-xs text-muted transition hover:border-accent/50 hover:text-accent"
          @click="addRow"
        >
          <span class="text-sm leading-none">+</span>
          <span>{{ $t("plan.add") }}</span>
        </button>

        <button
          type="button"
          class="text-xs text-muted transition hover:text-red-400 px-2 py-1.5"
          :disabled="busy"
          @click="emit('cancel')"
        >
          {{ $t("research.cancel") }}
        </button>
      </div>

      <button
        type="button"
        class="rounded-xl bg-accent px-4 py-2 text-xs sm:text-sm font-medium text-bg shadow-sm transition-all duration-200 hover:shadow hover:brightness-105 active:scale-95 disabled:cursor-not-allowed disabled:opacity-40 flex items-center gap-1.5"
        :disabled="busy || !rows.length"
        @click="approve"
      >
        <span v-if="busy" class="inline-block h-3 w-3 animate-spin rounded-full border-2 border-bg border-t-transparent" />
        <span>{{ busy ? $t("plan.busy") : $t("plan.run") }}</span>
      </button>
    </div>
  </div>
</template>

<style scoped>
.plan-item-enter-active,
.plan-item-leave-active {
  transition: all 0.25s ease;
}
.plan-item-enter-from {
  opacity: 0;
  transform: translateY(-8px);
}
.plan-item-leave-to {
  opacity: 0;
  transform: translateY(8px);
}
</style>
