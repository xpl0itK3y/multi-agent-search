<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import type { PlanItem } from "@/lib/types";

const props = defineProps<{ prompt: string; items: PlanItem[]; busy?: boolean }>();
const emit = defineEmits<{ approve: [PlanItem[]]; cancel: [] }>();

interface EditableRow {
  id: string;
  description: string;
  queriesText: string;
}

const rows = ref<EditableRow[]>([]);
const listEl = ref<HTMLElement | null>(null);

// A removed row can be put back (apple-design §16 Agency: easy undo for slips rather
// than a confirmation). Its place shows an undo line for 6 s, or until the next edit
// or removal.
const UNDO_MS = 6000;
const removed = ref<{ row: EditableRow; index: number } | null>(null);
let undoTimer: ReturnType<typeof setTimeout> | undefined;

function dismissUndo() {
  if (undoTimer !== undefined) clearTimeout(undoTimer);
  undoTimer = undefined;
  removed.value = null;
}

function removeRow(i: number) {
  dismissUndo();
  const [row] = rows.value.splice(i, 1);
  if (!row) return;
  removed.value = { row, index: i };
  undoTimer = setTimeout(dismissUndo, UNDO_MS);
}

function undoRemove() {
  const r = removed.value;
  if (!r) return;
  rows.value.splice(Math.min(r.index, rows.value.length), 0, r.row);
  dismissUndo();
}

onBeforeUnmount(dismissUndo);

watch(
  () => props.items,
  (items) => {
    dismissUndo();
    rows.value = items.map((it) => ({
      id: it.id,
      description: it.description,
      queriesText: (it.queries || []).join("\n"),
    }));
  },
  { immediate: true },
);

// The rows as shown: the undo line sits where the removed row was.
type ListEntry = { kind: "row"; key: string; row: EditableRow; index: number } | { kind: "undo"; key: string };
const list = computed<ListEntry[]>(() => {
  const out: ListEntry[] = rows.value.map((row, index) => ({ kind: "row", key: row.id, row, index }));
  const r = removed.value;
  if (r) out.splice(Math.min(r.index, out.length), 0, { kind: "undo", key: `undo-${r.row.id}` });
  return out;
});

function addRow() {
  dismissUndo();
  const id = `plan-${crypto.randomUUID()}`;
  rows.value.push({ id, description: "", queriesText: "" });
  // Straight into typing the new sub-question.
  nextTick(() => listEl.value?.querySelector<HTMLInputElement>(`[data-row="${id}"] input`)?.focus());
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
  dismissUndo();
  const items: PlanItem[] = rows.value
    .map((r) => {
      const description = r.description.trim();
      const queries = r.queriesText
        .split("\n")
        .map((q) => q.trim())
        .filter(Boolean);
      // A sub-question typed without queries is searched as written, not dropped.
      return { id: r.id, description, queries: queries.length ? queries : description ? [description] : [] };
    })
    .filter((it) => it.queries.length > 0); // only rows left completely empty go
  emit("approve", items);
}
</script>

<template>
  <div class="animate-rise mx-auto w-full max-w-2xl rounded-2xl border border-bd/80 bg-surface shadow-xl overflow-hidden flex flex-col">
    <!-- Header -->
    <div class="px-5 pt-4 pb-3 border-b border-bd/60 bg-surface/95 shrink-0">
      <div class="flex items-center justify-between gap-2 mb-1">
        <div class="flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-wider text-accent">
          <span class="text-sm">✶</span> {{ $t("plan.tag") }}
        </div>
        <span class="text-2xs font-medium text-muted tabular-nums bg-bg/60 border border-bd px-2 py-0.5 rounded-full">
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
    <div ref="listEl" class="relative max-h-[46vh] overflow-y-auto px-5 py-3 space-y-2.5">
      <TransitionGroup name="plan-item">
        <div
          v-for="entry in list"
          :key="entry.key"
          :data-row="entry.kind === 'row' ? entry.row.id : undefined"
          :class="
            entry.kind === 'row'
              ? 'field-host group rounded-xl border border-bd/60 bg-bg/40 p-2.5 transition-colors duration-200 hover:border-accent/40 hover:bg-bg/70'
              : 'flex items-center justify-between gap-2 rounded-xl border border-dashed border-bd px-3 py-2 text-xs text-muted'
          "
        >
          <template v-if="entry.kind === 'row'">
            <div class="flex items-center gap-2 mb-1.5">
              <span class="flex items-center justify-center h-5 w-5 rounded-md bg-accent/15 text-accent text-2xs font-bold tabular-nums shrink-0">
                {{ entry.index + 1 }}
              </span>
              <input
                v-model="entry.row.description"
                :placeholder="$t('plan.subquestion')"
                class="field-bare flex-1 truncate bg-transparent text-xs sm:text-sm font-medium text-ink placeholder:text-muted/60 focus:text-accent"
                @input="dismissUndo"
              />
              <!-- Full muted at rest (4.5:1 or more on the row in every theme): a touch
                   screen never hovers, so a control dimmed until hover stays unreadable there. -->
              <button
                type="button"
                class="hit text-muted hover:text-danger p-1 rounded transition-colors text-xs"
                :title="$t('plan.delete')"
                :aria-label="$t('plan.delete')"
                @click="removeRow(entry.index)"
              >
                ✕
              </button>
            </div>
            <textarea
              v-model="entry.row.queriesText"
              rows="1"
              :placeholder="$t('plan.queries')"
              class="block w-full resize-none rounded-lg border border-bd/60 bg-surface/50 px-2.5 py-1.5 text-2xs font-mono text-muted/90 placeholder:text-muted/50 focus:text-ink transition-colors"
              @focus="onQueryFocus"
              @blur="onQueryBlur($event, entry.row)"
              @input="dismissUndo"
            />
          </template>
          <template v-else>
            <span>{{ $t("plan.removed") }}</span>
            <button type="button" class="press text-accent hover:underline" @click="undoRemove">
              {{ $t("plan.undo") }}
            </button>
          </template>
        </div>
      </TransitionGroup>

      <div v-if="!rows.length && !removed" class="py-6 text-center text-xs text-muted">
        {{ $t("plan.empty") }}
      </div>

      <!-- New rows appear where this button is: at the end of the list. -->
      <button
        type="button"
        class="press w-full rounded-xl border border-dashed border-bd py-2 text-xs text-muted hover:text-accent hover:border-accent/50"
        @click="addRow"
      >
        {{ $t("plan.add") }}
      </button>
    </div>

    <!-- Footer Actions: the two ways out of the plan, side by side -->
    <div class="px-5 py-3 bg-surface/95 border-t border-bd/70 flex items-center justify-end gap-2 shrink-0">
      <button
        type="button"
        class="press rounded-xl border border-bd px-3 py-2 text-xs text-muted hover:text-ink"
        :disabled="busy"
        @click="emit('cancel')"
      >
        {{ $t("research.cancel") }}
      </button>

      <button
        type="button"
        class="press rounded-xl bg-accent px-4 py-2 text-xs sm:text-sm font-medium text-onAccent shadow-sm hover:shadow hover:brightness-105 disabled:cursor-not-allowed disabled:opacity-40 flex items-center gap-1.5"
        :disabled="busy || !rows.length"
        @click="approve"
      >
        <span v-if="busy" class="inline-block h-3 w-3 animate-spin rounded-full border-2 border-onAccent border-t-transparent" />
        <span>{{ busy ? $t("plan.busy") : $t("plan.run") }}</span>
      </button>
    </div>
  </div>
</template>

<style scoped>
/* Rows rise in from below, where they are added (§7). A removed row fades in place,
   out of the flow, while the rows below glide up into its space. */
.plan-item-enter-active {
  transition: opacity 200ms ease-out, transform 240ms var(--ease-emph);
}
.plan-item-enter-from {
  opacity: 0;
  transform: translateY(6px);
}
.plan-item-leave-active {
  transition: opacity 150ms ease-in, transform 150ms ease-in;
  position: absolute;
  left: 1.25rem;
  right: 1.25rem;
}
.plan-item-leave-to {
  opacity: 0;
  transform: scale(0.98);
}
.plan-item-move {
  transition: transform 240ms var(--ease-emph);
}
</style>
