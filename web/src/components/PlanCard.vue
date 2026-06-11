<script setup lang="ts">
import { ref, watch } from "vue";
import type { PlanItem } from "@/lib/types";

const props = defineProps<{ prompt: string; items: PlanItem[]; busy?: boolean }>();
const emit = defineEmits<{ approve: [PlanItem[]] }>();

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
  <div class="animate-rise mx-auto w-full max-w-2xl px-6 py-6">
    <div class="mb-1 flex items-center gap-2 text-xs uppercase tracking-wide text-muted">
      <span class="text-accent">✶</span> {{ $t("plan.tag") }}
    </div>
    <h1 v-if="prompt" class="mb-1.5 line-clamp-2 font-serif text-xl leading-snug text-ink">{{ prompt }}</h1>
    <p class="mb-4 text-sm text-muted">
      {{ $t("plan.subtitle") }}
    </p>

    <div class="space-y-2">
      <div
        v-for="(row, i) in rows"
        :key="row.id"
        class="rounded-card border border-bd bg-surface/50 p-3 transition-colors hover:border-accentSoft/50"
      >
        <div class="mb-1.5 flex items-center gap-2">
          <span class="text-xs text-muted">{{ i + 1 }}</span>
          <input
            v-model="row.description"
            :placeholder="$t('plan.subquestion')"
            class="flex-1 truncate bg-transparent text-sm text-ink placeholder:text-muted focus:outline-none"
          />
          <button class="text-muted hover:text-red-400" :title="$t('plan.delete')" @click="removeRow(i)">✕</button>
        </div>
        <textarea
          v-model="row.queriesText"
          rows="2"
          :placeholder="$t('plan.queries')"
          class="block w-full resize-none rounded-lg border border-bd bg-bg/40 px-3 py-2 text-xs text-muted placeholder:text-muted focus:text-ink focus:outline-none"
        />
      </div>
    </div>

    <button class="mt-3 text-sm text-muted hover:text-ink" @click="addRow">
      {{ $t("plan.add") }}
    </button>

    <div class="mt-6 flex justify-end">
      <button
        class="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition disabled:cursor-not-allowed disabled:opacity-40"
        :disabled="busy || !rows.length"
        @click="approve"
      >
        <span v-if="busy">{{ $t("plan.busy") }}</span>
        <span v-else>{{ $t("plan.run") }}</span>
      </button>
    </div>
  </div>
</template>
