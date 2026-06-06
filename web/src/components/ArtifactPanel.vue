<script setup lang="ts">
import { ref, watch } from "vue";
import { api } from "@/lib/api";
import type { GraphTrailEntry, SourcePreview } from "@/lib/types";
import MarkdownView from "./MarkdownView.vue";
import SourceCard from "./SourceCard.vue";

const props = defineProps<{ id: string; report: string; isFinal: boolean }>();

type Tab = "report" | "sources" | "trail";
const tab = ref<Tab>("report");

const sources = ref<SourcePreview[] | null>(null);
const trail = ref<GraphTrailEntry[] | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);

async function ensureSources() {
  if (sources.value || loading.value) return;
  loading.value = true;
  error.value = null;
  try {
    sources.value = await api.getSources(props.id);
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    loading.value = false;
  }
}

async function ensureTrail() {
  if (trail.value || loading.value) return;
  loading.value = true;
  error.value = null;
  try {
    trail.value = (await api.getGraph(props.id)).graph_trail;
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    loading.value = false;
  }
}

watch(tab, (t) => {
  if (t === "sources") ensureSources();
  if (t === "trail") ensureTrail();
});

const tabs: { key: Tab; label: string }[] = [
  { key: "report", label: "Отчёт" },
  { key: "sources", label: "Источники" },
  { key: "trail", label: "Трасса" },
];

const STEP_LABELS: Record<string, string> = {
  collect_context: "Сбор источников",
  replan: "Дополнительный поиск",
  analyze: "Синтез отчёта",
  verify: "Проверка фактов",
  tie_break: "Разрешение противоречий",
  complete: "Готово",
  stale_recovered: "Восстановление",
};
</script>

<template>
  <div class="flex h-full flex-col">
    <div class="flex items-center gap-1 border-b border-bd px-4">
      <button
        v-for="t in tabs"
        :key="t.key"
        class="border-b-2 px-3 py-3 text-sm transition-colors"
        :class="tab === t.key ? 'border-accent text-ink' : 'border-transparent text-muted hover:text-ink'"
        @click="tab = t.key"
      >
        {{ t.label }}
      </button>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto px-6 py-6">
      <template v-if="tab === 'report'">
        <MarkdownView v-if="report" :source="report" :class="{ 'opacity-80': !isFinal }" />
        <p v-else class="text-muted">Отчёт формируется — он появится здесь.</p>
      </template>

      <template v-else-if="tab === 'sources'">
        <p v-if="loading" class="text-muted">Загрузка…</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <p v-else-if="sources && !sources.length" class="text-muted">Источников пока нет.</p>
        <div v-else class="space-y-2">
          <SourceCard v-for="(s, i) in sources" :key="s.url" :source="s" :index="i + 1" />
        </div>
      </template>

      <template v-else>
        <p v-if="loading" class="text-muted">Загрузка…</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <p v-else-if="trail && !trail.length" class="text-muted">Трасса пуста.</p>
        <ol v-else class="space-y-3">
          <li v-for="(e, i) in trail" :key="i" class="flex gap-3 text-sm">
            <span class="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-accentSoft" />
            <div class="min-w-0">
              <div class="text-ink">{{ STEP_LABELS[e.step || ""] || e.step }}</div>
              <div v-if="e.detail" class="text-muted">{{ e.detail }}</div>
            </div>
          </li>
        </ol>
      </template>
    </div>
  </div>
</template>
