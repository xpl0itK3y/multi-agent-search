<script setup lang="ts">
import { ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import type { Conflict, GraphTrailEntry, SourcePreview } from "@/lib/types";
import MarkdownView from "./MarkdownView.vue";
import SourceCard from "./SourceCard.vue";

const props = defineProps<{ id: string; report: string; isFinal: boolean }>();

type Tab = "report" | "sources" | "conflicts" | "trail";
const tab = ref<Tab>("report");

const sources = ref<SourcePreview[] | null>(null);
const conflicts = ref<Conflict[] | null>(null);
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

async function ensureConflicts() {
  if (conflicts.value || loading.value) return;
  loading.value = true;
  error.value = null;
  try {
    conflicts.value = await api.getConflicts(props.id);
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    loading.value = false;
  }
}

watch(tab, (t) => {
  if (t === "sources") ensureSources();
  if (t === "conflicts") ensureConflicts();
  if (t === "trail") ensureTrail();
});

const tabKeys: Tab[] = ["report", "sources", "conflicts", "trail"];

const { t, te } = useI18n();
function stepLabel(step: string): string {
  return te(`trace.${step}`) ? t(`trace.${step}`) : step;
}
</script>

<template>
  <div class="flex h-full flex-col">
    <div class="flex items-center gap-1 border-b border-bd px-4">
      <button
        v-for="tb in tabKeys"
        :key="tb"
        class="border-b-2 px-3 py-3 text-sm transition-colors"
        :class="tab === tb ? 'border-accent text-ink' : 'border-transparent text-muted hover:text-ink'"
        @click="tab = tb"
      >
        {{ $t("artifact." + tb) }}
      </button>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto px-6 py-6">
      <template v-if="tab === 'report'">
        <MarkdownView v-if="report" :source="report" :class="{ 'opacity-80': !isFinal }" />
        <p v-else class="text-muted">{{ $t("artifact.reportForming") }}</p>
      </template>

      <template v-else-if="tab === 'sources'">
        <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <p v-else-if="sources && !sources.length" class="text-muted">{{ $t("artifact.sourcesEmpty") }}</p>
        <div v-else class="space-y-2">
          <SourceCard v-for="(s, i) in sources" :key="s.url" :source="s" :index="i + 1" />
        </div>
      </template>

      <template v-else-if="tab === 'conflicts'">
        <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <p v-else-if="conflicts && !conflicts.length" class="text-muted">
          {{ $t("artifact.conflictsEmpty") }}
        </p>
        <div v-else class="space-y-4">
          <div
            v-for="(c, i) in conflicts"
            :key="i"
            class="rounded-lg border border-bd bg-surface/50 p-4"
          >
            <div class="mb-1 text-sm font-medium text-ink">{{ c.topic || $t("artifact.disputedPoint") }}</div>
            <div v-if="c.reason" class="mb-3 text-xs text-muted">{{ c.reason }}</div>
            <div class="space-y-2">
              <div v-for="(s, j) in c.sentences" :key="j" class="flex gap-2 text-sm">
                <span class="shrink-0 text-xs font-semibold text-accent">
                  {{ c.source_ids[j] ? "[" + c.source_ids[j] + "]" : "" }}
                </span>
                <span class="text-muted">«{{ s }}»</span>
              </div>
            </div>
          </div>
        </div>
      </template>

      <template v-else>
        <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <p v-else-if="trail && !trail.length" class="text-muted">{{ $t("artifact.trailEmpty") }}</p>
        <ol v-else class="space-y-3">
          <li v-for="(e, i) in trail" :key="i" class="flex gap-3 text-sm">
            <span class="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-accentSoft" />
            <div class="min-w-0">
              <div class="text-ink">{{ stepLabel(e.step || "") }}</div>
              <div v-if="e.detail" class="text-muted">{{ e.detail }}</div>
            </div>
          </li>
        </ol>
      </template>
    </div>
  </div>
</template>
