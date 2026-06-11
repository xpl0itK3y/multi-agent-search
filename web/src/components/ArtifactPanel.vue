<script setup lang="ts">
import { ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import type { Conflict, GraphTrailEntry, SourcePreview, VerificationReport } from "@/lib/types";
import MarkdownView from "./MarkdownView.vue";
import SourceCard from "./SourceCard.vue";

const props = defineProps<{ id: string; report: string; isFinal: boolean }>();

type Tab = "report" | "sources" | "confidence" | "conflicts" | "trail";
const tab = ref<Tab>("report");

const sources = ref<SourcePreview[] | null>(null);
const conflicts = ref<Conflict[] | null>(null);
const verification = ref<VerificationReport | null>(null);
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

async function ensureVerification() {
  if (verification.value || loading.value) return;
  loading.value = true;
  error.value = null;
  try {
    verification.value = await api.getVerification(props.id);
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    loading.value = false;
  }
}

watch(tab, (t) => {
  if (t === "sources") ensureSources();
  if (t === "confidence") ensureVerification();
  if (t === "conflicts") ensureConflicts();
  if (t === "trail") ensureTrail();
});

const tabKeys: Tab[] = ["report", "sources", "confidence", "conflicts", "trail"];

const levelClass: Record<string, string> = {
  strong: "text-emerald-500 border-emerald-500/40",
  medium: "text-amber-500 border-amber-500/40",
  weak: "text-red-400 border-red-400/40",
};

const { t, te } = useI18n();
function stepLabel(step: string): string {
  return te(`trace.${step}`) ? t(`trace.${step}`) : step;
}

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";
const exporting = ref<string | null>(null);
async function exportReport(fmt: "pdf" | "docx") {
  exporting.value = fmt;
  try {
    const res = await fetch(`${BASE}/v1/research/${props.id}/export?format=${fmt}`, {
      credentials: "include",
    });
    if (!res.ok) return;
    const blob = await res.blob();
    const cd = res.headers.get("Content-Disposition") || "";
    const m = cd.match(/filename\*=UTF-8''([^;]+)/) || cd.match(/filename="?([^";]+)"?/);
    const name = m ? decodeURIComponent(m[1]) : `research.${fmt}`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } finally {
    exporting.value = null;
  }
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

      <div v-if="report" class="ml-auto flex items-center gap-1">
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="!!exporting"
          :title="$t('artifact.export')"
          @click="exportReport('pdf')"
        >
          PDF
        </button>
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="!!exporting"
          :title="$t('artifact.export')"
          @click="exportReport('docx')"
        >
          DOCX
        </button>
      </div>
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

      <template v-else-if="tab === 'confidence'">
        <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <div v-else-if="verification" class="space-y-6">
          <div>
            <div class="mb-2 flex items-center justify-between text-sm">
              <span class="font-medium text-ink">{{ $t("artifact.planCoverage") }}</span>
              <span class="text-muted">{{ Math.round(verification.coverage_ratio * 100) }}%</span>
            </div>
            <div class="h-1.5 w-full overflow-hidden rounded-full bg-surface">
              <div class="h-full rounded-full bg-accent" :style="{ width: verification.coverage_ratio * 100 + '%' }" />
            </div>
            <ul v-if="verification.uncovered_questions.length" class="mt-3 space-y-1">
              <li class="text-xs font-medium text-muted">{{ $t("artifact.uncovered") }}</li>
              <li v-for="(q, i) in verification.uncovered_questions" :key="i" class="flex gap-2 text-sm text-muted">
                <span class="mt-0.5 shrink-0 text-red-400">○</span><span class="line-clamp-2">{{ q }}</span>
              </li>
            </ul>
          </div>

          <div v-if="verification.findings.length">
            <div class="mb-2 text-sm font-medium text-ink">{{ $t("artifact.keyFindings") }}</div>
            <div class="space-y-2">
              <div
                v-for="(f, i) in verification.findings"
                :key="i"
                class="rounded-lg border border-bd bg-surface/50 p-3"
              >
                <div class="mb-1 flex items-center gap-2">
                  <span
                    class="rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase"
                    :class="levelClass[f.support_level] || 'text-muted border-bd'"
                  >
                    {{ $t("confidence." + f.support_level) }}
                  </span>
                  <span class="text-xs text-muted">{{ f.source_ids.map((s) => "[" + s + "]").join("") }}</span>
                </div>
                <div class="text-sm text-ink">{{ f.statement }}</div>
              </div>
            </div>
          </div>

          <p v-if="!verification.findings.length && !verification.uncovered_questions.length" class="text-muted">
            {{ $t("artifact.confidenceEmpty") }}
          </p>
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
