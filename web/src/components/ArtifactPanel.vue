<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import type { CitationAudit, ComparisonRow, ComparisonTable, Conflict, GraphTrailEntry, RedTeamReport, ResearchDiff, SourcePreview, VerificationReport } from "@/lib/types";
import MarkdownView from "./MarkdownView.vue";
import ResearchDashboard from "./ResearchDashboard.vue";
import SourceCard from "./SourceCard.vue";

const props = defineProps<{ id: string; report: string; isFinal: boolean }>();
const emit = defineEmits<{ refreshed: [string] }>();

type Tab = "report" | "dashboard" | "comparison" | "sources" | "confidence" | "conflicts" | "redteam" | "trail";
const tab = ref<Tab>("report");

const sources = ref<SourcePreview[] | null>(null);
const conflicts = ref<Conflict[] | null>(null);
const verification = ref<VerificationReport | null>(null);
const redTeam = ref<RedTeamReport | null>(null);
const citations = ref<CitationAudit | null>(null);
const showWeak = ref(false);
const diff = ref<ResearchDiff | null>(null);
const showDiff = ref(false);
const refreshing = ref(false);
const comparison = ref<ComparisonTable | null>(null);
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

async function ensureRedTeam() {
  if (redTeam.value || loading.value) return;
  loading.value = true;
  error.value = null;
  try {
    redTeam.value = await api.getRedTeam(props.id);
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    loading.value = false;
  }
}

async function ensureCitations() {
  if (citations.value) return;
  try {
    citations.value = await api.getCitations(props.id);
  } catch {
    /* grounding is optional — the report still renders without it */
  }
}

watch(tab, (t) => {
  if (t === "sources") ensureSources();
  if (t === "confidence") ensureVerification();
  if (t === "conflicts") ensureConflicts();
  if (t === "redteam") ensureRedTeam();
  if (t === "trail") ensureTrail();
});

async function ensureDiff() {
  if (diff.value) return;
  try {
    diff.value = await api.getDiff(props.id);
  } catch {
    /* no diff (first run) — banner stays hidden */
  }
}

async function ensureComparison() {
  if (comparison.value) return;
  try {
    comparison.value = await api.getComparison(props.id);
  } catch {
    /* not a comparison — tab stays hidden */
  }
}

// Citation grounding, living-research diff and the comparison table load once final.
watch(
  () => props.isFinal,
  (final) => {
    if (final) {
      ensureCitations();
      ensureDiff();
      ensureComparison();
    }
  },
  { immediate: true },
);

function cellFor(row: ComparisonRow, option: string) {
  return row.cells.find((c) => c.option === option) || null;
}

const integrityClass = computed(() => {
  const r = citations.value?.integrity ?? 0;
  if (r >= 0.8) return "text-emerald-500";
  if (r >= 0.5) return "text-amber-500";
  return "text-red-400";
});

const diffHasChanges = computed(() => {
  const d = diff.value;
  return !!d && (d.new_claims.length > 0 || d.dropped_claims.length > 0 || d.shifted_claims.length > 0 || d.new_sources > 0);
});

async function onRefresh() {
  refreshing.value = true;
  error.value = null;
  try {
    const res = await api.refreshResearch(props.id);
    emit("refreshed", res.research_id);
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    refreshing.value = false;
  }
}

const tabKeys = computed<Tab[]>(() => {
  const base: Tab[] = ["report", "dashboard", "sources", "confidence", "conflicts", "redteam", "trail"];
  // The comparison tab only appears when the query actually produced a table.
  if (comparison.value && comparison.value.options.length >= 2) base.splice(2, 0, "comparison");
  return base;
});

function shortUrl(u: string): string {
  try {
    return new URL(u).hostname.replace(/^www\./, "");
  } catch {
    return u;
  }
}

const verdictClass: Record<string, string> = {
  refuted: "text-red-400 border-red-400/40",
  contested: "text-amber-500 border-amber-500/40",
  qualified: "text-sky-500 border-sky-500/40",
  holds: "text-emerald-500 border-emerald-500/40",
};

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
const siteMenuOpen = ref(false);
const customAccent = ref("#c15f3c");
const customBase = ref<"light" | "dark">("light");
const siteThemes = ["auto", "light", "dark", "editorial", "slate"] as const;
const appOpen = ref(false);
const appPrompt = ref("");

function filenameFrom(res: Response, fallback: string): string {
  const cd = res.headers.get("Content-Disposition") || "";
  const m = cd.match(/filename\*=UTF-8''([^;]+)/) || cd.match(/filename="?([^";]+)"?/);
  return m ? decodeURIComponent(m[1]) : fallback;
}
function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function exportReport(fmt: "pdf" | "docx" | "html" | "md" | "json", opts?: { theme?: string; accent?: string; base?: string }) {
  exporting.value = fmt;
  try {
    const params = new URLSearchParams({ format: fmt });
    if (opts?.theme) params.set("theme", opts.theme);
    if (opts?.accent) params.set("accent", opts.accent);
    if (opts?.base) params.set("base", opts.base);
    const res = await fetch(`${BASE}/v1/research/${props.id}/export?${params.toString()}`, {
      credentials: "include",
    });
    if (!res.ok) return;
    saveBlob(await res.blob(), filenameFrom(res, `research.${fmt}`));
  } finally {
    exporting.value = null;
    siteMenuOpen.value = false;
  }
}

// AI-generated custom export: send the user's brief, download the generated HTML.
async function exportApp() {
  if (!appPrompt.value.trim() || exporting.value) return;
  exporting.value = "app";
  error.value = null;
  try {
    const token = localStorage.getItem("access_token");
    const res = await fetch(`${BASE}/v1/research/${props.id}/export/app`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify({ prompt: appPrompt.value }),
    });
    if (!res.ok) {
      error.value = res.status === 502 ? t("site.appFailed") : `HTTP ${res.status}`;
      return;
    }
    saveBlob(await res.blob(), filenameFrom(res, "research-app.html"));
    siteMenuOpen.value = false;
    appOpen.value = false;
  } catch (e) {
    error.value = (e as Error).message;
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

      <div v-if="report && isFinal" class="ml-auto flex items-center gap-1">
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="refreshing"
          :title="$t('diff.refreshHint')"
          @click="onRefresh"
        >
          {{ refreshing ? $t("diff.refreshing") : "↻ " + $t("diff.refresh") }}
        </button>
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
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="!!exporting"
          :title="$t('artifact.export')"
          @click="exportReport('md')"
        >
          MD
        </button>
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="!!exporting"
          :title="$t('artifact.exportJson')"
          @click="exportReport('json')"
        >
          JSON
        </button>
        <div class="relative">
          <button
            class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
            :disabled="!!exporting"
            :title="$t('artifact.exportHtml')"
            @click="siteMenuOpen = !siteMenuOpen"
          >
            {{ $t("artifact.site") }} ▾
          </button>
          <div v-if="siteMenuOpen" class="absolute right-0 z-20 mt-1 w-52 rounded-lg border border-bd bg-surface p-2 shadow-lg">
            <div class="mb-1 px-1 text-[11px] uppercase tracking-wide text-muted">{{ $t("site.title") }}</div>
            <button
              v-for="th in siteThemes"
              :key="th"
              class="block w-full rounded px-2 py-1 text-left text-sm text-ink transition-colors hover:bg-surfaceHover"
              @click="exportReport('html', { theme: th })"
            >
              {{ $t("site." + th) }}
            </button>
            <div class="mt-2 border-t border-bd pt-2">
              <div class="mb-1 px-1 text-[11px] text-muted">{{ $t("site.custom") }}</div>
              <div class="flex items-center gap-1.5 px-1">
                <input v-model="customAccent" type="color" class="h-7 w-8 shrink-0 cursor-pointer rounded border border-bd bg-transparent" />
                <select v-model="customBase" class="min-w-0 flex-1 rounded border border-bd bg-bg px-1 py-1 text-xs text-ink">
                  <option value="light">{{ $t("site.light") }}</option>
                  <option value="dark">{{ $t("site.dark") }}</option>
                </select>
                <button
                  class="shrink-0 rounded bg-accent px-2 py-1 text-xs font-medium text-bg"
                  @click="exportReport('html', { theme: 'custom', accent: customAccent, base: customBase })"
                >
                  {{ $t("site.download") }}
                </button>
              </div>
            </div>
            <div class="mt-2 border-t border-bd pt-2">
              <button class="flex w-full items-center justify-between px-1 text-[11px] text-muted hover:text-ink" @click="appOpen = !appOpen">
                <span>✨ {{ $t("site.app") }}</span>
                <span>{{ appOpen ? "▾" : "▸" }}</span>
              </button>
              <div v-if="appOpen" class="mt-1 px-1">
                <textarea
                  v-model="appPrompt"
                  :placeholder="$t('site.appPlaceholder')"
                  rows="3"
                  class="w-full resize-none rounded border border-bd bg-bg px-2 py-1 text-xs text-ink placeholder:text-muted focus:outline-none"
                />
                <button
                  class="mt-1 w-full rounded bg-accent px-2 py-1 text-xs font-medium text-bg disabled:opacity-50"
                  :disabled="exporting === 'app' || !appPrompt.trim()"
                  @click="exportApp"
                >
                  {{ exporting === "app" ? $t("site.appBusy") : $t("site.appGo") }}
                </button>
                <p class="mt-1 text-[10px] leading-snug text-muted">{{ $t("site.appHint") }}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto px-6 py-6">
      <template v-if="tab === 'report'">
        <div v-if="citations && citations.total" class="mb-4 rounded-lg border border-bd bg-surface/40 px-3 py-2 text-xs">
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span class="font-medium text-ink">{{ $t("citations.integrity") }}</span>
            <span class="font-semibold" :class="integrityClass">{{ Math.round(citations.integrity * 100) }}%</span>
            <span class="text-muted">{{ citations.supported }}/{{ citations.total }} {{ $t("citations.matched") }}</span>
            <button
              v-if="citations.unsupported_claims.length"
              class="ml-auto text-red-400 hover:underline"
              @click="showWeak = !showWeak"
            >
              ⚠ {{ citations.unsupported_claims.length }} {{ $t("citations.weak") }}
            </button>
          </div>
          <ul v-if="showWeak && citations.unsupported_claims.length" class="mt-2 space-y-1 border-t border-bd pt-2">
            <li v-for="(c, i) in citations.unsupported_claims" :key="i" class="line-clamp-2 text-muted">○ {{ c }}</li>
          </ul>
        </div>
        <div v-if="diffHasChanges && diff" class="mb-4 rounded-lg border border-accent/40 bg-accent/5 px-3 py-2 text-xs">
          <button class="flex w-full items-center gap-2 text-left" @click="showDiff = !showDiff">
            <span class="font-medium text-ink">↻ {{ $t("diff.title") }}</span>
            <span class="text-muted">
              +{{ diff.new_claims.length }} {{ $t("diff.new") }} · {{ diff.shifted_claims.length }} {{ $t("diff.shifted") }} · {{ diff.new_sources }} {{ $t("diff.sources") }}
            </span>
            <span class="ml-auto text-muted">{{ showDiff ? "▾" : "▸" }}</span>
          </button>
          <div v-if="showDiff" class="mt-2 space-y-3 border-t border-bd pt-2">
            <div v-if="diff.shifted_claims.length">
              <div class="mb-1 font-medium text-muted">{{ $t("diff.shiftedH") }}</div>
              <div v-for="(s, i) in diff.shifted_claims" :key="i" class="line-clamp-2 text-ink">
                <span :class="levelClass[s.old_level]">{{ $t("confidence." + s.old_level) }}</span>
                →
                <span :class="levelClass[s.new_level]">{{ $t("confidence." + s.new_level) }}</span>
                {{ s.statement }}
              </div>
            </div>
            <div v-if="diff.new_claims.length">
              <div class="mb-1 font-medium text-muted">{{ $t("diff.newH") }}</div>
              <div v-for="(c, i) in diff.new_claims" :key="i" class="line-clamp-2 text-emerald-600 dark:text-emerald-400">+ {{ c }}</div>
            </div>
            <div v-if="diff.dropped_claims.length">
              <div class="mb-1 font-medium text-muted">{{ $t("diff.droppedH") }}</div>
              <div v-for="(c, i) in diff.dropped_claims" :key="i" class="line-clamp-2 text-muted">− {{ c }}</div>
            </div>
          </div>
        </div>
        <MarkdownView v-if="report" :source="report" :grounding="citations?.grounding" :class="{ 'opacity-80': !isFinal }" />
        <p v-else class="text-muted">{{ $t("artifact.reportForming") }}</p>
      </template>

      <template v-else-if="tab === 'dashboard'">
        <ResearchDashboard :id="id" @navigate="(t) => (tab = t as Tab)" />
      </template>

      <template v-else-if="tab === 'comparison'">
        <div v-if="comparison && comparison.options.length >= 2" class="space-y-4">
          <div class="overflow-x-auto">
            <table class="w-full border-collapse text-sm">
              <thead>
                <tr>
                  <th class="border-b border-bd px-2 py-2 text-left"></th>
                  <th v-for="o in comparison.options" :key="o" class="border-b border-bd px-2 py-2 text-left font-medium text-ink">{{ o }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, i) in comparison.rows" :key="i" class="align-top">
                  <td class="border-b border-bd px-2 py-2 font-medium text-muted">{{ row.criterion }}</td>
                  <td v-for="o in comparison.options" :key="o" class="border-b border-bd px-2 py-2 text-ink">
                    <template v-if="cellFor(row, o)">
                      {{ cellFor(row, o)!.value }}
                      <span
                        v-if="cellFor(row, o)!.source_ids.length"
                        class="ml-1 text-[10px] font-semibold text-accent"
                      >{{ cellFor(row, o)!.source_ids.map((s) => "[" + s + "]").join("") }}</span>
                    </template>
                    <span v-else class="text-muted">—</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p v-if="comparison.recommendation" class="rounded-lg border border-bd bg-surface/40 p-3 text-sm text-ink">
            <span class="font-medium">{{ $t("comparison.recommendation") }}:</span> {{ comparison.recommendation }}
          </p>
        </div>
        <p v-else class="text-muted">{{ $t("comparison.empty") }}</p>
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

      <template v-else-if="tab === 'redteam'">
        <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>
        <p v-else-if="error" class="text-red-400">{{ error }}</p>
        <template v-else-if="redTeam && redTeam.findings.length">
          <div class="mb-4 flex gap-4 text-xs text-muted">
            <span><span class="font-semibold text-amber-500">{{ redTeam.challenged }}</span> {{ $t("redteam.challenged") }}</span>
            <span><span class="font-semibold text-emerald-500">{{ redTeam.held }}</span> {{ $t("redteam.held") }}</span>
          </div>
          <div class="space-y-3">
            <div
              v-for="(f, i) in redTeam.findings"
              :key="i"
              class="rounded-lg border border-bd bg-surface/50 p-3"
            >
              <span
                class="rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase"
                :class="verdictClass[f.verdict] || 'text-muted border-bd'"
              >
                {{ $t("redteam." + f.verdict) }}
              </span>
              <div class="mt-2 text-sm text-ink">{{ f.claim }}</div>
              <div v-if="f.challenge" class="mt-1 text-sm text-muted">{{ f.challenge }}</div>
              <div v-if="f.source_urls.length" class="mt-2 flex flex-wrap gap-x-3 gap-y-1">
                <a
                  v-for="(u, j) in f.source_urls"
                  :key="j"
                  :href="u"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-xs text-accent hover:underline"
                >{{ shortUrl(u) }}</a>
              </div>
            </div>
          </div>
        </template>
        <p v-else class="text-muted">{{ $t("redteam.empty") }}</p>
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
