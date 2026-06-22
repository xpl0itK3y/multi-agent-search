<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import type { CitationAudit, ComparisonRow, ComparisonTable, ConfidenceReport, Conflict, CrossLanguageReport, GraphTrailEntry, NumericCheck, RedTeamReport, ResearchDiff, SourceIndependence, SourceReputation, SourceIntegrity, StanceBalance, SourcePreview, VerificationReport } from "@/lib/types";
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
const independence = ref<SourceIndependence | null>(null);
const reputation = ref<SourceReputation | null>(null);
const integrity = ref<SourceIntegrity | null>(null);
const crossLang = ref<CrossLanguageReport | null>(null);
const stance = ref<StanceBalance | null>(null);
const confidence = ref<ConfidenceReport | null>(null);
const numbers = ref<NumericCheck | null>(null);
const showNumbers = ref(false);
const showWeak = ref(false);

// Inline verification: a persisted toggle plus the signals MarkdownView decorates with.
const verifyInline = ref((typeof localStorage !== "undefined" ? localStorage.getItem("verify.inline") : null) !== "0");
watch(verifyInline, (v) => {
  if (typeof localStorage !== "undefined") localStorage.setItem("verify.inline", v ? "1" : "0");
});
const weakClaims = computed<string[]>(() => {
  const a = citations.value?.unsupported_claims || [];
  const b = (numbers.value?.unsupported || []).map((u) => u.sentence).filter(Boolean);
  return [...a, ...b];
});
const contradictionSentences = computed<string[]>(() =>
  (numbers.value?.contradictions || []).flatMap((c) => c.sentences || []),
);
const diff = ref<ResearchDiff | null>(null);
const showDiff = ref(false);
const refreshing = ref(false);
const watch_ = ref<import("@/lib/types").ResearchWatch | null>(null);
const watchMenuOpen = ref(false);
const WATCH_INTERVALS = [
  { key: "hourly", seconds: 3600 },
  { key: "daily", seconds: 86400 },
  { key: "weekly", seconds: 604800 },
];
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

async function ensureIndependence() {
  if (independence.value) return;
  try {
    independence.value = await api.getSourceIndependence(props.id);
  } catch {
    /* independence analysis is optional — sources still render without it */
  }
}

async function ensureReputation() {
  if (reputation.value) return;
  try {
    reputation.value = await api.getSourceReputation(props.id);
  } catch {
    /* reputation flags are optional */
  }
}

async function ensureStance() {
  if (stance.value) return;
  try {
    stance.value = await api.getStance(props.id);
  } catch {
    /* stance balance is optional — only debate questions have one */
  }
}

async function ensureIntegrity() {
  if (integrity.value) return;
  try {
    integrity.value = await api.getSourceIntegrity(props.id);
  } catch {
    /* retraction check is optional — only academic sources have DOIs */
  }
}

async function ensureCrossLang() {
  if (crossLang.value) return;
  try {
    crossLang.value = await api.getCrossLanguage(props.id);
  } catch {
    /* cross-language is optional */
  }
}

async function ensureConfidence() {
  if (confidence.value) return;
  try {
    confidence.value = await api.getConfidence(props.id);
  } catch {
    /* honesty meter is optional — the report still renders without it */
  }
}

async function ensureNumbers() {
  if (numbers.value) return;
  try {
    numbers.value = await api.getNumericCheck(props.id);
  } catch {
    /* numeric check is optional — the report still renders without it */
  }
}

watch(tab, (t) => {
  if (t === "sources") {
    ensureSources();
    ensureIndependence();
    ensureReputation();
    ensureStance();
    ensureIntegrity();
    ensureCrossLang();
  }
  if (t === "confidence") {
    ensureVerification();
    ensureConfidence();
  }
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
      ensureIndependence();
      ensureReputation();
      ensureStance();
      ensureIntegrity();
      ensureCrossLang();
      ensureConfidence();
      ensureNumbers();
      ensureWatch();
      ensureShare();
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

const numericClass = computed(() => {
  const r = numbers.value?.integrity ?? 1;
  if (r >= 0.9) return "text-emerald-500";
  if (r >= 0.6) return "text-amber-500";
  return "text-red-400";
});
const numericHasIssues = computed(() => {
  const n = numbers.value;
  return !!n && (n.total > 0 || n.contradictions.length > 0);
});

const independenceScore = computed(() => independence.value?.independence_score ?? null);
const independenceClass = computed(() => {
  const r = independenceScore.value ?? 1;
  if (r >= 0.8) return "text-emerald-500";
  if (r >= 0.5) return "text-amber-500";
  return "text-red-400";
});
const independenceBar = computed(() => {
  const r = independenceScore.value ?? 1;
  if (r >= 0.8) return "bg-emerald-500";
  if (r >= 0.5) return "bg-amber-500";
  return "bg-red-400";
});
const clusterKindClass: Record<string, string> = {
  syndicated: "border-red-400/40 text-red-400",
  "single-domain": "border-amber-500/40 text-amber-500",
};
const reputationClass: Record<string, string> = {
  satire: "border-amber-500/40 text-amber-500",
  fabricated: "border-red-400/40 text-red-400",
  conspiracy: "border-red-400/40 text-red-400",
  state_media: "border-amber-500/40 text-amber-500",
};

// ── stance / viewpoint balance ────────────────────────────────────────────────
const stanceTotal = computed(() => {
  const s = stance.value;
  return s && s.applicable ? s.supports + s.opposes + s.neutral : 0;
});
function stancePct(n: number): number {
  return stanceTotal.value ? Math.round((n / stanceTotal.value) * 100) : 0;
}
const stanceOneSided = computed(() => {
  const s = stance.value;
  if (!s?.applicable) return false;
  // Only "one-sided" when the sources that take a side skew hard AND actually outnumber the
  // neutral ones — otherwise 7-for / 0-against / 7-neutral wrongly reads as one-sided.
  return s.skew >= 0.7 && s.supports + s.opposes > s.neutral;
});

// ── confidence / honesty meter ────────────────────────────────────────────────
const gradeClass = computed(() => {
  const g = confidence.value?.grade;
  if (g === "high") return "text-emerald-500";
  if (g === "medium") return "text-amber-500";
  return "text-red-400";
});
function bandPct(n: number): number {
  const total = confidence.value?.total_claims || 0;
  return total ? Math.round((n / total) * 100) : 0;
}
const bandClass: Record<string, string> = {
  solid: "bg-emerald-500",
  contested: "bg-amber-500",
  speculative: "bg-red-400",
};

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

async function ensureWatch() {
  if (watch_.value) return;
  try {
    watch_.value = await api.getWatch(props.id);
  } catch {
    /* watch is optional */
  }
}
async function startWatch(seconds: number) {
  watchMenuOpen.value = false;
  try {
    watch_.value = await api.setWatch(props.id, true, seconds);
  } catch (e) {
    error.value = (e as Error).message;
  }
}
async function stopWatch() {
  watchMenuOpen.value = false;
  try {
    watch_.value = await api.setWatch(props.id, false);
  } catch (e) {
    error.value = (e as Error).message;
  }
}
async function ackWatch() {
  try {
    watch_.value = await api.ackWatch(props.id);
  } catch {
    /* ignore */
  }
}
const watchIntervalKey = computed(() => {
  const s = watch_.value?.interval_seconds ?? 0;
  return WATCH_INTERVALS.find((i) => i.seconds === s)?.key ?? "custom";
});

// ── public share link ─────────────────────────────────────────────────────────
const share = ref<import("@/lib/types").ShareInfo | null>(null);
const shareMenuOpen = ref(false);
const shareCopied = ref(false);
const shareUrl = computed(() => (share.value?.token ? `${window.location.origin}/r/${share.value.token}` : ""));
async function ensureShare() {
  if (share.value) return;
  try {
    share.value = await api.getShare(props.id);
  } catch {
    /* share is optional */
  }
}
async function toggleShareMenu() {
  shareMenuOpen.value = !shareMenuOpen.value;
  if (shareMenuOpen.value && !share.value?.shared) {
    try {
      share.value = await api.createShare(props.id);
    } catch (e) {
      error.value = (e as Error).message;
    }
  }
}
async function copyShare() {
  if (!shareUrl.value) return;
  try {
    await navigator.clipboard.writeText(shareUrl.value);
    shareCopied.value = true;
    setTimeout(() => (shareCopied.value = false), 1500);
  } catch {
    /* clipboard blocked — the field is selectable as a fallback */
  }
}
async function revokeShare() {
  try {
    share.value = await api.revokeShare(props.id);
    shareMenuOpen.value = false;
  } catch (e) {
    error.value = (e as Error).message;
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
const exportMenuOpen = ref(false);
const siteMenuOpen = ref(false);
const siteThemes = [
  "auto", "light", "dark", "editorial", "sepia", "mono", "rose",
  "lavender", "ocean", "slate", "midnight", "emerald", "forest", "sunset",
] as const;

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

async function exportReport(fmt: "pdf" | "docx" | "html" | "md" | "json" | "trail", opts?: { theme?: string; accent?: string; base?: string }) {
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
    saveBlob(await res.blob(), filenameFrom(res, fmt === "trail" ? "audit-trail.md" : `research.${fmt}`));
  } finally {
    exporting.value = null;
    siteMenuOpen.value = false;
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

      <!-- report is streaming in / being edited live -->
      <span v-if="report && !isFinal" class="ml-auto flex shrink-0 items-center gap-1.5 pr-2 text-xs text-accent">
        <span class="h-1.5 w-1.5 animate-pulse rounded-full bg-accent" />
        {{ $t("artifact.generating") }}
      </span>

      <div v-if="report && isFinal" class="ml-auto flex items-center gap-1">
        <div class="relative">
          <button
            class="flex items-center gap-1 rounded-md border px-2 py-1 text-xs transition"
            :class="watch_ && watch_.enabled ? 'border-accent/50 text-accent' : 'border-bd text-muted hover:text-ink'"
            :title="$t('watch.hint')"
            @click="watchMenuOpen = !watchMenuOpen"
          >
            <span>👁</span>
            <span>{{ watch_ && watch_.enabled ? $t("watch.intervals." + watchIntervalKey) : $t("watch.watch") }}</span>
            <span
              v-if="watch_ && watch_.has_unseen_change"
              class="ml-0.5 h-1.5 w-1.5 rounded-full bg-amber-500"
              :style="{ animation: 'soft-pulse 1.6s ease-in-out infinite' }"
            />
          </button>
          <div
            v-if="watchMenuOpen"
            class="absolute right-0 z-30 mt-1 w-44 rounded-xl border border-bd bg-surface p-1 text-xs shadow-lg"
          >
            <div class="px-2 py-1 text-[10px] uppercase tracking-wide text-muted">{{ $t("watch.every") }}</div>
            <button
              v-for="iv in WATCH_INTERVALS"
              :key="iv.key"
              class="flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-ink hover:bg-surface-hover"
              @click="startWatch(iv.seconds)"
            >
              {{ $t("watch.intervals." + iv.key) }}
              <span v-if="watch_ && watch_.enabled && watchIntervalKey === iv.key" class="text-accent">✓</span>
            </button>
            <button
              v-if="watch_ && watch_.enabled"
              class="mt-1 w-full rounded-lg border-t border-bd px-2 py-1.5 text-left text-red-400 hover:bg-surface-hover"
              @click="stopWatch"
            >
              {{ $t("watch.stop") }}
            </button>
          </div>
        </div>
        <button
          class="rounded-md border border-bd px-2 py-1 text-xs text-muted transition hover:text-ink disabled:opacity-50"
          :disabled="refreshing"
          :title="$t('diff.refreshHint')"
          @click="onRefresh"
        >
          {{ refreshing ? $t("diff.refreshing") : "↻ " + $t("diff.refresh") }}
        </button>
        <div class="relative">
          <button
            class="flex items-center gap-1 rounded-md border px-2 py-1 text-xs transition"
            :class="share && share.shared ? 'border-accent/50 text-accent' : 'border-bd text-muted hover:text-ink'"
            :title="$t('share.hint')"
            @click="toggleShareMenu"
          >
            🔗 {{ share && share.shared ? $t("share.shared") : $t("share.share") }}
          </button>
          <div
            v-if="shareMenuOpen"
            class="absolute right-0 z-30 mt-1 w-72 rounded-xl border border-bd bg-surface p-3 text-xs shadow-lg"
          >
            <div class="mb-1.5 font-medium text-ink">{{ $t("share.title") }}</div>
            <p class="mb-2 text-muted">{{ $t("share.desc") }}</p>
            <div class="flex items-center gap-1">
              <input
                :value="shareUrl"
                readonly
                class="min-w-0 flex-1 rounded-md border border-bd bg-surface/50 px-2 py-1 text-ink focus:outline-none"
                @focus="($event.target as HTMLInputElement).select()"
              />
              <button class="shrink-0 rounded-md border border-bd px-2 py-1 text-muted hover:text-ink" @click="copyShare">
                {{ shareCopied ? "✓" : $t("share.copy") }}
              </button>
            </div>
            <button class="mt-2 text-red-400 hover:underline" @click="revokeShare">{{ $t("share.revoke") }}</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Always-visible download bar (outside the scroll area so the menu never clips). -->
    <div
      v-if="tab === 'report' && report && isFinal"
      class="flex shrink-0 items-center gap-2 border-b border-bd px-6 py-2"
    >
      <div class="relative">
        <button
          class="flex items-center gap-1.5 rounded-lg border border-accent/40 bg-accent/10 px-3 py-1.5 text-sm font-medium text-accent transition hover:bg-accent/15 disabled:opacity-50"
          :disabled="!!exporting"
          @click="exportMenuOpen = !exportMenuOpen"
        >
          <span>⤓</span> {{ $t("artifact.download") }} <span class="text-xs">{{ exporting ? "…" : "▾" }}</span>
        </button>
        <div
          v-if="exportMenuOpen"
          class="absolute left-0 z-30 mt-1 max-h-[55vh] w-[22rem] overflow-y-auto overscroll-contain rounded-xl border border-bd bg-surface p-1.5 text-sm shadow-xl"
        >
          <div class="px-2 pb-0.5 pt-1 text-[10px] uppercase tracking-wide text-muted">{{ $t("artifact.docGroup") }}</div>
          <div class="grid grid-cols-2 gap-1">
            <button class="export-item" :disabled="!!exporting" @click="exportReport('pdf'); exportMenuOpen = false">
              <span>📄 PDF</span><span class="text-[10px] text-muted">.pdf</span>
            </button>
            <button class="export-item" :disabled="!!exporting" @click="exportReport('docx'); exportMenuOpen = false">
              <span>📝 Word</span><span class="text-[10px] text-muted">.docx</span>
            </button>
            <button class="export-item" :disabled="!!exporting" @click="exportReport('md'); exportMenuOpen = false">
              <span>⬇ Markdown</span><span class="text-[10px] text-muted">.md</span>
            </button>
          </div>

          <div class="mt-1 border-t border-bd px-2 pb-0.5 pt-1.5 text-[10px] uppercase tracking-wide text-muted">{{ $t("artifact.dataGroup") }}</div>
          <div class="grid grid-cols-2 gap-1">
            <button class="export-item" :disabled="!!exporting" @click="exportReport('json'); exportMenuOpen = false">
              <span>{ } JSON</span><span class="text-[10px] text-muted">.json</span>
            </button>
            <button class="export-item" :disabled="!!exporting" :title="$t('audit.hint')" @click="exportReport('trail'); exportMenuOpen = false">
              <span>🧾 {{ $t("audit.trail") }}</span><span class="text-[10px] text-muted">.md</span>
            </button>
          </div>

          <div class="mt-1 border-t border-bd px-2 pb-1 pt-1.5 text-[10px] uppercase tracking-wide text-muted">{{ $t("artifact.webGroup") }}</div>
          <div class="flex flex-wrap gap-1 px-1.5 pb-1">
            <button
              v-for="th in siteThemes"
              :key="th"
              class="rounded-md border border-bd px-2 py-0.5 text-xs text-ink transition hover:bg-surfaceHover"
              :disabled="!!exporting"
              @click="exportReport('html', { theme: th }); exportMenuOpen = false"
            >
              {{ $t("site." + th) }}
            </button>
          </div>
        </div>
      </div>
      <span class="text-xs text-muted">{{ $t("artifact.docGroup") }} · {{ $t("artifact.dataGroup") }} · {{ $t("artifact.webGroup") }}</span>
    </div>

    <div class="min-h-0 flex-1 overflow-y-auto px-6 py-6">
      <template v-if="tab === 'report'">
        <div v-if="report && isFinal" class="mb-4 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-xs">
          <button
            class="flex items-center gap-1.5 rounded-full border px-2.5 py-1 transition"
            :class="verifyInline ? 'border-accent/50 bg-accent/10 text-accent' : 'border-bd text-muted hover:text-ink'"
            @click="verifyInline = !verifyInline"
          >
            <span>{{ verifyInline ? "✓" : "○" }}</span> {{ $t("verify.on") }}
          </button>
          <template v-if="verifyInline">
            <span class="text-muted">{{ $t("verify.legend") }}</span>
            <span class="inline-flex items-center gap-1 text-muted">
              <span class="h-2 w-2 rounded-full bg-emerald-500" /> {{ $t("verify.strong") }}
            </span>
            <span class="inline-flex items-center gap-1 text-muted">
              <span class="h-2 w-2 rounded-full bg-amber-500" /> {{ $t("verify.weak") }}
            </span>
            <span class="inline-flex items-center gap-1 text-muted">
              <span class="h-2 w-2 rounded-full bg-red-400" /> {{ $t("verify.contested") }}
            </span>
          </template>
        </div>
        <div
          v-if="watch_ && watch_.has_unseen_change"
          class="mb-4 flex items-center gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 px-3 py-2 text-xs animate-rise"
        >
          <span class="text-amber-500">🔔</span>
          <span class="font-medium text-ink">{{ $t("watch.changed") }}</span>
          <span class="text-muted">{{ $t("watch.changedHint") }}</span>
          <button class="ml-auto text-accent hover:underline" @click="ackWatch">{{ $t("watch.markSeen") }}</button>
        </div>
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
        <div
          v-if="independence && independence.total_sources > 1"
          class="mb-4 rounded-lg border border-bd bg-surface/40 px-3 py-2 text-xs"
        >
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span class="font-medium text-ink">{{ $t("independence.title") }}</span>
            <span class="font-semibold" :class="independenceClass">{{ Math.round(independence.independence_score * 100) }}%</span>
            <span class="text-muted">{{ independence.independent_origins }}/{{ independence.total_sources }} {{ $t("independence.origins") }}</span>
            <button
              v-if="independence.clusters.length"
              class="ml-auto text-amber-500 hover:underline"
              @click="tab = 'sources'"
            >
              ⚠ {{ independence.clusters.length }} {{ $t("independence.echoClusters") }}
            </button>
          </div>
        </div>
        <button
          v-if="confidence && confidence.components.length"
          class="mb-4 flex w-full items-center gap-3 rounded-lg border border-bd bg-surface/40 px-3 py-2 text-left text-xs"
          @click="tab = 'confidence'"
        >
          <span class="font-medium text-ink">{{ $t("confidence.meter") }}</span>
          <span class="text-lg font-semibold leading-none" :class="gradeClass">{{ Math.round(confidence.overall * 100) }}%</span>
          <span class="text-muted">{{ $t("confidence.grade." + confidence.grade) }}</span>
          <span v-if="confidence.total_claims" class="ml-auto text-muted">
            {{ bandPct(confidence.solid) }}% {{ $t("confidence.band.solid") }} ·
            {{ bandPct(confidence.contested) }}% {{ $t("confidence.band.contested") }} ·
            {{ bandPct(confidence.speculative) }}% {{ $t("confidence.band.speculative") }}
          </span>
        </button>
        <div
          v-if="numericHasIssues"
          class="mb-4 rounded-lg border border-bd bg-surface/40 px-3 py-2 text-xs"
        >
          <div class="flex flex-wrap items-center gap-x-3 gap-y-1">
            <span class="font-medium text-ink">{{ $t("numbers.title") }}</span>
            <template v-if="numbers!.total">
              <span class="font-semibold" :class="numericClass">{{ Math.round(numbers!.integrity * 100) }}%</span>
              <span class="text-muted">{{ numbers!.supported }}/{{ numbers!.total }} {{ $t("numbers.matched") }}</span>
            </template>
            <span v-else class="text-muted">{{ $t("numbers.none") }}</span>
            <button
              v-if="numbers!.unsupported.length || numbers!.contradictions.length"
              class="ml-auto text-red-400 hover:underline"
              @click="showNumbers = !showNumbers"
            >
              ⚠ {{ numbers!.unsupported.length + numbers!.contradictions.length }} {{ $t("numbers.issues") }}
            </button>
          </div>
          <div v-if="showNumbers" class="mt-2 space-y-2 border-t border-bd pt-2">
            <div v-if="numbers!.unsupported.length">
              <div class="mb-1 font-medium text-muted">{{ $t("numbers.unsupported") }}</div>
              <ul class="space-y-1">
                <li v-for="(c, i) in numbers!.unsupported" :key="'u' + i" class="flex gap-2 text-muted">
                  <span class="shrink-0 font-semibold text-red-400">{{ c.value }}</span>
                  <span class="line-clamp-2">{{ c.sentence }} <span class="text-accent">[{{ c.source_id }}]</span></span>
                </li>
              </ul>
            </div>
            <div v-if="numbers!.contradictions.length">
              <div class="mb-1 font-medium text-muted">{{ $t("numbers.contradictions") }}</div>
              <ul class="space-y-1">
                <li v-for="(c, i) in numbers!.contradictions" :key="'c' + i" class="text-muted">
                  <span class="font-semibold text-amber-500">{{ c.values.join(" ≠ ") }}</span>
                  <span v-for="(s, j) in c.sentences" :key="j" class="ml-2 block line-clamp-1 pl-2 text-[11px]">○ {{ s }}</span>
                </li>
              </ul>
            </div>
          </div>
        </div>
        <button
          v-if="reputation && reputation.flagged_count"
          class="mb-4 flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-red-400/40 bg-red-400/5 px-3 py-2 text-left text-xs"
          @click="tab = 'sources'"
        >
          <span class="text-red-400">⚑</span>
          <span class="font-medium text-ink">{{ $t("reputation.title") }}</span>
          <span class="text-red-400">{{ reputation.flagged_count }} {{ $t("reputation.flagged") }}</span>
          <span class="text-muted">{{ reputation.categories.map((c) => $t("reputation.category." + c)).join(", ") }}</span>
        </button>
        <button
          v-if="integrity && integrity.flagged.length"
          class="mb-4 flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-red-500/50 bg-red-500/10 px-3 py-2 text-left text-xs"
          @click="tab = 'sources'"
        >
          <span class="text-red-500">⛔</span>
          <span class="font-medium text-ink">{{ $t("integrity.title") }}</span>
          <span class="font-semibold text-red-500">
            {{ integrity.retracted_count }} {{ $t("integrity.retracted") }}
          </span>
          <span class="text-muted">{{ $t("integrity.hintShort") }}</span>
        </button>
        <button
          v-if="stance && stance.applicable"
          class="mb-4 flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border px-3 py-2 text-left text-xs"
          :class="stanceOneSided ? 'border-amber-500/40 bg-amber-500/5' : 'border-bd bg-surface/40'"
          @click="tab = 'sources'"
        >
          <span class="font-medium text-ink">{{ $t("stance.title") }}</span>
          <span class="text-emerald-500">{{ stancePct(stance.supports) }}% {{ $t("stance.for") }}</span>
          <span class="text-red-400">{{ stancePct(stance.opposes) }}% {{ $t("stance.against") }}</span>
          <span class="text-muted">{{ stancePct(stance.neutral) }}% {{ $t("stance.neutral") }}</span>
          <span v-if="stanceOneSided" class="ml-auto text-amber-500">⚠ {{ $t("stance.oneSided") }}</span>
        </button>
        <button
          v-if="crossLang && crossLang.languages.length > 1"
          class="mb-4 flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border px-3 py-2 text-left text-xs"
          :class="crossLang.monolingual ? 'border-bd bg-surface/40' : 'border-accent/40 bg-accent/5'"
          @click="tab = 'sources'"
        >
          <span class="font-medium text-ink">🌐 {{ $t("crosslang.title") }}</span>
          <span class="text-muted">{{ crossLang.languages.slice(0, 5).map((l) => l.lang + "·" + l.count).join(" ") }}</span>
          <span v-if="crossLang.monolingual" class="ml-auto text-amber-500">⚠ {{ $t("crosslang.bubble") }}</span>
          <span v-else-if="crossLang.unique_findings.length" class="ml-auto text-accent">+{{ crossLang.unique_findings.length }} {{ $t("crosslang.added") }}</span>
        </button>
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
        <MarkdownView
          v-if="report"
          :source="report"
          :grounding="citations?.grounding"
          :independence="independence"
          :weak-claims="weakClaims"
          :contradictions="contradictionSentences"
          :verify="verifyInline && isFinal"
          :class="{ 'opacity-80': !isFinal }"
        />
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
          <!-- Source-independence / echo-chamber summary: how many independent origins these sources really are -->
          <div
            v-if="independence && independence.total_sources > 1"
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4 animate-rise"
          >
            <div class="flex items-center gap-3">
              <div class="text-2xl font-semibold leading-none" :class="independenceClass">
                {{ Math.round(independence.independence_score * 100) }}%
              </div>
              <div class="min-w-0">
                <div class="text-sm font-medium text-ink">{{ $t("independence.title") }}</div>
                <div class="text-xs text-muted">
                  {{ independence.independent_origins }} {{ $t("independence.of") }}
                  {{ independence.total_sources }} {{ $t("independence.origins") }}
                </div>
              </div>
            </div>
            <div class="mt-3 h-1.5 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full rounded-full transition-all"
                :class="independenceBar"
                :style="{ width: independence.independence_score * 100 + '%' }"
              />
            </div>
            <p class="mt-2 text-xs text-muted">{{ $t("independence.hint") }}</p>
            <ul v-if="independence.clusters.length" class="mt-3 space-y-2 border-t border-bd pt-3">
              <li v-for="(c, i) in independence.clusters" :key="i" class="flex items-start gap-2">
                <span
                  class="mt-0.5 shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase"
                  :class="clusterKindClass[c.kind] || 'border-bd text-muted'"
                >
                  {{ $t("independence.kind." + c.kind) }}
                </span>
                <div class="min-w-0 text-xs">
                  <div class="text-ink">{{ c.label }} · {{ c.size }} {{ $t("independence.sources") }}</div>
                  <div class="truncate text-muted">
                    <span class="text-accent">{{ c.source_ids.map((s) => "[" + s + "]").join(" ") }}</span>
                    <span v-if="c.domains.length"> · {{ c.domains.join(", ") }}</span>
                  </div>
                </div>
              </li>
            </ul>
            <p v-else class="mt-3 border-t border-bd pt-3 text-xs text-emerald-500">
              ✓ {{ $t("independence.allIndependent") }}
            </p>
          </div>
          <!-- Domain-credibility flags: satire / fabricated / conspiracy / state-controlled -->
          <div
            v-if="reputation && reputation.flagged_count"
            class="mb-3 rounded-xl border border-red-400/40 bg-red-400/5 p-4 animate-rise"
          >
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-ink">
              <span class="text-red-400">⚑</span>{{ $t("reputation.title") }}
              <span class="text-red-400">· {{ reputation.flagged_count }}/{{ reputation.total_sources }}</span>
            </div>
            <p class="mb-3 text-xs text-muted">{{ $t("reputation.hint") }}</p>
            <ul class="space-y-2">
              <li v-for="(f, i) in reputation.flagged" :key="i" class="flex items-start gap-2 text-xs">
                <span
                  class="mt-0.5 shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase"
                  :class="reputationClass[f.category] || 'border-bd text-muted'"
                >
                  {{ $t("reputation.category." + f.category) }}
                </span>
                <div class="min-w-0">
                  <span class="text-accent">[{{ f.source_id }}]</span>
                  <span class="text-ink"> {{ f.domain }}</span>
                  <span class="text-muted"> — {{ f.reason }}</span>
                </div>
              </li>
            </ul>
          </div>
          <!-- Retraction check: cited DOIs flagged as retracted / under concern -->
          <div
            v-if="integrity && integrity.flagged.length"
            class="mb-3 rounded-xl border border-red-500/50 bg-red-500/10 p-4 animate-rise"
          >
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-ink">
              <span class="text-red-500">⛔</span>{{ $t("integrity.title") }}
              <span class="text-red-500">· {{ integrity.retracted_count }}/{{ integrity.checked_dois }}</span>
            </div>
            <p class="mb-3 text-xs text-muted">{{ $t("integrity.hint") }}</p>
            <ul class="space-y-2">
              <li v-for="(f, i) in integrity.flagged" :key="i" class="flex items-start gap-2 text-xs">
                <span
                  class="mt-0.5 shrink-0 rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase"
                  :class="f.kind === 'retraction' ? 'border-red-500/50 text-red-500' : 'border-amber-500/40 text-amber-500'"
                >
                  {{ $t("integrity.kind." + f.kind) }}
                </span>
                <div class="min-w-0">
                  <span class="text-accent">[{{ f.source_id }}]</span>
                  <a :href="`https://doi.org/${f.doi}`" target="_blank" rel="noopener noreferrer" class="text-ink hover:underline"> {{ f.doi }}</a>
                  <span v-if="f.detail" class="text-muted"> — {{ f.detail }}</span>
                </div>
              </li>
            </ul>
          </div>
          <!-- Viewpoint balance: how the evidence splits for/against the central claim -->
          <div
            v-if="stance && stance.applicable"
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4 animate-rise"
          >
            <div class="mb-1 flex items-center gap-2 text-sm font-medium text-ink">
              {{ $t("stance.title") }}
              <span v-if="stanceOneSided" class="rounded border border-amber-500/40 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-amber-500">
                ⚠ {{ $t("stance.oneSided") }}
              </span>
            </div>
            <p v-if="stance.proposition" class="mb-3 text-xs text-muted">«{{ stance.proposition }}»</p>
            <div class="flex h-2 w-full overflow-hidden rounded-full bg-surface">
              <div class="bg-emerald-500" :style="{ width: stancePct(stance.supports) + '%' }" />
              <div class="bg-red-400" :style="{ width: stancePct(stance.opposes) + '%' }" />
              <div class="bg-muted/40" :style="{ width: stancePct(stance.neutral) + '%' }" />
            </div>
            <div class="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
              <span><span class="font-semibold text-emerald-500">{{ stance.supports }}</span> {{ $t("stance.for") }}</span>
              <span><span class="font-semibold text-red-400">{{ stance.opposes }}</span> {{ $t("stance.against") }}</span>
              <span><span class="font-semibold">{{ stance.neutral }}</span> {{ $t("stance.neutral") }}</span>
            </div>
            <p class="mt-2 text-xs text-muted">{{ $t("stance.hint") }}</p>
          </div>
          <!-- Cross-language coverage: language spread + what non-query-language sources add -->
          <div
            v-if="crossLang && crossLang.languages.length > 1"
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4 animate-rise"
          >
            <div class="mb-2 text-sm font-medium text-ink">🌐 {{ $t("crosslang.title") }}</div>
            <div class="mb-3 flex flex-wrap gap-1.5">
              <span
                v-for="l in crossLang.languages"
                :key="l.lang"
                class="rounded-md border px-2 py-0.5 text-xs"
                :class="l.lang === crossLang.query_language ? 'border-bd text-muted' : 'border-accent/40 text-accent'"
              >
                {{ l.lang }} · {{ l.count }}
              </span>
            </div>
            <p v-if="crossLang.monolingual" class="text-xs text-amber-500">⚠ {{ $t("crosslang.bubbleHint") }}</p>
            <template v-else>
              <p class="mb-2 text-xs text-muted">
                {{ crossLang.foreign_source_count }} {{ $t("crosslang.foreignSources") }}
              </p>
              <ul v-if="crossLang.unique_findings.length" class="space-y-1.5 border-t border-bd pt-2">
                <li v-for="(f, i) in crossLang.unique_findings" :key="i" class="flex items-start gap-2 text-xs">
                  <span class="mt-0.5 shrink-0 rounded border border-accent/40 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-accent">{{ f.lang }}</span>
                  <span class="text-ink">{{ f.finding }}</span>
                </li>
              </ul>
            </template>
          </div>
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
          <!-- Honesty meter: one calibrated confidence fused from all trust signals, with its inputs shown -->
          <div v-if="confidence && confidence.components.length" class="rounded-xl border border-bd bg-surface/40 p-4 animate-rise">
            <div class="flex items-center gap-4">
              <div class="text-3xl font-semibold leading-none" :class="gradeClass">
                {{ Math.round(confidence.overall * 100) }}%
              </div>
              <div>
                <div class="text-sm font-medium text-ink">{{ $t("confidence.meter") }}</div>
                <div class="text-xs font-medium" :class="gradeClass">{{ $t("confidence.grade." + confidence.grade) }}</div>
              </div>
            </div>

            <template v-if="confidence.total_claims">
              <div class="mt-3 flex h-2 w-full overflow-hidden rounded-full bg-surface">
                <div :class="bandClass.solid" :style="{ width: bandPct(confidence.solid) + '%' }" />
                <div :class="bandClass.contested" :style="{ width: bandPct(confidence.contested) + '%' }" />
                <div :class="bandClass.speculative" :style="{ width: bandPct(confidence.speculative) + '%' }" />
              </div>
              <div class="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                <span><span class="font-semibold text-emerald-500">{{ bandPct(confidence.solid) }}%</span> {{ $t("confidence.band.solid") }}</span>
                <span><span class="font-semibold text-amber-500">{{ bandPct(confidence.contested) }}%</span> {{ $t("confidence.band.contested") }}</span>
                <span><span class="font-semibold text-red-400">{{ bandPct(confidence.speculative) }}%</span> {{ $t("confidence.band.speculative") }}</span>
              </div>
            </template>

            <div class="mt-3 space-y-1.5 border-t border-bd pt-3">
              <div class="text-xs font-medium text-muted">{{ $t("confidence.fromSignals") }}</div>
              <div v-for="c in confidence.components" :key="c.key" class="flex items-center gap-2 text-xs">
                <span class="w-32 shrink-0 text-ink">{{ $t("confidence.component." + c.key) }}</span>
                <div class="h-1.5 flex-1 overflow-hidden rounded-full bg-surface">
                  <div class="h-full rounded-full bg-accent transition-all" :style="{ width: c.score * 100 + '%' }" />
                </div>
                <span class="w-9 shrink-0 text-right font-medium text-ink">{{ Math.round(c.score * 100) }}%</span>
                <span class="hidden shrink-0 text-muted md:inline">{{ c.detail }}</span>
              </div>
            </div>
          </div>

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

<style scoped>
.export-item {
  display: flex;
  width: 100%;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
  border-radius: 0.375rem;
  padding: 0.375rem 0.5rem;
  text-align: left;
  color: rgb(var(--c-ink));
  transition: background-color 0.15s;
}
.export-item:hover {
  background: rgb(var(--c-surface-hover));
}
.export-item:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
</style>
