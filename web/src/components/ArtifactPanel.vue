<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, useId, watch, type Ref } from "vue";
import { useI18n } from "vue-i18n";
import { api, apiErrorMessage } from "@/lib/api";
import { confirm } from "@/lib/confirm";
import { saveFile } from "@/lib/download";
import { smoothOrAuto } from "@/lib/motion";
import { safeHttpUrl } from "@/lib/url";
import { useDismiss } from "@/lib/useDismiss";
import type { CitationAudit, ComparisonRow, ComparisonTable, ConfidenceReport, Conflict, CrossLanguageReport, NumericCheck, RedTeamReport, ShareInfo, SourceIndependence, SourceReputation, SourceIntegrity, StanceBalance, SourcePreview, VerificationReport } from "@/lib/types";
import MarkdownView from "./MarkdownView.vue";
import ResearchDashboard from "./ResearchDashboard.vue";
import SourceCard from "./SourceCard.vue";
import ReportSkeletonCanvas from "./ReportSkeletonCanvas.vue";

const props = defineProps<{ id: string; report: string; isFinal: boolean }>();

// Declared before every watcher: the immediate isFinal watcher below reads `t` and the
// share state, and a `const` read before its line throws (a TDZ ReferenceError).
const { t, locale } = useI18n();
// Unique per panel: a thread can show several reports, each with its own tabs.
const uid = useId();

// ── public share link ─────────────────────────────────────────────────────────
// Private by default: opening the popover only shows the state. A link is created by
// the explicit Create button, and revoking asks first, because it breaks the link for
// everyone it was sent to. Share errors stay inside the popover, next to the action.
const share = ref<ShareInfo | null>(null);
const shareMenuOpen = ref(false);
const shareCopied = ref(false);
const shareBusy = ref(false);
const shareError = ref<string | null>(null);
const shareRevoked = ref(false);
const shareUrl = computed(() => (share.value?.token ? `${window.location.origin}/r/${share.value.token}` : ""));
let copiedTimer: ReturnType<typeof setTimeout> | undefined;
onBeforeUnmount(() => clearTimeout(copiedTimer));

async function ensureShare() {
  if (share.value) return;
  try {
    share.value = await api.getShare(props.id);
  } catch {
    /* share state is optional — the popover still offers Create */
  }
}
function toggleShareMenu() {
  shareMenuOpen.value = !shareMenuOpen.value;
  if (shareMenuOpen.value) {
    exportMenuOpen.value = false;
    shareError.value = null;
    shareRevoked.value = false;
  }
}
async function createShare() {
  if (shareBusy.value) return;
  shareBusy.value = true;
  shareError.value = null;
  shareRevoked.value = false;
  try {
    share.value = await api.createShare(props.id);
  } catch (e) {
    shareError.value = apiErrorMessage(e, t);
  } finally {
    shareBusy.value = false;
  }
}
async function copyShare() {
  if (!shareUrl.value) return;
  try {
    await navigator.clipboard.writeText(shareUrl.value);
    shareCopied.value = true;
    clearTimeout(copiedTimer);
    copiedTimer = setTimeout(() => (shareCopied.value = false), 1500);
  } catch {
    /* clipboard blocked — the field is selectable as a fallback */
  }
}
async function revokeShare() {
  if (shareBusy.value) return;
  const ok = await confirm({
    title: t("share.revoke"),
    message: t("share.revokeConfirm"),
    confirmText: t("share.revoke"),
    cancelText: t("common.cancel"),
    danger: true,
  });
  if (!ok) return;
  shareBusy.value = true;
  shareError.value = null;
  try {
    share.value = await api.revokeShare(props.id);
    shareCopied.value = false;
    shareRevoked.value = true;
  } catch (e) {
    shareError.value = apiErrorMessage(e, t);
  } finally {
    shareBusy.value = false;
  }
}

// Both menus grow from their trigger and close on an outside press or Escape, which
// gives focus back to the trigger (apple-design §7, §16 "how do I get out?").
const shareRoot = ref<HTMLElement | null>(null);
const shareBtn = ref<HTMLElement | null>(null);
useDismiss(shareRoot, shareMenuOpen, { trigger: shareBtn });

type Tab = "report" | "dashboard" | "comparison" | "sources" | "confidence" | "conflicts" | "redteam";
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
const comparison = ref<ComparisonTable | null>(null);

// ── per-tab loading ───────────────────────────────────────────────────────────
// Each tab owns its request, its pending flag and its error, so switching tabs while one
// request is in flight still starts the next one, and a failure shows (with Retry) only
// on the tab it belongs to. A tab never claims "no findings" for data it never fetched.
type TabDataKey = "sources" | "conflicts" | "verification" | "redteam";
const pending = reactive(new Set<TabDataKey>());
const errors = reactive<Partial<Record<TabDataKey, string | null>>>({});
// A newer request for the same key (a refetch) wins over one still in flight.
const requestSeq: Partial<Record<TabDataKey, number>> = {};

async function load<T>(key: TabDataKey, target: Ref<T | null>, fetcher: () => Promise<T>, force = false) {
  if (!force && (target.value !== null || pending.has(key))) return;
  const seq = (requestSeq[key] = (requestSeq[key] ?? 0) + 1);
  errors[key] = null;
  pending.add(key);
  try {
    const value = await fetcher();
    if (requestSeq[key] === seq) target.value = value;
  } catch (e) {
    if (requestSeq[key] === seq) errors[key] = apiErrorMessage(e, t);
  } finally {
    if (requestSeq[key] === seq) pending.delete(key);
  }
}

// What a tab shows: skeleton rows while its data is on the way, its own error, or data.
function tabState(key: TabDataKey, value: unknown): "loading" | "error" | "ready" {
  if (pending.has(key)) return "loading";
  if (errors[key]) return "error";
  return value === null ? "loading" : "ready";
}

const ensureSources = (force = false) => load("sources", sources, () => api.getSources(props.id), force);
const ensureConflicts = () => load("conflicts", conflicts, () => api.getConflicts(props.id));
const ensureVerification = () => load("verification", verification, () => api.getVerification(props.id));
const ensureRedTeam = () => load("redteam", redTeam, () => api.getRedTeam(props.id));

const tabData: Record<TabDataKey, { target: Ref<unknown>; ensure: () => Promise<void> }> = {
  sources: { target: sources, ensure: () => ensureSources() },
  conflicts: { target: conflicts, ensure: ensureConflicts },
  verification: { target: verification, ensure: ensureVerification },
  redteam: { target: redTeam, ensure: ensureRedTeam },
};
function retry(key: TabDataKey) {
  tabData[key].target.value = null;
  tabData[key].ensure();
}
// The data tab on screen and its state; the report, dashboard and comparison tabs load
// their own way.
const TAB_DATA: Partial<Record<Tab, TabDataKey>> = {
  sources: "sources",
  conflicts: "conflicts",
  confidence: "verification",
  redteam: "redteam",
};
const activeData = computed(() => {
  const key = TAB_DATA[tab.value];
  return key ? { key, state: tabState(key, tabData[key].target.value) } : null;
});

// ── trust signals ─────────────────────────────────────────────────────────────
// Optional analyses: each may be missing (only debate questions have a stance, only
// academic sources have DOIs, …) and the report renders without it. A request already
// in flight is shared, so the Sources tab opening during the final load never
// duplicates it.
const optionalInFlight = new Map<string, Promise<void>>();
function loadOptional<T>(key: string, target: Ref<T | null>, fetcher: () => Promise<T>): Promise<void> {
  if (target.value !== null) return Promise.resolve();
  const running = optionalInFlight.get(key);
  if (running) return running;
  const request = fetcher()
    .then(
      (value) => {
        target.value = value;
      },
      () => {
        /* optional signal — its chip or card simply stays hidden */
      },
    )
    .finally(() => optionalInFlight.delete(key));
  optionalInFlight.set(key, request);
  return request;
}
const ensureCitations = () => loadOptional("citations", citations, () => api.getCitations(props.id));
const ensureIndependence = () => loadOptional("independence", independence, () => api.getSourceIndependence(props.id));
const ensureReputation = () => loadOptional("reputation", reputation, () => api.getSourceReputation(props.id));
const ensureStance = () => loadOptional("stance", stance, () => api.getStance(props.id));
const ensureIntegrity = () => loadOptional("integrity", integrity, () => api.getSourceIntegrity(props.id));
const ensureCrossLang = () => loadOptional("crossLang", crossLang, () => api.getCrossLanguage(props.id));
const ensureConfidence = () => loadOptional("confidence", confidence, () => api.getConfidence(props.id));
const ensureNumbers = () => loadOptional("numbers", numbers, () => api.getNumericCheck(props.id));

// The trust row appears once, when every signal has answered (or after a cap, so one
// hung request cannot hide the rest): one layout change above the report instead of up
// to eight separate jumps while the reader has just started reading.
const TRUST_REVEAL_CAP_MS = 6000;
const trustReady = ref(false);
let trustCapTimer: ReturnType<typeof setTimeout> | undefined;
onBeforeUnmount(() => clearTimeout(trustCapTimer));
function revealTrust() {
  if (trustReady.value || trustCapTimer !== undefined) return;
  trustCapTimer = setTimeout(() => (trustReady.value = true), TRUST_REVEAL_CAP_MS);
  Promise.allSettled([
    ensureCitations(),
    ensureIndependence(),
    ensureReputation(),
    ensureStance(),
    ensureIntegrity(),
    ensureCrossLang(),
    ensureConfidence(),
    ensureNumbers(),
  ]).then(() => {
    clearTimeout(trustCapTimer);
    trustReady.value = true;
  });
}

// Chips that expand a list below the row (the others open a tab).
const openList = ref<"citations" | "numbers" | null>(null);
function toggleList(list: "citations" | "numbers") {
  openList.value = openList.value === list ? null : list;
}
const pct = (ratio: number) => Math.round(ratio * 100);
const weakCount = computed(() => citations.value?.unsupported_claims.length ?? 0);
const numericIssueCount = computed(() =>
  numbers.value ? numbers.value.unsupported.length + numbers.value.contradictions.length : 0,
);
const echoCount = computed(() => independence.value?.clusters.length ?? 0);

// A chip is tinted (border and value) only when it carries an issue.
type Tone = "danger" | "warning" | null;
function chipBorder(tone: Tone): string {
  return tone === "danger" ? "border-danger/40" : tone === "warning" ? "border-warning/40" : "border-bd";
}
function chipValue(tone: Tone): string {
  return tone === "danger" ? "text-danger" : tone === "warning" ? "text-warning" : "text-ink";
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
});

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
  (final, wasFinal) => {
    if (final) {
      // While finalizing, /sources serves a fallback pool; the canonical [Sn] table lands with
      // the report, so a list fetched before completion is refetched once.
      if (wasFinal === false && (sources.value || pending.has("sources"))) {
        sources.value = null;
        ensureSources(true);
      }
      revealTrust();
      ensureShare();
      ensureComparison();
    }
  },
  { immediate: true },
);

function cellFor(row: ComparisonRow, option: string) {
  return row.cells.find((c) => c.option === option) || null;
}

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

const tabKeys = computed<Tab[]>(() => {
  const base: Tab[] = ["report", "dashboard", "sources", "confidence", "conflicts", "redteam"];
  // The comparison tab only appears when the query actually produced a table.
  if (comparison.value && comparison.value.options.length >= 2) base.splice(2, 0, "comparison");
  return base;
});

// ── tab strip ────────────────────────────────────────────────────────────────
const tabStrip = ref<HTMLElement | null>(null);
const canScrollRight = ref(false);
const EDGE_FADE_PX = 28; // .edge-fade-x
function updateOverflow() {
  const el = tabStrip.value;
  canScrollRight.value = !!el && el.scrollLeft + el.clientWidth < el.scrollWidth - 1;
}
// A vertical wheel scrolls the strip sideways while it can still move that way; at either
// end the wheel is left to scroll the page.
function onStripWheel(e: WheelEvent) {
  const el = tabStrip.value;
  if (!el || el.scrollWidth <= el.clientWidth || Math.abs(e.deltaY) <= Math.abs(e.deltaX)) return;
  const unit = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? el.clientWidth : 1; // lines, pages
  const delta = e.deltaY * unit;
  const max = el.scrollWidth - el.clientWidth;
  if ((delta > 0 && el.scrollLeft >= max - 1) || (delta < 0 && el.scrollLeft <= 0)) return;
  e.preventDefault();
  el.scrollLeft = Math.min(max, Math.max(0, el.scrollLeft + delta));
}
// The chosen tab scrolls fully into view, clear of the fade. Horizontal only, so the
// thread around the panel never jumps.
function revealActiveTab() {
  const strip = tabStrip.value;
  const btn = strip?.querySelector<HTMLElement>('[role="tab"][aria-selected="true"]');
  if (!strip || !btn) return;
  const s = strip.getBoundingClientRect();
  const b = btn.getBoundingClientRect();
  const fade = canScrollRight.value ? EDGE_FADE_PX : 0;
  const delta = b.left < s.left ? b.left - s.left : b.right > s.right - fade ? b.right - s.right + fade : 0;
  if (!delta) return;
  if (typeof strip.scrollBy === "function") strip.scrollBy({ left: delta, behavior: smoothOrAuto() });
  else strip.scrollLeft += delta;
}
// Tabs pattern: arrows, Home and End move the selection and focus along the strip.
function onTabKey(e: KeyboardEvent) {
  const keys = tabKeys.value;
  const i = keys.indexOf(tab.value);
  const next =
    e.key === "ArrowRight" ? (i + 1) % keys.length
    : e.key === "ArrowLeft" ? (i - 1 + keys.length) % keys.length
    : e.key === "Home" ? 0
    : e.key === "End" ? keys.length - 1
    : -1;
  if (next < 0) return;
  e.preventDefault();
  tab.value = keys[next];
  nextTick(() => tabStrip.value?.querySelector<HTMLElement>('[role="tab"][aria-selected="true"]')?.focus());
}
let stripObserver: ResizeObserver | undefined;
onMounted(() => {
  updateOverflow();
  if (typeof ResizeObserver !== "undefined" && tabStrip.value) {
    stripObserver = new ResizeObserver(updateOverflow);
    stripObserver.observe(tabStrip.value);
  }
});
onBeforeUnmount(() => stripObserver?.disconnect());
watch([tabKeys, locale], () => nextTick(updateOverflow));
watch(tab, () => nextTick(revealActiveTab));

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

const exporting = ref<string | null>(null);
const exportError = ref<string | null>(null);
const exportMenuOpen = ref(false);
const exportRoot = ref<HTMLElement | null>(null);
const exportBtn = ref<HTMLElement | null>(null);
useDismiss(exportRoot, exportMenuOpen, { trigger: exportBtn });
function toggleExportMenu() {
  exportMenuOpen.value = !exportMenuOpen.value;
  if (exportMenuOpen.value) shareMenuOpen.value = false;
}
// Menu keys: the first item takes focus on open; arrows, Home and End move between items.
function menuItems(): HTMLElement[] {
  return Array.from(exportRoot.value?.querySelectorAll<HTMLElement>('[role="menuitem"]:not(:disabled)') ?? []);
}
watch(exportMenuOpen, (open) => {
  if (open) nextTick(() => menuItems()[0]?.focus({ preventScroll: true }));
});
function onExportMenuKey(e: KeyboardEvent) {
  if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(e.key)) return;
  const items = menuItems();
  if (!items.length) return;
  e.preventDefault();
  const i = items.indexOf(document.activeElement as HTMLElement);
  const last = items.length - 1;
  const next =
    e.key === "Home" ? 0 : e.key === "End" ? last : e.key === "ArrowDown" ? (i + 1) % items.length : i <= 0 ? last : i - 1;
  items[next].focus();
}
const siteThemes = [
  "auto", "light", "dark", "midnight", "emerald", "rose", "sand",
] as const;

async function exportReport(fmt: "pdf" | "docx" | "html" | "md" | "json" | "trail", opts?: { theme?: string; accent?: string; base?: string }) {
  exporting.value = fmt;
  exportError.value = null;
  try {
    const params = new URLSearchParams({ format: fmt });
    if (opts?.theme) params.set("theme", opts.theme);
    if (opts?.accent) params.set("accent", opts.accent);
    if (opts?.base) params.set("base", opts.base);
    saveFile(await api.exportReport(props.id, params), fmt === "trail" ? "audit-trail.md" : `research.${fmt}`);
  } catch (e) {
    // Failed download must be visible — previously a non-2xx silently did nothing.
    exportError.value = apiErrorMessage(e, t);
  } finally {
    exporting.value = null;
  }
}

</script>

<template>
  <div class="flex h-full flex-col">
    <!-- Tab bar: a scrolling segmented control. The wheel scrolls it sideways, the chosen
         tab scrolls into view, and a fade on the right shows only while more tabs wait. -->
    <div class="flex min-w-0 items-center border-b border-bd px-3">
      <div
        ref="tabStrip"
        role="tablist"
        class="flex min-w-0 flex-1 items-center gap-0.5 overflow-x-auto scrollbar-none"
        :class="{ 'edge-fade-x': canScrollRight }"
        @scroll.passive="updateOverflow"
        @wheel="onStripWheel"
        @keydown="onTabKey"
      >
        <button
          v-for="tb in tabKeys"
          :id="`${uid}-tab-${tb}`"
          :key="tb"
          role="tab"
          :aria-selected="tab === tb"
          :aria-controls="`${uid}-panel`"
          :tabindex="tab === tb ? 0 : -1"
          class="shrink-0 whitespace-nowrap rounded-t-md border-b-2 px-2.5 py-3 text-[0.8125rem] font-medium transition-colors focus-visible:!outline-offset-[-2px] sm:px-3 sm:text-sm"
          :class="tab === tb ? 'border-accent text-ink' : 'border-transparent text-muted hover:text-ink'"
          @click="tab = tb"
        >
          {{ $t("artifact." + tb) }}
        </button>
      </div>

      <!-- The report is streaming in or being edited live. -->
      <span v-if="report && !isFinal" class="ml-2 flex shrink-0 items-center gap-1.5 whitespace-nowrap pr-1 text-xs text-accent">
        <span class="live-dot h-1.5 w-1.5 rounded-full bg-accent" />
        {{ $t("artifact.generating") }}
      </span>
    </div>

    <!-- Actions for a finished report, on every tab (outside the scroller, so its menus
         never clip): Download on the left, Share on the right. -->
    <div v-if="report && isFinal" class="flex shrink-0 items-center gap-2 px-4 py-2 sm:px-6">
      <div class="relative">
        <button
          ref="exportBtn"
          class="press flex items-center gap-1.5 rounded-lg border border-accent/40 bg-accent/10 px-3 py-1.5 text-sm font-medium text-accent hover:bg-accent/15 disabled:opacity-50"
          :disabled="!!exporting"
          aria-haspopup="menu"
          :aria-expanded="exportMenuOpen"
          @click="toggleExportMenu"
        >
          <span aria-hidden="true">⤓</span> {{ $t("artifact.download") }}
          <span class="text-xs" aria-hidden="true">{{ exporting ? "…" : "▾" }}</span>
        </button>
        <Transition name="pop">
          <div
            v-if="exportMenuOpen"
            ref="exportRoot"
            role="menu"
            :aria-label="$t('artifact.download')"
            class="material-popover absolute left-0 z-30 mt-1 max-h-[55vh] w-[22rem] max-w-[calc(100vw-3rem)] origin-top-left overflow-y-auto overscroll-contain rounded-xl border border-bd p-1.5 text-sm"
            @keydown="onExportMenuKey"
          >
            <div class="px-2 pb-0.5 pt-1 text-[10px] uppercase tracking-wide text-muted" aria-hidden="true">{{ $t("artifact.docGroup") }}</div>
            <div role="group" :aria-label="$t('artifact.docGroup')" class="grid grid-cols-2 gap-1">
              <button role="menuitem" class="export-item" :disabled="!!exporting" @click="exportReport('pdf'); exportMenuOpen = false">
                <span>📄 PDF</span><span class="text-[10px] text-muted">.pdf</span>
              </button>
              <button role="menuitem" class="export-item" :disabled="!!exporting" @click="exportReport('docx'); exportMenuOpen = false">
                <span>📝 Word</span><span class="text-[10px] text-muted">.docx</span>
              </button>
              <button role="menuitem" class="export-item" :disabled="!!exporting" @click="exportReport('md'); exportMenuOpen = false">
                <span>⬇ Markdown</span><span class="text-[10px] text-muted">.md</span>
              </button>
            </div>

            <div class="mt-1 border-t border-bd px-2 pb-0.5 pt-1.5 text-[10px] uppercase tracking-wide text-muted" aria-hidden="true">{{ $t("artifact.dataGroup") }}</div>
            <div role="group" :aria-label="$t('artifact.dataGroup')" class="grid grid-cols-2 gap-1">
              <button role="menuitem" class="export-item" :disabled="!!exporting" @click="exportReport('json'); exportMenuOpen = false">
                <span>{ } JSON</span><span class="text-[10px] text-muted">.json</span>
              </button>
              <button role="menuitem" class="export-item" :disabled="!!exporting" :title="$t('audit.hint')" @click="exportReport('trail'); exportMenuOpen = false">
                <span>🧾 {{ $t("audit.trail") }}</span><span class="text-[10px] text-muted">.md</span>
              </button>
            </div>

            <div class="mt-1 border-t border-bd px-2 pb-1 pt-1.5 text-[10px] uppercase tracking-wide text-muted" aria-hidden="true">{{ $t("artifact.webGroup") }}</div>
            <div role="group" :aria-label="$t('artifact.webGroup')" class="flex flex-wrap gap-1 px-1.5 pb-1">
              <button
                v-for="th in siteThemes"
                :key="th"
                role="menuitem"
                class="rounded-md border border-bd px-2 py-0.5 text-xs text-ink hover:bg-surfaceHover"
                :disabled="!!exporting"
                @click="exportReport('html', { theme: th }); exportMenuOpen = false"
              >
                {{ $t("site." + th) }}
              </button>
            </div>
          </div>
        </Transition>
      </div>
      <p v-if="exportError" role="alert" class="min-w-0 text-xs text-danger">{{ exportError }}</p>

      <div class="relative ml-auto shrink-0">
        <button
          ref="shareBtn"
          class="press flex items-center gap-1.5 whitespace-nowrap rounded-lg border px-3 py-1.5 text-sm font-medium"
          :class="share && share.shared ? 'border-accent/50 bg-accent/5 text-accent' : 'border-bd text-muted hover:bg-surface/60 hover:text-ink'"
          :title="$t('share.hint')"
          aria-haspopup="dialog"
          :aria-expanded="shareMenuOpen"
          @click="toggleShareMenu"
        >
          <span aria-hidden="true">🔗</span>
          <span>{{ share && share.shared ? $t("share.shared") : $t("share.share") }}</span>
        </button>

        <Transition name="pop">
          <div
            v-if="shareMenuOpen"
            ref="shareRoot"
            role="dialog"
            :aria-label="$t('share.title')"
            class="material-popover absolute right-0 z-30 mt-1.5 w-80 max-w-[calc(100vw-3rem)] origin-top-right rounded-xl border border-bd p-3.5 text-xs"
          >
            <div class="mb-1 font-semibold text-ink text-xs">{{ $t("share.title") }}</div>
            <p class="mb-2 text-muted leading-relaxed text-2xs">{{ $t("share.desc") }}</p>
            <template v-if="share && share.shared">
              <div class="flex items-center gap-1.5">
                <input
                  :value="shareUrl"
                  readonly
                  :aria-label="$t('share.title')"
                  class="min-w-0 flex-1 rounded-md border border-bd bg-surface/50 px-2 py-1.5 text-ink text-xs font-mono select-all"
                  @focus="($event.target as HTMLInputElement).select()"
                />
                <button
                  class="press shrink-0 rounded-md border border-bd px-2.5 py-1.5 text-xs font-medium text-muted hover:text-ink hover:bg-surface/80"
                  @click="copyShare"
                >
                  {{ shareCopied ? "✓" : $t("share.copy") }}
                </button>
              </div>
              <div class="mt-2.5 pt-2 border-t border-bd/40 flex items-center justify-between">
                <button
                  class="text-xs text-danger hover:text-danger/80 hover:underline disabled:opacity-50"
                  :disabled="shareBusy"
                  @click="revokeShare"
                >
                  {{ $t("share.revoke") }}
                </button>
                <span v-if="shareCopied" role="status" class="text-2xs text-success font-medium">
                  {{ $t("share.copied") }}
                </span>
              </div>
            </template>
            <template v-else>
              <p v-if="shareRevoked" role="status" class="mb-2 text-2xs text-muted">{{ $t("share.revoked") }}</p>
              <button
                class="press rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-onAccent disabled:opacity-60"
                :disabled="shareBusy"
                :aria-busy="shareBusy"
                @click="createShare"
              >
                {{ shareBusy ? "…" : $t("share.create") }}
              </button>
            </template>
            <p v-if="shareError" role="alert" class="mt-2 text-2xs text-danger">{{ shareError }}</p>
          </div>
        </Transition>
      </div>
    </div>

    <div
      :id="`${uid}-panel`"
      role="tabpanel"
      :aria-labelledby="`${uid}-tab-${tab}`"
      class="edge-fade-y min-h-0 flex-1 overflow-y-auto px-4 py-6 sm:px-6"
    >
      <!-- A data tab whose request is on the way, or failed: its own skeleton or error. -->
      <div v-if="activeData && activeData.state === 'loading'" class="space-y-2" aria-busy="true">
        <span class="sr-only">{{ $t("common.loading") }}</span>
        <div v-for="n in 3" :key="n" class="h-16 rounded-lg border border-bd bg-surface/50 animate-pulse" />
      </div>
      <div v-else-if="activeData && activeData.state === 'error'" role="alert" class="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm">
        <span class="text-danger">{{ errors[activeData.key] }}</span>
        <button class="press text-accent hover:underline" @click="retry(activeData.key)">{{ $t("common.retry") }}</button>
      </div>

      <template v-else-if="tab === 'report'">
        <!-- Trust summary: one chip row that appears once, when the signals have answered.
             Each chip opens its tab or its list; only a chip with an issue is tinted. -->
        <Transition name="fade-quick" appear>
          <div v-if="report && isFinal && trustReady" class="mb-5 space-y-2 text-xs">
            <div class="flex flex-wrap items-center gap-1.5">
              <button
                v-if="citations && (citations.total || citations.unverified)"
                class="press rounded-full border bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :class="chipBorder(weakCount ? 'danger' : null)"
                :aria-expanded="openList === 'citations'"
                :title="citations.total ? `${citations.supported}/${citations.total} ${$t('citations.matched')}` : undefined"
                @click="toggleList('citations')"
              >
                {{ $t("citations.integrity") }}
                <b class="font-semibold tabular-nums" :class="chipValue(weakCount ? 'danger' : null)">{{ citations.total ? pct(citations.integrity) + "%" : "—" }}</b>
                <span v-if="weakCount" class="tabular-nums text-danger"> · ⚠ {{ weakCount }}</span>
              </button>
              <button
                v-if="independence && independence.total_sources > 1"
                class="press rounded-full border bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :class="chipBorder(echoCount ? 'warning' : null)"
                :title="`${independence.independent_origins}/${independence.total_sources} ${$t('independence.origins')}` + (echoCount ? ` · ${echoCount} ${$t('independence.echoClusters')}` : '')"
                @click="tab = 'sources'"
              >
                {{ $t("independence.title") }}
                <b class="font-semibold tabular-nums" :class="chipValue(echoCount ? 'warning' : null)">{{ pct(independence.independence_score) }}%</b>
                <span v-if="echoCount" class="tabular-nums text-warning"> · ⚠ {{ echoCount }}</span>
              </button>
              <button
                v-if="confidence && confidence.components.length"
                class="press rounded-full border border-bd bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :title="confidence.total_claims ? `${bandPct(confidence.solid)}% ${$t('confidence.band.solid')} · ${bandPct(confidence.contested)}% ${$t('confidence.band.contested')} · ${bandPct(confidence.speculative)}% ${$t('confidence.band.speculative')}` : undefined"
                @click="tab = 'confidence'"
              >
                {{ $t("confidence.meter") }}
                <b class="font-semibold tabular-nums text-ink">{{ pct(confidence.overall) }}%</b>
                · {{ $t("confidence.grade." + confidence.grade) }}
              </button>
              <button
                v-if="numbers && numericHasIssues"
                class="press rounded-full border bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :class="chipBorder(numericIssueCount ? 'danger' : null)"
                :aria-expanded="openList === 'numbers'"
                @click="toggleList('numbers')"
              >
                {{ $t("numbers.title") }}
                <b class="font-semibold tabular-nums" :class="chipValue(numericIssueCount ? 'danger' : null)">{{ numbers.total ? `${numbers.supported}/${numbers.total}` : "—" }}</b>
                <span v-if="numericIssueCount" class="tabular-nums text-danger"> · ⚠ {{ numericIssueCount }}</span>
              </button>
              <button
                v-if="stance && stance.applicable"
                class="press rounded-full border bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :class="chipBorder(stanceOneSided ? 'warning' : null)"
                :title="`${stancePct(stance.neutral)}% ${$t('stance.neutral')}`"
                @click="tab = 'sources'"
              >
                {{ $t("stance.title") }}
                <b class="font-semibold tabular-nums" :class="chipValue(stanceOneSided ? 'warning' : null)">{{ stancePct(stance.supports) }}%</b> {{ $t("stance.for") }} ·
                <b class="font-semibold tabular-nums" :class="chipValue(stanceOneSided ? 'warning' : null)">{{ stancePct(stance.opposes) }}%</b> {{ $t("stance.against") }}
                <span v-if="stanceOneSided" class="text-warning"> · ⚠ {{ $t("stance.oneSided") }}</span>
              </button>
              <button
                v-if="crossLang && crossLang.languages.length > 1"
                class="press rounded-full border bg-surface/40 px-2.5 py-1 text-xs text-muted hover:text-ink"
                :class="chipBorder(crossLang.monolingual ? 'warning' : null)"
                :title="crossLang.languages.slice(0, 5).map((l) => l.lang + '·' + l.count).join(' ')"
                @click="tab = 'sources'"
              >
                {{ $t("crosslang.title") }}
                <b class="font-semibold tabular-nums" :class="chipValue(crossLang.monolingual ? 'warning' : null)">{{ crossLang.languages.length }}</b>
                <span v-if="crossLang.monolingual" class="text-warning"> · ⚠ {{ $t("crosslang.bubble") }}</span>
                <span v-else-if="crossLang.unique_findings.length" class="text-accent"> · +{{ crossLang.unique_findings.length }} {{ $t("crosslang.added") }}</span>
              </button>

              <!-- The in-text verification switch and its legend wrap together, last in the
                   row: closest to the text they change. -->
              <div class="flex flex-wrap items-center gap-x-2.5 gap-y-1">
                <button
                  class="press flex items-center gap-1.5 rounded-full border px-2.5 py-1"
                  :class="verifyInline ? 'border-accent/50 bg-accent/10 text-accent' : 'border-bd text-muted hover:text-ink'"
                  :aria-pressed="verifyInline"
                  @click="verifyInline = !verifyInline"
                >
                  <span aria-hidden="true">{{ verifyInline ? "✓" : "○" }}</span> {{ $t("verify.on") }}
                </button>
                <template v-if="verifyInline">
                  <span class="text-muted">{{ $t("verify.legend") }}</span>
                  <span class="inline-flex items-center gap-1 text-muted">
                    <span class="h-2 w-2 rounded-full bg-success" /> {{ $t("verify.strong") }}
                  </span>
                  <span class="inline-flex items-center gap-1 text-muted">
                    <span class="h-2 w-2 rounded-full bg-warning" /> {{ $t("verify.weak") }}
                  </span>
                  <span class="inline-flex items-center gap-1 text-muted">
                    <span class="h-2 w-2 rounded-full bg-danger" /> {{ $t("verify.contested") }}
                  </span>
                </template>
              </div>
            </div>

            <!-- Citations chip: match counts and the claims the sources don't back. -->
            <div v-if="openList === 'citations' && citations" class="rounded-lg border border-bd bg-surface/40 px-3 py-2">
              <div class="flex flex-wrap gap-x-3 gap-y-1 text-muted">
                <span v-if="citations.total" class="tabular-nums">{{ citations.supported }}/{{ citations.total }} {{ $t("citations.matched") }}</span>
                <span v-if="citations.unverified" :title="$t('citations.unverifiedHint')">
                  <span class="tabular-nums">{{ citations.unverified }}</span> {{ $t("citations.unverified") }}
                </span>
              </div>
              <div v-if="citations.unsupported_claims.length" class="mt-2 border-t border-bd pt-2">
                <div class="mb-1 font-medium text-danger">⚠ {{ citations.unsupported_claims.length }} {{ $t("citations.weak") }}</div>
                <ul class="space-y-1">
                  <li v-for="(c, i) in citations.unsupported_claims" :key="i" class="line-clamp-2 text-muted">○ {{ c }}</li>
                </ul>
              </div>
            </div>

            <!-- Numbers chip: figures missing from their source and internal contradictions. -->
            <div v-if="openList === 'numbers' && numbers" class="rounded-lg border border-bd bg-surface/40 px-3 py-2">
              <div class="text-muted">
                <template v-if="numbers.total">
                  <span class="tabular-nums">{{ pct(numbers.integrity) }}%</span> ·
                  <span class="tabular-nums">{{ numbers.supported }}/{{ numbers.total }}</span> {{ $t("numbers.matched") }}
                </template>
                <template v-else>{{ $t("numbers.none") }}</template>
              </div>
              <div v-if="numericIssueCount" class="mt-2 space-y-2 border-t border-bd pt-2">
                <div v-if="numbers.unsupported.length">
                  <div class="mb-1 font-medium text-muted">{{ $t("numbers.unsupported") }}</div>
                  <ul class="space-y-1">
                    <li v-for="(c, i) in numbers.unsupported" :key="'u' + i" class="flex gap-2 text-muted">
                      <span class="shrink-0 font-semibold tabular-nums text-danger">{{ c.value }}</span>
                      <span class="line-clamp-2">{{ c.sentence }} <span class="text-accent">[{{ c.source_id }}]</span></span>
                    </li>
                  </ul>
                </div>
                <div v-if="numbers.contradictions.length">
                  <div class="mb-1 font-medium text-muted">{{ $t("numbers.contradictions") }}</div>
                  <ul class="space-y-1">
                    <li v-for="(c, i) in numbers.contradictions" :key="'c' + i" class="text-muted">
                      <span class="font-semibold tabular-nums text-warning">{{ c.values.join(" ≠ ") }}</span>
                      <span v-for="(s, j) in c.sentences" :key="j" class="ml-2 block line-clamp-1 pl-2 text-2xs">○ {{ s }}</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>

            <!-- Full-width alerts only for the two findings that undermine a source. -->
            <button
              v-if="integrity && integrity.flagged.length"
              class="flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-danger/40 bg-danger/5 px-3 py-2 text-left"
              @click="tab = 'sources'"
            >
              <span class="text-danger" aria-hidden="true">⛔</span>
              <span class="font-medium text-ink">{{ $t("integrity.title") }}</span>
              <span class="font-semibold tabular-nums text-danger">
                {{ integrity.retracted_count }} {{ $t("integrity.retracted") }}
              </span>
              <span class="text-muted">{{ $t("integrity.hintShort") }}</span>
            </button>
            <button
              v-if="reputation && reputation.flagged_count"
              class="flex w-full flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-danger/40 bg-danger/5 px-3 py-2 text-left"
              @click="tab = 'sources'"
            >
              <span class="text-danger" aria-hidden="true">⚑</span>
              <span class="font-medium text-ink">{{ $t("reputation.title") }}</span>
              <span class="tabular-nums text-danger">{{ reputation.flagged_count }} {{ $t("reputation.flagged") }}</span>
              <span class="text-muted">{{ reputation.categories.map((c) => $t("reputation.category." + c)).join(", ") }}</span>
            </button>
          </div>
        </Transition>
        <MarkdownView
          v-if="report"
          :source="report"
          :sources="sources || []"
          :grounding="citations?.grounding"
          :independence="independence"
          :weak-claims="weakClaims"
          :contradictions="contradictionSentences"
          :verify="verifyInline && isFinal && trustReady"
          class="transition-opacity duration-300"
          :class="{ 'opacity-80': !isFinal }"
        />
        <ReportSkeletonCanvas v-else :source-count="sources?.length" />
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
        <p v-if="sources && !sources.length" class="text-muted">{{ $t("artifact.sourcesEmpty") }}</p>
        <div v-else class="space-y-2">
          <!-- Source-independence / echo-chamber summary: how many independent origins these sources really are -->
          <div
            v-if="independence && independence.total_sources > 1"
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4"
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
            class="mb-3 rounded-xl border border-red-400/40 bg-red-400/5 p-4"
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
            class="mb-3 rounded-xl border border-red-500/50 bg-red-500/10 p-4"
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
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4"
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
            class="mb-3 rounded-xl border border-bd bg-surface/40 p-4"
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
        <p v-if="conflicts && !conflicts.length" class="text-muted">
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
        <div v-if="verification" class="space-y-6">
          <!-- Honesty meter: one calibrated confidence fused from all trust signals, with its inputs shown -->
          <div v-if="confidence && confidence.components.length" class="rounded-xl border border-bd bg-surface/40 p-4">
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
        <template v-if="redTeam && redTeam.findings.length">
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
                  :href="safeHttpUrl(u) ?? undefined"
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
  transition: background-color 0.15s, opacity 0.2s;
}
/* Hover only where a real hover exists, so a tap never leaves an item lit. */
@media (hover: hover) and (pointer: fine) {
  .export-item:hover {
    background: rgb(var(--c-surface-hover));
  }
}
.export-item:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
</style>
