<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, toRaw, watch } from "vue";
import { useI18n } from "vue-i18n";
import type { TraceEntry } from "@/lib/stream";
import { prefersReducedMotion, smoothOrAuto } from "@/lib/motion";
import { traceKey } from "@/lib/trace";
import { safeHttpUrl } from "@/lib/url";
import { useStickToBottom } from "@/lib/useStickToBottom";

const props = withDefaults(
  defineProps<{
    entries: TraceEntry[];
    reasoning?: string;
    live?: boolean;
    status?: string;
    embedded?: boolean;
  }>(),
  {
    embedded: false,
  }
);

const { t, te, locale } = useI18n();

// Persist open/collapse state across visits (defaults to collapsed if completed).
// The Settings preference "auto-expand the agent console" always starts it open.
const CONSOLE_OPEN_KEY = "activity_console.open";
const AUTO_EXPAND_KEY = "research.auto_expand_console";
function initialOpen(): boolean {
  try {
    if (localStorage.getItem(AUTO_EXPAND_KEY) === "true") return true;
    if (props.status === "completed") return false;
    return localStorage.getItem(CONSOLE_OPEN_KEY) !== "0";
  } catch {
    return props.status !== "completed"; // storage unavailable
  }
}
const open = ref(initialOpen());

watch(
  () => props.status,
  (s) => {
    if (s === "completed") open.value = false;
  }
);

watch(open, (v) => {
  try {
    localStorage.setItem(CONSOLE_OPEN_KEY, v ? "1" : "0");
  } catch {
    /* storage unavailable */
  }
});

// Elapsed time comes from the run's own step timestamps, not from when this page was
// opened (status must be real, apple-design §16): live, it counts on from the first step;
// finished, it is the first-to-last span. Without timestamps there is no timer at all.
function stampMs(entry: TraceEntry): number | null {
  if (!entry.timestamp) return null;
  const ms = Date.parse(entry.timestamp);
  return Number.isNaN(ms) ? null : ms;
}
const startMs = computed(() => {
  for (const entry of props.entries) {
    const ms = stampMs(entry);
    if (ms !== null) return ms;
  }
  return null;
});
const lastMs = computed(() => {
  for (let i = props.entries.length - 1; i >= 0; i--) {
    const ms = stampMs(props.entries[i]);
    if (ms !== null) return ms;
  }
  return null;
});

// Ticks once a second, and only while the run is live.
const now = ref(Date.now());
let ticker: number | undefined;
function syncTicker() {
  if (props.live && ticker === undefined) {
    now.value = Date.now();
    ticker = window.setInterval(() => (now.value = Date.now()), 1000);
  } else if (!props.live && ticker !== undefined) {
    clearInterval(ticker);
    ticker = undefined;
  }
}
onMounted(syncTicker);
watch(() => props.live, syncTicker);
onBeforeUnmount(() => {
  if (ticker !== undefined) clearInterval(ticker);
});

const elapsedMs = computed<number | null>(() => {
  const start = startMs.value;
  if (start === null) return null;
  const last = lastMs.value ?? start;
  // A client clock behind the server's never shows less than the steps already span.
  const end = props.live ? Math.max(now.value, last) : last;
  return Math.max(0, end - start);
});

function formatDuration(ms: number): string {
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const pad = (n: number) => n.toString().padStart(2, "0");
  return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${pad(m)}:${pad(s)}`;
}
const formattedElapsed = computed(() => (elapsedMs.value === null ? null : formatDuration(elapsedMs.value)));

// A run that ended without a report: failed or timed out (an error), or cancelled or
// deleted (a plain stop). Neither may look like a success.
const STOPPED = new Set(["failed", "timeout", "cancelled", "not_found"]);
const stopped = computed(() => STOPPED.has(props.status ?? ""));
const errored = computed(() => props.status === "failed" || props.status === "timeout");

// A finished, collapsed console says "done" once, in one quiet line (§13 Utility: one
// signal per state), instead of a header, a stepper of ticks and a badge repeating it.
const quietDone = computed(() => props.status === "completed" && !props.live && !open.value);

// Phases definition
export interface PipelinePhase {
  id: string;
  key: string;
  icon: string;
}

const PHASES: PipelinePhase[] = [
  { id: "plan", key: "phase_plan", icon: "🧭" },
  { id: "search", key: "phase_search", icon: "🔎" },
  { id: "critic", key: "phase_critic", icon: "⚖️" },
  { id: "synthesis", key: "phase_synthesis", icon: "✨" },
  { id: "verify", key: "phase_verify", icon: "🛡️" },
];

function stepToPhase(entry?: TraceEntry): string {
  if (!entry) return "plan";
  if (entry.phase) return entry.phase;
  const s = entry.step || "";
  if (["plan_start", "clarify", "decompose", "plan_review", "plan_ready", "cross_language"].includes(s)) {
    return "plan";
  }
  if (["search", "crawl", "scrape"].includes(s)) {
    return "search";
  }
  if (["collect_context", "replan", "tie_break", "reputation", "independence"].includes(s)) {
    return "critic";
  }
  if (["analyze"].includes(s)) {
    return "synthesis";
  }
  if (["verify", "redteam", "audit", "viewpoints", "numeric_check", "cross_language_analysis"].includes(s)) {
    return "verify";
  }
  if (["complete", "completed"].includes(s)) {
    return "complete";
  }
  return "plan";
}

const currentEntry = computed(() => {
  if (!props.entries || !props.entries.length) return null;
  return props.entries[props.entries.length - 1];
});

const currentPhase = computed(() => {
  if (props.status === "completed") return "complete";
  return stepToPhase(currentEntry.value || undefined);
});

const phaseIndex = computed(() => {
  const p = currentPhase.value;
  if (p === "complete") return 5;
  const idx = PHASES.findIndex((item) => item.id === p);
  return idx >= 0 ? idx : 0;
});

// The stepper scrolls sideways where the five phases don't fit (the 420px research column,
// a phone). A fade marks each side where more phases wait (§12), and the phase in progress
// (or the one a stopped run ended on) scrolls into view clear of the fade: horizontally
// only, so the thread around the console never jumps.
const stepStrip = ref<HTMLElement | null>(null);
const stripMore = ref({ start: false, end: false });
const EDGE_FADE_PX = 28; // .edge-fade-x
function updateStripOverflow() {
  const el = stepStrip.value;
  stripMore.value = {
    start: !!el && el.scrollLeft > 1,
    end: !!el && el.scrollLeft + el.clientWidth < el.scrollWidth - 1,
  };
}
const stripFade = computed(() => {
  const { start, end } = stripMore.value;
  if (start) return end ? "stepper-fade-both" : "stepper-fade-start";
  return end ? "edge-fade-x" : "";
});
function revealCurrentPhase(animate: boolean) {
  const strip = stepStrip.value;
  if (!strip || props.status === "completed") return updateStripOverflow();
  const item = strip.querySelectorAll<HTMLElement>("[data-phase-state]")[Math.min(phaseIndex.value, PHASES.length - 1)];
  if (!item) return updateStripOverflow();
  const s = strip.getBoundingClientRect();
  const b = item.getBoundingClientRect();
  if (!s.width) return updateStripOverflow(); // not laid out (hidden): nothing to measure
  const delta =
    b.right > s.right - EDGE_FADE_PX ? b.right - s.right + EDGE_FADE_PX
    : b.left < s.left + EDGE_FADE_PX && strip.scrollLeft > 0 ? b.left - s.left - EDGE_FADE_PX
    : 0;
  if (delta) {
    if (typeof strip.scrollBy === "function") strip.scrollBy({ left: delta, behavior: animate ? smoothOrAuto() : "auto" });
    else strip.scrollLeft += delta;
  }
  updateStripOverflow();
}
let stripObserver: ResizeObserver | undefined;
watch(
  stepStrip,
  (el) => {
    stripObserver?.disconnect();
    stripObserver = undefined;
    if (!el) return;
    if (typeof ResizeObserver !== "undefined") {
      stripObserver = new ResizeObserver(updateStripOverflow);
      stripObserver.observe(el);
    }
    revealCurrentPhase(false); // on first sight, already in place
  },
  { flush: "post" },
);
watch(phaseIndex, () => nextTick(() => revealCurrentPhase(true)));
watch(locale, () => nextTick(updateStripOverflow));
onBeforeUnmount(() => stripObserver?.disconnect());

type PhaseState = "done" | "active" | "pending" | "error" | "stopped";

function phaseState(idx: number): PhaseState {
  if (props.status === "completed") return "done";
  if (stopped.value) {
    // The phase the run ended in carries the stop; nothing after it ever ran.
    const at = Math.min(phaseIndex.value, PHASES.length - 1);
    if (idx < at) return "done";
    if (idx > at) return "pending";
    return errored.value ? "error" : "stopped";
  }
  if (phaseIndex.value > idx) return "done";
  if (phaseIndex.value === idx && props.live) return "active";
  if (phaseIndex.value === idx) return "done";
  return "pending";
}

// Active Agent metadata (the role badge is localized: console.agents.<id>)
interface AgentMeta {
  name: string;
  badge: string;
  avatar: string;
  colorClass: string;
}

const AGENT_MAP: Record<string, Omit<AgentMeta, "badge">> = {
  OrchestratorAgent: { name: "Orchestrator", avatar: "🧠", colorClass: "text-indigo-700 dark:text-indigo-400 bg-indigo-500/10 border-indigo-500/30" },
  ClarifierAgent: { name: "Clarifier", avatar: "💬", colorClass: "text-blue-700 dark:text-blue-400 bg-blue-500/10 border-blue-500/30" },
  CrossLanguageAgent: { name: "CrossLanguage", avatar: "🌐", colorClass: "text-cyan-700 dark:text-cyan-400 bg-cyan-500/10 border-cyan-500/30" },
  SearchAgent: { name: "SearchAgent", avatar: "🔍", colorClass: "text-emerald-700 dark:text-emerald-400 bg-emerald-500/10 border-emerald-500/30" },
  SourceCriticAgent: { name: "SourceCritic", avatar: "📑", colorClass: "text-amber-700 dark:text-amber-400 bg-amber-500/10 border-amber-500/30" },
  EvidenceMapperAgent: { name: "EvidenceMapper", avatar: "🗺️", colorClass: "text-amber-700 dark:text-amber-400 bg-amber-500/10 border-amber-500/30" },
  ReplanAgent: { name: "ReplanAgent", avatar: "🔄", colorClass: "text-sky-700 dark:text-sky-400 bg-sky-500/10 border-sky-500/30" },
  SourceReputationAgent: { name: "ReputationAuditor", avatar: "🏛️", colorClass: "text-orange-700 dark:text-orange-400 bg-orange-500/10 border-orange-500/30" },
  SourceIndependenceAgent: { name: "IndependenceAuditor", avatar: "🔗", colorClass: "text-purple-700 dark:text-purple-400 bg-purple-500/10 border-purple-500/30" },
  AnalyzerAgent: { name: "AnalyzerAgent", avatar: "✨", colorClass: "text-violet-700 dark:text-violet-400 bg-violet-500/10 border-violet-500/30" },
  ReportCriticAgent: { name: "ReportCritic", avatar: "📝", colorClass: "text-fuchsia-700 dark:text-fuchsia-400 bg-fuchsia-500/10 border-fuchsia-500/30" },
  RedTeamAgent: { name: "RedTeamAgent", avatar: "⚔️", colorClass: "text-rose-700 dark:text-rose-300 bg-rose-500/10 border-rose-500/30" },
  CitationAuditAgent: { name: "CitationAudit", avatar: "🔬", colorClass: "text-emerald-700 dark:text-emerald-400 bg-emerald-500/10 border-emerald-500/30" },
  NumericCheckAgent: { name: "NumericCheck", avatar: "📊", colorClass: "text-teal-700 dark:text-teal-400 bg-teal-500/10 border-teal-500/30" },
  StanceAgent: { name: "StanceAgent", avatar: "⚖️", colorClass: "text-yellow-700 dark:text-yellow-400 bg-yellow-500/10 border-yellow-500/30" },
  System: { name: "System", avatar: "⚡", colorClass: "text-slate-600 dark:text-slate-400 bg-slate-500/10 border-slate-500/30" },
};

function agentMeta(id: string): AgentMeta {
  const style = AGENT_MAP[id];
  if (style) return { ...style, badge: t(`console.agents.${id}`) };
  return {
    name: id,
    badge: t("console.genericAgent"),
    avatar: "🤖",
    colorClass: "text-accent bg-accent/10 border-accent/30",
  };
}

const currentAgent = computed<AgentMeta>(() => {
  if (props.status === "completed") {
    return {
      name: t("console.reportName"),
      badge: t("console.reportBadge"),
      avatar: "📄",
      colorClass: "text-success bg-success/10 border-success/30",
    };
  }
  if (props.status === "failed") {
    return {
      name: t("console.failedName"),
      badge: t("console.failedBadge"),
      avatar: "⚠️",
      colorClass: "text-danger bg-danger/10 border-danger/30",
    };
  }
  if (props.status === "timeout") {
    return {
      name: t("console.stoppedName"),
      badge: t("status.timeout"),
      avatar: "⚠️",
      colorClass: "text-danger bg-danger/10 border-danger/30",
    };
  }
  if (props.status === "cancelled" || props.status === "not_found") {
    return {
      name: t("console.stoppedName"),
      badge: props.status === "cancelled" ? t("status.cancelled") : t("status.not_found"),
      avatar: "⏹️",
      colorClass: "text-muted bg-surface border-bd",
    };
  }
  const entry = currentEntry.value;
  if (!entry || !entry.agent) {
    if (currentPhase.value === "synthesis") return agentMeta("AnalyzerAgent");
    if (currentPhase.value === "search") return agentMeta("SearchAgent");
    return agentMeta("OrchestratorAgent");
  }
  return agentMeta(entry.agent);
});

function isLoopback(entry?: TraceEntry | null): boolean {
  if (!entry) return false;
  const s = entry.step || "";
  const a = entry.action || "";
  // Loop-backs are identified by their step/action codes only: trail details are written in
  // the research's language and must never be parsed.
  return (
    s === "replan" ||
    s === "tie_break" ||
    s === "verify_retry" ||
    a.includes("loop") ||
    a.includes("replan") ||
    a.includes("tie_break") ||
    a.includes("revision")
  );
}

function loopbackBadgeText(entry: TraceEntry): string {
  const s = entry.step || "";
  const a = entry.action || "";
  if (s === "replan" || a.includes("gap")) return t("console.loopGap");
  if (s === "tie_break" || a.includes("conflict")) return t("console.loopConflict");
  if (s === "verify_retry" || a.includes("revision")) return t("console.loopRevision");
  return t("console.loopGeneric");
}

// What the run is doing now: the latest real step, never text made up from a timer.
const currentStatusDetail = computed(() => {
  switch (props.status) {
    case "completed":
      return t("console.statusCompleted");
    case "failed":
    case "timeout":
      return t("console.statusFailed");
    case "cancelled":
      return t("status.cancelled");
    case "not_found":
      return t("status.not_found");
  }
  return currentEntry.value?.detail || t("trace.live_status");
});

// Filters
type FilterTab = "all" | "search" | "critic" | "thoughts";
const activeFilter = ref<FilterTab>("all");

const filteredEntries = computed(() => {
  if (activeFilter.value === "all") return props.entries;
  if (activeFilter.value === "search") {
    return props.entries.filter(
      (e) =>
        e.phase === "search" ||
        e.step === "search" ||
        (e.sources && e.sources.length > 0)
    );
  }
  if (activeFilter.value === "critic") {
    return props.entries.filter(
      (e) =>
        e.phase === "critic" ||
        e.phase === "verify" ||
        isLoopback(e) ||
        ["collect_context", "verify", "audit", "redteam", "viewpoints", "numeric_check"].includes(e.step)
    );
  }
  return props.entries;
});

// The feed and the reasoning follow new content only while the reader is at their bottom
// (apple-design §3): scrolling up to read lets go, the "to latest" pill re-pins. Follows
// are instant and at most one per frame, never a smooth scroll per streamed token.
const feedBox = ref<HTMLElement | null>(null);
const feed = useStickToBottom(feedBox, { threshold: 60 });
const reasoningBox = ref<HTMLElement | null>(null);
const reasoningOpen = ref(true);
const cot = useStickToBottom(reasoningBox);

// Only a step that really arrives live rises in (§7); a reload, a replay of the whole
// trail or a filter switch renders at once instead of as a wave.
const animateFeed = ref(false);
watch(
  () => props.entries.length,
  (n, o) => {
    animateFeed.value = !!props.live && n - (o ?? 0) === 1;
    nextTick(feed.follow);
  },
);
watch(activeFilter, () => {
  animateFeed.value = false;
  nextTick(feed.follow);
});
watch(
  () => props.reasoning,
  () => nextTick(cot.follow),
);
watch(reasoningOpen, (v) => {
  if (v) nextTick(cot.follow);
});

// v-show drops a scroller's position, so collapsing remembers where the reader was.
const savedFeedScrollTop = ref<number | null>(null);
const savedReasoningScrollTop = ref<number | null>(null);

function onReasoningScroll() {
  if (!reasoningBox.value) return;
  savedReasoningScrollTop.value = reasoningBox.value.scrollTop;
}

function toggleOpen() {
  if (open.value) {
    if (feedBox.value) savedFeedScrollTop.value = feedBox.value.scrollTop;
    if (reasoningBox.value) savedReasoningScrollTop.value = reasoningBox.value.scrollTop;
    open.value = false;
  } else {
    open.value = true;
    nextTick(() => {
      // Still following: show what arrived while closed. Otherwise: the reader's place.
      if (feed.pinned.value) feed.follow();
      else if (feedBox.value && savedFeedScrollTop.value !== null) feedBox.value.scrollTop = savedFeedScrollTop.value;
      if (cot.pinned.value) cot.follow();
      else if (reasoningBox.value && savedReasoningScrollTop.value !== null) {
        reasoningBox.value.scrollTop = savedReasoningScrollTop.value;
      }
    });
  }
}

// The latest step overall (not the last one a filter shows) is the one running, or the
// one a stopped run ended on; every step before it finished.
type EntryState = "done" | "running" | "error" | "stopped";

function entryState(entry: TraceEntry): EntryState {
  const latest = currentEntry.value;
  const isLatest = !!latest && toRaw(entry) === toRaw(latest);
  if (!isLatest || props.status === "completed") return "done";
  if (stopped.value) return errored.value ? "error" : "stopped";
  return props.live ? "running" : "done";
}

// The journal body opens and closes on its real height, at one speed both ways (§7),
// and a toggle mid-way reverses from the height on screen (§3). The live value is read
// when the next hook starts: Vue has already hidden a v-show element by the time it
// reports a cancelled leave, so its height would read 0 there. The interrupted
// animation is still applied at that moment, so the element shows where it was.
const EXPAND_MS = 280;
const EXPAND_EASE = "cubic-bezier(0.32, 0.72, 0, 1)";
let expandAnim: Animation | null = null;

function animateBody(el: Element, opening: boolean, done: () => void) {
  const node = el as HTMLElement;
  const interrupted = expandAnim;
  expandAnim = null;
  if (typeof node.animate !== "function" || prefersReducedMotion()) {
    interrupted?.cancel();
    node.style.overflow = "";
    done();
    return;
  }
  const fromHeight = interrupted || !opening ? node.getBoundingClientRect().height : 0;
  const fromOpacity = interrupted ? Number(getComputedStyle(node).opacity) : opening ? 0 : 1;
  interrupted?.cancel();
  const toHeight = opening ? node.offsetHeight : 0; // measured without the old animation
  node.style.overflow = "hidden";
  const anim = node.animate(
    [
      { height: `${fromHeight}px`, opacity: fromOpacity },
      { height: `${toHeight}px`, opacity: opening ? 1 : 0 },
    ],
    { duration: EXPAND_MS, easing: EXPAND_EASE },
  );
  expandAnim = anim;
  anim.onfinish = () => {
    if (expandAnim === anim) expandAnim = null;
    node.style.overflow = "";
    done();
  };
}
const onBodyEnter = (el: Element, done: () => void) => animateBody(el, true, done);
const onBodyLeave = (el: Element, done: () => void) => animateBody(el, false, done);
onBeforeUnmount(() => expandAnim?.cancel());

function stepLabel(step: string): string {
  return te(`trace.${step}`) ? t(`trace.${step}`) : step;
}

function formatTime(isoStr?: string): string {
  if (!isoStr) return "";
  try {
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return "";
  }
}
</script>

<template>
  <div :class="embedded ? '' : 'rounded-xl border border-bd/80 bg-surface shadow-sm overflow-hidden'">
    <!-- FINISHED AND COLLAPSED: one completion line -->
    <div v-if="quietDone" class="flex items-center gap-1.5 px-4 py-2.5 text-xs text-muted">
      <span class="text-success" aria-hidden="true">✓</span>
      <span class="tabular-nums">{{ formattedElapsed ? t("console.doneIn", { time: formattedElapsed }) : t("status.completed") }}</span>
      <span aria-hidden="true">·</span>
      <button
        type="button"
        class="press inline-flex items-center gap-1 rounded-md px-1 py-0.5 hover:text-ink"
        :aria-expanded="open"
        @click="toggleOpen"
      >
        <span class="tabular-nums">{{ t("console.journal", { n: entries.length }) }}</span>
        <span class="text-3xs" aria-hidden="true">▸</span>
      </button>
    </div>

    <template v-else>
      <!-- TOP AGENT HUD HEADER BAR -->
      <!-- The running agent's name is the main live signal, so it never gives way: where the
           row is too narrow for it and the controls (the 420px research column, a phone),
           the controls wrap under it, still at the right edge. -->
      <div data-console-header class="px-4 py-2.5 flex flex-wrap items-center justify-between gap-x-3 gap-y-2 min-w-0">
        <!-- Active Agent Identity -->
        <div class="flex items-center gap-2.5 min-w-0 flex-[1_1_10rem]">
          <div class="relative flex items-center justify-center h-8 w-8 rounded-lg border text-base shadow-inner shrink-0" :class="currentAgent.colorClass">
            <span>{{ currentAgent.avatar }}</span>
            <!-- The console's one live signal: a slow breath, not a pulse or a ping. Accent
                 like every running marker here; green means a finished step (§16 Familiarity). -->
            <span
              v-if="live"
              data-live-dot
              class="live-dot absolute -top-1 -right-1 h-2.5 w-2.5 rounded-full bg-accent ring-2 ring-surface"
            />
          </div>

          <div class="min-w-0 flex-1">
            <!-- Short of room, the role badge gives way before the name does. -->
            <div class="flex items-center gap-2 min-w-0 overflow-hidden">
              <span data-agent-name class="max-w-full shrink-0 truncate font-semibold text-sm text-ink leading-tight">
                {{ currentAgent.name }}
              </span>
              <span
                class="min-w-0 truncate rounded px-1.5 py-px text-3xs font-semibold uppercase tracking-wider border"
                :class="currentAgent.colorClass"
                :title="currentAgent.badge"
              >
                {{ currentAgent.badge }}
              </span>
            </div>
            <div class="text-xs text-muted flex items-center gap-1.5 min-w-0 mt-0.5" :title="currentStatusDetail">
              <span class="truncate block min-w-0">{{ currentStatusDetail }}</span>
            </div>
          </div>
        </div>

        <!-- Controls & Elapsed Timer -->
        <div class="ml-auto flex items-center gap-2 text-xs shrink-0">
          <div v-if="formattedElapsed" class="hidden sm:flex items-center gap-1 text-muted/90 bg-bg/50 px-2 py-1 rounded-md border border-bd shrink-0">
            <span class="text-muted">⏱</span>
            <span class="tabular-nums text-2xs">{{ formattedElapsed }}</span>
          </div>

          <!-- Both labels share one cell, the idle one invisible, so the button keeps the
               wider one's width in every language: toggling never moves it or rewraps the row. -->
          <button
            data-console-toggle
            class="flex items-center justify-center gap-1 px-2.5 py-1 rounded-md border border-bd hover:bg-surface/60 text-muted hover:text-ink transition shrink-0 whitespace-nowrap"
            :title="open ? t('console.collapse') : t('console.expand')"
            :aria-expanded="open"
            @click="toggleOpen"
          >
            <span class="grid tabular-nums">
              <span class="col-start-1 row-start-1" :class="!open && 'invisible'">{{ t("console.collapse") }}</span>
              <span class="col-start-1 row-start-1" :class="open && 'invisible'">{{ t("console.journal", { n: entries.length }) }}</span>
            </span>
            <span
              class="text-3xs transition-transform duration-200 motion-reduce:transition-none"
              :class="open && 'rotate-90'"
              aria-hidden="true"
            >▸</span>
          </button>
        </div>
      </div>

      <!-- PIPELINE STEPPER BAR (while live, or with the journal open) -->
      <!-- Its bottom line only separates it from an open body; closed, it would double the card's edge. -->
      <!-- The fade masks only the scrolling row, never the band's tint or its bottom line. -->
      <div v-if="open || live" class="bg-bg/30" :class="open && 'border-b border-bd/60'">
        <div
          ref="stepStrip"
          data-stepper
          class="px-4 py-2.5 flex items-center justify-between gap-1 overflow-x-auto text-xs scrollbar-none"
          :class="stripFade"
          @scroll.passive="updateStripOverflow"
        >
          <div
            v-for="(phase, idx) in PHASES"
            :key="phase.id"
            class="flex items-center gap-1.5 shrink-0 transition-opacity duration-300"
            :data-phase-state="phaseState(idx)"
            :class="{
              'opacity-100': phaseState(idx) !== 'pending',
              'opacity-40': phaseState(idx) === 'pending',
            }"
          >
            <div
              class="flex items-center justify-center h-5 w-5 rounded-full text-2xs font-semibold border transition-colors duration-300"
              :class="{
                'bg-success/15 text-success border-success/40': phaseState(idx) === 'done',
                'bg-accent text-onAccent border-accent ring-4 ring-accent/20': phaseState(idx) === 'active',
                'bg-danger/15 text-danger border-danger/40': phaseState(idx) === 'error',
                'bg-surface text-muted border-bd': phaseState(idx) === 'pending' || phaseState(idx) === 'stopped',
              }"
            >
              <span v-if="phaseState(idx) === 'done'">✓</span>
              <span v-else-if="phaseState(idx) === 'error'">✕</span>
              <span v-else-if="phaseState(idx) === 'stopped'">–</span>
              <span v-else class="tabular-nums">{{ idx + 1 }}</span>
            </div>

            <span
              class="text-xs whitespace-nowrap font-medium transition-colors"
              :class="phaseState(idx) === 'active' ? 'text-accent font-semibold' : phaseState(idx) === 'error' ? 'text-danger' : phaseState(idx) === 'done' ? 'text-ink' : 'text-muted'"
            >
              {{ $t('trace.' + phase.key) }}
            </span>

            <!-- Connector line -->
            <div
              v-if="idx < PHASES.length - 1"
              class="h-0.5 w-4 sm:w-8 rounded-full mx-1 transition-colors duration-300"
              :class="idx < phaseIndex ? 'bg-success/60' : idx === phaseIndex && live ? 'bg-accent/60' : 'bg-bd'"
            />
          </div>
        </div>
      </div>
    </template>

    <!-- EXPANDABLE DETAILS BODY (State preserved across collapse/expand) -->
    <Transition name="console-expand" :css="false" @enter="onBodyEnter" @leave="onBodyLeave">
      <div v-show="open" class="p-4 space-y-4 max-h-[60vh] overflow-y-auto">
        <!-- LIVE SYNTHESIS DEEP PROGRESS CARD -->
        <div
          v-if="live && currentPhase === 'synthesis'"
          class="rounded-xl border border-violet-500/30 bg-gradient-to-r from-violet-500/10 via-accent/10 to-indigo-500/10 p-3.5 shadow-sm relative overflow-hidden"
        >
          <div class="flex items-start justify-between gap-3 relative z-10">
            <div class="flex items-center gap-2.5">
              <div class="h-8 w-8 rounded-lg bg-violet-500/20 border border-violet-500/40 flex items-center justify-center text-base shrink-0">
                ✨
              </div>
              <div>
                <div class="flex items-center gap-2">
                  <span class="font-semibold text-xs text-ink">{{ t("console.synthesisTitle") }}</span>
                  <span v-if="currentEntry?.metrics?.attempt" class="rounded bg-bg/70 text-muted px-1.5 py-px text-3xs tabular-nums">
                    {{ t("console.iteration", { n: currentEntry.metrics.attempt }) }}
                  </span>
                </div>
                <p class="text-xs text-ink mt-1 font-medium leading-snug">
                  {{ currentStatusDetail }}
                </p>
              </div>
            </div>

            <div v-if="formattedElapsed" class="flex items-center gap-1.5 text-xs text-accent shrink-0 tabular-nums bg-bg/60 px-2 py-1 rounded-md border border-violet-500/30">
              <span>{{ formattedElapsed }}</span>
            </div>
          </div>

          <!-- Indeterminate progress: synthesis has no percentage to show. With reduced
               motion the moving segment becomes a still, striped fill (see the style block),
               never a plain full bar that would read as finished. -->
          <div
            role="progressbar"
            :aria-label="t('console.synthesisTitle')"
            :aria-valuetext="currentStatusDetail"
            class="mt-3 h-1.5 w-full rounded-full bg-surface/80 overflow-hidden relative"
          >
            <div class="h-full bg-gradient-to-r from-violet-500 via-accent to-emerald-400 rounded-full animate-progress-indeterminate" />
          </div>
        </div>

        <!-- LIVE CHAIN-OF-THOUGHT (THINKING) TERMINAL -->
        <div v-if="reasoning" class="rounded-lg border border-bd bg-rail shadow-inner overflow-hidden">
          <div class="flex items-center justify-between px-3 py-1.5 bg-surface/60 border-b border-bd text-xs">
            <div class="flex items-center gap-2">
              <span class="font-mono text-2xs text-muted flex items-center gap-1.5">
                <span class="text-accentSoft">🧠</span>
                {{ $t("trace.chain_of_thought") }}
              </span>
            </div>

            <div class="flex items-center gap-2">
              <span class="text-3xs text-muted tabular-nums">
                {{ t("console.chars", { n: reasoning.length }) }}
              </span>
              <button
                type="button"
                class="text-muted hover:text-ink transition px-1"
                :aria-expanded="reasoningOpen"
                @click="reasoningOpen = !reasoningOpen"
              >
                {{ reasoningOpen ? "▾" : "▸" }}
              </button>
            </div>
          </div>

          <!-- The caret marks where the text grows and holds still: the streaming text is
               motion enough, and a blink would be one more loop beside the live dot. -->
          <div
            v-if="reasoningOpen"
            ref="reasoningBox"
            class="p-3 max-h-56 overflow-y-auto font-mono text-xs leading-relaxed whitespace-pre-wrap text-ink/85 selection:bg-accent/30"
            @scroll="onReasoningScroll"
          >
            {{ reasoning }}<span v-if="live" data-caret aria-hidden="true" class="inline-block h-3.5 w-1.5 ml-0.5 bg-accent/70 align-middle" />
          </div>
        </div>

        <!-- FILTER TABS FOR EVENT FEED -->
        <div class="flex items-center gap-2 border-b border-bd pb-2 text-xs">
          <button
            class="px-2.5 py-1 rounded-md transition"
            :class="activeFilter === 'all' ? 'bg-accent/15 text-accent font-medium border border-accent/30' : 'text-muted hover:text-ink'"
            @click="activeFilter = 'all'"
          >
            {{ $t("trace.filter_all") }} <span class="opacity-70 tabular-nums">({{ entries.length }})</span>
          </button>
          <button
            class="px-2.5 py-1 rounded-md transition"
            :class="activeFilter === 'search' ? 'bg-accent/15 text-accent font-medium border border-accent/30' : 'text-muted hover:text-ink'"
            @click="activeFilter = 'search'"
          >
            {{ $t("trace.filter_search") }}
          </button>
          <button
            class="px-2.5 py-1 rounded-md transition"
            :class="activeFilter === 'critic' ? 'bg-accent/15 text-accent font-medium border border-accent/30' : 'text-muted hover:text-ink'"
            @click="activeFilter = 'critic'"
          >
            {{ $t("trace.filter_critic") }}
          </button>
        </div>

        <!-- STREAM OF MICRO-ACTIONS -->
        <div class="relative">
          <div ref="feedBox" class="max-h-[380px] overflow-y-auto pr-1">
            <TransitionGroup
              name="feed-item"
              tag="div"
              class="relative space-y-2.5"
              :css="animateFeed"
              :move-class="animateFeed ? 'feed-item-move' : 'feed-item-still'"
            >
              <div
                v-for="entry in filteredEntries"
                :key="traceKey(entry)"
                :data-entry-state="entryState(entry)"
                class="group rounded-lg border border-bd/40 bg-surface/50 p-2.5 hover:border-bd hover:bg-surface/80 transition-colors duration-150 text-xs"
                :class="{
                  'border-success/30 bg-surface/60': entryState(entry) === 'done' && !isLoopback(entry),
                  'border-accent/40 ring-1 ring-accent/20 bg-accent/5': entryState(entry) === 'running' && !isLoopback(entry),
                  'border-danger/30 bg-danger/5': entryState(entry) === 'error' && !isLoopback(entry),
                  'border-warning/40 bg-warning/5 ring-1 ring-warning/20': isLoopback(entry),
                }"
              >
                <!-- Loopback Badge when agent is sent back -->
                <div
                  v-if="isLoopback(entry)"
                  class="mb-2 inline-flex items-center gap-1.5 rounded-md bg-warning/10 border border-warning/30 px-2 py-0.5 text-3xs font-semibold text-warning"
                >
                  <!-- Spins only while this loop-back is the running step; the label's own ↩ stays. -->
                  <span v-if="entryState(entry) === 'running'" class="animate-spin">↺</span>
                  <span>{{ loopbackBadgeText(entry) }}</span>
                </div>

                <div class="flex items-start justify-between gap-2">
                  <div class="flex items-center gap-2 min-w-0">
                    <!-- Status icon/number -->
                    <span
                      v-if="entryState(entry) === 'done'"
                      class="flex items-center justify-center h-4 w-4 rounded-full bg-success/15 border border-success/40 text-success text-3xs font-bold shrink-0 shadow-e1"
                      :title="t('console.entryDone')"
                    >
                      ✓
                    </span>
                    <span
                      v-else-if="entryState(entry) === 'running'"
                      class="flex items-center justify-center h-4 w-4 rounded-full bg-accent text-onAccent text-[9px] font-bold shrink-0 ring-2 ring-accent/30"
                      :title="t('console.entryRunning')"
                    >
                      ⚡
                    </span>
                    <span
                      v-else-if="entryState(entry) === 'error'"
                      class="flex items-center justify-center h-4 w-4 rounded-full bg-danger/15 border border-danger/40 text-danger text-3xs font-bold shrink-0"
                      :title="t('console.entryStopped')"
                    >
                      ✕
                    </span>
                    <span
                      v-else
                      class="flex items-center justify-center h-4 w-4 rounded-full bg-surface border border-bd text-muted text-3xs font-bold shrink-0"
                      :title="t('console.entryStopped')"
                    >
                      –
                    </span>

                    <span v-if="entry.agent" class="font-medium text-ink truncate">
                      {{ entry.agent }}
                    </span>
                    <span v-else class="font-medium text-ink truncate">
                      {{ stepLabel(entry.step) }}
                    </span>

                    <span
                      v-if="entry.action"
                      class="rounded bg-bg/60 border border-bd/60 px-1 py-px text-3xs text-muted font-mono"
                    >
                      {{ entry.action }}
                    </span>

                    <!-- Status label: only for the step that is running or where the run stopped;
                         a finished step's ✓ already says it (one signal, §13). -->
                    <span
                      v-if="entryState(entry) === 'running'"
                      class="inline-flex items-center rounded bg-accent/15 border border-accent/30 px-1.5 py-px text-3xs font-medium text-accent shrink-0"
                    >
                      {{ t("console.entryRunning") }}
                    </span>
                    <span
                      v-else-if="entryState(entry) !== 'done'"
                      class="inline-flex items-center rounded border px-1.5 py-px text-3xs font-medium shrink-0"
                      :class="entryState(entry) === 'error' ? 'bg-danger/10 border-danger/30 text-danger' : 'bg-surface border-bd text-muted'"
                    >
                      {{ t("console.entryStopped") }}
                    </span>
                  </div>

                  <span v-if="entry.timestamp" class="text-3xs tabular-nums text-muted/60 shrink-0">
                    {{ formatTime(entry.timestamp) }}
                  </span>
                </div>

                <p v-if="entry.detail" class="mt-1 text-muted leading-snug pl-6 break-words">
                  {{ entry.detail }}
                </p>

                <!-- Metrics badges if present -->
                <div v-if="entry.metrics" class="mt-1.5 pl-6 flex flex-wrap gap-1.5">
                  <span
                    v-for="(val, key) in entry.metrics"
                    :key="key"
                    class="inline-flex items-center gap-1 rounded bg-bg/80 border border-bd/80 px-1.5 py-0.5 text-3xs text-muted"
                  >
                    <span class="opacity-60">{{ key }}:</span>
                    <span class="font-semibold text-ink tabular-nums">{{ val }}</span>
                  </span>
                </div>

                <!-- Discovered / Scraped Sources Chips -->
                <div v-if="entry.sources && entry.sources.length" class="mt-2 pl-6 flex flex-wrap gap-1.5">
                  <a
                    v-for="(s, j) in entry.sources"
                    :key="j"
                    :href="safeHttpUrl(s.url) ?? safeHttpUrl(`https://${s.domain}`) ?? undefined"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="inline-flex max-w-[220px] items-center gap-1.5 rounded-md border border-bd/70 bg-bg/60 px-2 py-0.5 text-2xs text-ink hover:border-accent hover:text-accent transition shadow-e1"
                    :title="s.title || s.domain"
                  >
                    <img
                      :src="`https://www.google.com/s2/favicons?domain=${s.domain}&sz=32`"
                      alt=""
                      class="h-3 w-3 shrink-0 rounded-sm"
                      loading="lazy"
                      @error="($event.target as HTMLImageElement).style.display = 'none'"
                    />
                    <span class="truncate">{{ s.domain }}</span>
                    <span class="text-[9px] text-muted opacity-60">↗</span>
                  </a>
                </div>
              </div>
            </TransitionGroup>
          </div>

          <div v-if="!filteredEntries.length" class="py-6 text-center text-xs text-muted">
            {{ $t("artifact.trailEmpty") }}
          </div>

          <!-- Floating button to return to latest actions -->
          <Transition name="fade-quick">
            <div v-if="live && !feed.pinned.value" class="absolute bottom-2 inset-x-0 flex justify-center pointer-events-none">
              <button
                type="button"
                class="press pointer-events-auto flex items-center gap-1.5 rounded-full bg-accent px-3 py-1 text-2xs font-medium text-onAccent shadow-e2 hover:bg-accent/90"
                @click="feed.jumpToLatest()"
              >
                <span aria-hidden="true">↓</span>
                <span>{{ t("trace.to_latest") }}</span>
              </button>
            </div>
          </Transition>
        </div>
      </div>
    </Transition>
  </div>
</template>

<style scoped>
@keyframes progress-indeterminate {
  0% { transform: translateX(-100%) scaleX(0.2); }
  50% { transform: translateX(0%) scaleX(0.7); }
  100% { transform: translateX(100%) scaleX(0.2); }
}
.animate-progress-indeterminate {
  animation: progress-indeterminate 2.2s infinite ease-in-out;
  transform-origin: 0% 50%;
}
/* Reduced motion stops the slide (§14, here and in style.css). On its own that would leave
   the fill as a full, plain bar that looks finished while synthesis still runs. Still
   stripes instead: the familiar "working" pattern, with no motion at all. */
@media (prefers-reduced-motion: reduce) {
  .animate-progress-indeterminate {
    animation: none;
    transform: none;
    background-image: repeating-linear-gradient(
      -45deg,
      rgb(var(--c-accent) / 0.75) 0 4px,
      rgb(var(--c-accent) / 0.2) 4px 8px
    );
  }
}

/* The stepper's fades at its start, and at both ends (.edge-fade-x is the end-only one). */
.stepper-fade-start {
  -webkit-mask-image: linear-gradient(to right, transparent, #000 28px);
  mask-image: linear-gradient(to right, transparent, #000 28px);
}
.stepper-fade-both {
  -webkit-mask-image: linear-gradient(to right, transparent, #000 28px, #000 calc(100% - 28px), transparent);
  mask-image: linear-gradient(to right, transparent, #000 28px, #000 calc(100% - 28px), transparent);
}

/* A live step rises in from below, where the feed grows (§7); nothing animates out. */
.feed-item-enter-active {
  transition: opacity 220ms var(--ease-emph), transform 260ms var(--ease-emph);
}
.feed-item-enter-from {
  opacity: 0;
  transform: translateY(6px);
}
.feed-item-move {
  transition: transform 260ms var(--ease-emph);
}
</style>
