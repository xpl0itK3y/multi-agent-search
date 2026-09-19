<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import type { TraceEntry } from "@/lib/stream";

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

const { t, te } = useI18n();

// Persist open/collapse state across visits (defaults to collapsed if completed)
const CONSOLE_OPEN_KEY = "activity_console.open";
const open = ref(
  props.status === "completed"
    ? false
    : typeof localStorage !== "undefined"
    ? localStorage.getItem(CONSOLE_OPEN_KEY) !== "0"
    : true
);

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

// Timer for elapsed seconds
const elapsedSeconds = ref(0);
let timerInterval: number | undefined;

onMounted(() => {
  if (props.live) {
    timerInterval = window.setInterval(() => {
      elapsedSeconds.value += 1;
    }, 1000);
  }
});

watch(
  () => props.live,
  (isLive) => {
    if (isLive && !timerInterval) {
      timerInterval = window.setInterval(() => {
        elapsedSeconds.value += 1;
      }, 1000);
    } else if (!isLive && timerInterval) {
      clearInterval(timerInterval);
      timerInterval = undefined;
    }
  }
);

onBeforeUnmount(() => {
  if (timerInterval) clearInterval(timerInterval);
});

const formattedElapsed = computed(() => {
  const m = Math.floor(elapsedSeconds.value / 60);
  const s = elapsedSeconds.value % 60;
  return `${m.toString().padStart(2, "0")}:${s.toString().padStart(2, "0")}`;
});

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

function phaseState(idx: number): "done" | "active" | "pending" {
  if (props.status === "completed" || phaseIndex.value > idx) return "done";
  if (phaseIndex.value === idx && props.live) return "active";
  if (phaseIndex.value === idx) return "done";
  return "pending";
}

// Active Agent metadata
interface AgentMeta {
  name: string;
  badge: string;
  avatar: string;
  colorClass: string;
}

const AGENT_MAP: Record<string, AgentMeta> = {
  OrchestratorAgent: { name: "Orchestrator", badge: "Архитектор", avatar: "🧠", colorClass: "text-indigo-400 bg-indigo-500/10 border-indigo-500/30" },
  ClarifierAgent: { name: "Clarifier", badge: "Уточнение", avatar: "💬", colorClass: "text-blue-400 bg-blue-500/10 border-blue-500/30" },
  CrossLanguageAgent: { name: "CrossLanguage", badge: "Мультиязычность", avatar: "🌐", colorClass: "text-cyan-400 bg-cyan-500/10 border-cyan-500/30" },
  SearchAgent: { name: "SearchAgent", badge: "Поисковик", avatar: "🔍", colorClass: "text-emerald-400 bg-emerald-500/10 border-emerald-500/30" },
  SourceCriticAgent: { name: "SourceCritic", badge: "Критик данных", avatar: "📑", colorClass: "text-amber-400 bg-amber-500/10 border-amber-500/30" },
  EvidenceMapperAgent: { name: "EvidenceMapper", badge: "Картограф", avatar: "🗺️", colorClass: "text-amber-400 bg-amber-500/10 border-amber-500/30" },
  ReplanAgent: { name: "ReplanAgent", badge: "Корректировщик", avatar: "🔄", colorClass: "text-sky-400 bg-sky-500/10 border-sky-500/30" },
  SourceReputationAgent: { name: "ReputationAuditor", badge: "Репутация", avatar: "🏛️", colorClass: "text-orange-400 bg-orange-500/10 border-orange-500/30" },
  SourceIndependenceAgent: { name: "IndependenceAuditor", badge: "Аффилиации", avatar: "🔗", colorClass: "text-purple-400 bg-purple-500/10 border-purple-500/30" },
  AnalyzerAgent: { name: "AnalyzerAgent", badge: "Синтез", avatar: "✨", colorClass: "text-violet-400 bg-violet-500/10 border-violet-500/30" },
  ReportCriticAgent: { name: "ReportCritic", badge: "Рецензент", avatar: "📝", colorClass: "text-fuchsia-400 bg-fuchsia-500/10 border-fuchsia-500/30" },
  RedTeamAgent: { name: "RedTeamAgent", badge: "Стресс-тест", avatar: "⚔️", colorClass: "text-rose-400 bg-rose-500/10 border-rose-500/30" },
  CitationAuditAgent: { name: "CitationAudit", badge: "Фактчекинг", avatar: "🔬", colorClass: "text-emerald-400 bg-emerald-500/10 border-emerald-500/30" },
  NumericCheckAgent: { name: "NumericCheck", badge: "Числа & Даты", avatar: "📊", colorClass: "text-teal-400 bg-teal-500/10 border-teal-500/30" },
  StanceAgent: { name: "StanceAgent", badge: "Нейтральность", avatar: "⚖️", colorClass: "text-yellow-400 bg-yellow-500/10 border-yellow-500/30" },
  System: { name: "System", badge: "Система", avatar: "⚡", colorClass: "text-slate-400 bg-slate-500/10 border-slate-500/30" },
};

const currentAgent = computed<AgentMeta>(() => {
  if (props.status === "completed") {
    return {
      name: "Аналитический отчёт",
      badge: "Готово ✓",
      avatar: "📄",
      colorClass: "text-emerald-400 bg-emerald-500/10 border-emerald-500/30",
    };
  }
  if (props.status === "failed") {
    return {
      name: "Сбой анализа",
      badge: "Ошибка",
      avatar: "⚠️",
      colorClass: "text-red-400 bg-red-500/10 border-red-500/30",
    };
  }
  const entry = currentEntry.value;
  if (!entry || !entry.agent) {
    if (currentPhase.value === "synthesis") return AGENT_MAP.AnalyzerAgent;
    if (currentPhase.value === "search") return AGENT_MAP.SearchAgent;
    return AGENT_MAP.OrchestratorAgent;
  }
  return (
    AGENT_MAP[entry.agent] || {
      name: entry.agent,
      badge: "Агент",
      avatar: "🤖",
      colorClass: "text-accent bg-accent/10 border-accent/30",
    }
  );
});

const currentStatusDetail = computed(() => {
  if (props.status === "completed") {
    return "Все 5 этапов исследования завершены. Итоговый отчёт готов к изучению.";
  }
  if (props.status === "failed") {
    return "Произошла ошибка при формировании отчёта.";
  }
  return currentEntry.value?.detail || (te("trace.live_status") ? t("trace.live_status") : "Агенты работают в реальном времени");
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
        ["collect_context", "verify", "audit", "redteam", "viewpoints", "numeric_check"].includes(e.step)
    );
  }
  return props.entries;
});

// Micro-actions feed auto-scroll
const feedBox = ref<HTMLElement | null>(null);
const userHasScrolledUp = ref(false);

function onFeedScroll() {
  if (!feedBox.value) return;
  const { scrollTop, scrollHeight, clientHeight } = feedBox.value;
  const atBottom = scrollHeight - (scrollTop + clientHeight) < 60;
  userHasScrolledUp.value = !atBottom;
}

function scrollToBottom(behavior: ScrollBehavior = "smooth") {
  if (!feedBox.value) return;
  feedBox.value.scrollTo({
    top: feedBox.value.scrollHeight,
    behavior,
  });
}

watch(
  () => props.entries.length,
  () => {
    if (props.live && !userHasScrolledUp.value) {
      nextTick(() => {
        scrollToBottom("smooth");
      });
    }
  }
);

// Reasoning auto-scroll
const reasoningBox = ref<HTMLElement | null>(null);
const reasoningOpen = ref(true);

watch(
  () => props.reasoning,
  () => {
    if (reasoningOpen.value && reasoningBox.value) {
      nextTick(() => {
        reasoningBox.value?.scrollTo({
          top: reasoningBox.value.scrollHeight,
          behavior: "smooth",
        });
      });
    }
  }
);

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
  <div
    class="transition-all duration-300"
    :class="embedded ? '' : 'rounded-xl border border-bd/80 bg-surface/80 backdrop-blur-md shadow-sm overflow-hidden'"
  >
    <!-- TOP AGENT HUD HEADER BAR -->
    <div class="px-4 py-3 bg-surface/90 border-b border-bd flex flex-wrap items-center justify-between gap-3">
      <!-- Active Agent Identity -->
      <div class="flex items-center gap-2.5 min-w-0">
        <div class="relative flex items-center justify-center h-8 w-8 rounded-lg border text-base shadow-inner shrink-0" :class="currentAgent.colorClass">
          <span>{{ currentAgent.avatar }}</span>
          <span
            v-if="live"
            class="absolute -top-1 -right-1 h-2.5 w-2.5 rounded-full bg-emerald-400 ring-2 ring-surface animate-pulse"
          />
        </div>

        <div class="min-w-0">
          <div class="flex items-center gap-2">
            <span class="font-semibold text-sm text-ink leading-tight truncate">
              {{ currentAgent.name }}
            </span>
            <span class="rounded px-1.5 py-0.2 text-[10px] font-medium uppercase tracking-wider border" :class="currentAgent.colorClass">
              {{ currentAgent.badge }}
            </span>
          </div>
          <div class="text-xs text-muted truncate flex items-center gap-1.5">
            <span v-if="live" class="inline-block h-1.5 w-1.5 rounded-full bg-accent animate-ping" />
            <span class="truncate">{{ currentStatusDetail }}</span>
          </div>
        </div>
      </div>

      <!-- Controls & Elapsed Timer -->
      <div class="flex items-center gap-2 text-xs">
        <div class="flex items-center gap-1 text-muted/90 bg-bg/50 px-2 py-1 rounded-md border border-bd">
          <span class="text-muted">⏱</span>
          <span class="font-mono text-[11px]">{{ formattedElapsed }}</span>
        </div>

        <button
          class="flex items-center gap-1 px-2.5 py-1 rounded-md border border-bd hover:bg-surface/60 text-muted hover:text-ink transition"
          :title="open ? 'Свернуть журнал' : 'Развернуть журнал'"
          @click="open = !open"
        >
          <span>{{ open ? "Свернуть журнал" : `Журнал агентов (${entries.length})` }}</span>
          <span class="text-[10px]">{{ open ? "▾" : "▸" }}</span>
        </button>
      </div>
    </div>

    <!-- PIPELINE STEPPER BAR (Always visible or under header) -->
    <div class="px-4 py-2.5 bg-bg/30 border-b border-bd/60 flex items-center justify-between gap-1 overflow-x-auto text-xs scrollbar-none">
      <div
        v-for="(phase, idx) in PHASES"
        :key="phase.id"
        class="flex items-center gap-1.5 shrink-0 transition-opacity"
        :class="{
          'opacity-100': phaseState(idx) !== 'pending',
          'opacity-40': phaseState(idx) === 'pending',
        }"
      >
        <div
          class="flex items-center justify-center h-5 w-5 rounded-full text-[11px] font-semibold border transition-all"
          :class="{
            'bg-emerald-500/20 text-emerald-400 border-emerald-500/40': phaseState(idx) === 'done',
            'bg-accent/20 text-accent border-accent animate-pulse ring-2 ring-accent/20': phaseState(idx) === 'active',
            'bg-surface text-muted border-bd': phaseState(idx) === 'pending',
          }"
        >
          <span v-if="phaseState(idx) === 'done'">✓</span>
          <span v-else>{{ idx + 1 }}</span>
        </div>

        <span
          class="text-[12px] whitespace-nowrap font-medium"
          :class="phaseState(idx) === 'active' ? 'text-accent font-semibold' : phaseState(idx) === 'done' ? 'text-ink' : 'text-muted'"
        >
          {{ $t('trace.' + phase.key) }}
        </span>

        <!-- Connector line -->
        <div
          v-if="idx < PHASES.length - 1"
          class="h-0.5 w-4 sm:w-6 rounded-full mx-1"
          :class="idx < phaseIndex ? 'bg-emerald-500/50' : 'bg-bd'"
        />
      </div>
    </div>

    <!-- EXPANDABLE DETAILS BODY -->
    <div v-if="open" class="p-4 space-y-4">
      <!-- LIVE CHAIN-OF-THOUGHT (THINKING) TERMINAL -->
      <div v-if="reasoning" class="rounded-lg border border-bd/90 bg-[#0d1117] text-[#c9d1d9] shadow-inner overflow-hidden">
        <div class="flex items-center justify-between px-3 py-1.5 bg-[#161b22] border-b border-[#30363d] text-xs">
          <div class="flex items-center gap-2">
            <div class="flex items-center gap-1">
              <span class="h-2.5 w-2.5 rounded-full bg-[#ff5f56]" />
              <span class="h-2.5 w-2.5 rounded-full bg-[#ffbd2e]" />
              <span class="h-2.5 w-2.5 rounded-full bg-[#27c93f]" />
            </div>
            <span class="font-mono text-[11px] text-muted ml-1 flex items-center gap-1.5">
              <span class="text-accentSoft">🧠</span>
              {{ $t("trace.chain_of_thought") }}
            </span>
          </div>

          <div class="flex items-center gap-2">
            <span class="text-[10px] text-muted font-mono">
              {{ reasoning.length }} симв.
            </span>
            <button
              class="text-muted hover:text-white transition px-1"
              @click="reasoningOpen = !reasoningOpen"
            >
              {{ reasoningOpen ? "▾" : "▸" }}
            </button>
          </div>
        </div>

        <div
          v-if="reasoningOpen"
          ref="reasoningBox"
          class="p-3 max-h-56 overflow-y-auto font-mono text-[12px] leading-relaxed whitespace-pre-wrap selection:bg-accent/30 scrollbar-thin scrollbar-thumb-bd"
        >
          {{ reasoning }}<span v-if="live" class="inline-block h-3.5 w-1.5 ml-0.5 bg-accent animate-pulse align-middle" />
        </div>
      </div>

      <!-- FILTER TABS FOR EVENT FEED -->
      <div class="flex items-center gap-2 border-b border-bd pb-2 text-xs">
        <button
          class="px-2.5 py-1 rounded-md transition"
          :class="activeFilter === 'all' ? 'bg-accent/15 text-accent font-medium border border-accent/30' : 'text-muted hover:text-ink'"
          @click="activeFilter = 'all'"
        >
          {{ $t("trace.filter_all") }} <span class="opacity-70">({{ entries.length }})</span>
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
        <div
          ref="feedBox"
          class="space-y-2.5 max-h-[380px] overflow-y-auto pr-1 scrollbar-thin scrollbar-thumb-bd"
          @scroll="onFeedScroll"
        >
          <div
            v-for="(entry, idx) in filteredEntries"
            :key="idx"
            class="group rounded-lg border border-bd/40 bg-surface/50 p-2.5 hover:border-bd hover:bg-surface/80 transition-all text-xs"
            :class="{
              'border-accent/40 ring-1 ring-accent/20 bg-accent/5': live && idx === filteredEntries.length - 1,
            }"
          >
            <div class="flex items-start justify-between gap-2">
              <div class="flex items-center gap-2 min-w-0">
                <span
                  class="flex items-center justify-center h-4 w-4 rounded-full text-[9px] font-bold shrink-0"
                  :class="live && idx === filteredEntries.length - 1 ? 'bg-accent text-white animate-pulse' : 'bg-surface border border-bd text-muted'"
                >
                  {{ idx + 1 }}
                </span>

                <span v-if="entry.agent" class="font-medium text-ink truncate">
                  {{ entry.agent }}
                </span>
                <span v-else class="font-medium text-ink truncate">
                  {{ stepLabel(entry.step) }}
                </span>

                <span
                  v-if="entry.action"
                  class="rounded bg-bg/60 border border-bd/60 px-1 py-0.2 text-[10px] text-muted font-mono"
                >
                  {{ entry.action }}
                </span>
              </div>

              <span v-if="entry.timestamp" class="text-[10px] font-mono text-muted/60 shrink-0">
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
                class="inline-flex items-center gap-1 rounded bg-bg/80 border border-bd/80 px-1.5 py-0.5 text-[10px] text-muted"
              >
                <span class="opacity-60">{{ key }}:</span>
                <span class="font-semibold text-ink">{{ val }}</span>
              </span>
            </div>

            <!-- Discovered / Scraped Sources Chips -->
            <div v-if="entry.sources && entry.sources.length" class="mt-2 pl-6 flex flex-wrap gap-1.5">
              <a
                v-for="(s, j) in entry.sources"
                :key="j"
                :href="s.url || `https://${s.domain}`"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex max-w-[220px] items-center gap-1.5 rounded-md border border-bd/70 bg-bg/60 px-2 py-0.5 text-[11px] text-ink hover:border-accent hover:text-accent transition shadow-2xs"
                :title="s.title || s.domain"
              >
                <img
                  :src="`https://www.google.com/s2/favicons?domain=${s.domain}&sz=32`"
                  alt=""
                  class="h-3 w-3 shrink-0 rounded-xs"
                  loading="lazy"
                  @error="($event.target as HTMLImageElement).style.display = 'none'"
                />
                <span class="truncate">{{ s.domain }}</span>
                <span class="text-[9px] text-muted opacity-60">↗</span>
              </a>
            </div>
          </div>

          <div v-if="!filteredEntries.length" class="py-6 text-center text-xs text-muted">
            {{ $t("artifact.trailEmpty") }}
          </div>
        </div>

        <!-- Floating button to return to latest actions -->
        <div v-if="userHasScrolledUp && live" class="absolute bottom-2 inset-x-0 flex justify-center pointer-events-none">
          <button
            type="button"
            class="pointer-events-auto flex items-center gap-1.5 rounded-full bg-accent px-3 py-1 text-[11px] font-medium text-white shadow-lg transition hover:bg-accent/90"
            @click="userHasScrolledUp = false; scrollToBottom('smooth');"
          >
            <span>↓</span>
            <span>{{ te('trace.to_latest') ? t('trace.to_latest') : 'К новым действиям' }}</span>
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.scrollbar-none::-webkit-scrollbar {
  display: none;
}
.scrollbar-none {
  -ms-overflow-style: none;
  scrollbar-width: none;
}
</style>
