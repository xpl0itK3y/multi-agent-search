<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { api, ApiError, apiErrorMessage } from "@/lib/api";
import { openResearchStream } from "@/lib/stream";
import { createTraceDeduper, reconnectDelayMs, traceFromGraph } from "@/lib/trace";
import type { Clarification, PlanItem, ResearchPlan } from "@/lib/types";
import AgentActivityConsole from "./AgentActivityConsole.vue";
import type { TraceEntry } from "@/lib/stream";
import ArtifactPanel from "./ArtifactPanel.vue";
import PlanCard from "./PlanCard.vue";
import ClarifyCard from "./ClarifyCard.vue";

// One deep-research run rendered as a conversation turn: the prompt, its live
// progress, and the final report (with sources/confidence/conflicts tabs).
const props = defineProps<{ id: string; initialPrompt?: string }>();
const emit = defineEmits<{ done: [status: string]; refreshed: [{ id: string; prompt: string }]; grow: [] }>();
const { t, te } = useI18n();

const prompt = ref(props.initialPrompt ?? "");
const status = ref("processing");
const report = ref("");
const isFinal = ref(false);
const trace = ref<TraceEntry[]>([]);
const reasoning = ref("");
const done = ref(false);
const usage = ref<Record<string, number> | null>(null);
const errorMsg = ref<string | null>(null);
const streamLost = ref(false);

// Every (re)connect replays the whole trail — show each step once.
const traceSeen = createTraceDeduper();
function addTrace(entry: TraceEntry) {
  if (traceSeen.accept(entry)) trace.value.push(entry);
}
function resetTrace() {
  traceSeen.reset();
  trace.value = [];
  reasoning.value = "";
}

const plan = ref<ResearchPlan | null>(null);
const planBusy = ref(false);
const clarification = ref<Clarification | null>(null);
const clarifyBusy = ref(false);

// "not_found" is client-side only: the research no longer exists (see markGone).
const DONE = new Set(["completed", "failed", "timeout", "cancelled", "not_found"]);
const queuePos = ref<number | null>(null);
const cancelling = ref(false);
const promptExpanded = ref(false);

async function onCancel() {
  cancelling.value = true;
  try {
    await api.cancelResearch(props.id);
    status.value = "cancelled";
    done.value = true;
    emit("done", "cancelled");
  } catch (e) {
    errorMsg.value = apiErrorMessage(e, t);
  } finally {
    cancelling.value = false;
  }
}

let notified = false;
function notifyDone(s: string) {
  if (notified || s !== "completed" || typeof Notification === "undefined") return;
  notified = true;
  // Only notify if the tab isn't focused (the user stepped away).
  if (typeof document !== "undefined" && document.visibilityState === "visible") return;
  const fire = () => new Notification("Veris", { body: t("research.notifyReady"), icon: "/favicon.svg" });
  if (Notification.permission === "granted") fire();
  else if (Notification.permission !== "denied") Notification.requestPermission().then((p) => p === "granted" && fire());
}
let close: (() => void) | undefined;
let queuePoll: number | undefined;
// Pending auto-reconnect after a stream error; consecutive failures back off.
let reconnectTimer: number | undefined;
let reconnectAttempt = 0;
let unmounted = false;

function statusLabel(s: string): string {
  return te(`status.${s}`) ? t(`status.${s}`) : s;
}

// While queued, poll for the (shrinking) queue position; SSE flips status on promotion.
function startQueuePoll() {
  if (queuePoll) return;
  queuePoll = window.setInterval(async () => {
    if (status.value !== "queued") return stopQueuePoll();
    try {
      const s = await api.getStatus(props.id);
      status.value = s.status;
      queuePos.value = s.queue_position ?? null;
      if (s.status !== "queued") stopQueuePoll();
    } catch (e) {
      if (isGone(e)) markGone();
      else if (isSignedOut(e)) markSignedOut(e); // otherwise transient — keep polling
    }
  }, 4000);
}
function stopQueuePoll() {
  if (queuePoll) {
    clearInterval(queuePoll);
    queuePoll = undefined;
  }
}

// Live running commentary: the latest trace step, shown next to the status even when the
// trace panel is collapsed (so the user always sees what's happening right now).
const currentActivity = computed(() => {
  const last = trace.value[trace.value.length - 1];
  if (!last) return "";
  return (last.detail || "")
    .replace(/^[\p{Emoji_Presentation}\p{Extended_Pictographic}✓✔]+\s*/u, "")
    .replace(/\s*—\s*\d+\s*$/, "")
    .trim();
});

const costLabel = computed(() => {
  const u = usage.value;
  if (!u || !u.total_tokens) return null;
  const parts: string[] = [];
  if (typeof u.estimated_cost_usd === "number") parts.push(`≈ $${u.estimated_cost_usd.toFixed(4)}`);
  if (typeof u.total_tokens === "number") parts.push(`${u.total_tokens.toLocaleString()} ${t("research.tokens")}`);
  return parts.join(" · ");
});

const costTooltip = computed(() => {
  const u = usage.value;
  if (!u) return t("research.costTitle");
  const lines: string[] = [t("research.costTitle")];
  if (u.prompt_tokens) lines.push(t("research.costInput", { n: u.prompt_tokens.toLocaleString() }));
  if (u.cache_hit_tokens) {
    const pct = Math.round((u.cache_hit_tokens / u.prompt_tokens) * 100);
    lines.push(t("research.costCache", { n: u.cache_hit_tokens.toLocaleString(), pct }));
  }
  if (u.completion_tokens) lines.push(t("research.costOutput", { n: u.completion_tokens.toLocaleString() }));
  return lines.join(" · ");
});

async function loadPlan() {
  try { plan.value = await api.getPlan(props.id); emit("grow"); } catch (e) { errorMsg.value = apiErrorMessage(e, t); }
}
async function loadClarifications() {
  try { clarification.value = await api.getClarifications(props.id); emit("grow"); } catch (e) { errorMsg.value = apiErrorMessage(e, t); }
}

async function onSubmitClarify(answers: string[]) {
  clarifyBusy.value = true;
  errorMsg.value = null;
  try {
    await api.submitClarify(props.id, answers);
    clarification.value = null;
    status.value = "processing";
  } catch (e) { errorMsg.value = apiErrorMessage(e, t); } finally { clarifyBusy.value = false; }
}

async function onApprove(items: PlanItem[]) {
  planBusy.value = true;
  errorMsg.value = null;
  try {
    await api.updatePlan(props.id, items);
    await api.approvePlan(props.id);
    plan.value = null;
    status.value = "processing";
  } catch (e) { errorMsg.value = apiErrorMessage(e, t); } finally { planBusy.value = false; }
}

// 404/403: the research was deleted (another tab, its owner removed, the retention
// sweep) or is no longer ours. Network errors and 5xx are transient, never this.
function isGone(e: unknown): boolean {
  return e instanceof ApiError && (e.status === 404 || e.status === 403);
}

// Settle a research that no longer exists: stop streaming, reconnecting and polling,
// and tell the thread it is over so its composer is released.
function markGone() {
  clearReconnect();
  close?.();
  close = undefined;
  stopQueuePoll();
  streamLost.value = false;
  errorMsg.value = t("research.notFound");
  status.value = "not_found";
  if (!done.value) {
    done.value = true;
    emit("done", "not_found");
  }
}

// 401: the session is gone (expired, or revoked: a logout signs out every device).
// Reconnecting cannot bring it back; api's session recovery sends the tab to /login.
function isSignedOut(e: unknown): boolean {
  return e instanceof ApiError && e.status === 401;
}

// Stop streaming, reconnecting and polling until the user signs in again. The research
// itself goes on, so the thread keeps waiting and a manual resume stays offered.
function markSignedOut(e: unknown) {
  clearReconnect();
  close?.();
  close = undefined;
  stopQueuePoll();
  errorMsg.value = apiErrorMessage(e, t);
  streamLost.value = true;
}

// Fetch the current status/report (also used to catch up after a dropped stream). Returns
// true if no live stream is needed: the research is in a terminal state, is gone, or the
// session is.
async function syncStatus(): Promise<boolean> {
  try {
    const s = await api.getStatus(props.id);
    if (!prompt.value) prompt.value = s.prompt;
    status.value = s.status;
    usage.value = s.llm_token_usage ?? null;
    if (!trace.value.length) {
      try {
        const g = await api.getGraph(props.id);
        if (g.graph_trail && g.graph_trail.length && !trace.value.length) {
          traceFromGraph(g.graph_trail).forEach(addTrace);
        }
      } catch {
        /* non-fatal */
      }
    }
    if (s.status === "queued") startQueuePoll();
    if (s.status === "clarifying") loadClarifications();
    if (s.status === "plan_review") loadPlan();
    if (DONE.has(s.status)) {
      done.value = true;
      emit("done", s.status);
      if (s.status === "completed" || s.has_final_report) {
        try {
          const r = await api.getReport(props.id);
          report.value = r.final_report ?? "";
          isFinal.value = s.status === "completed";
          emit("grow");
        } catch {
          /* SSE may still deliver it */
        }
      }
      return true;
    }
  } catch (e) {
    if (isGone(e)) {
      markGone();
      return true;
    }
    if (isSignedOut(e)) {
      markSignedOut(e);
      return true;
    }
    /* SSE still drives status/report */
  }
  return false;
}

function clearReconnect() {
  if (reconnectTimer !== undefined) {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  }
}

// Catch up and reopen the stream after a delay that grows with each consecutive
// failure. Only one reconnect is ever pending, and none survives unmount.
function scheduleReconnect() {
  clearReconnect();
  reconnectTimer = window.setTimeout(async () => {
    reconnectTimer = undefined;
    if (unmounted || done.value) return;
    const terminal = await syncStatus();
    if (!terminal) connect();
  }, reconnectDelayMs(reconnectAttempt++));
}

// The stream delivered data again: drop the pending reconnect and the error it showed.
function streamRecovered() {
  clearReconnect();
  reconnectAttempt = 0;
  if (streamLost.value) {
    streamLost.value = false;
    errorMsg.value = null;
  }
}

// (Re)open the live SSE stream. Re-callable so a dropped connection can be resumed.
function connect() {
  if (unmounted) return;
  clearReconnect();
  close?.();
  close = openResearchStream(props.id, {
    onStatus: (s) => {
      streamRecovered();
      status.value = s;
      if (s === "queued") startQueuePoll();
      else if (queuePos.value !== null) queuePos.value = null;
      if (s === "clarifying" && !clarification.value) loadClarifications();
      if (s !== "clarifying") clarification.value = null;
      if (s === "plan_review" && !plan.value) loadPlan();
      if (s !== "plan_review") plan.value = null;
      if (DONE.has(s) && !done.value) {
        done.value = true;
        emit("done", s); // ensure the thread learns of completion even without onDone
      }
    },
    onTrace: addTrace,
    onReasoning: (r) => (reasoning.value = r),
    onReport: (r, final) => {
      const wasEmpty = !report.value;
      report.value = r;
      isFinal.value = final;
      if (wasEmpty && r) emit("grow"); // first time the report panel appears → scroll to it
    },
    onDone: async (s) => {
      if (s === "timeout" || s === "failed") {
        const terminal = await syncStatus();
        // Research is still processing or analyzing on the server — reconnect stream.
        if (!terminal) scheduleReconnect();
        return;
      }
      status.value = s;
      done.value = true;
      emit("done", s);
      emit("grow");
      notifyDone(s);
      if (s === "completed") {
        api.getStatus(props.id).then((st) => (usage.value = st.llm_token_usage ?? null)).catch(() => {});
      }
    },
    onError: (m) => {
      errorMsg.value = m;
      if (!done.value) {
        streamLost.value = true; // offer a resume button
        scheduleReconnect(); // …and try to catch up / continue on our own
      }
    },
  });
}

async function resume() {
  errorMsg.value = null;
  reconnectAttempt = 0;
  const terminal = await syncStatus(); // catch up on anything missed while disconnected
  if (!terminal) connect();
}

const retrying = ref(false);

async function retry() {
  retrying.value = true;
  errorMsg.value = null;
  try {
    await api.retryResearch(props.id);
    status.value = "processing";
    done.value = false;
    report.value = "";
    isFinal.value = false;
    resetTrace(); // the retried run streams its own trail
    reconnectAttempt = 0;
    connect();
  } catch (e) {
    errorMsg.value = apiErrorMessage(e, t);
  } finally {
    retrying.value = false;
  }
}

onMounted(async () => {
  const terminal = await syncStatus();
  if (!terminal) connect();
});

onBeforeUnmount(() => {
  unmounted = true;
  clearReconnect();
  close?.();
  stopQueuePoll();
});
</script>

<template>
  <div class="space-y-3">
    <!-- user prompt bubble (long prompts clamp to keep the thread readable) -->
    <div class="flex justify-end">
      <div class="animate-rise max-w-[80%] rounded-2xl bg-surface px-4 py-2.5 text-[15px] text-ink">
        <div class="whitespace-pre-wrap" :class="{ 'line-clamp-5': !promptExpanded }">{{ prompt }}</div>
        <button
          v-if="prompt.length > 280"
          class="mt-1 text-xs text-muted transition hover:text-ink"
          @click="promptExpanded = !promptExpanded"
        >
          {{ promptExpanded ? $t("research.showLess") : $t("research.showMore") }}
        </button>
      </div>
    </div>

    <!-- clarifying questions -->
    <ClarifyCard
      v-if="status === 'clarifying' && clarification"
      :prompt="prompt"
      :questions="clarification.questions"
      :busy="clarifyBusy"
      @submit="onSubmitClarify"
      @cancel="onCancel"
    />

    <!-- editable plan -->
    <PlanCard
      v-else-if="status === 'plan_review' && plan"
      :prompt="prompt"
      :items="plan.items"
      :busy="planBusy"
      @approve="onApprove"
      @cancel="onCancel"
    />

    <!-- result -->
    <template v-else>
      <div class="flex items-center gap-2">
        <span
          class="h-2 w-2 rounded-full"
          :class="{
            'bg-emerald-400': status === 'completed',
            'bg-red-400': status === 'failed' || status === 'timeout',
            'bg-muted': status === 'cancelled' || status === 'not_found',
            'bg-accent animate-pulse': !DONE.has(status),
          }"
        />
        <span class="text-sm text-muted">{{ statusLabel(status) }}<template v-if="status === 'queued' && queuePos"> · #{{ queuePos }}</template></span>
        <button
          v-if="!DONE.has(status)"
          class="rounded-md border border-bd px-2 py-0.5 text-xs text-muted transition hover:border-red-400/50 hover:text-red-400 disabled:opacity-50"
          :disabled="cancelling"
          @click="onCancel"
        >
          {{ cancelling ? $t("research.cancelling") : $t("research.cancel") }}
        </button>
        <span v-if="costLabel" class="ml-auto text-xs text-muted cursor-help" :title="costTooltip">{{ costLabel }}</span>
      </div>

      <!-- live "what's happening now" commentary (visible even when the trace is collapsed) -->
      <transition name="fade" mode="out-in">
        <p v-if="!done && currentActivity" :key="currentActivity" class="line-clamp-1 pl-4 text-xs text-muted/80">
          {{ currentActivity }}
        </p>
      </transition>

      <div v-if="errorMsg" class="flex flex-wrap items-center gap-2 text-sm text-red-400">
        <span>{{ errorMsg }}</span>
        <button
          v-if="streamLost && !done"
          class="rounded-md border border-accent/50 px-2.5 py-0.5 text-xs text-accent transition hover:bg-accent/10"
          @click="resume"
        >
          {{ $t("research.resume") }}
        </button>
      </div>

      <!-- UNIFIED RESEARCH & AGENT CONTAINER -->
      <div class="rounded-xl border border-bd/80 bg-surface/80 backdrop-blur-md shadow-sm overflow-hidden transition-all duration-300">
        <!-- Agent Activity Console (embedded inside unified container) -->
        <AgentActivityConsole
          v-if="!done || trace.length"
          :entries="trace"
          :reasoning="reasoning"
          :live="!done"
          :status="status"
          :embedded="true"
        />

        <!-- Error Card (when status === 'failed' or 'timeout' and no report exists) -->
        <div
          v-if="(status === 'failed' || status === 'timeout') && !report"
          class="border-t border-red-500/30 bg-red-500/10 p-5"
        >
          <div class="flex items-start gap-3">
            <span class="text-2xl shrink-0">⚠️</span>
            <div class="min-w-0 flex-1">
              <h4 class="font-semibold text-red-400 text-sm">
                {{ $t("research.failedTitle") }}
              </h4>
              <p class="mt-1 text-xs text-red-300/90 leading-relaxed break-words">
                {{ errorMsg || $t("research.failedMessage") }}
              </p>
              <div class="mt-3 flex items-center gap-2">
                <button
                  type="button"
                  :disabled="retrying"
                  class="rounded-lg bg-red-500/20 hover:bg-red-500/30 text-red-200 border border-red-500/40 px-3.5 py-1.5 text-xs font-medium transition flex items-center gap-1.5 disabled:opacity-50"
                  @click="retry"
                >
                  <span :class="{ 'animate-spin': retrying }">↻</span>
                  <span>{{ retrying ? $t("research.retrying") : $t("research.retry") }}</span>
                </button>
              </div>
            </div>
          </div>
        </div>

        <!-- Artifact Panel (Report, Dashboard, Sources, etc.) -->
        <div
          v-else-if="report"
          class="border-t border-bd/80 h-[68vh] min-h-[380px] overflow-hidden bg-surface/30"
        >
          <ArtifactPanel
            :id="props.id"
            :report="report"
            :is-final="isFinal"
            @refreshed="(id) => emit('refreshed', { id, prompt })"
          />
        </div>
      </div>
    </template>
  </div>
</template>
