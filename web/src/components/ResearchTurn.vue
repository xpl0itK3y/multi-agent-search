<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import { openResearchStream } from "@/lib/stream";
import type { Clarification, PlanItem, ResearchPlan } from "@/lib/types";
import ProgressTrace, { type TraceEntry } from "./ProgressTrace.vue";
import ArtifactPanel from "./ArtifactPanel.vue";
import PlanCard from "./PlanCard.vue";
import ClarifyCard from "./ClarifyCard.vue";

// One deep-research run rendered as a conversation turn: the prompt, its live
// progress, and the final report (with sources/confidence/conflicts tabs).
const props = defineProps<{ id: string; initialPrompt?: string }>();
const emit = defineEmits<{ done: [status: string]; refreshed: [{ id: string; prompt: string }] }>();
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

const plan = ref<ResearchPlan | null>(null);
const planBusy = ref(false);
const clarification = ref<Clarification | null>(null);
const clarifyBusy = ref(false);

const DONE = new Set(["completed", "failed", "timeout"]);
const queuePos = ref<number | null>(null);
let close: (() => void) | undefined;
let queuePoll: number | undefined;

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
    } catch {
      /* transient — keep polling */
    }
  }, 4000);
}
function stopQueuePoll() {
  if (queuePoll) {
    clearInterval(queuePoll);
    queuePoll = undefined;
  }
}

const costLabel = computed(() => {
  const u = usage.value;
  if (!u || !u.total_tokens) return null;
  const parts: string[] = [];
  if (typeof u.estimated_cost_usd === "number") parts.push(`≈ $${u.estimated_cost_usd.toFixed(4)}`);
  if (typeof u.total_tokens === "number") parts.push(`${u.total_tokens.toLocaleString()} ${t("research.tokens")}`);
  return parts.join(" · ");
});

async function loadPlan() {
  try { plan.value = await api.getPlan(props.id); } catch (e) { errorMsg.value = (e as Error).message; }
}
async function loadClarifications() {
  try { clarification.value = await api.getClarifications(props.id); } catch (e) { errorMsg.value = (e as Error).message; }
}

async function onSubmitClarify(answers: string[]) {
  clarifyBusy.value = true;
  errorMsg.value = null;
  try {
    await api.submitClarify(props.id, answers);
    clarification.value = null;
    status.value = "processing";
  } catch (e) { errorMsg.value = (e as Error).message; } finally { clarifyBusy.value = false; }
}

async function onApprove(items: PlanItem[]) {
  planBusy.value = true;
  errorMsg.value = null;
  try {
    await api.updatePlan(props.id, items);
    await api.approvePlan(props.id);
    plan.value = null;
    status.value = "processing";
  } catch (e) { errorMsg.value = (e as Error).message; } finally { planBusy.value = false; }
}

onMounted(async () => {
  try {
    const s = await api.getStatus(props.id);
    if (!prompt.value) prompt.value = s.prompt;
    status.value = s.status;
    usage.value = s.llm_token_usage ?? null;
    queuePos.value = s.queue_position ?? null;
    if (s.status === "queued") startQueuePoll();
    if (s.status === "clarifying") loadClarifications();
    if (s.status === "plan_review") loadPlan();
    if (DONE.has(s.status)) {
      done.value = true;
      emit("done", s.status);
      // Already-finished research: load its report so the panel renders immediately.
      if (s.status === "completed") {
        try {
          const r = await api.getReport(props.id);
          report.value = r.final_report ?? "";
          isFinal.value = true;
        } catch {
          /* SSE may still deliver it */
        }
      }
    }
  } catch {
    /* SSE still drives status/report */
  }
  close = openResearchStream(props.id, {
    onStatus: (s) => {
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
    onTrace: (step, detail) => trace.value.push({ step, detail }),
    onReasoning: (r) => (reasoning.value = r),
    onReport: (r, final) => {
      report.value = r;
      isFinal.value = final;
    },
    onDone: (s) => {
      status.value = s;
      done.value = true;
      emit("done", s);
      if (s === "completed") {
        api.getStatus(props.id).then((st) => (usage.value = st.llm_token_usage ?? null)).catch(() => {});
      }
    },
    onError: (m) => (errorMsg.value = m),
  });
});

onBeforeUnmount(() => {
  close?.();
  stopQueuePoll();
});
</script>

<template>
  <div class="space-y-3">
    <!-- user prompt bubble -->
    <div class="flex justify-end">
      <div class="animate-rise max-w-[80%] whitespace-pre-wrap rounded-2xl bg-surface px-4 py-2.5 text-[15px] text-ink">
        {{ prompt }}
      </div>
    </div>

    <!-- clarifying questions -->
    <ClarifyCard
      v-if="status === 'clarifying' && clarification"
      :prompt="prompt"
      :questions="clarification.questions"
      :busy="clarifyBusy"
      @submit="onSubmitClarify"
    />

    <!-- editable plan -->
    <PlanCard
      v-else-if="status === 'plan_review' && plan"
      :prompt="prompt"
      :items="plan.items"
      :busy="planBusy"
      @approve="onApprove"
    />

    <!-- result -->
    <template v-else>
      <div class="flex items-center gap-2">
        <span
          class="h-2 w-2 rounded-full"
          :class="{
            'bg-emerald-400': status === 'completed',
            'bg-red-400': status === 'failed' || status === 'timeout',
            'bg-accent animate-pulse': !DONE.has(status),
          }"
        />
        <span class="text-sm text-muted">{{ statusLabel(status) }}<template v-if="status === 'queued' && queuePos"> · #{{ queuePos }}</template></span>
        <span v-if="costLabel" class="ml-auto text-xs text-muted" :title="$t('research.costTitle')">{{ costLabel }}</span>
      </div>

      <p v-if="errorMsg" class="text-sm text-red-400">{{ errorMsg }}</p>

      <ProgressTrace
        v-if="!done && (trace.length || reasoning)"
        :entries="trace"
        :reasoning="reasoning"
        :live="!done"
      />

      <!-- report + sources/confidence/conflicts/trail tabs (bounded, scrolls within) -->
      <div
        v-if="report || done"
        class="animate-fade-in h-[68vh] min-h-[380px] overflow-hidden rounded-xl border border-bd bg-surface/30"
      >
        <ArtifactPanel :id="props.id" :report="report" :is-final="isFinal" @refreshed="(id) => emit('refreshed', { id, prompt })" />
      </div>
    </template>
  </div>
</template>
