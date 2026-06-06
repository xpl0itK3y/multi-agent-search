<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
import { openResearchStream } from "@/lib/stream";
import type { PlanItem, ResearchPlan } from "@/lib/types";
import ProgressTrace, { type TraceEntry } from "@/components/ProgressTrace.vue";
import ArtifactPanel from "@/components/ArtifactPanel.vue";
import PlanCard from "@/components/PlanCard.vue";

const props = defineProps<{ id: string }>();
const router = useRouter();

const prompt = ref<string>("");
const status = ref<string>("processing");
const report = ref<string>("");
const isFinal = ref(false);
const trace = ref<TraceEntry[]>([]);
const reasoning = ref<string>("");
const errorMsg = ref<string | null>(null);
const done = ref(false);

const plan = ref<ResearchPlan | null>(null);
const planBusy = ref(false);

let close: (() => void) | undefined;

const DONE = new Set(["completed", "failed", "timeout"]);

function statusLabel(s: string): string {
  const map: Record<string, string> = {
    plan_review: "Ожидает подтверждения плана",
    processing: "Декомпозиция и поиск…",
    analyzing: "Синтез отчёта…",
    completed: "Готово",
    failed: "Ошибка",
    timeout: "Превышено время ожидания",
  };
  return map[s] ?? s;
}

async function loadPlan() {
  try {
    plan.value = await api.getPlan(props.id);
  } catch (e) {
    errorMsg.value = (e as Error).message;
  }
}

async function onApprove(items: PlanItem[]) {
  planBusy.value = true;
  errorMsg.value = null;
  try {
    await api.updatePlan(props.id, items);
    await api.approvePlan(props.id);
    plan.value = null;
    status.value = "processing";
  } catch (e) {
    errorMsg.value = (e as Error).message;
  } finally {
    planBusy.value = false;
  }
}

onMounted(async () => {
  try {
    const s = await api.getStatus(props.id);
    prompt.value = s.prompt;
    status.value = s.status;
    if (s.status === "plan_review") loadPlan();
  } catch {
    // Non-fatal — the SSE stream still drives status/report.
  }
  close = openResearchStream(props.id, {
    onStatus: (s) => {
      status.value = s;
      if (s === "plan_review" && !plan.value) loadPlan();
      if (s !== "plan_review") plan.value = null;
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
    },
    onError: (m) => (errorMsg.value = m),
  });
});

onBeforeUnmount(() => close?.());
</script>

<template>
  <!-- Plan review: editable plan before search starts -->
  <div v-if="status === 'plan_review' && plan" class="h-full overflow-y-auto">
    <button class="px-6 pt-6 text-sm text-muted hover:text-ink" @click="router.push('/')">
      ← На главную
    </button>
    <PlanCard :prompt="prompt" :items="plan.items" :busy="planBusy" @approve="onApprove" />
    <p v-if="errorMsg" class="px-6 pb-6 text-sm text-red-400">{{ errorMsg }}</p>
  </div>

  <!-- Active research: thread + artifact panel -->
  <div v-else class="flex h-full flex-col lg:flex-row">
    <section
      class="flex w-full flex-col overflow-y-auto border-b border-bd lg:w-[400px] lg:shrink-0 lg:border-b-0 lg:border-r"
    >
      <div class="px-6 py-6">
        <button class="mb-5 text-sm text-muted hover:text-ink" @click="router.push('/')">
          ← На главную
        </button>

        <h1 v-if="prompt" class="mb-4 font-serif text-xl leading-snug text-ink">
          {{ prompt }}
        </h1>

        <div class="mb-5 flex items-center gap-2">
          <span
            class="h-2 w-2 rounded-full"
            :class="{
              'bg-emerald-400': status === 'completed',
              'bg-red-400': status === 'failed',
              'bg-accent animate-pulse': !DONE.has(status),
            }"
          />
          <span class="text-sm text-muted">{{ statusLabel(status) }}</span>
        </div>

        <p v-if="errorMsg" class="mb-4 text-sm text-red-400">{{ errorMsg }}</p>

        <ProgressTrace
          v-if="trace.length || reasoning"
          :entries="trace"
          :reasoning="reasoning"
          :live="!done"
        />
      </div>
    </section>

    <section class="min-h-0 flex-1">
      <ArtifactPanel :id="props.id" :report="report" :is-final="isFinal" />
    </section>
  </div>
</template>
