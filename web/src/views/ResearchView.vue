<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
import { openResearchStream, streamChatAnswer } from "@/lib/stream";
import type { ChatMessage, PlanItem, ResearchPlan } from "@/lib/types";
import ProgressTrace, { type TraceEntry } from "@/components/ProgressTrace.vue";
import ArtifactPanel from "@/components/ArtifactPanel.vue";
import PlanCard from "@/components/PlanCard.vue";
import MarkdownView from "@/components/MarkdownView.vue";

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

const messages = ref<ChatMessage[]>([]);
const chatInput = ref("");
const chatBusy = ref(false);
const threadScroll = ref<HTMLElement | null>(null);

const canChat = computed(() => status.value === "completed");
const awaitingAnswer = computed(() => {
  const last = messages.value[messages.value.length - 1];
  return chatBusy.value && (!last || last.role !== "assistant" || !last.content);
});

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

async function scrollThreadToBottom() {
  await nextTick();
  threadScroll.value?.scrollTo({ top: threadScroll.value.scrollHeight, behavior: "smooth" });
}

async function sendChat() {
  const question = chatInput.value.trim();
  if (!question || chatBusy.value) return;
  chatInput.value = "";
  messages.value.push({ role: "user", content: question });
  const assistantIndex = messages.value.push({ role: "assistant", content: "" }) - 1;
  chatBusy.value = true;
  errorMsg.value = null;
  scrollThreadToBottom();
  await streamChatAnswer(props.id, question, {
    onDelta: (answer) => {
      messages.value[assistantIndex].content = answer;
      scrollThreadToBottom();
    },
    onDone: (answer) => {
      messages.value[assistantIndex].content = answer;
      chatBusy.value = false;
      scrollThreadToBottom();
    },
    onError: (m) => {
      errorMsg.value = m;
      messages.value.splice(assistantIndex, 1);
      chatBusy.value = false;
    },
  });
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
  try {
    messages.value = await api.getMessages(props.id);
  } catch {
    // ignore — no messages yet
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

  <!-- Active / completed research: thread (+ chat) on the left, artifact on the right -->
  <div v-else class="flex h-full flex-col lg:flex-row">
    <section
      class="flex w-full flex-col overflow-hidden border-b border-bd lg:w-[420px] lg:shrink-0 lg:border-b-0 lg:border-r"
    >
      <div ref="threadScroll" class="flex-1 overflow-y-auto px-6 py-6">
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

        <!-- Follow-up conversation -->
        <div v-if="messages.length" class="mt-6 space-y-4 border-t border-bd pt-5">
          <template v-for="(m, i) in messages" :key="i">
            <div v-if="m.role === 'user'" class="ml-6 rounded-lg bg-surface px-3 py-2 text-sm text-ink">
              {{ m.content }}
            </div>
            <MarkdownView v-else-if="m.content" :source="m.content" />
          </template>
          <div v-if="awaitingAnswer" class="flex items-center gap-2 text-sm text-muted">
            <span class="h-1.5 w-1.5 animate-pulse rounded-full bg-accent" /> Думаю…
          </div>
        </div>
      </div>

      <!-- Pinned chat composer (available once the report is ready) -->
      <div v-if="canChat" class="shrink-0 border-t border-bd p-3">
        <div class="flex items-end gap-2 rounded-xl border border-bd bg-surface px-3 py-2">
          <textarea
            v-model="chatInput"
            rows="1"
            placeholder="Спросить по исследованию…"
            class="max-h-32 flex-1 resize-none bg-transparent text-sm text-ink placeholder:text-muted focus:outline-none"
            @keydown.enter.exact.prevent="sendChat"
          />
          <button
            class="grid h-7 w-7 shrink-0 place-items-center rounded-full bg-accent text-bg disabled:opacity-40"
            :disabled="!chatInput.trim() || chatBusy"
            @click="sendChat"
          >
            ↑
          </button>
        </div>
      </div>
    </section>

    <section class="min-h-0 flex-1">
      <ArtifactPanel :id="props.id" :report="report" :is-final="isFinal" />
    </section>
  </div>
</template>
