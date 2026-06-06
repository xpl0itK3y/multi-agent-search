<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
import { openResearchStream } from "@/lib/stream";
import ProgressTrace, { type TraceEntry } from "@/components/ProgressTrace.vue";
import ArtifactPanel from "@/components/ArtifactPanel.vue";

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

let close: (() => void) | undefined;

const DONE = new Set(["completed", "failed", "timeout"]);

function statusLabel(s: string): string {
  const map: Record<string, string> = {
    processing: "Декомпозиция и поиск…",
    analyzing: "Синтез отчёта…",
    completed: "Готово",
    failed: "Ошибка",
    timeout: "Превышено время ожидания",
  };
  return map[s] ?? s;
}

onMounted(async () => {
  try {
    const s = await api.getStatus(props.id);
    prompt.value = s.prompt;
    status.value = s.status;
  } catch {
    // Non-fatal — the SSE stream still drives status/report.
  }
  close = openResearchStream(props.id, {
    onStatus: (s) => (status.value = s),
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
  <div class="flex h-full flex-col lg:flex-row">
    <!-- Thread -->
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

    <!-- Artifact -->
    <section class="min-h-0 flex-1">
      <ArtifactPanel :id="props.id" :report="report" :is-final="isFinal" />
    </section>
  </div>
</template>
