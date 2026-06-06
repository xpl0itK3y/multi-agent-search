<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { openResearchStream } from "@/lib/stream";
import ProgressTrace, { type TraceEntry } from "@/components/ProgressTrace.vue";
import MarkdownView from "@/components/MarkdownView.vue";

const props = defineProps<{ id: string }>();
const router = useRouter();

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

onMounted(() => {
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
  <div class="mx-auto max-w-3xl px-6 py-10">
    <button class="mb-6 text-sm text-muted hover:text-ink" @click="router.push('/')">
      ← На главную
    </button>

    <div class="mb-6 flex items-center gap-3">
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
      class="mb-6"
    />

    <MarkdownView
      v-if="report"
      :source="report"
      :class="{ 'opacity-80': !isFinal }"
    />
    <span v-if="report && !isFinal" class="ml-0.5 inline-block animate-pulse text-accent">▍</span>

    <p v-else-if="!DONE.has(status)" class="text-muted">
      Отчёт формируется — он появится здесь в реальном времени.
    </p>
    <p v-else-if="status === 'failed'" class="text-muted">Не удалось сформировать отчёт.</p>
  </div>
</template>
