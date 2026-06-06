<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";

const props = defineProps<{ id: string }>();
const router = useRouter();

const status = ref<string>("processing");
const report = ref<string | null>(null);
const loading = ref(true);
const errorMsg = ref<string | null>(null);
let timer: number | undefined;

const DONE = new Set(["completed", "failed"]);

async function poll() {
  try {
    const res = await api.getReport(props.id);
    status.value = res.status;
    report.value = res.final_report;
    loading.value = false;
    if (DONE.has(res.status) && timer) {
      window.clearInterval(timer);
      timer = undefined;
    }
  } catch (e) {
    errorMsg.value = (e as Error).message;
    loading.value = false;
  }
}

function statusLabel(s: string): string {
  const map: Record<string, string> = {
    processing: "Декомпозиция и поиск…",
    analyzing: "Синтез отчёта…",
    completed: "Готово",
    failed: "Ошибка",
  };
  return map[s] ?? s;
}

onMounted(() => {
  poll();
  timer = window.setInterval(poll, 3000);
});
onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
});
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
          'bg-accent animate-pulse': status !== 'completed' && status !== 'failed',
        }"
      />
      <span class="text-sm text-muted">{{ statusLabel(status) }}</span>
    </div>

    <p v-if="errorMsg" class="text-sm text-red-400">{{ errorMsg }}</p>

    <p v-else-if="loading" class="text-muted">Загрузка…</p>

    <div
      v-else-if="report"
      class="whitespace-pre-wrap font-sans text-[15px] leading-relaxed text-ink"
    >{{ report }}</div>

    <p v-else class="text-muted">
      Отчёт ещё формируется. Эта страница обновится автоматически.
    </p>
  </div>
</template>
