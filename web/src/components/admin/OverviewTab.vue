<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi, apiErrorMessage } from "@/lib/api";
import type { AdminOverviewResponse } from "@/lib/types";

const { t } = useI18n();

const props = defineProps<{
  initialOverview?: AdminOverviewResponse | null;
}>();
// Every fresh snapshot (a load or a stream event) goes up, so the admin header reports
// the same health as this tab.
const emit = defineEmits<{ (e: "update", data: AdminOverviewResponse): void }>();

const overview = ref<AdminOverviewResponse | null>(props.initialOverview || null);
const loading = ref(false);
const loadError = ref<string | null>(null);
const isStreaming = ref(false);
const streamError = ref(false);
let closeStream: (() => void) | null = null;

function setOverview(data: AdminOverviewResponse) {
  overview.value = data;
  loadError.value = null;
  emit("update", data);
}

// A snapshot the header fetched after this tab mounted.
watch(
  () => props.initialOverview,
  (data) => {
    if (data && !overview.value) overview.value = data;
  },
);

function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function timeAgo(isoString: string): string {
  if (!isoString) return "—";
  const diffSec = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000);
  if (diffSec < 5) return t("admin.overview.justNow");
  if (diffSec < 60) return t("admin.overview.secAgo", { n: diffSec });
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return t("admin.overview.minAgo", { n: diffMin });
  const diffHours = Math.floor(diffMin / 60);
  return t("admin.overview.hoursAgo", { n: diffHours });
}

// A failed load is said out loud (apple-design §16: error feedback), with a retry.
async function loadData() {
  if (loading.value) return;
  loading.value = true;
  loadError.value = null;
  try {
    setOverview(await adminApi.getOverview());
  } catch (err) {
    loadError.value = apiErrorMessage(err, t);
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  if (!overview.value) {
    loadData();
  }
  // Start SSE stream for real-time overview updates
  try {
    isStreaming.value = true;
    closeStream = adminApi.connectStream(
      (data) => {
        setOverview(data);
        streamError.value = false;
      },
      () => {
        // EventSource reconnects on its own; say so instead of claiming a live feed.
        streamError.value = true;
      }
    );
  } catch {
    streamError.value = true;
  }
});

onUnmounted(() => {
  if (closeStream) {
    closeStream();
    closeStream = null;
  }
});

const isHealthy = computed(() => {
  return (
    overview.value?.system_health?.overall === "healthy" &&
    (overview.value?.failed_tasks_count ?? 0) === 0
  );
});
</script>

<template>
  <!-- No snapshot yet: its own loading state, or the failure with a retry. -->
  <div v-if="!overview" class="flex h-48 flex-col items-center justify-center gap-3 text-sm" data-test="overview-empty">
    <template v-if="loadError && !loading">
      <p class="text-danger" role="alert">{{ loadError }}</p>
      <button
        type="button"
        class="press rounded-lg border border-bd bg-surface px-3 py-1.5 text-xs font-medium text-ink hover:bg-surfaceHover"
        @click="loadData"
      >
        {{ $t("common.retry") }}
      </button>
    </template>
    <span v-else class="text-muted">{{ t("common.loading") }}</span>
  </div>

  <div v-else class="space-y-6">
    <!-- Top System Health & Live Indicator -->
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-2">
        <!-- Breathes only while the live stream is actually connected. -->
        <span
          class="inline-block h-3 w-3 rounded-full"
          :class="[isHealthy ? 'bg-success' : 'bg-warning', isStreaming && !streamError ? 'live-dot' : '']"
          aria-hidden="true"
        />
        <h2 class="text-base font-semibold text-ink">
          {{ isHealthy ? t("admin.overview.systemHealthy") : t("admin.overview.systemDegradedTitle") }}
        </h2>
        <span class="rounded bg-surface px-2 py-0.5 text-xs text-muted" data-test="overview-stream">
          {{ isStreaming && !streamError ? t("admin.overview.liveSse") : t("admin.overview.reconnecting") }}
        </span>
      </div>

      <button
        type="button"
        class="press flex items-center gap-1.5 rounded-lg border border-bd bg-surface px-3 py-1.5 text-xs font-medium text-ink hover:bg-surfaceHover disabled:opacity-60"
        :disabled="loading"
        :aria-busy="loading ? 'true' : undefined"
        @click="loadData"
      >
        <span class="inline-block leading-none" :class="loading ? 'animate-spin' : ''" aria-hidden="true">↻</span>
        <span>{{ t("admin.overview.refresh") }}</span>
      </button>
    </div>
    <p v-if="loadError" class="-mt-4 text-right text-xs text-danger" role="alert">{{ loadError }}</p>

    <!-- Metric KPI Cards -->
    <div class="grid grid-cols-2 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      <!-- PostgreSQL Health -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.overview.postgres") }}</span>
          <span class="text-sm">🗄️</span>
        </div>
        <div class="mt-2 flex items-baseline gap-2">
          <span
            class="text-xl font-bold uppercase"
            :class="(overview?.system_health?.postgres || 'ok') === 'ok' ? 'text-success' : 'text-danger'"
          >
            {{ overview?.system_health?.postgres || "ok" }}
          </span>
          <span class="text-xs text-muted">{{ t("admin.overview.active") }}</span>
        </div>
      </div>

      <!-- Active Researches -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.overview.activeResearches") }}</span>
          <span class="text-sm">🔬</span>
        </div>
        <div class="mt-2 flex items-baseline gap-2">
          <span class="text-2xl font-bold tabular-nums text-ink">
            {{ overview?.active_researches_count ?? 0 }}
          </span>
          <span v-if="(overview?.active_researches_count ?? 0) > 0" class="live-dot text-xs text-accent">
            {{ t("admin.overview.running") }}
          </span>
        </div>
      </div>

      <!-- Pending Tasks -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.overview.pendingTasks") }}</span>
          <span class="text-sm">⏳</span>
        </div>
        <div class="mt-2 flex items-baseline gap-2">
          <span class="text-2xl font-bold tabular-nums text-ink">
            {{ overview?.pending_tasks_count ?? 0 }}
          </span>
          <span class="text-xs text-muted">{{ t("admin.overview.inQueues") }}</span>
        </div>
      </div>

      <!-- Failed / Dead-Letter Tasks -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.overview.failedTasks") }}</span>
          <span class="text-sm">⚠️</span>
        </div>
        <div class="mt-2 flex items-baseline gap-2">
          <span
            class="text-2xl font-bold tabular-nums"
            :class="(overview?.failed_tasks_count ?? 0) > 0 ? 'text-danger' : 'text-ink'"
          >
            {{ overview?.failed_tasks_count ?? 0 }}
          </span>
          <span class="text-xs text-muted">{{ t("admin.overview.deadLetters") }}</span>
        </div>
      </div>
    </div>

    <!-- Worker Fleet Section -->
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <h3 class="text-sm font-semibold uppercase tracking-wider text-muted">
          {{ t("admin.overview.workersTitle") }} ({{ overview?.workers?.length ?? 0 }})
        </h3>
      </div>

      <div
        v-if="!overview?.workers || overview.workers.length === 0"
        class="rounded-xl border border-dashed border-bd p-8 text-center text-sm text-muted"
      >
        {{ t("admin.overview.noWorkers") }}
      </div>

      <div v-else class="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
        <div
          v-for="w in overview.workers"
          :key="w.worker_name"
          class="flex flex-col justify-between rounded-xl border border-bd bg-surface/30 p-5 transition-all hover:border-bd/80 hover:bg-surface/50"
        >
          <div>
            <!-- Worker Header -->
            <div class="flex items-start justify-between gap-2">
              <div class="min-w-0">
                <div class="flex items-center gap-2">
                  <span
                    class="h-2 w-2 shrink-0 rounded-full"
                    :class="w.is_alive ? 'bg-success' : 'bg-danger'"
                  />
                  <h4 class="truncate font-semibold text-ink text-sm">{{ w.worker_name }}</h4>
                </div>
                <p class="mt-0.5 text-xs text-muted">
                  {{ t("admin.overview.lastSeen") }}: {{ timeAgo(w.last_seen_at) }}
                </p>
              </div>

              <span
                class="rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider"
                :class="w.is_alive ? 'bg-success/15 text-success' : 'bg-danger/15 text-danger'"
              >
                {{ w.is_alive ? t("admin.overview.alive") : t("admin.overview.offline") }}
              </span>
            </div>

            <!-- Stats Bar -->
            <div class="mt-4 grid grid-cols-2 gap-2 rounded-lg bg-bg/50 p-2 text-xs">
              <div>
                <span class="text-muted block text-[10px] uppercase tracking-wider">{{ t("admin.overview.status") }}</span>
                <span class="font-medium text-ink capitalize">{{ w.status }}</span>
              </div>
              <div>
                <span class="text-muted block text-[10px] uppercase tracking-wider">{{ t("admin.overview.jobsProcessed") }}</span>
                <span class="font-bold tabular-nums text-accent">{{ w.processed_jobs }}</span>
              </div>
            </div>

            <!-- Extraction Metrics if present -->
            <div
              v-if="w.extraction_metrics && Object.keys(w.extraction_metrics).length > 0"
              class="mt-3 space-y-1 text-xs border-t border-bd/40 pt-2"
            >
              <div class="text-[11px] font-medium text-muted flex justify-between">
                <span>{{ t("admin.overview.metrics") }}:</span>
                <span class="tabular-nums text-ink">
                  {{ w.extraction_metrics.success_count || 0 }} ok / {{ w.extraction_metrics.attempts || 0 }} total
                </span>
              </div>
              <div class="text-[11px] text-muted flex justify-between">
                <span>{{ t("admin.overview.downloaded") }}:</span>
                <span class="tabular-nums text-ink">{{ formatBytes(w.extraction_metrics.downloaded_bytes || 0) }}</span>
              </div>
            </div>

            <!-- Error Banner -->
            <div
              v-if="w.last_error"
              class="mt-3 rounded-lg border border-danger/20 bg-danger/10 p-2 text-[11px] text-danger truncate"
              :title="w.last_error"
            >
              {{ w.last_error }}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
