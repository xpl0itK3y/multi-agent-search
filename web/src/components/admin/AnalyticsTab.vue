<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi, apiErrorMessage } from "@/lib/api";
import { saveFile } from "@/lib/download";
import type { AdminTokenAnalyticsResponse } from "@/lib/types";

const { t } = useI18n();

const analytics = ref<AdminTokenAnalyticsResponse | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
const page = ref(1);
const pageSize = ref(15);

function formatNumber(num: number): string {
  if (!num) return "0";
  return new Intl.NumberFormat().format(num);
}

function formatCurrency(num: number): string {
  if (!num) return "$0.0000";
  return "$" + num.toFixed(4);
}

function formatDate(iso: string): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

async function fetchAnalytics() {
  try {
    loading.value = true;
    error.value = null;
    analytics.value = await adminApi.getTokens(page.value, pageSize.value);
  } catch (err: any) {
    error.value = err.message || "Failed to load token analytics";
  } finally {
    loading.value = false;
  }
}

watch([page, pageSize], () => {
  fetchAnalytics();
});

onMounted(() => {
  fetchAnalytics();
});

const totalPages = computed(() => {
  if (!analytics.value || !analytics.value.total_researches) return 1;
  return Math.ceil(analytics.value.total_researches / pageSize.value);
});

// Fetched with the bearer token (window.open sends only the cookie); a failed
// export is shown instead of opening a tab with a raw 401.
const exporting = ref(false);
const exportError = ref<string | null>(null);

async function exportCsv() {
  if (exporting.value) return;
  exporting.value = true;
  exportError.value = null;
  try {
    saveFile(await adminApi.exportTokensCsv(), "token_usage.csv");
  } catch (err) {
    exportError.value = apiErrorMessage(err, t);
  } finally {
    exporting.value = false;
  }
}
</script>

<template>
  <div class="space-y-6">
    <!-- Header & CSV Export -->
    <div class="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h2 class="text-base font-semibold text-ink">{{ t("admin.analytics.title") }}</h2>
        <p class="text-xs text-muted">{{ t("admin.analytics.subtitle") }}</p>
      </div>

      <button
        class="flex items-center gap-2 rounded-lg bg-accent px-4 py-2 text-xs font-semibold text-white shadow-sm transition hover:bg-accent/90 disabled:opacity-50"
        :disabled="exporting"
        @click="exportCsv"
      >
        <span>📥</span>
        <span>{{ t("admin.analytics.exportCsv") }}</span>
      </button>
    </div>

    <p v-if="exportError" class="text-right text-xs text-red-400">{{ exportError }}</p>

    <!-- Error state -->
    <div v-if="error" class="rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-xs text-red-400">
      {{ error }}
    </div>

    <!-- KPI Summary Grid -->
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-4">
      <!-- Total Tokens -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="text-xs font-medium text-muted">{{ t("admin.analytics.totalTokens") }}</div>
        <div class="mt-2 text-2xl font-black text-ink">
          {{ formatNumber(analytics?.total_tokens ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">{{ t("admin.analytics.promptPlusCompletion") }}</div>
      </div>

      <!-- Total Cost -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="text-xs font-medium text-muted">{{ t("admin.analytics.totalCost") }}</div>
        <div class="mt-2 text-2xl font-black text-emerald-400">
          {{ formatCurrency(analytics?.total_cost_usd ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">{{ t("admin.analytics.estimatedSpend") }}</div>
      </div>

      <!-- Prompt Tokens -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="text-xs font-medium text-muted">{{ t("admin.analytics.promptTokens") }}</div>
        <div class="mt-2 text-2xl font-black text-ink">
          {{ formatNumber(analytics?.total_prompt_tokens ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">{{ t("admin.analytics.inputContextProcessed") }}</div>
      </div>

      <!-- Completion Tokens -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4">
        <div class="text-xs font-medium text-muted">{{ t("admin.analytics.completionTokens") }}</div>
        <div class="mt-2 text-2xl font-black text-ink">
          {{ formatNumber(analytics?.total_completion_tokens ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">{{ t("admin.analytics.generatedReports") }}</div>
      </div>
    </div>

    <!-- Breakdown Grid (By Model & By Depth) -->
    <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
      <!-- By Model -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5">
        <h3 class="text-xs font-bold uppercase tracking-wider text-muted mb-4">
          {{ t("admin.analytics.byModel") }}
        </h3>

        <div v-if="!analytics?.by_model?.length" class="text-xs text-muted py-4 text-center">
          {{ t("admin.analytics.noModelUsage") }}
        </div>

        <div v-else class="space-y-4">
          <div
            v-for="m in analytics.by_model"
            :key="m.model"
            class="space-y-1 rounded-lg bg-bg/50 p-3"
          >
            <div class="flex items-center justify-between text-xs">
              <span class="font-mono font-semibold text-ink">{{ m.model }}</span>
              <span class="font-bold text-emerald-400">{{ formatCurrency(m.estimated_cost_usd) }}</span>
            </div>

            <!-- Mini Progress bar -->
            <div class="h-1.5 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full bg-accent rounded-full transition-all"
                :style="{
                  width: `${Math.min(100, Math.max(5, (m.total_tokens / (analytics?.total_tokens || 1)) * 100))}%`,
                }"
              />
            </div>

            <div class="flex items-center justify-between text-[11px] text-muted pt-1">
              <span>{{ formatNumber(m.total_tokens) }} {{ t("admin.analytics.tokens") }} ({{ m.calls_count }} {{ t("admin.analytics.calls") }})</span>
              <span>{{ t("admin.analytics.inTokens") }}: {{ formatNumber(m.prompt_tokens) }} / {{ t("admin.analytics.outTokens") }}: {{ formatNumber(m.completion_tokens) }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- By Depth -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5">
        <h3 class="text-xs font-bold uppercase tracking-wider text-muted mb-4">
          {{ t("admin.analytics.byDepth") }}
        </h3>

        <div v-if="!analytics?.by_depth?.length" class="text-xs text-muted py-4 text-center">
          {{ t("admin.analytics.noDepthData") }}
        </div>

        <div v-else class="grid grid-cols-1 gap-3 sm:grid-cols-3">
          <div
            v-for="d in analytics.by_depth"
            :key="d.depth"
            class="rounded-xl border border-bd bg-bg/60 p-4 text-center"
          >
            <div class="inline-block rounded px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider"
              :class="{
                'bg-emerald-500/15 text-emerald-400': d.depth.toLowerCase() === 'easy',
                'bg-blue-500/15 text-blue-400': d.depth.toLowerCase() === 'medium',
                'bg-purple-500/15 text-purple-400': d.depth.toLowerCase() === 'hard',
              }"
            >
              {{ d.depth }}
            </div>
            <div class="mt-2 text-lg font-bold text-ink">{{ formatNumber(d.total_tokens) }}</div>
            <div class="text-[11px] text-emerald-400 font-semibold">{{ formatCurrency(d.estimated_cost_usd) }}</div>
            <div class="mt-1 text-[10px] text-muted">{{ d.researches_count }} {{ t("admin.analytics.researches") }}</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Researches Usage Table -->
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <h3 class="text-xs font-bold uppercase tracking-wider text-muted">
          {{ t("admin.analytics.researchesTitle") }} ({{ analytics?.total_researches ?? 0 }})
        </h3>
      </div>

      <div class="overflow-x-auto rounded-xl border border-bd bg-surface/20">
        <table class="w-full text-left text-xs">
          <thead class="border-b border-bd bg-surface/60 text-muted uppercase text-[10px] tracking-wider">
            <tr>
              <th class="px-4 py-3">{{ t("admin.analytics.colResearchId") }}</th>
              <th class="px-4 py-3">{{ t("admin.analytics.colPrompt") }}</th>
              <th class="px-4 py-3">{{ t("admin.analytics.colDepth") }}</th>
              <th class="px-4 py-3">{{ t("admin.analytics.colStatus") }}</th>
              <th class="px-4 py-3 text-right">{{ t("admin.analytics.colTokens") }}</th>
              <th class="px-4 py-3 text-right">{{ t("admin.analytics.colCost") }}</th>
              <th class="px-4 py-3 text-right">{{ t("admin.analytics.colCreated") }}</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-bd/40">
            <tr v-if="loading" class="text-center">
              <td colspan="7" class="py-8 text-muted">{{ t("common.loading") }}</td>
            </tr>
            <tr v-else-if="!analytics?.researches?.length" class="text-center">
              <td colspan="7" class="py-8 text-muted">{{ t("admin.analytics.noResearches") }}</td>
            </tr>
            <tr
              v-for="r in analytics?.researches"
              :key="r.research_id"
              class="transition hover:bg-surface/50"
            >
              <td class="px-4 py-3 font-mono text-[11px] text-muted whitespace-nowrap">
                <router-link :to="`/research/${r.research_id}`" class="text-accent hover:underline">
                  {{ r.research_id.slice(0, 8) }}…
                </router-link>
              </td>
              <td class="px-4 py-3 max-w-xs truncate text-ink font-medium" :title="r.prompt">
                {{ r.prompt }}
              </td>
              <td class="px-4 py-3 whitespace-nowrap">
                <span
                  class="rounded px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider"
                  :class="{
                    'bg-emerald-500/15 text-emerald-400': r.depth.toLowerCase() === 'easy',
                    'bg-blue-500/15 text-blue-400': r.depth.toLowerCase() === 'medium',
                    'bg-purple-500/15 text-purple-400': r.depth.toLowerCase() === 'hard',
                  }"
                >
                  {{ r.depth }}
                </span>
              </td>
              <td class="px-4 py-3 whitespace-nowrap">
                <span class="rounded bg-surface px-1.5 py-0.5 text-[10px] text-ink capitalize">
                  {{ r.status }}
                </span>
              </td>
              <td class="px-4 py-3 text-right font-mono font-bold text-ink whitespace-nowrap">
                {{ formatNumber(r.total_tokens) }}
              </td>
              <td class="px-4 py-3 text-right font-mono font-semibold text-emerald-400 whitespace-nowrap">
                {{ formatCurrency(r.estimated_cost_usd) }}
              </td>
              <td class="px-4 py-3 text-right text-muted whitespace-nowrap">
                {{ formatDate(r.created_at) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Pagination Controls -->
      <div class="flex items-center justify-between px-2 pt-2 text-xs text-muted">
        <div>
          <span>{{ t("admin.analytics.page") }} {{ page }} {{ t("admin.analytics.of") }} {{ totalPages }}</span>
        </div>

        <div class="flex items-center gap-2">
          <button
            class="rounded border border-bd px-2.5 py-1 text-xs font-medium text-ink transition hover:bg-surface disabled:opacity-30"
            :disabled="page <= 1"
            @click="page--"
          >
            {{ t("admin.analytics.prev") }}
          </button>
          <button
            class="rounded border border-bd px-2.5 py-1 text-xs font-medium text-ink transition hover:bg-surface disabled:opacity-30"
            :disabled="page >= totalPages"
            @click="page++"
          >
            {{ t("admin.analytics.next") }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
