<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi } from "@/lib/api";
import type { AdminOverviewResponse } from "@/lib/types";

const { t } = useI18n();

type Tab = "overview" | "analytics" | "agents" | "operations";
const activeTab = ref<Tab>("overview");

const overview = ref<AdminOverviewResponse | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);

async function loadOverview() {
  try {
    loading.value = true;
    error.value = null;
    overview.value = await adminApi.getOverview();
  } catch (err: any) {
    error.value = err.message || "Failed to load admin overview";
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  loadOverview();
});
</script>

<template>
  <div class="flex h-full flex-col overflow-y-auto bg-bg p-6 text-ink">
    <!-- Header -->
    <div class="mb-6 flex flex-wrap items-center justify-between gap-4 border-b border-bd pb-4">
      <div class="flex items-center gap-3">
        <div class="grid h-10 w-10 place-items-center rounded-xl bg-accent/15 text-xl text-accent">
          🛡️
        </div>
        <div>
          <h1 class="text-xl font-bold tracking-tight text-ink">{{ t("admin.title") }}</h1>
          <p class="text-xs text-muted">
            {{ overview?.system_health?.overall === "healthy" ? "All systems operational" : "System operational" }}
          </p>
        </div>
      </div>

      <!-- Dev mode warning banner if auth is disabled -->
      <div
        v-if="overview?.is_dev_mode"
        class="flex items-center gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-1.5 text-xs font-medium text-amber-400"
      >
        <span>⚠️</span>
        <span>{{ t("admin.devModeBadge") }}</span>
      </div>
    </div>

    <!-- Navigation Tabs -->
    <div class="mb-6 flex border-b border-bd">
      <button
        class="border-b-2 px-4 py-2.5 text-sm font-medium transition-colors"
        :class="activeTab === 'overview' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
        @click="activeTab = 'overview'"
      >
        {{ t("admin.tabs.overview") }}
      </button>
      <button
        class="border-b-2 px-4 py-2.5 text-sm font-medium transition-colors"
        :class="activeTab === 'analytics' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
        @click="activeTab = 'analytics'"
      >
        {{ t("admin.tabs.analytics") }}
      </button>
      <button
        class="border-b-2 px-4 py-2.5 text-sm font-medium transition-colors"
        :class="activeTab === 'agents' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
        @click="activeTab = 'agents'"
      >
        {{ t("admin.tabs.agents") }}
      </button>
      <button
        class="border-b-2 px-4 py-2.5 text-sm font-medium transition-colors"
        :class="activeTab === 'operations' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
        @click="activeTab = 'operations'"
      >
        {{ t("admin.tabs.operations") }}
      </button>
    </div>

    <!-- Tab Viewport (Stages 5-8 will flesh these out) -->
    <div class="flex-1">
      <div v-if="loading" class="flex h-48 items-center justify-center text-sm text-muted">
        {{ t("common.loading") }}
      </div>
      <div v-else-if="error" class="rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
        {{ error }}
      </div>
      <div v-else>
        <!-- Placeholders will be replaced in Stage 5, 6, 7, 8 -->
        <div v-if="activeTab === 'overview'" class="rounded-xl border border-bd bg-surface/50 p-6">
          <h2 class="text-base font-semibold">{{ t("admin.tabs.overview") }}</h2>
          <div class="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div class="rounded-lg border border-bd bg-bg p-4">
              <div class="text-xs text-muted">{{ t("admin.overview.activeResearches") }}</div>
              <div class="mt-1 text-2xl font-bold">{{ overview?.active_researches_count ?? 0 }}</div>
            </div>
            <div class="rounded-lg border border-bd bg-bg p-4">
              <div class="text-xs text-muted">{{ t("admin.overview.pendingTasks") }}</div>
              <div class="mt-1 text-2xl font-bold">{{ overview?.pending_tasks_count ?? 0 }}</div>
            </div>
            <div class="rounded-lg border border-bd bg-bg p-4">
              <div class="text-xs text-muted">{{ t("admin.overview.failedTasks") }}</div>
              <div class="mt-1 text-2xl font-bold text-red-400">{{ overview?.failed_tasks_count ?? 0 }}</div>
            </div>
          </div>
        </div>

        <div v-else-if="activeTab === 'analytics'" class="rounded-xl border border-bd bg-surface/50 p-6">
          <h2 class="text-base font-semibold">{{ t("admin.tabs.analytics") }}</h2>
        </div>

        <div v-else-if="activeTab === 'agents'" class="rounded-xl border border-bd bg-surface/50 p-6">
          <h2 class="text-base font-semibold">{{ t("admin.tabs.agents") }}</h2>
        </div>

        <div v-else-if="activeTab === 'operations'" class="rounded-xl border border-bd bg-surface/50 p-6">
          <h2 class="text-base font-semibold">{{ t("admin.tabs.operations") }}</h2>
        </div>
      </div>
    </div>
  </div>
</template>
