<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi } from "@/lib/api";
import type { AdminAuditLogItem, AdminDryRunResult } from "@/lib/types";

const { t } = useI18n();

const auditLogs = ref<AdminAuditLogItem[]>([]);
const loadingLogs = ref(false);

const actionLoading = ref(false);
const message = ref<{ type: "success" | "error"; text: string } | null>(null);

// Dry Run Modal state
const modalOpen = ref(false);
const previewResult = ref<AdminDryRunResult | null>(null);
const pendingAction = ref<{ action: string; params: Record<string, any> } | null>(null);

// Single Requeue input
const requeueJobId = ref("");
const requeueJobType = ref<"finalize" | "search">("finalize");

function formatDate(iso: string): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

async function fetchAuditLogs() {
  try {
    loadingLogs.value = true;
    auditLogs.value = await adminApi.getAuditLogs(50, 0);
  } catch {
    /* handled */
  } finally {
    loadingLogs.value = false;
  }
}

onMounted(() => {
  fetchAuditLogs();
});

async function triggerDryRun(action: string, params: Record<string, any> = {}) {
  try {
    actionLoading.value = true;
    message.value = null;
    const res = await adminApi.previewOperation(action, params);
    previewResult.value = res;
    pendingAction.value = { action, params };
    modalOpen.value = true;
  } catch (err: any) {
    message.value = { type: "error", text: err.message || "Failed to preview operation" };
  } finally {
    actionLoading.value = false;
  }
}

async function confirmExecute() {
  if (!pendingAction.value) return;
  try {
    actionLoading.value = true;
    modalOpen.value = false;
    const res = await adminApi.executeOperation(
      pendingAction.value.action,
      pendingAction.value.params
    );
    message.value = {
      type: "success",
      text: res.summary || `Successfully executed ${pendingAction.value.action}`,
    };
    pendingAction.value = null;
    previewResult.value = null;
    // Refresh audit logs
    await fetchAuditLogs();
  } catch (err: any) {
    message.value = { type: "error", text: err.message || "Failed to execute operation" };
  } finally {
    actionLoading.value = false;
  }
}

async function executeRequeue() {
  const jid = requeueJobId.value.trim();
  if (!jid) return;
  const action = requeueJobType.value === "finalize" ? "requeue_finalize_job" : "requeue_search_job";
  await triggerDryRun(action, { target_id: jid });
}
</script>

<template>
  <div class="space-y-8">
    <!-- Header -->
    <div>
      <h2 class="text-base font-semibold text-ink">{{ t("admin.operations.title") }}</h2>
      <p class="text-xs text-muted">
        {{ t("admin.operations.subtitle") }}
      </p>
    </div>

    <!-- Alert / Status message -->
    <div
      v-if="message"
      class="rounded-xl border p-4 text-xs font-medium"
      :class="
        message.type === 'success'
          ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400'
          : 'border-red-500/30 bg-red-500/10 text-red-400'
      "
    >
      {{ message.text }}
    </div>

    <!-- Maintenance Operations Cards -->
    <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
      <!-- Stale Finalize Jobs -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center gap-2 font-semibold text-sm text-ink">
            <span>🔄</span>
            <span>{{ t("admin.operations.recoverStaleFinalizeTitle") }}</span>
          </div>
          <p class="mt-1 text-xs text-muted">
            {{ t("admin.operations.recoverStaleFinalizeDesc") }}
          </p>
        </div>
        <div class="mt-4 flex gap-2">
          <button
            class="rounded-lg bg-surface border border-bd px-3 py-1.5 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-50"
            :disabled="actionLoading"
            @click="triggerDryRun('recover_stale_finalize_jobs', { stale_seconds: 300 })"
          >
            {{ t("admin.operations.previewBtn") }}
          </button>
        </div>
      </div>

      <!-- Stale Search Jobs -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center gap-2 font-semibold text-sm text-ink">
            <span>🔎</span>
            <span>{{ t("admin.operations.recoverStaleSearchTitle") }}</span>
          </div>
          <p class="mt-1 text-xs text-muted">
            {{ t("admin.operations.recoverStaleSearchDesc") }}
          </p>
        </div>
        <div class="mt-4 flex gap-2">
          <button
            class="rounded-lg bg-surface border border-bd px-3 py-1.5 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-50"
            :disabled="actionLoading"
            @click="triggerDryRun('recover_stale_search_jobs', { stale_seconds: 300 })"
          >
            {{ t("admin.operations.previewBtn") }}
          </button>
        </div>
      </div>

      <!-- Cleanup Old Jobs -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center gap-2 font-semibold text-sm text-ink">
            <span>🧹</span>
            <span>{{ t("admin.operations.cleanupOldTitle") }}</span>
          </div>
          <p class="mt-1 text-xs text-muted">
            {{ t("admin.operations.cleanupOldDesc") }}
          </p>
        </div>
        <div class="mt-4 flex gap-2">
          <button
            class="rounded-lg bg-surface border border-bd px-3 py-1.5 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-50"
            :disabled="actionLoading"
            @click="triggerDryRun('cleanup_old_jobs', { days: 7 })"
          >
            {{ t("admin.operations.previewBtn") }}
          </button>
        </div>
      </div>

      <!-- Cleanup Search Cache -->
      <div class="rounded-xl border border-bd bg-surface/30 p-5 flex flex-col justify-between">
        <div>
          <div class="flex items-center gap-2 font-semibold text-sm text-ink">
            <span>⚡</span>
            <span>{{ t("admin.operations.cleanupCacheTitle") }}</span>
          </div>
          <p class="mt-1 text-xs text-muted">
            {{ t("admin.operations.cleanupCacheDesc") }}
          </p>
        </div>
        <div class="mt-4 flex gap-2">
          <button
            class="rounded-lg bg-surface border border-bd px-3 py-1.5 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-50"
            :disabled="actionLoading"
            @click="triggerDryRun('cleanup_search_cache', { days: 3 })"
          >
            {{ t("admin.operations.previewBtn") }}
          </button>
        </div>
      </div>
    </div>

    <!-- Targeted Job Requeue Tool -->
    <div class="rounded-xl border border-bd bg-surface/30 p-5">
      <h3 class="text-sm font-semibold text-ink">{{ t("admin.operations.singleRequeueTitle") }}</h3>
      <p class="mt-1 text-xs text-muted">{{ t("admin.operations.singleRequeueDesc") }}</p>

      <div class="mt-4 flex flex-wrap items-center gap-3">
        <select
          v-model="requeueJobType"
          class="rounded-lg border border-bd bg-bg px-3 py-1.5 text-xs text-ink focus:outline-none"
        >
          <option value="finalize">{{ t("admin.operations.finalizeJobOpt") }}</option>
          <option value="search">{{ t("admin.operations.searchJobOpt") }}</option>
        </select>

        <input
          v-model="requeueJobId"
          type="text"
          :placeholder="t('admin.operations.jobIdPlaceholder')"
          class="min-w-64 flex-1 rounded-lg border border-bd bg-bg px-3 py-1.5 font-mono text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none"
        />

        <button
          class="rounded-lg bg-accent px-4 py-1.5 text-xs font-semibold text-white transition hover:bg-accent/90 disabled:opacity-50"
          :disabled="!requeueJobId.trim() || actionLoading"
          @click="executeRequeue"
        >
          {{ t("admin.operations.previewAndRequeueBtn") }}
        </button>
      </div>
    </div>

    <!-- Audit Log Table -->
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <div>
          <h3 class="text-xs font-bold uppercase tracking-wider text-muted">
            {{ t("admin.operations.auditTitle") }}
          </h3>
          <p class="text-[11px] text-muted">{{ t("admin.operations.auditSubtitle") }}</p>
        </div>

        <button
          class="rounded-lg border border-bd bg-surface px-2.5 py-1 text-xs font-medium text-ink hover:bg-surface/80"
          @click="fetchAuditLogs"
        >
          {{ t("admin.operations.refreshLogBtn") }}
        </button>
      </div>

      <div class="overflow-x-auto rounded-xl border border-bd bg-surface/20">
        <table class="w-full text-left text-xs">
          <thead class="border-b border-bd bg-surface/60 text-muted uppercase text-[10px] tracking-wider">
            <tr>
              <th class="px-4 py-3">{{ t("admin.operations.time") }}</th>
              <th class="px-4 py-3">{{ t("admin.operations.actor") }}</th>
              <th class="px-4 py-3">{{ t("admin.operations.action") }}</th>
              <th class="px-4 py-3">{{ t("admin.operations.target") }}</th>
              <th class="px-4 py-3">{{ t("admin.operations.details") }}</th>
              <th class="px-4 py-3 text-right">IP</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-bd/40 font-mono text-[11px]">
            <tr v-if="loadingLogs" class="text-center font-sans">
              <td colspan="6" class="py-8 text-muted">{{ t("common.loading") }}</td>
            </tr>
            <tr v-else-if="auditLogs.length === 0" class="text-center font-sans">
              <td colspan="6" class="py-8 text-muted">{{ t("admin.operations.noAuditLogs") }}</td>
            </tr>
            <tr v-for="log in auditLogs" :key="log.id" class="transition hover:bg-surface/50">
              <td class="px-4 py-2.5 whitespace-nowrap text-muted">{{ formatDate(log.created_at) }}</td>
              <td class="px-4 py-2.5 whitespace-nowrap text-ink font-semibold font-sans">{{ log.actor_email }}</td>
              <td class="px-4 py-2.5 whitespace-nowrap">
                <span class="rounded bg-accent/15 px-2 py-0.5 text-accent font-semibold">
                  {{ log.action }}
                </span>
              </td>
              <td class="px-4 py-2.5 whitespace-nowrap text-ink">
                {{ log.target_type }} {{ log.target_id ? `(${log.target_id.slice(0, 8)}…)` : '' }}
              </td>
              <td class="px-4 py-2.5 max-w-xs truncate text-muted font-sans" :title="JSON.stringify(log.details)">
                {{ log.details.summary || JSON.stringify(log.details) }}
              </td>
              <td class="px-4 py-2.5 text-right whitespace-nowrap text-muted">
                {{ log.ip_address || "local" }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Dry-Run Preview Modal -->
    <div
      v-if="modalOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
    >
      <div class="w-full max-w-md rounded-2xl border border-bd bg-bg p-6 shadow-2xl space-y-4">
        <div class="flex items-center justify-between border-b border-bd pb-3">
          <div class="flex items-center gap-2">
            <span class="text-lg">🛡️</span>
            <h3 class="font-bold text-ink text-sm">{{ t("admin.operations.dryRunModalTitle") }}</h3>
          </div>
          <button class="text-muted hover:text-ink text-xs" @click="modalOpen = false">✕</button>
        </div>

        <div class="space-y-3 text-xs">
          <div>
            <span class="text-muted block text-[10px] uppercase font-bold tracking-wider">{{ t("admin.operations.dryRunActionLabel") }}</span>
            <span class="font-mono font-semibold text-accent text-sm">{{ previewResult?.action }}</span>
          </div>

          <div class="rounded-xl border border-bd bg-surface/50 p-3">
            <span class="text-muted block text-[10px] uppercase font-bold tracking-wider">{{ t("admin.operations.dryRunAffectedLabel") }}</span>
            <div class="text-2xl font-black text-ink mt-1">
              {{ previewResult?.affected_count ?? 0 }}
            </div>
            <p class="text-muted text-[11px] mt-1">{{ previewResult?.summary }}</p>
          </div>

          <div v-if="previewResult?.sample_affected_ids?.length" class="space-y-1">
            <span class="text-muted block text-[10px] uppercase font-bold tracking-wider">{{ t("admin.operations.dryRunSampleIdsLabel") }}</span>
            <div class="max-h-24 overflow-y-auto rounded-lg bg-bg p-2 font-mono text-[10px] text-muted space-y-0.5">
              <div v-for="sid in previewResult.sample_affected_ids" :key="sid">
                {{ sid }}
              </div>
            </div>
          </div>
        </div>

        <div class="flex items-center justify-end gap-2 border-t border-bd pt-4">
          <button
            class="rounded-lg border border-bd px-3 py-1.5 text-xs font-medium text-muted hover:bg-surface hover:text-ink"
            @click="modalOpen = false"
          >
            {{ t("common.cancel") }}
          </button>
          <button
            class="rounded-lg bg-accent px-4 py-1.5 text-xs font-bold text-white shadow transition hover:bg-accent/90 disabled:opacity-50"
            :disabled="actionLoading"
            @click="confirmExecute"
          >
            {{ t("admin.operations.confirmAndExecuteBtn") }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
