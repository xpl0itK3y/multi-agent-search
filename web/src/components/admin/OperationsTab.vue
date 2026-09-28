<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi, apiErrorMessage } from "@/lib/api";
import type { AdminAuditLogItem, AdminDryRunResult } from "@/lib/types";

const { t } = useI18n();

const auditLogs = ref<AdminAuditLogItem[]>([]);
const loadingLogs = ref(false);

const actionLoading = ref(false);
const message = ref<{ type: "success" | "error"; text: string } | null>(null);

// A success notice steps aside after 8 s; an error stays until dismissed or the next run,
// so it can't vanish before it's read (apple-design §16: errors persist until addressed).
const MESSAGE_MS = 8000;
let messageTimer: ReturnType<typeof setTimeout> | null = null;
function setMessage(next: { type: "success" | "error"; text: string } | null) {
  message.value = next;
  if (messageTimer) clearTimeout(messageTimer);
  messageTimer = null;
  if (next?.type === "success") {
    messageTimer = setTimeout(() => {
      message.value = null;
      messageTimer = null;
    }, MESSAGE_MS);
  }
}

// Dry Run Modal state
const modalOpen = ref(false);
const previewResult = ref<AdminDryRunResult | null>(null);
const pendingAction = ref<{ action: string; params: Record<string, any> } | null>(null);

// Single Requeue input
const requeueJobId = ref("");
const requeueJobType = ref<"finalize" | "search">("finalize");

// The confirmation says in words what will happen (§16: direct, specific labels): the
// card's own title instead of the raw action code, and a verb with the count.
const ACTION_TITLE: Record<string, string> = {
  recover_stale_finalize_jobs: "admin.operations.recoverStaleFinalizeTitle",
  recover_stale_search_jobs: "admin.operations.recoverStaleSearchTitle",
  cleanup_old_jobs: "admin.operations.cleanupOldTitle",
  cleanup_search_cache: "admin.operations.cleanupCacheTitle",
  requeue_finalize_job: "admin.operations.singleRequeueTitle",
  requeue_search_job: "admin.operations.singleRequeueTitle",
};

const modalAction = computed(() => previewResult.value?.action ?? pendingAction.value?.action ?? "");
const modalTitle = computed(() => {
  const key = ACTION_TITLE[modalAction.value];
  return key ? t(key) : t("admin.operations.dryRunModalTitle");
});
const isDeletion = computed(() => modalAction.value.startsWith("cleanup_"));
const affectedCount = computed(() => previewResult.value?.affected_count ?? 0);
const nothingToRun = computed(() => affectedCount.value === 0);

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
    setMessage(null);
    const res = await adminApi.previewOperation(action, params);
    previewResult.value = res;
    pendingAction.value = { action, params };
    modalOpen.value = true;
  } catch (err) {
    setMessage({ type: "error", text: apiErrorMessage(err, t) });
  } finally {
    actionLoading.value = false;
  }
}

async function confirmExecute() {
  if (!pendingAction.value || nothingToRun.value) return;
  try {
    actionLoading.value = true;
    closeModal();
    const res = await adminApi.executeOperation(
      pendingAction.value.action,
      pendingAction.value.params
    );
    setMessage({
      type: "success",
      text: res.summary || t("admin.operations.executed", { action: pendingAction.value.action }),
    });
    pendingAction.value = null;
    previewResult.value = null;
    // Refresh audit logs
    await fetchAuditLogs();
  } catch (err) {
    setMessage({ type: "error", text: apiErrorMessage(err, t) });
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

// ── The dry-run dialog: Escape and the backdrop close it, focus starts inside (on
// Cancel for a deletion or when there is nothing to run) and goes back on close. ──
const dialogId = useId();
const panelRef = ref<HTMLElement | null>(null);
const cancelRef = ref<HTMLButtonElement | null>(null);
const confirmRef = ref<HTMLButtonElement | null>(null);
let returnFocus: HTMLElement | null = null;

function closeModal() {
  modalOpen.value = false;
}

function onModalKey(e: KeyboardEvent) {
  if (!modalOpen.value) return;
  if (e.key === "Escape") {
    e.preventDefault();
    closeModal();
  } else if (e.key === "Tab" && panelRef.value) {
    const items = [...panelRef.value.querySelectorAll<HTMLElement>("button:not([disabled])")];
    if (!items.length) return;
    const at = items.indexOf(document.activeElement as HTMLElement);
    if (e.shiftKey && at <= 0) {
      e.preventDefault();
      items[items.length - 1].focus();
    } else if (!e.shiftKey && (at === -1 || at === items.length - 1)) {
      e.preventDefault();
      items[0].focus();
    }
  }
}

watch(modalOpen, async (open) => {
  if (typeof document === "undefined") return;
  if (open) {
    returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    document.addEventListener("keydown", onModalKey);
    await nextTick();
    if (!modalOpen.value) return;
    const start = isDeletion.value || nothingToRun.value ? cancelRef.value : confirmRef.value;
    start?.focus({ preventScroll: true });
  } else {
    document.removeEventListener("keydown", onModalKey);
    const back = returnFocus;
    returnFocus = null;
    if (back?.isConnected) back.focus({ preventScroll: true });
  }
});

onBeforeUnmount(() => {
  if (typeof document !== "undefined") document.removeEventListener("keydown", onModalKey);
  if (messageTimer) clearTimeout(messageTimer);
});

// The backdrop closes only when the press starts and ends on it, so a text selection
// dragged out of the panel doesn't close the dialog.
let pressedBackdrop = false;
function onBackdropPointerDown(e: PointerEvent) {
  pressedBackdrop = e.target === e.currentTarget;
}
function onBackdropClick(e: MouseEvent) {
  const close = pressedBackdrop && e.target === e.currentTarget;
  pressedBackdrop = false;
  if (close) closeModal();
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
      class="flex items-start justify-between gap-3 rounded-xl border p-4 text-xs font-medium"
      :class="
        message.type === 'success'
          ? 'border-success/30 bg-success/10 text-success'
          : 'border-danger/30 bg-danger/10 text-danger'
      "
      :role="message.type === 'success' ? 'status' : 'alert'"
      data-test="op-message"
    >
      <span>{{ message.text }}</span>
      <button
        type="button"
        class="press hit -m-1 shrink-0 rounded p-1 leading-none opacity-80 hover:opacity-100"
        :aria-label="t('common.close')"
        :title="t('common.close')"
        @click="setMessage(null)"
      >
        ✕
      </button>
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
            @click="triggerDryRun('recover_stale_finalize_jobs')"
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
            @click="triggerDryRun('recover_stale_search_jobs')"
          >
            {{ t("admin.operations.previewBtn") }}
          </button>
        </div>
      </div>

      <!-- Cleanup Old Jobs (a deletion: marked apart from the recoveries) -->
      <div class="rounded-xl border border-danger/20 bg-surface/30 p-5 flex flex-col justify-between">
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
            {{ t("admin.operations.previewDelete") }}
          </button>
        </div>
      </div>

      <!-- Cleanup Search Cache (a deletion) -->
      <div class="rounded-xl border border-danger/20 bg-surface/30 p-5 flex flex-col justify-between">
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
            {{ t("admin.operations.previewDelete") }}
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

    <!-- Dry-Run Preview Modal: the shared scrim and modal transition (§12, §7), teleported
         so the fixed layer sits outside this tab's spaced column. -->
    <Teleport to="body">
      <Transition name="modal">
        <div
          v-if="modalOpen"
          class="scrim fixed inset-0 z-50 flex items-center justify-center p-4"
          data-test="op-modal"
          @pointerdown="onBackdropPointerDown"
          @click="onBackdropClick"
        >
          <div
            ref="panelRef"
            role="dialog"
            aria-modal="true"
            :aria-labelledby="`${dialogId}-title`"
            class="modal-panel w-full max-w-md space-y-4 rounded-2xl border border-bd bg-surface p-5 text-ink shadow-e3"
          >
            <div class="flex items-start justify-between gap-3 border-b border-bd pb-3">
              <div class="min-w-0">
                <span class="block text-3xs font-semibold uppercase tracking-wider text-muted">{{ t("admin.operations.dryRunModalTitle") }}</span>
                <h3 :id="`${dialogId}-title`" class="mt-1 text-sm font-semibold text-ink">{{ modalTitle }}</h3>
                <span class="mt-0.5 block font-mono text-3xs text-muted">{{ modalAction }}</span>
              </div>
              <button
                type="button"
                class="press hit -m-1 shrink-0 rounded-lg p-1.5 text-xs leading-none text-muted hover:text-ink"
                :aria-label="t('common.close')"
                :title="t('common.close')"
                @click="closeModal"
              >
                ✕
              </button>
            </div>

            <div class="space-y-3 text-xs">
              <div class="rounded-xl border border-bd bg-bg/60 p-3">
                <span class="block text-3xs font-semibold uppercase tracking-wider text-muted">{{ t("admin.operations.dryRunAffectedLabel") }}</span>
                <div class="mt-1 text-2xl font-bold tabular-nums text-ink" data-test="op-affected">
                  {{ affectedCount }}
                </div>
                <p v-if="previewResult?.summary" class="mt-1 text-[11px] text-muted">{{ previewResult.summary }}</p>
              </div>

              <div v-if="previewResult?.sample_affected_ids?.length" class="space-y-1">
                <span class="block text-3xs font-semibold uppercase tracking-wider text-muted">{{ t("admin.operations.dryRunSampleIdsLabel") }}</span>
                <div class="max-h-24 space-y-0.5 overflow-y-auto rounded-lg bg-bg p-2 font-mono text-[10px] text-muted">
                  <div v-for="sid in previewResult.sample_affected_ids" :key="sid">
                    {{ sid }}
                  </div>
                </div>
              </div>

              <p v-if="nothingToRun" class="text-xs text-muted" data-test="op-nothing">{{ t("admin.operations.nothingToRun") }}</p>
            </div>

            <div class="flex items-center justify-end gap-2 border-t border-bd pt-4">
              <button
                ref="cancelRef"
                type="button"
                class="press rounded-lg border border-bd px-3 py-1.5 text-xs font-medium text-muted hover:bg-surfaceHover hover:text-ink"
                @click="closeModal"
              >
                {{ t("common.cancel") }}
              </button>
              <button
                ref="confirmRef"
                type="button"
                data-test="confirm-op"
                class="press rounded-lg px-4 py-1.5 text-xs font-semibold shadow-e1 disabled:opacity-50"
                :class="isDeletion ? 'bg-red-600 text-white hover:bg-red-700' : 'bg-accent text-onAccent hover:bg-accent/90'"
                :disabled="actionLoading || nothingToRun"
                @click="confirmExecute"
              >
                {{
                  isDeletion
                    ? t("admin.operations.confirmDeleteN", { n: affectedCount })
                    : t("admin.operations.confirmRequeueN", { n: affectedCount })
                }}
              </button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </div>
</template>
