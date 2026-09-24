<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { useAuthStore } from "@/stores/auth";
import { adminApi, apiErrorMessage, type ApiFile } from "@/lib/api";
import { saveFile } from "@/lib/download";
import type {
  AdminEventLogItem,
  AdminPromptItem,
  AdminTelemetrySummaryResponse,
  AdminUserDetailResponse,
  AdminUserListItem,
} from "@/lib/types";

const { t } = useI18n();
const auth = useAuthStore();
const deletingUserId = ref<string | null>(null);

// ── KPI & Summary ────────────────────────────────────────────────────────────
const summary = ref<AdminTelemetrySummaryResponse | null>(null);
const summaryLoading = ref(true);

// ── Views & Filters ──────────────────────────────────────────────────────────
type SubView = "directory" | "prompts" | "platforms" | "feed";
const activeSubView = ref<SubView>("directory");

const users = ref<AdminUserListItem[]>([]);
const totalUsers = ref(0);
const onlineUsers = ref(0);
const usersLoading = ref(false);
const usersError = ref<string | null>(null);

const page = ref(1);
const pageSize = ref(15);
const searchQuery = ref("");
const roleFilter = ref("");
const onlineOnly = ref(false);
const sortBy = ref("activity");

// ── Prompts Stream ───────────────────────────────────────────────────────────
const prompts = ref<AdminPromptItem[]>([]);
const promptsTotal = ref(0);
const promptsLoading = ref(false);
const promptsError = ref<string | null>(null);
const promptsPage = ref(1);
const promptsPageSize = ref(15);
const promptsSearch = ref("");
const promptsTypeFilter = ref<"all" | "research" | "chat">("all");
const copiedPromptId = ref<string | null>(null);

// ── Live Feed ────────────────────────────────────────────────────────────────
const events = ref<AdminEventLogItem[]>([]);
const eventsTotal = ref(0);
const eventsLoading = ref(false);
const eventsError = ref<string | null>(null);
const eventCategory = ref("");
const autoRefresh = ref(true);
let autoRefreshTimer: ReturnType<typeof setInterval> | null = null;

// ── User Detail Drawer ───────────────────────────────────────────────────────
const drawerOpen = ref(false);
const selectedUserId = ref<string | null>(null);
const selectedUserDetail = ref<AdminUserDetailResponse | null>(null);
const drawerLoading = ref(false);
const drawerError = ref<string | null>(null);
const drawerTab = ref<"profile" | "sessions" | "researches" | "events">("profile");

const activeUser = computed<AdminUserListItem | null>(() => {
  if (selectedUserDetail.value?.user) return selectedUserDetail.value.user;
  if (selectedUserId.value) {
    return users.value.find((u) => u.id === selectedUserId.value) || null;
  }
  return null;
});

const drawerResearches = computed(() => {
  return selectedUserDetail.value?.researches || selectedUserDetail.value?.recent_researches || [];
});

const drawerSessions = computed(() => {
  return selectedUserDetail.value?.sessions || [];
});

const drawerEvents = computed(() => {
  return selectedUserDetail.value?.recent_events || [];
});

// ── Formatters ───────────────────────────────────────────────────────────────
function formatNumber(num: number | undefined | null): string {
  if (num === undefined || num === null) return "0";
  return new Intl.NumberFormat().format(num);
}

function formatCurrency(num: number | undefined | null): string {
  if (num === undefined || num === null) return "$0.0000";
  return "$" + num.toFixed(4);
}

function formatDate(iso: string | undefined | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function timeAgo(iso: string | undefined | null): string {
  if (!iso) return "—";
  const diffSec = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diffSec < 10) return t("admin.users.justNow");
  if (diffSec < 60) return `${diffSec} ${t("admin.users.secAgo")}`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin} ${t("admin.users.minAgo")}`;
  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) return `${diffHours} ${t("admin.users.hoursAgo")}`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays} ${t("admin.users.daysAgo")}`;
}

// ── API Fetchers ─────────────────────────────────────────────────────────────
async function loadSummary() {
  try {
    summaryLoading.value = true;
    summary.value = await adminApi.getTelemetrySummary();
    if (summary.value) {
      onlineUsers.value = summary.value.online_users_now ?? summary.value.online_now ?? 0;
    }
  } catch {
    // handled gracefully
  } finally {
    summaryLoading.value = false;
  }
}

async function loadUsers() {
  try {
    if (users.value.length === 0) {
      usersLoading.value = true;
    }
    usersError.value = null;
    const resp = await adminApi.getUsers(
      page.value,
      pageSize.value,
      searchQuery.value.trim() || undefined,
      roleFilter.value || undefined,
      onlineOnly.value,
      sortBy.value
    );
    users.value = resp.users;
    totalUsers.value = resp.total_users;
    onlineUsers.value = resp.online_users;
  } catch (err: any) {
    usersError.value = err.message || t("admin.users.loadError");
  } finally {
    usersLoading.value = false;
  }
}

async function loadEvents() {
  try {
    eventsLoading.value = true;
    const resp = await adminApi.getUserEvents(
      40,
      0,
      undefined,
      undefined,
      eventCategory.value || undefined
    );
    events.value = resp.events;
    eventsTotal.value = resp.total_count;
    eventsError.value = null;
  } catch (err) {
    eventsError.value = apiErrorMessage(err, t);
  } finally {
    eventsLoading.value = false;
  }
}

async function openUserDrawer(userId: string) {
  selectedUserId.value = userId;
  drawerOpen.value = true;
  drawerLoading.value = true;
  drawerError.value = null;
  drawerTab.value = "profile";
  try {
    selectedUserDetail.value = await adminApi.getUserDetail(userId);
  } catch (err: any) {
    drawerError.value = err.message || t("admin.users.detailError");
  } finally {
    drawerLoading.value = false;
  }
}

function closeUserDrawer() {
  drawerOpen.value = false;
  selectedUserId.value = null;
  selectedUserDetail.value = null;
}

// CSV exports need the bearer token too (window.open would send only the cookie),
// and a failed export must be visible.
const exporting = ref(false);
const exportError = ref<string | null>(null);

async function runExport(fetchCsv: () => Promise<ApiFile>, fallbackName: string) {
  if (exporting.value) return;
  exporting.value = true;
  exportError.value = null;
  try {
    saveFile(await fetchCsv(), fallbackName);
  } catch (err) {
    exportError.value = apiErrorMessage(err, t);
  } finally {
    exporting.value = false;
  }
}

function exportCsv() {
  return runExport(adminApi.exportUsersCsv, "users_telemetry.csv");
}

async function loadPrompts() {
  try {
    promptsLoading.value = true;
    promptsError.value = null;
    const res = await adminApi.getPrompts(
      promptsPage.value,
      promptsPageSize.value,
      promptsSearch.value || undefined,
      undefined,
      promptsTypeFilter.value
    );
    prompts.value = res.prompts;
    promptsTotal.value = res.total_count;
  } catch (err: any) {
    promptsError.value = err.message || "Failed to load prompts";
  } finally {
    promptsLoading.value = false;
  }
}

function exportPromptsCsv() {
  return runExport(adminApi.exportPromptsCsv, "user_prompts.csv");
}

async function copyPromptText(item: AdminPromptItem) {
  try {
    await navigator.clipboard.writeText(item.prompt);
    copiedPromptId.value = item.id;
    setTimeout(() => {
      if (copiedPromptId.value === item.id) copiedPromptId.value = null;
    }, 2000);
  } catch {
    // ignore
  }
}

async function handleDeleteUser(user: AdminUserListItem) {
  if (user.id === auth.user?.id || (auth.user?.email && user.email.toLowerCase() === auth.user.email.toLowerCase())) {
    alert(t("admin.users.cannotDeleteSelf"));
    return;
  }
  const confirmed = window.confirm(
    t("admin.users.deleteConfirm", { email: user.email })
  );
  if (!confirmed) return;

  try {
    deletingUserId.value = user.id;
    await adminApi.deleteUser(user.id);
    if (selectedUserId.value === user.id) {
      closeUserDrawer();
    }
    await loadUsers();
    await loadSummary();
  } catch (err: any) {
    alert(err.message || "Failed to delete user");
  } finally {
    deletingUserId.value = null;
  }
}

// ── Watchers & Lifecycle ─────────────────────────────────────────────────────
let searchDebounce: ReturnType<typeof setTimeout> | null = null;
watch(searchQuery, () => {
  if (searchDebounce) clearTimeout(searchDebounce);
  searchDebounce = setTimeout(() => {
    page.value = 1;
    loadUsers();
  }, 350);
});

let promptsSearchDebounce: ReturnType<typeof setTimeout> | null = null;
watch(promptsSearch, () => {
  if (promptsSearchDebounce) clearTimeout(promptsSearchDebounce);
  promptsSearchDebounce = setTimeout(() => {
    promptsPage.value = 1;
    loadPrompts();
  }, 350);
});

watch([roleFilter, onlineOnly, sortBy, page], () => {
  loadUsers();
});

watch([promptsTypeFilter, promptsPage], () => {
  if (activeSubView.value === "prompts") {
    loadPrompts();
  }
});

watch(eventCategory, () => {
  if (activeSubView.value === "feed") {
    loadEvents();
  }
});

watch(activeSubView, (val) => {
  if (val === "feed") {
    loadEvents();
  } else if (val === "prompts") {
    loadPrompts();
  } else if (val === "directory") {
    loadUsers();
  }
});

function setupAutoRefresh() {
  if (autoRefreshTimer) clearInterval(autoRefreshTimer);
  if (autoRefresh.value) {
    autoRefreshTimer = setInterval(() => {
      if (activeSubView.value === "feed") {
        loadEvents();
      } else if (activeSubView.value === "prompts") {
        loadPrompts();
      } else if (activeSubView.value === "directory") {
        loadUsers();
      }
      loadSummary();
    }, 6000);
  }
}

watch(autoRefresh, () => {
  setupAutoRefresh();
});

onMounted(() => {
  loadSummary();
  loadUsers();
  setupAutoRefresh();
});

onUnmounted(() => {
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer);
    autoRefreshTimer = null;
  }
  if (searchDebounce) clearTimeout(searchDebounce);
  if (promptsSearchDebounce) clearTimeout(promptsSearchDebounce);
});

const totalPages = computed(() => {
  return Math.max(1, Math.ceil(totalUsers.value / pageSize.value));
});

const promptsTotalPages = computed(() => {
  return Math.max(1, Math.ceil(promptsTotal.value / promptsPageSize.value));
});

// Helper for percentage breakdown
function getSortedBreakdown(mapObj: Record<string, number> | undefined) {
  if (!mapObj) return [];
  const entries = Object.entries(mapObj);
  const total = entries.reduce((acc, [, val]) => acc + val, 0) || 1;
  return entries
    .map(([key, val]) => ({
      key,
      count: val,
      percent: Math.round((val / total) * 100),
    }))
    .sort((a, b) => b.count - a.count);
}
</script>

<template>
  <div class="space-y-6">
    <!-- Top KPI Cards -->
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-5">
      <!-- Total Users -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4 transition-all hover:border-accent/30">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.users.kpiTotalUsers") }}</span>
          <svg class="h-4 w-4 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
        </div>
        <div class="mt-2 text-2xl font-black text-ink">
          {{ formatNumber(summary?.total_users ?? totalUsers) }}
        </div>
        <div class="mt-1 flex items-center gap-1.5 text-[11px] text-muted">
          <span>{{ t("admin.users.registeredAccounts") }}</span>
        </div>
      </div>

      <!-- Online Now -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4 transition-all hover:border-emerald-500/30">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.users.kpiOnlineNow") }}</span>
          <span class="relative flex h-2.5 w-2.5">
            <span class="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75"></span>
            <span class="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-500"></span>
          </span>
        </div>
        <div class="mt-2 text-2xl font-black text-emerald-400">
          {{ formatNumber(summary?.online_users_now ?? summary?.online_now ?? onlineUsers) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">
          {{ t("admin.users.activeLast5Min") }}
        </div>
      </div>

      <!-- Active Users (DAU / MAU) -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4 transition-all hover:border-accent/30">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">DAU / WAU / MAU</span>
          <svg class="h-4 w-4 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
          </svg>
        </div>
        <div class="mt-2 flex items-baseline gap-1 text-2xl font-black text-ink">
          <span>{{ formatNumber(summary?.dau_today ?? summary?.dau ?? 0) }}</span>
          <span class="text-xs font-normal text-muted">/ {{ formatNumber(summary?.wau_7d ?? summary?.wau ?? 0) }} / {{ formatNumber(summary?.mau_30d ?? summary?.mau ?? 0) }}</span>
        </div>
        <div class="mt-1 text-[11px] text-muted">
          {{ t("admin.users.dailyWeeklyMonthly") }}
        </div>
      </div>

      <!-- Total Researches -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4 transition-all hover:border-accent/30">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.users.kpiTotalResearches") }}</span>
          <svg class="h-4 w-4 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
        <div class="mt-2 text-2xl font-black text-ink">
          {{ formatNumber(summary?.total_researches ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">
          {{ t("admin.users.researchesLaunched") }}
        </div>
      </div>

      <!-- Total Cost -->
      <div class="rounded-xl border border-bd bg-surface/40 p-4 transition-all hover:border-accent/30">
        <div class="flex items-center justify-between">
          <span class="text-xs font-medium text-muted">{{ t("admin.users.kpiTotalCost") }}</span>
          <svg class="h-4 w-4 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </div>
        <div class="mt-2 text-2xl font-black text-emerald-400">
          {{ formatCurrency(summary?.total_cost_usd ?? 0) }}
        </div>
        <div class="mt-1 text-[11px] text-muted">
          {{ formatNumber(summary?.total_tokens ?? 0) }} {{ t("admin.users.tokensConsumed") }}
        </div>
      </div>
    </div>

    <!-- Navigation Sub-Tabs & Actions Header -->
    <div class="flex flex-wrap items-center justify-between gap-4 border-b border-bd pb-3">
      <!-- Sub-view switcher pills -->
      <div class="flex items-center gap-1 rounded-xl border border-bd bg-surface/60 p-1 text-xs">
        <button
          type="button"
          class="flex items-center gap-2 rounded-lg px-3 py-1.5 font-semibold transition"
          :class="activeSubView === 'directory' ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
          @click="activeSubView = 'directory'"
        >
          <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
          </svg>
          <span>{{ t("admin.users.tabDirectory") }}</span>
        </button>

        <button
          type="button"
          class="flex items-center gap-2 rounded-lg px-3 py-1.5 font-semibold transition"
          :class="activeSubView === 'prompts' ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
          @click="activeSubView = 'prompts'"
        >
          <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
          </svg>
          <span>{{ t("admin.users.tabPrompts") }}</span>
          <span v-if="promptsTotal > 0" class="ml-0.5 rounded-full bg-surface/80 px-1.5 py-0.5 text-[10px] font-mono">
            {{ promptsTotal }}
          </span>
        </button>

        <button
          type="button"
          class="flex items-center gap-2 rounded-lg px-3 py-1.5 font-semibold transition"
          :class="activeSubView === 'platforms' ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
          @click="activeSubView = 'platforms'"
        >
          <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
          </svg>
          <span>{{ t("admin.users.tabPlatforms") }}</span>
        </button>

        <button
          type="button"
          class="flex items-center gap-2 rounded-lg px-3 py-1.5 font-semibold transition"
          :class="activeSubView === 'feed' ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
          @click="activeSubView = 'feed'"
        >
          <span class="relative flex h-2 w-2">
            <span class="absolute inline-flex h-full w-full animate-ping rounded-full bg-accent opacity-75"></span>
            <span class="relative inline-flex h-2 w-2 rounded-full bg-accent"></span>
          </span>
          <span>{{ t("admin.users.tabLiveFeed") }}</span>
        </button>
      </div>

      <!-- Action controls -->
      <div class="flex items-center gap-2">
        <label
          v-if="activeSubView === 'feed' || activeSubView === 'prompts'"
          class="flex cursor-pointer items-center gap-2 rounded-lg border border-bd bg-surface/50 px-3 py-1.5 text-xs text-muted hover:text-ink"
        >
          <input
            v-model="autoRefresh"
            type="checkbox"
            class="rounded border-bd text-accent focus:ring-0"
          />
          <span>{{ t("admin.users.autoRefresh") }}</span>
        </label>

        <button
          type="button"
          class="flex items-center gap-1.5 rounded-lg border border-bd bg-surface/60 px-3 py-1.5 text-xs font-medium text-muted transition hover:border-accent/40 hover:text-ink"
          :title="t('admin.users.refreshTitle')"
          @click="loadSummary(); activeSubView === 'feed' ? loadEvents() : (activeSubView === 'prompts' ? loadPrompts() : loadUsers());"
        >
          <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          <span>{{ t("admin.users.refresh") }}</span>
        </button>

        <button
          type="button"
          class="flex items-center gap-2 rounded-lg bg-accent px-3 py-1.5 text-xs font-bold text-white shadow-sm transition hover:bg-accent/90 disabled:opacity-50"
          :disabled="exporting"
          @click="activeSubView === 'prompts' ? exportPromptsCsv() : exportCsv()"
        >
          <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
          </svg>
          <span>{{ activeSubView === 'prompts' ? t("admin.users.exportPromptsCsv") : t("admin.users.exportCsv") }}</span>
        </button>
      </div>
    </div>

    <p v-if="exportError" class="text-right text-xs text-red-400">{{ exportError }}</p>

    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <!-- VIEW 1: USER DIRECTORY                                                  -->
    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <div v-if="activeSubView === 'directory'" class="space-y-4">
      <!-- Search & Filter Toolbar -->
      <div class="flex flex-wrap items-center gap-3">
        <!-- Search Input -->
        <div class="relative min-w-[260px] flex-1">
          <svg class="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            v-model="searchQuery"
            type="text"
            :placeholder="t('admin.users.searchPlaceholder')"
            class="w-full rounded-xl border border-bd bg-surface/60 py-2 pl-9 pr-4 text-xs text-ink placeholder-muted transition focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
          />
        </div>

        <!-- Role Filter -->
        <select
          v-model="roleFilter"
          class="rounded-xl border border-bd bg-surface/60 px-3 py-2 text-xs text-ink transition focus:border-accent focus:outline-none"
        >
          <option value="">{{ t("admin.users.allRoles") }}</option>
          <option value="user">{{ t("admin.users.roleUsersOnly") }}</option>
          <option value="admin">{{ t("admin.users.roleAdminsOnly") }}</option>
        </select>

        <!-- Sort By -->
        <select
          v-model="sortBy"
          class="rounded-xl border border-bd bg-surface/60 px-3 py-2 text-xs text-ink transition focus:border-accent focus:outline-none"
        >
          <option value="activity">{{ t("admin.users.sortByActivity") }}</option>
          <option value="registered">{{ t("admin.users.sortByRegistered") }}</option>
          <option value="researches">{{ t("admin.users.sortByResearches") }}</option>
          <option value="tokens">{{ t("admin.users.sortByTokens") }}</option>
          <option value="cost">{{ t("admin.users.sortByCost") }}</option>
        </select>

        <!-- Online Only Toggle -->
        <label class="flex cursor-pointer items-center gap-2 rounded-xl border border-bd bg-surface/60 px-3 py-2 text-xs text-muted hover:text-ink">
          <input
            v-model="onlineOnly"
            type="checkbox"
            class="rounded border-bd text-accent focus:ring-0"
          />
          <span>{{ t("admin.users.filterOnlineOnly") }}</span>
        </label>
      </div>

      <!-- Users Table -->
      <div class="overflow-hidden rounded-xl border border-bd bg-surface/40">
        <div v-if="usersLoading && users.length === 0" class="flex h-64 items-center justify-center text-xs text-muted">
          {{ t("common.loading") }}
        </div>

        <div v-else-if="usersError" class="p-6 text-center text-xs text-red-400">
          {{ usersError }}
        </div>

        <div v-else-if="users.length === 0" class="p-12 text-center text-xs text-muted">
          {{ t("admin.users.noUsersFound") }}
        </div>

        <div v-else class="overflow-x-auto">
          <table class="w-full text-left text-xs">
            <thead class="border-b border-bd bg-surface/60 font-semibold text-muted">
              <tr>
                <th class="px-4 py-3">{{ t("admin.users.colUser") }}</th>
                <th class="px-4 py-3">{{ t("admin.users.colLastSeen") }}</th>
                <th class="px-4 py-3">{{ t("admin.users.colDevice") }}</th>
                <th class="px-4 py-3">{{ t("admin.users.colLastIp") }}</th>
                <th class="px-4 py-3 text-right">{{ t("admin.users.colResearches") }}</th>
                <th class="px-4 py-3 text-right">{{ t("admin.users.colSpend") }}</th>
                <th class="px-4 py-3 text-center">{{ t("admin.users.colAction") }}</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-bd font-normal text-ink">
              <tr
                v-for="u in users"
                :key="u.id"
                class="cursor-pointer transition-colors hover:bg-surface/80"
                @click="openUserDrawer(u.id)"
              >
                <!-- User / Avatar / Email / Role -->
                <td class="px-4 py-3.5">
                  <div class="flex items-center gap-3">
                    <div class="relative">
                      <div class="grid h-9 w-9 place-items-center rounded-full bg-accent/15 text-xs font-bold text-accent">
                        {{ u.name ? u.name[0].toUpperCase() : (u.email[0] || "U").toUpperCase() }}
                      </div>
                      <span
                        v-if="u.is_online"
                        class="absolute bottom-0 right-0 block h-2.5 w-2.5 rounded-full bg-emerald-500 ring-2 ring-bg"
                        :title="t('admin.users.onlineNow')"
                      ></span>
                    </div>
                    <div>
                      <div class="flex items-center gap-1.5 font-semibold text-ink">
                        <span>{{ u.name || t("admin.users.unnamed") }}</span>
                        <span
                          v-if="u.is_admin"
                          class="rounded bg-accent/20 px-1.5 py-0.5 text-[9.5px] font-bold text-accent"
                        >
                          ADMIN
                        </span>
                      </div>
                      <div class="font-mono text-[11px] text-muted">{{ u.email }}</div>
                    </div>
                  </div>
                </td>

                <!-- Last Seen -->
                <td class="px-4 py-3.5">
                  <div class="flex items-center gap-1.5">
                    <span
                      v-if="u.is_online"
                      class="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500"
                    ></span>
                    <span :class="u.is_online ? 'font-semibold text-emerald-400' : 'text-muted'">
                      {{ u.is_online ? t("admin.users.online") : timeAgo(u.last_seen_at) }}
                    </span>
                  </div>
                  <div class="text-[10px] text-muted">
                    {{ t("admin.users.registered") }} {{ formatDate(u.created_at).split(",")[0] }}
                  </div>
                </td>

                <!-- Device / Browser / OS -->
                <td class="px-4 py-3.5">
                  <div class="flex flex-wrap items-center gap-1">
                    <span
                      v-if="u.last_os"
                      class="rounded border border-bd bg-surface px-1.5 py-0.5 font-mono text-[10.5px] text-muted"
                    >
                      {{ u.last_os }}
                    </span>
                    <span
                      v-if="u.last_browser"
                      class="rounded border border-bd bg-surface px-1.5 py-0.5 font-mono text-[10.5px] text-muted"
                    >
                      {{ u.last_browser }}
                    </span>
                    <span
                      v-if="u.last_device"
                      class="rounded bg-accent/10 px-1.5 py-0.5 text-[10px] font-medium text-accent"
                    >
                      {{ u.last_device }}
                    </span>
                    <span v-if="!u.last_os && !u.last_browser" class="text-muted">—</span>
                  </div>
                </td>

                <!-- IP Address -->
                <td class="px-4 py-3.5">
                  <span class="font-mono text-[11px] text-muted">
                    {{ u.last_ip || "—" }}
                  </span>
                </td>

                <!-- Researches count -->
                <td class="px-4 py-3.5 text-right font-medium">
                  {{ formatNumber(u.researches_count) }}
                </td>

                <!-- Tokens & Cost -->
                <td class="px-4 py-3.5 text-right">
                  <div class="font-semibold text-ink">
                    {{ formatCurrency(u.total_cost_usd) }}
                  </div>
                  <div class="font-mono text-[10px] text-muted">
                    {{ formatNumber(u.total_tokens) }} tok
                  </div>
                </td>

                <!-- Action button -->
                <td class="px-4 py-3.5 text-center" @click.stop>
                  <div class="flex items-center justify-center gap-1.5">
                    <button
                      type="button"
                      class="rounded-lg border border-bd bg-surface px-2.5 py-1 text-[11px] font-medium text-muted transition hover:border-accent/50 hover:text-ink"
                      @click="openUserDrawer(u.id)"
                    >
                      {{ t("admin.users.inspect") }}
                    </button>
                    <button
                      v-if="u.id !== auth.user?.id && (!auth.user?.email || u.email.toLowerCase() !== auth.user.email.toLowerCase())"
                      type="button"
                      :disabled="deletingUserId === u.id"
                      class="rounded-lg border border-red-500/30 bg-red-500/10 p-1 text-red-400 transition hover:bg-red-500/20 hover:text-red-300 disabled:opacity-50"
                      :title="t('admin.users.deleteUser')"
                      @click="handleDeleteUser(u)"
                    >
                      <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Pagination Bar -->
        <div class="flex flex-wrap items-center justify-between gap-4 border-t border-bd px-4 py-3 text-xs text-muted">
          <div>
            {{ t("admin.users.showingCount", { count: users.length, total: totalUsers }) }}
          </div>
          <div class="flex items-center gap-2">
            <button
              type="button"
              class="rounded-lg border border-bd bg-surface px-2.5 py-1 transition disabled:opacity-40"
              :disabled="page <= 1"
              @click="page--"
            >
              {{ t("admin.users.prev") }}
            </button>
            <span class="px-2 font-mono text-[11px] text-ink">
              {{ page }} / {{ totalPages }}
            </span>
            <button
              type="button"
              class="rounded-lg border border-bd bg-surface px-2.5 py-1 transition disabled:opacity-40"
              :disabled="page >= totalPages"
              @click="page++"
            >
              {{ t("admin.users.next") }}
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <!-- VIEW 2: PROMPTS & QUERIES                                                -->
    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <div v-else-if="activeSubView === 'prompts'" class="space-y-4">
      <!-- Search & Filter Toolbar -->
      <div class="flex flex-wrap items-center gap-3">
        <!-- Search Input -->
        <div class="relative min-w-[260px] flex-1">
          <svg class="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            v-model="promptsSearch"
            type="text"
            :placeholder="t('admin.users.searchPromptsPlaceholder')"
            class="w-full rounded-xl border border-bd bg-surface/50 py-2 pl-9 pr-4 text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
          />
        </div>

        <!-- Filter by Prompt Type -->
        <select
          v-model="promptsTypeFilter"
          class="rounded-xl border border-bd bg-surface/50 px-3 py-2 text-xs text-ink focus:border-accent focus:outline-none"
        >
          <option value="all">{{ t("admin.users.promptTypeAll") }}</option>
          <option value="research">{{ t("admin.users.promptTypeResearch") }}</option>
          <option value="chat">{{ t("admin.users.promptTypeChat") }}</option>
        </select>

        <span class="text-xs text-muted font-mono">
          {{ t("admin.users.totalPromptsCount", { count: promptsTotal }) }}
        </span>
      </div>

      <!-- Prompts Feed / Cards List -->
      <div v-if="promptsLoading" class="flex h-64 items-center justify-center text-xs text-muted">
        {{ t("common.loading") }}
      </div>

      <div v-else-if="promptsError" class="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-xs text-red-400">
        {{ promptsError }}
      </div>

      <div v-else-if="prompts.length === 0" class="flex h-64 flex-col items-center justify-center rounded-2xl border border-dashed border-bd text-muted">
        <svg class="h-8 w-8 mb-2 opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
        </svg>
        <span class="text-xs">{{ t("admin.users.noPromptsFound") }}</span>
      </div>

      <div v-else class="space-y-3">
        <div
          v-for="p in prompts"
          :key="p.id"
          class="group rounded-xl border border-bd bg-surface/40 p-4 text-xs transition hover:border-accent/30 hover:bg-surface/60 space-y-3 shadow-sm"
        >
          <!-- Prompt Card Header -->
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="flex items-center gap-2">
              <!-- Type Badge -->
              <span
                v-if="p.prompt_type === 'research'"
                class="rounded-md border border-accent/30 bg-accent/15 px-2 py-0.5 text-[10.5px] font-bold text-accent"
              >
                {{ t("admin.users.promptTypeResearch") }}
              </span>
              <span
                v-else
                class="rounded-md border border-emerald-500/30 bg-emerald-500/15 px-2 py-0.5 text-[10.5px] font-bold text-emerald-400"
              >
                {{ t("admin.users.promptTypeChat") }}
              </span>

              <!-- User Chip -->
              <button
                v-if="p.user_id"
                type="button"
                class="flex items-center gap-1.5 rounded-lg border border-bd bg-surface/80 px-2 py-0.5 font-medium text-ink transition hover:border-accent/40 hover:text-accent"
                @click="openUserDrawer(p.user_id)"
              >
                <span class="grid h-4 w-4 place-items-center rounded-full bg-accent/20 text-[9px] font-bold text-accent">
                  {{ p.user_name ? p.user_name[0].toUpperCase() : (p.user_email?.[0] || 'U').toUpperCase() }}
                </span>
                <span class="max-w-[160px] truncate">{{ p.user_email || p.user_name || p.user_id }}</span>
              </button>
              <span v-else class="text-muted italic">Anonymous</span>

              <!-- Depth Badge -->
              <span v-if="p.depth" class="rounded bg-surface px-1.5 py-0.5 font-mono text-[10px] uppercase text-muted">
                {{ p.depth }}
              </span>
            </div>

            <!-- Right Actions & Meta -->
            <div class="flex items-center gap-3">
              <!-- Tokens & Cost (if available) -->
              <span v-if="p.total_tokens > 0" class="font-mono text-[11px] text-muted">
                {{ formatNumber(p.total_tokens) }} tok
                <span class="text-emerald-400">({{ formatCurrency(p.cost_usd) }})</span>
              </span>

              <!-- Time -->
              <span class="font-mono text-[11px] text-muted" :title="formatDate(p.created_at)">
                {{ timeAgo(p.created_at) }}
              </span>

              <!-- Copy Button -->
              <button
                type="button"
                class="flex items-center gap-1 rounded-md border border-bd bg-surface px-2 py-1 text-[11px] text-muted transition hover:border-accent/40 hover:text-ink"
                @click="copyPromptText(p)"
              >
                <svg v-if="copiedPromptId !== p.id" class="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                <svg v-else class="h-3 w-3 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                </svg>
                <span>{{ copiedPromptId === p.id ? t("admin.users.promptCopied") : t("admin.users.copyPrompt") }}</span>
              </button>
              <!-- No "open research" link: research routes are owner-scoped (SEC-IDOR),
                   so an admin would only get a 404 for another user's run. -->
            </div>
          </div>

          <!-- Prompt Full Text -->
          <div class="rounded-lg border border-bd/60 bg-bg/80 p-3 font-mono text-[12px] leading-relaxed text-ink/90 whitespace-pre-wrap select-text">
            {{ p.prompt }}
          </div>
        </div>

        <!-- Prompts Pagination Controls -->
        <div v-if="promptsTotalPages > 1" class="flex items-center justify-between border-t border-bd pt-4 text-xs">
          <span class="text-muted">
            {{ t("admin.users.showingCount", { count: prompts.length, total: promptsTotal }) }}
          </span>
          <div class="flex items-center gap-2">
            <button
              type="button"
              :disabled="promptsPage === 1"
              class="rounded-lg border border-bd bg-surface/50 px-3 py-1.5 text-muted transition hover:text-ink disabled:opacity-40"
              @click="promptsPage--"
            >
              {{ t("admin.users.prev") }}
            </button>
            <span class="font-mono text-muted">
              {{ promptsPage }} / {{ promptsTotalPages }}
            </span>
            <button
              type="button"
              :disabled="promptsPage >= promptsTotalPages"
              class="rounded-lg border border-bd bg-surface/50 px-3 py-1.5 text-muted transition hover:text-ink disabled:opacity-40"
              @click="promptsPage++"
            >
              {{ t("admin.users.next") }}
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <!-- VIEW 3: PLATFORMS & DEMOGRAPHICS                                        -->
    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <div v-else-if="activeSubView === 'platforms'" class="grid grid-cols-1 gap-6 md:grid-cols-2">
      <!-- Operating Systems Breakdown -->
      <div class="rounded-xl border border-bd bg-surface/40 p-5">
        <h3 class="flex items-center gap-2 text-sm font-semibold text-ink">
          <svg class="h-4 w-4 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
          </svg>
          <span>{{ t("admin.users.breakdownOS") }}</span>
        </h3>
        <p class="mt-1 text-xs text-muted">{{ t("admin.users.breakdownOSDesc") }}</p>

        <div class="mt-4 space-y-3">
          <div
            v-for="item in getSortedBreakdown(summary?.os_breakdown)"
            :key="item.key"
            class="space-y-1"
          >
            <div class="flex items-center justify-between text-xs">
              <span class="font-medium text-ink">{{ item.key }}</span>
              <span class="font-mono text-muted">{{ item.count }} ({{ item.percent }}%)</span>
            </div>
            <div class="h-2 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full rounded-full bg-accent transition-all"
                :style="{ width: `${item.percent}%` }"
              ></div>
            </div>
          </div>
          <div v-if="!summary?.os_breakdown || Object.keys(summary.os_breakdown).length === 0" class="text-xs text-muted">
            {{ t("admin.users.noData") }}
          </div>
        </div>
      </div>

      <!-- Browsers Breakdown -->
      <div class="rounded-xl border border-bd bg-surface/40 p-5">
        <h3 class="flex items-center gap-2 text-sm font-semibold text-ink">
          <svg class="h-4 w-4 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9" />
          </svg>
          <span>{{ t("admin.users.breakdownBrowser") }}</span>
        </h3>
        <p class="mt-1 text-xs text-muted">{{ t("admin.users.breakdownBrowserDesc") }}</p>

        <div class="mt-4 space-y-3">
          <div
            v-for="item in getSortedBreakdown(summary?.browser_breakdown)"
            :key="item.key"
            class="space-y-1"
          >
            <div class="flex items-center justify-between text-xs">
              <span class="font-medium text-ink">{{ item.key }}</span>
              <span class="font-mono text-muted">{{ item.count }} ({{ item.percent }}%)</span>
            </div>
            <div class="h-2 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full rounded-full bg-indigo-500 transition-all"
                :style="{ width: `${item.percent}%` }"
              ></div>
            </div>
          </div>
          <div v-if="!summary?.browser_breakdown || Object.keys(summary.browser_breakdown).length === 0" class="text-xs text-muted">
            {{ t("admin.users.noData") }}
          </div>
        </div>
      </div>

      <!-- Device Types Breakdown -->
      <div class="rounded-xl border border-bd bg-surface/40 p-5">
        <h3 class="flex items-center gap-2 text-sm font-semibold text-ink">
          <svg class="h-4 w-4 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 18h.01M8 21h8a2 2 0 002-2V5a2 2 0 00-2-2H8a2 2 0 00-2 2v14a2 2 0 002 2z" />
          </svg>
          <span>{{ t("admin.users.breakdownDevices") }}</span>
        </h3>
        <p class="mt-1 text-xs text-muted">{{ t("admin.users.breakdownDevicesDesc") }}</p>

        <div class="mt-4 space-y-3">
          <div
            v-for="item in getSortedBreakdown(summary?.device_breakdown)"
            :key="item.key"
            class="space-y-1"
          >
            <div class="flex items-center justify-between text-xs">
              <span class="font-medium capitalize text-ink">{{ item.key }}</span>
              <span class="font-mono text-muted">{{ item.count }} ({{ item.percent }}%)</span>
            </div>
            <div class="h-2 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full rounded-full bg-emerald-500 transition-all"
                :style="{ width: `${item.percent}%` }"
              ></div>
            </div>
          </div>
          <div v-if="!summary?.device_breakdown || Object.keys(summary.device_breakdown).length === 0" class="text-xs text-muted">
            {{ t("admin.users.noData") }}
          </div>
        </div>
      </div>

      <!-- Research Depth Preferences -->
      <div class="rounded-xl border border-bd bg-surface/40 p-5">
        <h3 class="flex items-center gap-2 text-sm font-semibold text-ink">
          <svg class="h-4 w-4 text-accent" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          <span>{{ t("admin.users.breakdownDepth") }}</span>
        </h3>
        <p class="mt-1 text-xs text-muted">{{ t("admin.users.breakdownDepthDesc") }}</p>

        <div class="mt-4 space-y-3">
          <div
            v-for="item in getSortedBreakdown(summary?.depth_distribution)"
            :key="item.key"
            class="space-y-1"
          >
            <div class="flex items-center justify-between text-xs">
              <span class="font-medium uppercase text-ink">{{ item.key }}</span>
              <span class="font-mono text-muted">{{ item.count }} ({{ item.percent }}%)</span>
            </div>
            <div class="h-2 w-full overflow-hidden rounded-full bg-surface">
              <div
                class="h-full rounded-full bg-amber-500 transition-all"
                :style="{ width: `${item.percent}%` }"
              ></div>
            </div>
          </div>
          <div v-if="!summary?.depth_distribution || Object.keys(summary.depth_distribution).length === 0" class="text-xs text-muted">
            {{ t("admin.users.noData") }}
          </div>
        </div>
      </div>
    </div>

    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <!-- VIEW 3: LIVE ACTIVITY EVENT FEED                                        -->
    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <div v-else-if="activeSubView === 'feed'" class="space-y-4">
      <!-- Feed Filter Header -->
      <div class="flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center gap-2 text-xs">
          <span class="text-muted">{{ t("admin.users.filterCategory") }}:</span>
          <button
            type="button"
            class="rounded-lg px-2.5 py-1 font-medium transition"
            :class="eventCategory === '' ? 'bg-accent text-white' : 'border border-bd bg-surface text-muted hover:text-ink'"
            @click="eventCategory = ''"
          >
            {{ t("admin.users.catAll") }}
          </button>
          <button
            type="button"
            class="rounded-lg px-2.5 py-1 font-medium transition"
            :class="eventCategory === 'ui' ? 'bg-accent text-white' : 'border border-bd bg-surface text-muted hover:text-ink'"
            @click="eventCategory = 'ui'"
          >
            UI / Client
          </button>
          <button
            type="button"
            class="rounded-lg px-2.5 py-1 font-medium transition"
            :class="eventCategory === 'research' ? 'bg-accent text-white' : 'border border-bd bg-surface text-muted hover:text-ink'"
            @click="eventCategory = 'research'"
          >
            Research
          </button>
          <button
            type="button"
            class="rounded-lg px-2.5 py-1 font-medium transition"
            :class="eventCategory === 'system' ? 'bg-accent text-white' : 'border border-bd bg-surface text-muted hover:text-ink'"
            @click="eventCategory = 'system'"
          >
            System
          </button>
        </div>

        <div class="flex items-center gap-2 text-xs text-muted font-mono">
          <span class="inline-block h-2 w-2 rounded-full bg-emerald-500 animate-pulse"></span>
          <span>{{ t("admin.users.totalStreamEvents", { count: eventsTotal }) }}</span>
        </div>
      </div>

      <!-- Events List -->
      <div class="rounded-xl border border-bd bg-surface/40 overflow-hidden divide-y divide-bd">
        <div v-if="eventsError" class="p-4 text-center text-xs text-red-400">
          {{ eventsError }}
        </div>

        <div v-else-if="eventsLoading && events.length === 0" class="p-12 text-center text-xs text-muted">
          {{ t("common.loading") }}
        </div>

        <div v-else-if="events.length === 0" class="p-12 text-center text-xs text-muted">
          {{ t("admin.users.noEvents") }}
        </div>

        <div
          v-for="ev in events"
          :key="ev.id"
          class="p-4 transition hover:bg-surface/70"
        >
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="flex items-center gap-2">
              <span
                class="rounded-full px-2 py-0.5 font-mono text-[10px] font-bold uppercase"
                :class="{
                  'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30': ev.event_category === 'research',
                  'bg-blue-500/15 text-blue-400 border border-blue-500/30': ev.event_category === 'ui',
                  'bg-purple-500/15 text-purple-400 border border-purple-500/30': ev.event_category === 'system',
                  'bg-surface text-muted border border-bd': !['research', 'ui', 'system'].includes(ev.event_category),
                }"
              >
                {{ ev.event_category }}
              </span>
              <span class="font-semibold text-xs text-ink">{{ ev.event_name }}</span>
            </div>

            <div class="flex items-center gap-3 text-xs text-muted font-mono">
              <span v-if="ev.ip_address">{{ ev.ip_address }}</span>
              <span>{{ formatDate(ev.created_at) }}</span>
            </div>
          </div>

          <!-- Payload Details snippet -->
          <div v-if="ev.details && Object.keys(ev.details).length > 0" class="mt-2.5">
            <pre class="max-h-24 overflow-y-auto rounded-lg border border-bd/60 bg-surface/90 p-2 font-mono text-[10.5px] text-muted">{{ JSON.stringify(ev.details, null, 2) }}</pre>
          </div>
        </div>
      </div>
    </div>

    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <!-- SLIDE-OVER DRAWER: USER DETAIL & TELEMETRY INSPECTOR                     -->
    <!-- ──────────────────────────────────────────────────────────────────────── -->
    <div
      v-if="drawerOpen"
      class="fixed inset-0 z-50 flex justify-end bg-black/60 backdrop-blur-sm transition-opacity"
      @click.self="closeUserDrawer"
    >
      <div class="h-full w-full max-w-2xl overflow-y-auto border-l border-bd bg-bg p-6 text-ink shadow-2xl">
        <!-- Drawer Header -->
        <div class="flex items-start justify-between border-b border-bd pb-4">
          <div class="flex items-center gap-3">
            <div class="grid h-12 w-12 place-items-center rounded-2xl bg-accent/20 text-base font-bold text-accent shadow-inner">
              {{ activeUser?.name ? activeUser.name[0].toUpperCase() : (activeUser?.email ? activeUser.email[0].toUpperCase() : "U") }}
            </div>
            <div>
              <div class="flex items-center gap-2">
                <h3 class="text-base font-bold text-ink">
                  {{ activeUser?.name || activeUser?.email || t("admin.users.unnamed") }}
                </h3>
                <span
                  v-if="activeUser?.is_admin"
                  class="rounded bg-accent/20 px-2 py-0.5 text-[10px] font-bold text-accent"
                >
                  ADMIN
                </span>
                <span
                  v-if="activeUser?.is_online"
                  class="rounded-full bg-emerald-500/20 border border-emerald-500/40 px-2 py-0.5 text-[10px] font-semibold text-emerald-400"
                >
                  ONLINE
                </span>
              </div>
              <p class="font-mono text-xs text-muted">{{ activeUser?.email }}</p>
            </div>
          </div>

          <div class="flex items-center gap-2">
            <button
              v-if="activeUser && activeUser.id !== auth.user?.id && (!auth.user?.email || activeUser.email.toLowerCase() !== auth.user.email.toLowerCase())"
              type="button"
              :disabled="deletingUserId === activeUser.id"
              class="flex items-center gap-1.5 rounded-lg border border-red-500/30 bg-red-500/10 px-2.5 py-1.5 text-xs font-semibold text-red-400 transition hover:bg-red-500/20 hover:text-red-300 disabled:opacity-50"
              @click="handleDeleteUser(activeUser)"
            >
              <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
              <span>{{ deletingUserId === activeUser.id ? t("admin.users.deleting") : t("admin.users.deleteUser") }}</span>
            </button>

            <button
              type="button"
              class="rounded-lg border border-bd bg-surface p-1.5 text-muted transition hover:text-ink"
              @click="closeUserDrawer"
            >
              <svg class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        <div v-if="drawerLoading && !selectedUserDetail" class="mt-8 space-y-4 animate-pulse">
          <div class="grid grid-cols-3 gap-3">
            <div class="h-20 rounded-xl bg-surface/60"></div>
            <div class="h-20 rounded-xl bg-surface/60"></div>
            <div class="h-20 rounded-xl bg-surface/60"></div>
          </div>
          <div class="h-44 rounded-xl bg-surface/60"></div>
          <div class="h-28 rounded-xl bg-surface/60"></div>
        </div>

        <div v-else-if="drawerError" class="mt-6 rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-xs text-red-400">
          {{ drawerError }}
        </div>

        <div v-else-if="selectedUserDetail || activeUser" class="mt-6 space-y-6">
          <!-- Drawer Navigation Tabs -->
          <div class="flex border-b border-bd text-xs">
            <button
              type="button"
              class="border-b-2 px-3 py-2 font-medium transition"
              :class="drawerTab === 'profile' ? 'border-accent text-accent font-bold' : 'border-transparent text-muted hover:text-ink'"
              @click="drawerTab = 'profile'"
            >
              {{ t("admin.users.drawerTabProfile") }}
            </button>
            <button
              type="button"
              class="border-b-2 px-3 py-2 font-medium transition"
              :class="drawerTab === 'sessions' ? 'border-accent text-accent font-bold' : 'border-transparent text-muted hover:text-ink'"
              @click="drawerTab = 'sessions'"
            >
              {{ t("admin.users.drawerTabSessions") }} ({{ drawerSessions.length }})
            </button>
            <button
              type="button"
              class="border-b-2 px-3 py-2 font-medium transition"
              :class="drawerTab === 'researches' ? 'border-accent text-accent font-bold' : 'border-transparent text-muted hover:text-ink'"
              @click="drawerTab = 'researches'"
            >
              {{ t("admin.users.drawerTabResearches") }} ({{ drawerResearches.length }})
            </button>
            <button
              type="button"
              class="border-b-2 px-3 py-2 font-medium transition"
              :class="drawerTab === 'events' ? 'border-accent text-accent font-bold' : 'border-transparent text-muted hover:text-ink'"
              @click="drawerTab = 'events'"
            >
              {{ t("admin.users.drawerTabEvents") }} ({{ drawerEvents.length }})
            </button>
          </div>

          <!-- Drawer Content: Profile & Technical Fingerprint -->
          <div v-if="drawerTab === 'profile'" class="space-y-6">
            <!-- Usage Metrics Summary Card -->
            <div class="grid grid-cols-3 gap-3 rounded-xl border border-bd bg-surface/40 p-4 text-center">
              <div>
                <div class="text-[11px] text-muted">{{ t("admin.users.colResearches") }}</div>
                <div class="mt-1 text-lg font-bold text-ink">{{ activeUser?.researches_count ?? 0 }}</div>
              </div>
              <div>
                <div class="text-[11px] text-muted">{{ t("admin.users.totalTokens") }}</div>
                <div class="mt-1 text-lg font-bold text-ink">{{ formatNumber(activeUser?.total_tokens) }}</div>
              </div>
              <div>
                <div class="text-[11px] text-muted">{{ t("admin.users.colSpend") }}</div>
                <div class="mt-1 text-lg font-bold text-emerald-400">{{ formatCurrency(activeUser?.total_cost_usd) }}</div>
              </div>
            </div>

            <!-- Technical Fingerprint Card -->
            <div class="rounded-xl border border-bd bg-surface/40 p-5 space-y-4">
              <h4 class="text-xs font-bold uppercase tracking-wider text-muted">
                {{ t("admin.users.techFingerprint") }}
              </h4>

              <div class="grid grid-cols-2 gap-4 text-xs">
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.deviceType") }}</span>
                  <span class="font-medium text-ink capitalize">{{ activeUser?.last_device || "Desktop" }}</span>
                </div>
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.operatingSystem") }}</span>
                  <span class="font-medium text-ink">{{ activeUser?.last_os || "—" }}</span>
                </div>
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.browser") }}</span>
                  <span class="font-medium text-ink">{{ activeUser?.last_browser || "—" }}</span>
                </div>
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.lastIpAddress") }}</span>
                  <span class="font-mono text-ink">{{ activeUser?.last_ip || "—" }}</span>
                </div>
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.firstRegistered") }}</span>
                  <span class="text-ink">{{ formatDate(activeUser?.created_at) }}</span>
                </div>
                <div>
                  <span class="text-muted block text-[11px]">{{ t("admin.users.lastActiveAt") }}</span>
                  <span class="text-ink">{{ formatDate(activeUser?.last_seen_at) }}</span>
                </div>
              </div>
            </div>
          </div>

          <!-- Drawer Content: Sessions Log -->
          <div v-else-if="drawerTab === 'sessions'" class="space-y-3">
            <div v-if="drawerSessions.length === 0" class="p-6 text-center text-xs text-muted">
              {{ t("admin.users.noSessions") }}
            </div>
            <div
              v-for="s in drawerSessions"
              :key="s.id"
              class="rounded-xl border border-bd bg-surface/40 p-4 text-xs space-y-2"
            >
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2 font-medium text-ink">
                  <span class="rounded bg-accent/15 px-1.5 py-0.5 text-[10px] font-bold text-accent">{{ s.device_type }}</span>
                  <span>{{ s.browser || "Browser" }} on {{ s.os || "OS" }}</span>
                </div>
                <span class="font-mono text-[11px] text-muted">{{ timeAgo(s.last_active_at) }}</span>
              </div>
              <div class="grid grid-cols-2 gap-2 text-[11px] text-muted font-mono">
                <div>IP: {{ s.ip_address || "—" }}</div>
                <div>Screen: {{ s.screen_res || "—" }}</div>
                <div>Viewport: {{ s.viewport || "—" }}</div>
                <div>Timezone: {{ s.timezone || "—" }}</div>
              </div>
            </div>
          </div>

          <!-- Drawer Content: Research History -->
          <div v-else-if="drawerTab === 'researches'" class="space-y-3">
            <div v-if="drawerResearches.length === 0" class="p-6 text-center text-xs text-muted">
              {{ t("admin.users.noResearches") }}
            </div>
            <div
              v-for="r in drawerResearches"
              :key="r.id"
              class="rounded-xl border border-bd bg-surface/40 p-4 text-xs space-y-2"
            >
              <div class="flex items-start justify-between gap-2">
                <div class="font-medium text-ink select-text">{{ r.prompt }}</div>
                <div class="flex items-center gap-1.5 shrink-0">
                  <span class="rounded bg-surface px-1.5 py-0.5 font-mono text-[10px] uppercase text-muted">{{ r.depth }}</span>
                </div>
              </div>
              <div class="flex items-center justify-between text-[11px] text-muted">
                <span>{{ formatDate(r.created_at) }}</span>
                <span class="font-mono text-ink">{{ formatNumber(r.total_tokens) }} tok ({{ formatCurrency(r.cost_usd) }})</span>
              </div>
            </div>
          </div>

          <!-- Drawer Content: User Events Stream -->
          <div v-else-if="drawerTab === 'events'" class="space-y-3">
            <div v-if="drawerEvents.length === 0" class="p-6 text-center text-xs text-muted">
              {{ t("admin.users.noEvents") }}
            </div>
            <div
              v-for="ev in drawerEvents"
              :key="ev.id"
              class="rounded-xl border border-bd bg-surface/40 p-3 text-xs space-y-1.5"
            >
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                  <span class="rounded bg-accent/15 px-1.5 py-0.5 font-mono text-[9.5px] font-bold text-accent">{{ ev.event_category }}</span>
                  <span class="font-medium text-ink">{{ ev.event_name }}</span>
                </div>
                <span class="font-mono text-[10px] text-muted">{{ timeAgo(ev.created_at) }}</span>
              </div>
              <pre v-if="ev.details && Object.keys(ev.details).length > 0" class="max-h-20 overflow-y-auto rounded bg-surface/80 p-1.5 font-mono text-[10px] text-muted">{{ JSON.stringify(ev.details, null, 2) }}</pre>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
