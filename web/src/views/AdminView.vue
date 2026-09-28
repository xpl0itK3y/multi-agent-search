<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import { adminApi } from "@/lib/api";
import { smoothOrAuto } from "@/lib/motion";
import { useAuthStore } from "@/stores/auth";
import { useUiStore } from "@/stores/ui";
import type { AdminOverviewResponse } from "@/lib/types";
import OverviewTab from "@/components/admin/OverviewTab.vue";
import UsersTab from "@/components/admin/UsersTab.vue";
import AnalyticsTab from "@/components/admin/AnalyticsTab.vue";
import AgentsGraphTab from "@/components/admin/AgentsGraphTab.vue";
import OperationsTab from "@/components/admin/OperationsTab.vue";

const { t } = useI18n();
const router = useRouter();
const route = useRoute();
const auth = useAuthStore();
const ui = useUiStore();

type Tab = "overview" | "users" | "analytics" | "agents" | "operations";
const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "admin.tabs.overview" },
  { id: "users", label: "admin.tabs.users" },
  { id: "analytics", label: "admin.tabs.analytics" },
  { id: "agents", label: "admin.tabs.agents" },
  { id: "operations", label: "admin.tabs.operations" },
];

// The open tab lives in the address (/admin?tab=users), so a reload, a shared link or
// Back lands on the same tab (§16 Wayfinding: where am I?).
function tabFromQuery(): Tab {
  const q = route.query.tab;
  return typeof q === "string" && TABS.some((tab) => tab.id === q) ? (q as Tab) : "overview";
}
const activeTab = ref<Tab>(tabFromQuery());

watch(activeTab, (tab) => {
  if (tabFromQuery() === tab) return;
  router.replace({ query: { ...route.query, tab: tab === "overview" ? undefined : tab } });
});
watch(
  () => route.query.tab,
  () => {
    activeTab.value = tabFromQuery();
  },
);

// Tabs follow the WAI-ARIA pattern: arrows move between them, Home/End jump to the ends.
function selectTab(tab: Tab, focus = false) {
  activeTab.value = tab;
  nextTick(() => {
    const btn = typeof document !== "undefined" ? document.getElementById(`admin-tab-${tab}`) : null;
    if (!btn) return;
    if (focus) btn.focus({ preventScroll: true });
    // On a narrow screen the strip scrolls sideways: keep the chosen tab in view.
    btn.scrollIntoView?.({ inline: "nearest", block: "nearest", behavior: smoothOrAuto() });
  });
}
function onTabKey(e: KeyboardEvent) {
  const i = TABS.findIndex((tab) => tab.id === activeTab.value);
  const n = TABS.length;
  const next: Record<string, number> = { ArrowRight: (i + 1) % n, ArrowLeft: (i - 1 + n) % n, Home: 0, End: n - 1 };
  if (!(e.key in next)) return;
  e.preventDefault();
  selectTab(TABS[next[e.key]].id, true);
}

// The header's status line (apple-design §16 Feedback: status must reflect reality).
// On the Overview tab it follows that tab's data, live stream included; elsewhere the
// header fetches its own copy. Unknown health shows nothing rather than a guess.
const overview = ref<AdminOverviewResponse | null>(null);
const health = computed<"healthy" | "degraded" | null>(() => {
  const o = overview.value;
  if (!o) return null;
  return o.system_health?.overall === "healthy" && !o.failed_tasks_count ? "healthy" : "degraded";
});

async function handleLogout() {
  await auth.logout();
  overview.value = null;
  router.push("/login");
}

async function handleSwitchAccount() {
  await auth.logout();
  overview.value = null;
  router.push({ path: "/login", query: { redirect: "/admin" } });
}

let loadingOverview = false;
async function loadOverview() {
  if (!auth.user?.is_admin || loadingOverview) return;
  loadingOverview = true;
  try {
    overview.value = await adminApi.getOverview();
  } catch {
    // The header stays blank; the Overview tab shows the error with a retry, and no
    // other tab is hidden behind it.
  } finally {
    loadingOverview = false;
  }
}

function onOverviewUpdate(data: AdminOverviewResponse) {
  overview.value = data;
}

watch(activeTab, (tab) => {
  if (tab !== "overview" && !overview.value) loadOverview();
});

onMounted(() => {
  if (!auth.user) {
    router.replace({ path: "/login", query: { redirect: "/admin" } });
    return;
  }
  if (auth.user.is_admin && activeTab.value !== "overview") {
    loadOverview();
  }
});
</script>

<template>
  <div class="flex h-full flex-col overflow-y-auto bg-bg p-6 text-ink">
    <!-- Non-Admin Authorization Guard (User is logged in, but not an admin) -->
    <div
      v-if="!auth.user?.is_admin"
      class="flex flex-1 items-center justify-center py-8"
    >
      <div class="w-full max-w-md rounded-2xl border border-bd bg-surface p-8 shadow-e3 text-center">
        <!-- Language Switcher in Guard Card -->
        <div class="mb-4 flex justify-end">
          <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono">
            <button
              v-for="loc in (['ru', 'en', 'es'] as const)"
              :key="loc"
              type="button"
              class="press hit rounded-lg px-2.5 py-1 text-[10.5px] font-semibold uppercase"
              :class="ui.locale === loc ? 'bg-accent text-onAccent shadow' : 'text-muted hover:text-ink'"
              :aria-pressed="ui.locale === loc ? 'true' : 'false'"
              @click="ui.setLocale(loc)"
            >
              {{ loc }}
            </button>
          </div>
        </div>

        <!-- Shield / Warning Icon & Title -->
        <div class="mb-6 flex flex-col items-center">
          <div class="grid h-16 w-16 place-items-center rounded-2xl bg-warning/15 text-3xl text-warning mb-3 shadow-inner">
            🛡️
          </div>
          <h2 class="text-xl font-bold tracking-tight text-ink">
            {{ t("admin.authNotAdminWarning") }}
          </h2>
          <p class="mt-2 text-xs text-muted max-w-xs leading-relaxed">
            {{ t("admin.authNotAdminDesc", { email: auth.user?.email || "" }) }}
          </p>
        </div>

        <!-- Action Buttons -->
        <div class="space-y-3">
          <button
            type="button"
            class="press w-full rounded-xl bg-accent py-2.5 text-xs font-bold text-onAccent shadow hover:bg-accent/90"
            @click="router.push('/')"
          >
            {{ t("admin.backToSearch") }}
          </button>
          <button
            type="button"
            class="w-full rounded-xl border border-bd bg-bg py-2.5 text-xs font-semibold text-muted transition hover:text-ink hover:border-accent/40"
            @click="handleSwitchAccount"
          >
            {{ t("admin.authSwitchAccount") }}
          </button>
          <!-- Switching logs out first, and a logout ends every session of the account. -->
          <p class="text-center text-[11px] leading-relaxed text-muted">{{ t("auth.logoutEverywhereHint") }}</p>
        </div>
      </div>
    </div>

    <!-- Authenticated Admin Panel -->
    <template v-else>
      <!-- Header -->
      <div class="mb-6 flex flex-wrap items-center justify-between gap-4 border-b border-bd pb-4">
        <div class="flex items-center gap-3">
          <div class="grid h-10 w-10 place-items-center rounded-xl bg-accent/15 text-xl text-accent">
            🛡️
          </div>
          <div>
            <div class="flex items-center gap-2">
              <h1 class="text-xl font-bold tracking-tight text-ink">{{ t("admin.title") }}</h1>
              <span class="rounded-full bg-accent/15 border border-accent/30 px-2 py-0.5 font-mono text-[10px] font-semibold text-accent">
                {{ auth.user?.email }}
              </span>
            </div>
            <p
              v-if="health"
              class="flex items-center gap-1.5 text-xs"
              :class="health === 'healthy' ? 'text-muted' : 'text-warning'"
              role="status"
              data-test="admin-health"
            >
              <span class="h-2 w-2 shrink-0 rounded-full" :class="health === 'healthy' ? 'bg-success' : 'bg-warning'" aria-hidden="true" />
              {{ health === "healthy" ? t("admin.allSystemsOperational") : t("admin.systemDegraded") }}
            </p>
          </div>
        </div>

        <div class="flex items-center gap-2">
          <!-- Language Switcher in Header -->
          <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono mr-1">
            <button
              v-for="loc in (['ru', 'en', 'es'] as const)"
              :key="loc"
              type="button"
              class="press hit rounded-lg px-2.5 py-1 text-[11px] font-semibold uppercase"
              :class="ui.locale === loc ? 'bg-accent text-onAccent shadow' : 'text-muted hover:text-ink'"
              :aria-pressed="ui.locale === loc ? 'true' : 'false'"
              @click="ui.setLocale(loc)"
            >
              {{ loc }}
            </button>
          </div>

          <!-- Dev mode warning banner if auth is disabled -->
          <div
            v-if="overview?.is_dev_mode"
            class="flex items-center gap-2 rounded-lg border border-warning/30 bg-warning/10 px-3 py-1.5 text-xs font-medium text-warning"
          >
            <span>⚠️</span>
            <span>{{ t("admin.devModeBadge") }}</span>
          </div>

          <!-- Logout / Switch account button -->
          <button
            type="button"
            class="press rounded-lg border border-bd bg-surface px-3 py-1.5 text-xs font-medium text-muted hover:text-ink hover:bg-surfaceHover"
            :title="t('auth.logoutEverywhereHint')"
            @click="handleLogout"
          >
            {{ t("admin.logout") }}
          </button>
        </div>
      </div>

      <!-- Navigation Tabs: one line that scrolls sideways on a phone, and sticks under a
           translucent bar while the page scrolls (the view root, p-6, is the scroller).
           shrink-0: a scroller's automatic min-height is 0, so in the overflowing column
           it would otherwise be squeezed down to its 1px border. -->
      <div
        class="sticky -top-6 z-20 -mx-6 mb-6 flex shrink-0 overflow-x-auto scrollbar-none border-b border-bd px-6 material-bar"
        data-test="admin-tablist"
        role="tablist"
        :aria-label="t('admin.title')"
        @keydown="onTabKey"
      >
        <button
          v-for="tab in TABS"
          :id="`admin-tab-${tab.id}`"
          :key="tab.id"
          type="button"
          role="tab"
          :aria-selected="activeTab === tab.id ? 'true' : 'false'"
          aria-controls="admin-tabpanel"
          :tabindex="activeTab === tab.id ? 0 : -1"
          class="shrink-0 whitespace-nowrap border-b-2 px-4 py-2.5 text-sm font-medium transition-colors focus-visible:!outline-offset-[-2px]"
          :class="activeTab === tab.id ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
          @click="selectTab(tab.id)"
        >
          {{ t(tab.label) }}
        </button>
      </div>

      <!-- Every tab renders on its own; each one shows its own loading and errors. -->
      <div id="admin-tabpanel" class="flex-1" role="tabpanel" :aria-labelledby="`admin-tab-${activeTab}`">
        <OverviewTab v-if="activeTab === 'overview'" :initial-overview="overview" @update="onOverviewUpdate" />

        <UsersTab v-else-if="activeTab === 'users'" />

        <AnalyticsTab v-else-if="activeTab === 'analytics'" />

        <AgentsGraphTab v-else-if="activeTab === 'agents'" />

        <OperationsTab v-else-if="activeTab === 'operations'" />
      </div>
    </template>
  </div>
</template>
