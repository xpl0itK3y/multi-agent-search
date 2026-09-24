<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import { adminApi, apiErrorMessage } from "@/lib/api";
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
const auth = useAuthStore();
const ui = useUiStore();

type Tab = "overview" | "users" | "analytics" | "agents" | "operations";
const activeTab = ref<Tab>("overview");

const overview = ref<AdminOverviewResponse | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);

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

async function loadOverview() {
  if (!auth.user?.is_admin) return;
  try {
    loading.value = true;
    error.value = null;
    overview.value = await adminApi.getOverview();
  } catch (err) {
    error.value = apiErrorMessage(err, t);
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  if (!auth.user) {
    router.replace({ path: "/login", query: { redirect: "/admin" } });
    return;
  }
  if (auth.user.is_admin) {
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
      <div class="w-full max-w-md rounded-2xl border border-bd bg-surface/80 p-8 shadow-2xl backdrop-blur text-center">
        <!-- Language Switcher in Guard Card -->
        <div class="mb-4 flex justify-end">
          <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono">
            <button
              v-for="loc in (['ru', 'en', 'es'] as const)"
              :key="loc"
              type="button"
              class="rounded-lg px-2.5 py-1 text-[10.5px] font-bold uppercase transition"
              :class="ui.locale === loc ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
              @click="ui.setLocale(loc)"
            >
              {{ loc }}
            </button>
          </div>
        </div>

        <!-- Shield / Warning Icon & Title -->
        <div class="mb-6 flex flex-col items-center">
          <div class="grid h-16 w-16 place-items-center rounded-2xl bg-amber-500/15 text-3xl text-amber-400 mb-3 shadow-inner">
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
            class="w-full rounded-xl bg-accent py-2.5 text-xs font-bold text-white shadow transition hover:bg-accent/90"
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
            <p class="text-xs text-muted">
              {{ overview?.system_health?.overall === "healthy" ? t("admin.allSystemsOperational") : t("admin.systemOperational") }}
            </p>
          </div>
        </div>

        <div class="flex items-center gap-2">
          <!-- Language Switcher in Header -->
          <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono mr-1">
            <button
              v-for="loc in (['ru', 'en', 'es'] as const)"
              :key="loc"
              class="rounded-lg px-2.5 py-1 text-[11px] font-bold uppercase transition"
              :class="ui.locale === loc ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
              @click="ui.setLocale(loc)"
            >
              {{ loc }}
            </button>
          </div>

          <!-- Dev mode warning banner if auth is disabled -->
          <div
            v-if="overview?.is_dev_mode"
            class="flex items-center gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-1.5 text-xs font-medium text-amber-400"
          >
            <span>⚠️</span>
            <span>{{ t("admin.devModeBadge") }}</span>
          </div>

          <!-- Logout / Switch account button -->
          <button
            class="rounded-lg border border-bd bg-surface px-3 py-1.5 text-xs font-medium text-muted hover:text-ink hover:bg-surface/80 transition"
            :title="t('admin.logout')"
            @click="handleLogout"
          >
            {{ t("admin.logout") }}
          </button>
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
          :class="activeTab === 'users' ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-ink'"
          @click="activeTab = 'users'"
        >
          {{ t("admin.tabs.users") }}
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

      <!-- Tab Viewport -->
      <div class="flex-1">
        <div v-if="loading" class="flex h-48 items-center justify-center text-sm text-muted">
          {{ t("common.loading") }}
        </div>
        <div v-else-if="error" class="rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
          {{ error }}
        </div>
        <div v-else>
          <OverviewTab v-if="activeTab === 'overview'" :initial-overview="overview" />

          <UsersTab v-else-if="activeTab === 'users'" />

          <AnalyticsTab v-else-if="activeTab === 'analytics'" />

          <AgentsGraphTab v-else-if="activeTab === 'agents'" />

          <OperationsTab v-else-if="activeTab === 'operations'" />
        </div>
      </div>
    </template>
  </div>
</template>
