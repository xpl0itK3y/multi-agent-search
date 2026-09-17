<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi, api } from "@/lib/api";
import { useAuthStore } from "@/stores/auth";
import type { AdminOverviewResponse } from "@/lib/types";
import OverviewTab from "@/components/admin/OverviewTab.vue";
import AnalyticsTab from "@/components/admin/AnalyticsTab.vue";
import AgentsGraphTab from "@/components/admin/AgentsGraphTab.vue";
import OperationsTab from "@/components/admin/OperationsTab.vue";

const { t } = useI18n();
const auth = useAuthStore();

type Tab = "overview" | "analytics" | "agents" | "operations";
const activeTab = ref<Tab>("overview");

const overview = ref<AdminOverviewResponse | null>(null);
const loading = ref(false);
const error = ref<string | null>(null);

// Admin login form state
const adminEmail = ref("latundenis55@gmail.com");
const adminPassword = ref("");
const loginLoading = ref(false);
const loginError = ref<string | null>(null);
const googleEnabled = ref(false);

async function checkGoogleAuth() {
  try {
    googleEnabled.value = (await api.authConfig()).google_oauth;
  } catch {
    googleEnabled.value = false;
  }
}

function handleGoogleLogin() {
  window.location.href = api.googleLoginUrl();
}

async function handleAdminLogin() {
  if (loginLoading.value) return;
  loginLoading.value = true;
  loginError.value = null;
  try {
    await auth.login(adminEmail.value.trim(), adminPassword.value);
    if (!auth.user?.is_admin) {
      loginError.value = `Пользователь ${auth.user?.email} успешно авторизован, но не имеет прав администратора.`;
    } else {
      await loadOverview();
    }
  } catch (err: any) {
    loginError.value = err.message || "Ошибка авторизации администратора";
  } finally {
    loginLoading.value = false;
  }
}

async function handleLogout() {
  await auth.logout();
  overview.value = null;
}

async function loadOverview() {
  if (!auth.user?.is_admin) return;
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
  checkGoogleAuth();
  if (auth.user?.is_admin) {
    loadOverview();
  }
});
</script>

<template>
  <div class="flex h-full flex-col overflow-y-auto bg-bg p-6 text-ink">
    <!-- Unauthenticated or Non-Admin Authorization Guard -->
    <div
      v-if="!auth.user?.is_admin"
      class="flex flex-1 items-center justify-center py-8"
    >
      <div class="w-full max-w-md rounded-2xl border border-bd bg-surface/80 p-8 shadow-2xl backdrop-blur">
        <!-- Shield Icon & Title -->
        <div class="mb-6 flex flex-col items-center text-center">
          <div class="grid h-16 w-16 place-items-center rounded-2xl bg-accent/15 text-3xl text-accent mb-3 shadow-inner">
            🛡️
          </div>
          <h2 class="text-xl font-bold tracking-tight text-ink">
            Авторизация администратора
          </h2>
          <p class="mt-1 text-xs text-muted max-w-xs leading-relaxed">
            Доступ к панели управления ограничен администратором системы (<span class="font-mono font-semibold text-accent">latundenis55@gmail.com</span>).
          </p>
        </div>

        <!-- Warning if logged in as a non-admin user -->
        <div
          v-if="auth.user"
          class="mb-5 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3.5 text-xs text-amber-300 space-y-2"
        >
          <div class="flex items-center gap-2 font-semibold">
            <span>⚠️</span>
            <span>Текущий аккаунт не является администратором</span>
          </div>
          <p class="text-[11px] text-amber-300/80">
            Вы вошли как <strong class="font-mono">{{ auth.user.email }}</strong>. Для доступа к панели переключитесь на аккаунт администратора.
          </p>
          <button
            class="mt-1 w-full rounded-lg border border-amber-500/40 bg-amber-500/20 py-1.5 text-xs font-semibold text-amber-200 transition hover:bg-amber-500/30"
            @click="handleLogout"
          >
            Сменить аккаунт / Выйти
          </button>
        </div>

        <!-- Google OAuth Button -->
        <div v-if="googleEnabled" class="space-y-3">
          <button
            type="button"
            class="flex w-full items-center justify-center gap-2.5 rounded-xl border border-bd bg-bg px-4 py-2.5 text-xs font-semibold text-ink shadow-sm transition hover:border-accent/60 hover:bg-surface/60"
            @click="handleGoogleLogin"
          >
            <svg width="18" height="18" viewBox="0 0 48 48" aria-hidden="true">
              <path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/>
              <path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/>
              <path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/>
              <path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/>
            </svg>
            <span>Войти через Google (Администратор)</span>
          </button>

          <div class="flex items-center gap-3 text-xs text-muted">
            <span class="h-px flex-1 bg-bd/60" />
            <span class="text-[10px] uppercase font-bold tracking-wider">или по паролю</span>
            <span class="h-px flex-1 bg-bd/60" />
          </div>
        </div>

        <!-- Password login form -->
        <form class="space-y-3 mt-3" @submit.prevent="handleAdminLogin">
          <div>
            <label class="block text-[10px] uppercase font-bold tracking-wider text-muted mb-1">
              Email администратора
            </label>
            <input
              v-model="adminEmail"
              type="email"
              required
              placeholder="latundenis55@gmail.com"
              class="w-full rounded-xl border border-bd bg-bg px-3.5 py-2 font-mono text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none"
            />
          </div>

          <div>
            <label class="block text-[10px] uppercase font-bold tracking-wider text-muted mb-1">
              Пароль
            </label>
            <input
              v-model="adminPassword"
              type="password"
              required
              placeholder="Введите пароль..."
              class="w-full rounded-xl border border-bd bg-bg px-3.5 py-2 text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none"
            />
            <p class="mt-1 text-[10px] text-muted leading-normal">
              💡 Если вы входите с паролем впервые, указанный пароль будет автоматически привязан к аккаунту администратора.
            </p>
          </div>

          <div v-if="loginError" class="rounded-lg border border-red-500/30 bg-red-500/10 p-2.5 text-xs text-red-400">
            {{ loginError }}
          </div>

          <button
            type="submit"
            :disabled="loginLoading || !adminEmail || !adminPassword"
            class="w-full rounded-xl bg-accent py-2.5 text-xs font-bold text-white shadow-lg transition hover:bg-accent/90 disabled:opacity-50"
          >
            {{ loginLoading ? "Авторизация..." : "Войти как администратор" }}
          </button>
        </form>

        <div class="mt-6 text-center border-t border-bd/60 pt-3">
          <router-link to="/" class="text-xs text-muted hover:text-ink transition">
            ← Вернуться к поиску
          </router-link>
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
              {{ overview?.system_health?.overall === "healthy" ? "All systems operational" : "System operational" }}
            </p>
          </div>
        </div>

        <div class="flex items-center gap-2">
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
            title="Выйти из аккаунта администратора"
            @click="handleLogout"
          >
            Выйти
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

          <AnalyticsTab v-else-if="activeTab === 'analytics'" />

          <AgentsGraphTab v-else-if="activeTab === 'agents'" />

          <OperationsTab v-else-if="activeTab === 'operations'" />
        </div>
      </div>
    </template>
  </div>
</template>
