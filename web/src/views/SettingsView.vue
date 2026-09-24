<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from "vue";
import { useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import { useAuthStore } from "@/stores/auth";
import { useUiStore, THEMES } from "@/stores/ui";
import { api, ApiError, apiErrorMessage } from "@/lib/api";
import { saveFile } from "@/lib/download";
import type { Depth, UserTokenStats } from "@/lib/types";

const router = useRouter();
const auth = useAuthStore();
const ui = useUiStore();
const { t } = useI18n();

// Password change and account deletion verify the current password: a 401 then
// means it was wrong (api skips session recovery), a 400 that it is required.
function credentialErrorMessage(e: unknown, sentPassword: boolean): string {
  if (e instanceof ApiError) {
    if (e.status === 401 && sentPassword) return t("settings.errors.wrongCurrentPassword");
    if (e.status === 400 && !sentPassword) return t("settings.errors.currentPasswordRequired");
  }
  return apiErrorMessage(e, t);
}

type TabId = "profile" | "research" | "appearance" | "analytics" | "security";
const activeTab = ref<TabId>("profile");

// ── Profile Tab State ─────────────────────────────────────────────────────────
const name = ref(auth.user?.name || "");
const avatarUrl = ref(auth.user?.avatar_url || "");
const profileBusy = ref(false);
const profileSuccess = ref(false);
const profileError = ref<string | null>(null);

const PRESET_AVATARS = ["🤖", "🧠", "🔍", "⚡", "🛡️", "📊", "🦉", "🚀", "✨", "💎", "💻", "🌐"];

function selectPresetAvatar(emoji: string) {
  avatarUrl.value = emoji;
}

async function saveProfile() {
  profileBusy.value = true;
  profileSuccess.value = false;
  profileError.value = null;
  try {
    await auth.updateProfile({
      name: name.value.trim() || undefined,
      avatar_url: avatarUrl.value.trim() || undefined,
    });
    profileSuccess.value = true;
    setTimeout(() => (profileSuccess.value = false), 3000);
  } catch (e) {
    profileError.value = apiErrorMessage(e, t);
  } finally {
    profileBusy.value = false;
  }
}

// ── Research Preferences State ────────────────────────────────────────────────
const defaultDepth = ref<Depth>(
  (typeof localStorage !== "undefined" && (localStorage.getItem("research.default_depth") as Depth)) || "medium"
);
const defaultModel = ref<string>(
  (typeof localStorage !== "undefined" && localStorage.getItem("research.default_model")) || "deepseek-v4-pro"
);
const planFirst = ref<boolean>(
  typeof localStorage !== "undefined" ? localStorage.getItem("research.plan_first") !== "false" : true
);
const autoExpandConsole = ref<boolean>(
  typeof localStorage !== "undefined" ? localStorage.getItem("research.auto_expand_console") === "true" : false
);
const defaultLanguage = ref<string>(
  (typeof localStorage !== "undefined" && localStorage.getItem("research.default_language")) || "auto"
);
const researchSuccess = ref(false);

function saveResearchPreferences() {
  if (typeof localStorage !== "undefined") {
    localStorage.setItem("research.default_depth", defaultDepth.value);
    localStorage.setItem("research.default_model", defaultModel.value);
    localStorage.setItem("research.plan_first", String(planFirst.value));
    localStorage.setItem("research.auto_expand_console", String(autoExpandConsole.value));
    localStorage.setItem("research.default_language", defaultLanguage.value);
  }
  researchSuccess.value = true;
  setTimeout(() => (researchSuccess.value = false), 3000);
}

// ── Analytics & Token Stats State ─────────────────────────────────────────────
const tokenStats = ref<UserTokenStats | null>(null);
const statsLoading = ref(false);
const statsError = ref<string | null>(null);
const autoRefresh = ref(true);
const pollInterval = ref<number | null>(null);
const lastUpdated = ref<string>("");

async function loadTokenStats(silent = false) {
  if (!silent) {
    statsLoading.value = true;
  }
  try {
    const res = await api.getTokenStats();
    tokenStats.value = res;
    statsError.value = null;
    const now = new Date();
    lastUpdated.value = now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch (e) {
    if (!tokenStats.value) {
      statsError.value = apiErrorMessage(e, t);
    }
  } finally {
    if (!silent) {
      statsLoading.value = false;
    }
  }
}

function startPolling() {
  stopPolling();
  if (!autoRefresh.value) return;
  pollInterval.value = window.setInterval(() => {
    if (activeTab.value === "analytics" && document.visibilityState === "visible") {
      loadTokenStats(true);
    }
  }, 3000);
}

function stopPolling() {
  if (pollInterval.value !== null) {
    clearInterval(pollInterval.value);
    pollInterval.value = null;
  }
}

function toggleAutoRefresh() {
  autoRefresh.value = !autoRefresh.value;
  if (autoRefresh.value) {
    loadTokenStats(true);
    startPolling();
  } else {
    stopPolling();
  }
}

watch(activeTab, (tab) => {
  if (tab === "analytics") {
    loadTokenStats(tokenStats.value !== null);
    startPolling();
  } else {
    stopPolling();
  }
});

function onVisibilityChange() {
  if (document.visibilityState === "visible" && activeTab.value === "analytics" && autoRefresh.value) {
    loadTokenStats(true);
  }
}

// ── Security Tab State ────────────────────────────────────────────────────────
const currentPassword = ref("");
const newPassword = ref("");
const confirmPassword = ref("");
const passwordBusy = ref(false);
const passwordSuccess = ref(false);
const passwordError = ref<string | null>(null);

async function changePassword() {
  passwordError.value = null;
  passwordSuccess.value = false;

  if (newPassword.value.length < 8) {
    passwordError.value = "Пароль должен содержать минимум 8 символов";
    return;
  }
  if (newPassword.value !== confirmPassword.value) {
    passwordError.value = "Пароли не совпадают";
    return;
  }

  passwordBusy.value = true;
  const current = currentPassword.value || undefined;
  try {
    await api.setPassword(newPassword.value, current);
    passwordSuccess.value = true;
    currentPassword.value = "";
    newPassword.value = "";
    confirmPassword.value = "";
    setTimeout(() => (passwordSuccess.value = false), 3000);
  } catch (e) {
    passwordError.value = credentialErrorMessage(e, current !== undefined);
  } finally {
    passwordBusy.value = false;
  }
}

// Data Export
const exportBusy = ref(false);
const exportError = ref<string | null>(null);
async function exportHistoryJson() {
  exportBusy.value = true;
  exportError.value = null;
  try {
    const list = await api.listResearch(100);
    const blob = new Blob([JSON.stringify(list, null, 2)], { type: "application/json" });
    saveFile({ blob, filename: null }, `research-history-${new Date().toISOString().slice(0, 10)}.json`);
  } catch (e) {
    exportError.value = apiErrorMessage(e, t);
  } finally {
    exportBusy.value = false;
  }
}

// Delete Account Modal
const showDeleteModal = ref(false);
const deletePassword = ref("");
const deleteBusy = ref(false);
const deleteError = ref<string | null>(null);

async function confirmDeleteAccount() {
  deleteBusy.value = true;
  deleteError.value = null;
  const current = deletePassword.value || undefined;
  try {
    await api.deleteAccount(current);
    await auth.logout();
    router.push("/login");
  } catch (e) {
    deleteError.value = credentialErrorMessage(e, current !== undefined);
  } finally {
    deleteBusy.value = false;
  }
}

onMounted(() => {
  if (auth.user) {
    name.value = auth.user.name || "";
    avatarUrl.value = auth.user.avatar_url || "";
  }
  loadTokenStats();
  if (activeTab.value === "analytics") {
    startPolling();
  }
  document.addEventListener("visibilitychange", onVisibilityChange);
});

onUnmounted(() => {
  stopPolling();
  document.removeEventListener("visibilitychange", onVisibilityChange);
});
</script>

<template>
  <div class="h-full overflow-y-auto bg-bg text-ink p-4 sm:p-8">
    <div class="max-w-5xl mx-auto space-y-6">
      <!-- Top Navigation & Header -->
      <div class="flex items-center justify-between border-b border-bd pb-4">
        <div class="flex items-center gap-3">
          <button
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-bd text-muted hover:text-ink hover:bg-surface transition text-xs font-medium"
            @click="router.back()"
          >
            <span>←</span>
            <span>Назад</span>
          </button>
          <div class="grid h-10 w-10 place-items-center rounded-xl bg-accent/15 text-accent border border-accent/25 shrink-0">
            <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="3" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
          </div>
          <div>
            <h1 class="text-xl font-bold tracking-tight text-ink">Настройки аккаунта</h1>
            <p class="text-xs text-muted mt-0.5">Управление профилем, параметрами поиска, внешним видом и расходом токенов</p>
          </div>
        </div>

        <div class="flex items-center gap-2 text-xs">
          <span class="text-muted">{{ auth.user?.email }}</span>
          <span
            v-if="auth.user?.is_admin"
            class="rounded bg-accent/20 border border-accent/40 text-accent px-2 py-0.5 text-[10px] font-semibold"
          >
            ADMIN
          </span>
        </div>
      </div>

      <!-- Settings Layout: Left Tabs + Right Content -->
      <div class="grid grid-cols-1 md:grid-cols-4 gap-6 items-start">
        <!-- Sidebar Tabs -->
        <nav class="flex flex-row md:flex-col gap-1 p-1.5 rounded-xl border border-bd bg-surface/40 overflow-x-auto">
          <button
            class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
            :class="activeTab === 'profile' ? 'bg-accent/15 text-accent border border-accent/30 shadow-2xs' : 'text-muted hover:text-ink hover:bg-surface'"
            @click="activeTab = 'profile'"
          >
            <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
              <circle cx="12" cy="7" r="4" />
            </svg>
            <span>Профиль</span>
          </button>

          <button
            class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
            :class="activeTab === 'research' ? 'bg-accent/15 text-accent border border-accent/30 shadow-2xs' : 'text-muted hover:text-ink hover:bg-surface'"
            @click="activeTab = 'research'"
          >
            <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <path d="M4 21v-7m0-4V3m8 18v-9m0-4V3m8 18v-5m0-4V3M1 14h6m2-6h6m2 8h6" />
            </svg>
            <span>Исследования</span>
          </button>

          <button
            class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
            :class="activeTab === 'appearance' ? 'bg-accent/15 text-accent border border-accent/30 shadow-2xs' : 'text-muted hover:text-ink hover:bg-surface'"
            @click="activeTab = 'appearance'"
          >
            <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10" />
              <path d="M12 2a10 10 0 0 1 0 20v-2a8 8 0 0 0 0-16V2z" />
            </svg>
            <span>Внешний вид</span>
          </button>

          <button
            class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
            :class="activeTab === 'analytics' ? 'bg-accent/15 text-accent border border-accent/30 shadow-2xs' : 'text-muted hover:text-ink hover:bg-surface'"
            @click="activeTab = 'analytics'"
          >
            <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <line x1="18" y1="20" x2="18" y2="10" />
              <line x1="12" y1="20" x2="12" y2="4" />
              <line x1="6" y1="20" x2="6" y2="14" />
            </svg>
            <span>Статистика и токены</span>
          </button>

          <button
            class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
            :class="activeTab === 'security' ? 'bg-accent/15 text-accent border border-accent/30 shadow-2xs' : 'text-muted hover:text-ink hover:bg-surface'"
            @click="activeTab = 'security'"
          >
            <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
            </svg>
            <span>Безопасность и данные</span>
          </button>
        </nav>

        <!-- Tab Content Area -->
        <main class="md:col-span-3 space-y-6">
          <!-- ── TAB 1: PROFILE ───────────────────────────────────────────── -->
          <section v-if="activeTab === 'profile'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">Личные данные</h2>
              <p class="text-xs text-muted mt-1">Отображаемое имя и персонализированный аватар пользователя</p>
            </div>

            <!-- Avatar selection -->
            <div class="space-y-3">
              <label class="text-xs font-medium text-ink block">Аватар</label>
              <div class="flex items-center gap-4">
                <div class="h-16 w-16 rounded-2xl bg-surface border-2 border-bd flex items-center justify-center text-2xl shadow-inner shrink-0 overflow-hidden">
                  <img
                    v-if="avatarUrl && (avatarUrl.startsWith('http') || avatarUrl.startsWith('/'))"
                    :src="avatarUrl"
                    alt=""
                    class="h-full w-full object-cover"
                  />
                  <span v-else>{{ avatarUrl || (name ? name.charAt(0).toUpperCase() : '👤') }}</span>
                </div>

                <div class="space-y-2 flex-1">
                  <div class="text-xs text-muted">Выберите готовую иконку агента:</div>
                  <div class="flex flex-wrap gap-1.5">
                    <button
                      v-for="em in PRESET_AVATARS"
                      :key="em"
                      class="h-8 w-8 rounded-lg border text-sm transition hover:scale-110 flex items-center justify-center"
                      :class="avatarUrl === em ? 'border-accent bg-accent/20 ring-2 ring-accent/30' : 'border-bd bg-surface hover:bg-surface/80'"
                      @click="selectPresetAvatar(em)"
                    >
                      {{ em }}
                    </button>
                  </div>
                </div>
              </div>

              <!-- Custom URL -->
              <div class="pt-2">
                <input
                  v-model="avatarUrl"
                  type="text"
                  placeholder="Или вставьте URL картинки аватара (https://...)"
                  class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink placeholder-muted/60 focus:outline-none focus:border-accent"
                />
              </div>
            </div>

            <!-- Name -->
            <div class="space-y-1.5">
              <label class="text-xs font-medium text-ink block">Отображаемое имя</label>
              <input
                v-model="name"
                type="text"
                placeholder="Например, Денис"
                class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink placeholder-muted/60 focus:outline-none focus:border-accent"
              />
            </div>

            <!-- Email (readonly) -->
            <div class="space-y-1.5">
              <label class="text-xs font-medium text-ink block">Email адрес</label>
              <input
                :value="auth.user?.email"
                readonly
                disabled
                class="w-full rounded-lg border border-bd/60 bg-bg/30 px-3 py-2 text-xs text-muted cursor-not-allowed"
              />
            </div>

            <!-- Alerts -->
            <div v-if="profileSuccess" class="rounded-lg bg-emerald-500/15 border border-emerald-500/30 p-3 text-xs text-emerald-300 flex items-center gap-2">
              <span>✓</span>
              <span>Профиль успешно обновлён!</span>
            </div>
            <div v-if="profileError" class="rounded-lg bg-red-500/15 border border-red-500/30 p-3 text-xs text-red-400">
              {{ profileError }}
            </div>

            <div class="pt-2">
              <button
                :disabled="profileBusy"
                class="rounded-lg bg-accent text-white px-4 py-2 text-xs font-medium transition hover:bg-accent/90 disabled:opacity-50"
                @click="saveProfile"
              >
                {{ profileBusy ? "Сохранение..." : "Сохранить профиль" }}
              </button>
            </div>
          </section>

          <!-- ── TAB 2: RESEARCH PREFERENCES ──────────────────────────────── -->
          <section v-if="activeTab === 'research'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">Параметры исследований</h2>
              <p class="text-xs text-muted mt-1">Значения по умолчанию для формы создания новых исследовательских задач</p>
            </div>

            <!-- Default Depth -->
            <div class="space-y-2">
              <label class="text-xs font-medium text-ink block">Глубина поиска по умолчанию</label>
              <div class="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'easy' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="easy" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">Быстрый (Fast)</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">Экспресс-поиск (1-2 мин, базовые источники)</div>
                  </div>
                </label>

                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'medium' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="medium" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">Сбалансированный</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">Оптимальный отчёт с аудитом цитат</div>
                  </div>
                </label>

                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'hard' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="hard" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">Глубокий (Deep)</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">Полный цикл: Red-Team, арбитраж, кросс-язык</div>
                  </div>
                </label>
              </div>
            </div>

            <!-- Default Model -->
            <div class="space-y-2">
              <label class="text-xs font-medium text-ink block">Модель по умолчанию</label>
              <div class="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultModel === 'deepseek-v4-pro' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultModel" type="radio" value="deepseek-v4-pro" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink flex items-center gap-1.5">
                      <span>V4 Pro</span>
                      <span class="text-[10px] rounded bg-indigo-500/20 text-indigo-300 px-1 py-0.2">Рекомендуется</span>
                    </div>
                    <div class="text-[11px] text-muted leading-tight mt-1">Максимальная глубина рассуждений и точность синтеза</div>
                  </div>
                </label>

                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultModel === 'deepseek-flash' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultModel" type="radio" value="deepseek-flash" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink flex items-center gap-1.5">
                      <span>V4.1 Flash</span>
                      <span class="text-[10px] rounded bg-emerald-500/20 text-emerald-300 px-1 py-0.2">Экономично</span>
                    </div>
                    <div class="text-[11px] text-muted leading-tight mt-1">Сверхбыстрый ответ, минимальная стоимость токенов</div>
                  </div>
                </label>
              </div>
            </div>

            <!-- Agent Toggles -->
            <div class="space-y-3 pt-2">
              <label class="text-xs font-medium text-ink block">Поведение агентов</label>

              <label class="flex items-center justify-between p-3 rounded-lg border border-bd bg-surface/40 cursor-pointer">
                <div>
                  <div class="text-xs font-semibold text-ink">Согласование плана перед стартом (Plan First)</div>
                  <div class="text-[11px] text-muted">Предварительно генерировать черновой план с возможностью редактирования шагов</div>
                </div>
                <input v-model="planFirst" type="checkbox" class="h-4 w-4 accent-accent rounded cursor-pointer" />
              </label>

              <label class="flex items-center justify-between p-3 rounded-lg border border-bd bg-surface/40 cursor-pointer">
                <div>
                  <div class="text-xs font-semibold text-ink">Автоматически раскрывать журнал агентов</div>
                  <div class="text-[11px] text-muted">Показывать ленту микродействий и терминал рассуждений сразу в развернутом виде</div>
                </div>
                <input v-model="autoExpandConsole" type="checkbox" class="h-4 w-4 accent-accent rounded cursor-pointer" />
              </label>
            </div>

            <!-- Alerts -->
            <div v-if="researchSuccess" class="rounded-lg bg-emerald-500/15 border border-emerald-500/30 p-3 text-xs text-emerald-300 flex items-center gap-2">
              <span>✓</span>
              <span>Настройки исследований сохранены!</span>
            </div>

            <div class="pt-2">
              <button
                class="rounded-lg bg-accent text-white px-4 py-2 text-xs font-medium transition hover:bg-accent/90"
                @click="saveResearchPreferences"
              >
                Сохранить настройки исследований
              </button>
            </div>
          </section>

          <!-- ── TAB 3: APPEARANCE ────────────────────────────────────────── -->
          <section v-if="activeTab === 'appearance'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">Внешний вид и интерфейс</h2>
              <p class="text-xs text-muted mt-1">Цветовая схема, язык отображения и оформление рабочей области</p>
            </div>

            <!-- Theme Picker -->
            <div class="space-y-2.5">
              <label class="text-xs font-medium text-ink block">Цветовая тема</label>
              <div class="grid grid-cols-2 sm:grid-cols-3 gap-2.5">
                <button
                  v-for="t in THEMES"
                  :key="t.id"
                  class="flex items-center gap-2.5 p-3 rounded-lg border text-left transition"
                  :class="ui.theme === t.id ? 'border-accent bg-accent/15 ring-2 ring-accent/20' : 'border-bd bg-surface/40 hover:bg-surface'"
                  @click="ui.setTheme(t.id)"
                >
                  <span class="h-4 w-4 rounded-full shrink-0 shadow-xs" :style="{ backgroundColor: t.swatch }" />
                  <div class="min-w-0 flex-1">
                    <div class="text-xs font-semibold text-ink capitalize">{{ t.id }}</div>
                    <div class="text-[10px] text-muted">{{ t.dark ? "Тёмная" : "Светлая" }}</div>
                  </div>
                  <span v-if="ui.theme === t.id" class="text-accent text-xs font-bold">✓</span>
                </button>
              </div>
            </div>

            <!-- Language -->
            <div class="space-y-2.5">
              <label class="text-xs font-medium text-ink block">Язык интерфейса</label>
              <div class="grid grid-cols-3 gap-2.5">
                <button
                  class="p-3 rounded-lg border text-xs font-semibold transition"
                  :class="ui.locale === 'ru' ? 'border-accent bg-accent/15 text-accent ring-2 ring-accent/20' : 'border-bd bg-surface/40 text-ink hover:bg-surface'"
                  @click="ui.setLocale('ru')"
                >
                  🇷🇺 Русский (RU)
                </button>
                <button
                  class="p-3 rounded-lg border text-xs font-semibold transition"
                  :class="ui.locale === 'en' ? 'border-accent bg-accent/15 text-accent ring-2 ring-accent/20' : 'border-bd bg-surface/40 text-ink hover:bg-surface'"
                  @click="ui.setLocale('en')"
                >
                  🇬🇧 English (EN)
                </button>
                <button
                  class="p-3 rounded-lg border text-xs font-semibold transition"
                  :class="ui.locale === 'es' ? 'border-accent bg-accent/15 text-accent ring-2 ring-accent/20' : 'border-bd bg-surface/40 text-ink hover:bg-surface'"
                  @click="ui.setLocale('es')"
                >
                  🇪🇸 Español (ES)
                </button>
              </div>
            </div>

            <!-- Sidebar Width -->
            <div class="space-y-2.5 pt-2 border-t border-bd">
              <div class="flex items-center justify-between">
                <div>
                  <div class="text-xs font-semibold text-ink">Ширина боковой панели</div>
                  <div class="text-[11px] text-muted">Текущий размер: {{ ui.sidebarWidth }}px</div>
                </div>
                <button
                  class="px-3 py-1.5 rounded-lg border border-bd text-xs text-muted hover:text-ink hover:bg-surface transition"
                  @click="ui.setSidebarWidth(288)"
                >
                  Сбросить (288px)
                </button>
              </div>
            </div>
          </section>

          <!-- ── TAB 4: ANALYTICS & TOKEN USAGE ───────────────────────────── -->
          <section v-if="activeTab === 'analytics'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 class="text-base font-semibold text-ink flex items-center gap-2">
                  <svg class="h-4 w-4 text-accent" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="18" y1="20" x2="18" y2="10" />
                    <line x1="12" y1="20" x2="12" y2="4" />
                    <line x1="6" y1="20" x2="6" y2="14" />
                  </svg>
                  <span>Статистика и расход токенов</span>
                </h2>
                <p class="text-xs text-muted mt-1">Фактическое потребление токенов DeepSeek и оценка затрат на исследования</p>
              </div>

              <div class="flex items-center gap-2.5">
                <!-- Live Real-Time Badge Toggle -->
                <button
                  type="button"
                  class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border text-xs font-medium transition cursor-pointer"
                  :class="autoRefresh ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/20' : 'bg-surface border-bd text-muted hover:text-ink'"
                  @click="toggleAutoRefresh"
                  :title="autoRefresh ? 'Автообновление включено (каждые 3 сек). Нажмите, чтобы поставить на паузу' : 'Автообновление на паузе. Нажмите, чтобы включить'"
                >
                  <span class="relative flex h-2 w-2">
                    <span v-if="autoRefresh" class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                    <span class="relative inline-flex rounded-full h-2 w-2" :class="autoRefresh ? 'bg-emerald-500' : 'bg-muted'"></span>
                  </span>
                  <span>{{ autoRefresh ? 'Live' : 'Пауза' }}</span>
                </button>

                <!-- Last updated timestamp -->
                <span v-if="lastUpdated" class="text-[11px] text-muted hidden sm:inline font-mono" :title="'Последнее обновление: ' + lastUpdated">
                  {{ lastUpdated }}
                </span>

                <!-- Manual refresh button -->
                <button
                  :disabled="statsLoading"
                  class="px-3 py-1.5 rounded-lg border border-bd text-xs text-muted hover:text-ink hover:bg-surface transition flex items-center gap-1.5 cursor-pointer disabled:opacity-50"
                  @click="loadTokenStats(false)"
                  title="Обновить сейчас"
                >
                  <span :class="{ 'animate-spin': statsLoading }">↻</span>
                  <span>Обновить</span>
                </button>
              </div>
            </div>

            <div v-if="statsLoading && !tokenStats" class="py-12 text-center text-xs text-muted">
              Загрузка аналитики...
            </div>

            <div v-else-if="statsError" class="rounded-lg bg-red-500/15 border border-red-500/30 p-3 text-xs text-red-400">
              {{ statsError }}
            </div>

            <div v-else-if="tokenStats" class="space-y-6">
              <!-- KPI Cards Grid -->
              <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">Всего отчётов</div>
                  <div class="text-xl font-bold text-ink mt-1">{{ tokenStats.researches_count }}</div>
                  <div class="text-[10px] text-muted mt-0.5">{{ tokenStats.calls_count }} обращений к LLM</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">Всего токенов</div>
                  <div class="text-xl font-bold text-accent mt-1">{{ tokenStats.total_tokens.toLocaleString() }}</div>
                  <div class="text-[10px] text-muted mt-0.5">Вход + Генерация</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">Входные токены</div>
                  <div class="text-xl font-bold text-ink mt-1">{{ tokenStats.prompt_tokens.toLocaleString() }}</div>
                  <div class="text-[10px] text-emerald-400 mt-0.5">С учётом кэширования</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">Оценка стоимости</div>
                  <div class="text-xl font-bold text-emerald-400 mt-1">≈ ${{ tokenStats.estimated_cost_usd.toFixed(4) }}</div>
                  <div class="text-[10px] text-muted mt-0.5">По тарифам DeepSeek</div>
                </div>
              </div>

              <!-- Context Caching Info Banner -->
              <div class="rounded-xl border border-indigo-500/30 bg-indigo-500/10 p-4 text-xs text-indigo-200 flex items-start gap-3">
                <span class="text-xl shrink-0">⚡</span>
                <div class="space-y-1">
                  <div class="font-semibold text-white">Экономия с Context Caching</div>
                  <p class="text-indigo-200/80 leading-relaxed text-[11px]">
                    DeepSeek автоматически кэширует системные промпты и повторный контекст веб-страниц.
                    Повторные токены тарифицируются со скидкой до 90% ($0.003–$0.022 за 1M), что радикально снижает общие затраты.
                  </p>
                </div>
              </div>

              <!-- Breakdown by Model -->
              <div v-if="tokenStats.by_model.length" class="space-y-2.5">
                <h3 class="text-xs font-semibold text-ink">Расход по моделям</h3>
                <div class="overflow-x-auto rounded-xl border border-bd">
                  <table class="w-full text-left text-xs">
                    <thead class="bg-surface/80 border-b border-bd text-muted font-medium">
                      <tr>
                        <th class="p-2.5">Модель</th>
                        <th class="p-2.5">Вызовов</th>
                        <th class="p-2.5">Входные</th>
                        <th class="p-2.5">Выходные</th>
                        <th class="p-2.5">Всего токенов</th>
                        <th class="p-2.5">Стоимость (USD)</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-bd/60 bg-surface/30">
                      <tr v-for="m in tokenStats.by_model" :key="m.model" class="hover:bg-surface/60">
                        <td class="p-2.5 font-mono font-medium text-ink">{{ m.model }}</td>
                        <td class="p-2.5 text-muted">{{ m.calls_count }}</td>
                        <td class="p-2.5 font-mono text-muted">{{ m.prompt_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 font-mono text-muted">{{ m.completion_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 font-mono font-semibold text-ink">{{ m.total_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 font-mono font-semibold text-emerald-400">≈ ${{ m.estimated_cost_usd.toFixed(4) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <!-- Recent Researches Table -->
              <div v-if="tokenStats.recent && tokenStats.recent.length" class="space-y-2.5">
                <h3 class="text-xs font-semibold text-ink">Недавние исследования</h3>
                <div class="overflow-x-auto rounded-xl border border-bd">
                  <table class="w-full text-left text-xs">
                    <thead class="bg-surface/80 border-b border-bd text-muted font-medium">
                      <tr>
                        <th class="p-2.5">Запрос</th>
                        <th class="p-2.5">Глубина</th>
                        <th class="p-2.5">Статус</th>
                        <th class="p-2.5">Токены</th>
                        <th class="p-2.5">Стоимость</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-bd/60 bg-surface/30">
                      <tr
                        v-for="r in tokenStats.recent"
                        :key="r.id"
                        class="hover:bg-surface/60 cursor-pointer"
                        @click="router.push(`/research/${r.id}`)"
                      >
                        <td class="p-2.5 font-medium text-ink max-w-[320px] truncate" :title="r.prompt">{{ r.prompt }}</td>
                        <td class="p-2.5 text-muted uppercase text-[10px] font-mono">{{ r.depth }}</td>
                        <td class="p-2.5">
                          <span
                            class="rounded px-1.5 py-0.5 text-[10px] font-medium"
                            :class="r.status === 'completed' ? 'bg-emerald-500/20 text-emerald-300' : 'bg-accent/20 text-accent'"
                          >
                            {{ r.status }}
                          </span>
                        </td>
                        <td class="p-2.5 font-mono text-muted">{{ r.total_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 font-mono font-semibold text-emerald-400">≈ ${{ r.estimated_cost_usd.toFixed(4) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>

          <!-- ── TAB 5: SECURITY & DATA ───────────────────────────────────── -->
          <section v-if="activeTab === 'security'" class="space-y-6">
            <!-- Password Change -->
            <div class="rounded-xl border border-bd bg-surface/50 p-6 space-y-4">
              <div>
                <h2 class="text-base font-semibold text-ink">Смена пароля</h2>
                <p class="text-xs text-muted mt-1">Обновите пароль для входа в ваш аккаунт</p>
              </div>

              <div class="space-y-3 max-w-md">
                <div class="space-y-1">
                  <label class="text-xs font-medium text-ink block">Текущий пароль</label>
                  <input
                    v-model="currentPassword"
                    type="password"
                    placeholder="Если был установлен"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink focus:outline-none focus:border-accent"
                  />
                </div>

                <div class="space-y-1">
                  <label class="text-xs font-medium text-ink block">Новый пароль</label>
                  <input
                    v-model="newPassword"
                    type="password"
                    placeholder="Минимум 8 символов"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink focus:outline-none focus:border-accent"
                  />
                </div>

                <div class="space-y-1">
                  <label class="text-xs font-medium text-ink block">Повторите новый пароль</label>
                  <input
                    v-model="confirmPassword"
                    type="password"
                    placeholder="Повторите пароль"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink focus:outline-none focus:border-accent"
                  />
                </div>

                <div v-if="passwordSuccess" class="rounded-lg bg-emerald-500/15 border border-emerald-500/30 p-2.5 text-xs text-emerald-300 flex items-center gap-2">
                  <span>✓</span>
                  <span>Пароль успешно изменён!</span>
                </div>
                <div v-if="passwordError" class="rounded-lg bg-red-500/15 border border-red-500/30 p-2.5 text-xs text-red-400">
                  {{ passwordError }}
                </div>

                <button
                  :disabled="passwordBusy || !newPassword"
                  class="rounded-lg bg-accent text-white px-4 py-2 text-xs font-medium transition hover:bg-accent/90 disabled:opacity-50"
                  @click="changePassword"
                >
                  {{ passwordBusy ? "Обновление..." : "Обновить пароль" }}
                </button>
              </div>
            </div>

            <!-- Export History -->
            <div class="rounded-xl border border-bd bg-surface/50 p-6 space-y-3">
              <div>
                <h2 class="text-base font-semibold text-ink">Экспорт данных</h2>
                <p class="text-xs text-muted mt-1">Скачать резервную копию всех ваших исследований в формате JSON</p>
              </div>
              <button
                :disabled="exportBusy"
                class="px-4 py-2 rounded-lg border border-bd text-xs font-medium text-ink hover:bg-surface transition flex items-center gap-2"
                @click="exportHistoryJson"
              >
                <span>📦</span>
                <span>{{ exportBusy ? "Экспорт..." : "Скачать историю исследований (JSON)" }}</span>
              </button>
              <p v-if="exportError" class="text-xs text-red-400">{{ exportError }}</p>
            </div>

            <!-- Danger Zone: Delete Account -->
            <div class="rounded-xl border border-red-500/30 bg-red-500/5 p-6 space-y-3">
              <div>
                <h2 class="text-base font-semibold text-red-400">Опасная зона</h2>
                <p class="text-xs text-muted mt-1">Удаление аккаунта приведёт к безвозвратной очистке всех ваших отчетов и истории</p>
              </div>

              <button
                class="rounded-lg bg-red-500/20 hover:bg-red-500/30 border border-red-500/40 text-red-300 px-4 py-2 text-xs font-medium transition"
                @click="showDeleteModal = true"
              >
                Удалить мой аккаунт
              </button>
            </div>
          </section>
        </main>
      </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div
      v-if="showDeleteModal"
      class="fixed inset-0 z-50 bg-black/70 backdrop-blur-xs flex items-center justify-center p-4"
    >
      <div class="max-w-md w-full rounded-2xl border border-red-500/40 bg-surface p-6 shadow-2xl space-y-4">
        <h3 class="text-base font-bold text-red-400 flex items-center gap-2">
          <span>⚠️</span>
          <span>Удаление аккаунта</span>
        </h3>
        <p class="text-xs text-muted leading-relaxed">
          Это действие необратимо. Все ваши исследования, история диалогов и сгенерированные отчёты будут удалены навсегда.
        </p>

        <div class="space-y-1.5">
          <label class="text-xs font-medium text-ink block">Введите пароль для подтверждения:</label>
          <input
            v-model="deletePassword"
            type="password"
            placeholder="Ваш пароль"
            class="w-full rounded-lg border border-bd bg-bg/80 px-3 py-2 text-xs text-ink focus:outline-none focus:border-red-400"
          />
        </div>

        <div v-if="deleteError" class="text-xs text-red-400">
          {{ deleteError }}
        </div>

        <div class="flex items-center justify-end gap-2.5 pt-2">
          <button
            class="px-3.5 py-1.5 rounded-lg border border-bd text-xs text-muted hover:text-ink"
            @click="showDeleteModal = false; deletePassword = ''; deleteError = null;"
          >
            Отмена
          </button>
          <button
            :disabled="deleteBusy"
            class="px-3.5 py-1.5 rounded-lg bg-red-500 hover:bg-red-600 text-white text-xs font-medium transition disabled:opacity-50"
            @click="confirmDeleteAccount"
          >
            {{ deleteBusy ? "Удаление..." : "Подтвердить удаление" }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
