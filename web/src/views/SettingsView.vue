<script setup lang="ts">
import { nextTick, ref, onMounted, onUnmounted, useId, watch } from "vue";
import { useRoute, useRouter, type LocationQueryRaw } from "vue-router";
import { useI18n } from "vue-i18n";
import type { Locale } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import { useUiStore, THEMES, SIDEBAR_DEFAULT } from "@/stores/ui";
import { api, ApiError, apiErrorMessage, isReauthRequired, PASSWORD_MIN_LENGTH } from "@/lib/api";
import { saveFile } from "@/lib/download";
import { avatarGlyph, isAvatarImage } from "@/lib/avatar";
import { smoothOrAuto } from "@/lib/motion";
import type { Depth, UserTokenStats } from "@/lib/types";
import GoogleReauthNotice from "@/components/GoogleReauthNotice.vue";
import PasswordRuleHint from "@/components/PasswordRuleHint.vue";
import { googleReauthErrorKey, REAUTH_ERROR_PARAM } from "@/lib/googleSignIn";

const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const ui = useUiStore();
const { t, te, locale } = useI18n();

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
const TAB_IDS: readonly TabId[] = ["profile", "research", "appearance", "analytics", "security"];
const tabFromQuery = (value: unknown): TabId | undefined => TAB_IDS.find((id) => id === value);
// ?tab=<id> opens that tab, e.g. back from the Google sign-in a password change asked for.
// The open tab lives in the address, so a reload or Back keeps it (apple-design §16
// wayfinding); a one-time re-auth reason is dropped once the reader moves on.
const activeTab = ref<TabId>(tabFromQuery(route.query.tab) ?? "profile");
function selectTab(tab: TabId) {
  activeTab.value = tab;
  if (route.query.tab === tab) return;
  const query: LocationQueryRaw = { ...route.query, tab };
  delete query[REAUTH_ERROR_PARAM];
  router.replace({ query });
}
// The view stays mounted across ?tab= changes (App keys views by path), so it follows the
// address itself: Back, Forward, or the sidebar's plain /settings (Profile).
watch(
  () => route.query.tab,
  (tab) => {
    activeTab.value = tabFromQuery(tab) ?? "profile";
  },
);

// Below md the tabs are one row that scrolls sideways. A fade at its right edge shows
// while more tabs wait there, and the open tab is scrolled fully into view, clear of the
// fade: on arrival (a link or a reload with ?tab=), on a click, and on Back/Forward
// (§16 Wayfinding: where am I?). Sideways only, so the page never jumps.
const tabNav = ref<HTMLElement | null>(null);
const canScrollRight = ref(false);
const EDGE_FADE_PX = 28; // .edge-fade-x
const NAV_INSET_PX = 6; // the row's p-1.5
function updateTabOverflow() {
  const el = tabNav.value;
  canScrollRight.value = !!el && el.scrollLeft + el.clientWidth < el.scrollWidth - 1;
}
function revealActiveTab(behavior: ScrollBehavior) {
  const nav = tabNav.value;
  const btn = nav?.querySelector<HTMLElement>('[aria-current="page"]');
  if (!nav || !btn) return;
  const s = nav.getBoundingClientRect();
  if (!s.width) return; // not laid out
  const b = btn.getBoundingClientRect();
  const end = Math.max(NAV_INSET_PX, canScrollRight.value ? EDGE_FADE_PX : 0);
  const delta =
    b.left < s.left + NAV_INSET_PX - 1 ? b.left - s.left - NAV_INSET_PX
    : b.right > s.right - end + 1 ? b.right - s.right + end
    : 0;
  if (!delta) return;
  if (typeof nav.scrollBy === "function") nav.scrollBy({ left: delta, behavior });
  else nav.scrollLeft += delta;
}
let tabNavObserver: ResizeObserver | undefined;
onMounted(() => {
  updateTabOverflow();
  revealActiveTab("auto"); // arriving: already in place, no scroll to watch
  if (typeof ResizeObserver !== "undefined" && tabNav.value) {
    tabNavObserver = new ResizeObserver(updateTabOverflow);
    tabNavObserver.observe(tabNav.value);
  }
});
onUnmounted(() => tabNavObserver?.disconnect());
watch(locale, () => nextTick(updateTabOverflow));

// A switch shows the new tab from its start. The view keeps its scroll across a switch
// (App keys views by path), so from far down Analytics the next tab would open somewhere
// in its middle: beside the sticky tabs (md+) the page comes back up until the tab's
// start lines up with theirs, no further. Below the tabs (phones) it is already in view.
const viewRoot = ref<HTMLElement | null>(null);
const tabFrame = ref<HTMLElement | null>(null);
const tabPanel = ref<HTMLElement | null>(null);
function showPanelStart() {
  const root = viewRoot.value;
  if (!root?.scrollTop || !tabFrame.value || !tabPanel.value) return;
  const over = tabFrame.value.getBoundingClientRect().top - tabPanel.value.getBoundingClientRect().top;
  if (over > 1) root.scrollTop = Math.max(0, root.scrollTop - over);
}
watch(activeTab, () =>
  nextTick(() => {
    showPanelStart();
    revealActiveTab(smoothOrAuto());
  }),
);

// Back to where the reader came from; Settings opened on its own (a new tab, a link) goes
// home instead of leaving the app.
function goBack() {
  if (typeof window !== "undefined" && window.history.state?.back) router.back();
  else router.push("/");
}

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

// ── Email verification (Profile tab) ──────────────────────────────────────────
// Only where the server can send email (auth config). An address is verified by the
// emailed link, a password reset or a Google sign-in.
const emailVerificationEnabled = ref(false);
const verificationBusy = ref(false);
const verificationNotice = ref<"sent" | "already" | null>(null);
const verificationError = ref<string | null>(null);

async function loadAuthConfig() {
  try {
    emailVerificationEnabled.value = Boolean((await api.authConfig())?.email_verification);
  } catch {
    // Unknown: no verification section.
  }
}

async function sendVerificationEmail() {
  if (verificationBusy.value) return;
  verificationBusy.value = true;
  verificationNotice.value = null;
  verificationError.value = null;
  try {
    const { status } = await api.requestEmailVerification();
    if (status === "already_verified") {
      // Verified meanwhile (the link opened in another tab, a Google sign-in).
      verificationNotice.value = "already";
      await auth.refreshUser();
    } else {
      verificationNotice.value = "sent";
    }
  } catch (e) {
    // A 429 when links were asked for too often.
    verificationError.value = apiErrorMessage(e, t);
  } finally {
    verificationBusy.value = false;
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
// Read by AgentActivityConsole for its initial open state.
const autoExpandConsole = ref<boolean>(
  typeof localStorage !== "undefined" ? localStorage.getItem("research.auto_expand_console") === "true" : false
);
const researchSuccess = ref(false);

function saveResearchPreferences() {
  if (typeof localStorage !== "undefined") {
    localStorage.setItem("research.default_depth", defaultDepth.value);
    localStorage.setItem("research.default_model", defaultModel.value);
    localStorage.setItem("research.plan_first", String(planFirst.value));
    localStorage.setItem("research.auto_expand_console", String(autoExpandConsole.value));
  }
  researchSuccess.value = true;
  setTimeout(() => (researchSuccess.value = false), 3000);
}

// ── Appearance ────────────────────────────────────────────────────────────────
// Language names are endonyms: the same whatever the current UI language.
const LANGUAGES: { value: Locale; label: string }[] = [
  { value: "ru", label: "🇷🇺 Русский (RU)" },
  { value: "en", label: "🇬🇧 English (EN)" },
  { value: "es", label: "🇪🇸 Español (ES)" },
];

// ── Analytics & Token Stats State ─────────────────────────────────────────────
function statusLabel(s: string): string {
  return te(`status.${s}`) ? t(`status.${s}`) : s;
}
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
// The server wants a fresh Google sign-in first (a first password on a Google-only
// account, or a reset without the current password on a Google-linked one).
const passwordNeedsReauth = ref(false);
// The server's rule, stated under the new-password field (red after a refused save).
const newPasswordHelpId = useId();
const passwordFieldIds = { current: useId(), next: useId(), confirm: useId(), delete: useId() };
const passwordTried = ref(false);
const REAUTH_RETURN_TO = "/settings?tab=security";
// Back from that Google sign-in without it (cancelled, or another Google account): the
// router returns here with ?reauth_error=<code> (lib/googleSignIn.ts) and we say why.
const reauthErrorKey = ref(googleReauthErrorKey(route.query[REAUTH_ERROR_PARAM]));

async function changePassword() {
  passwordError.value = null;
  passwordNeedsReauth.value = false;
  reauthErrorKey.value = null;
  passwordSuccess.value = false;

  if (newPassword.value.length < PASSWORD_MIN_LENGTH) {
    passwordTried.value = true;
    passwordError.value = t("settings.errors.passwordTooShort", { min: PASSWORD_MIN_LENGTH });
    return;
  }
  passwordTried.value = false;
  if (newPassword.value !== confirmPassword.value) {
    passwordError.value = t("settings.errors.passwordMismatch");
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
    passwordTried.value = false;
    setTimeout(() => (passwordSuccess.value = false), 3000);
  } catch (e) {
    if (isReauthRequired(e)) passwordNeedsReauth.value = true;
    else passwordError.value = credentialErrorMessage(e, current !== undefined);
  } finally {
    passwordBusy.value = false;
  }
}

// Sessions: a logout revokes every token of the account (the server bumps its token
// version), so it signs out all devices, and the button says so.
const logoutBusy = ref(false);
async function logoutEverywhere() {
  logoutBusy.value = true;
  await auth.logout(); // never throws; local state is cleared either way
  router.push("/login");
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
// A passwordless (Google-only) account confirms the deletion with a fresh Google
// sign-in instead of a password: the server refuses it until then (reauth_required).
const deleteNeedsReauth = ref(false);

// The delete dialog (§16: a confirmation only for the one irreversible action here): focus
// goes to its password field and back to "Delete account" after; Escape, Cancel or a press
// that starts and ends on the backdrop closes it; Tab stays inside; Enter submits.
const deletePanel = ref<HTMLElement | null>(null);
const deletePasswordInput = ref<HTMLInputElement | null>(null);
let deleteReturnFocus: HTMLElement | null = null;
const deleteTitleId = useId();

function openDeleteModal() {
  deleteReturnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  showDeleteModal.value = true;
  nextTick(() => deletePasswordInput.value?.focus());
}

function closeDeleteModal() {
  showDeleteModal.value = false;
  deletePassword.value = "";
  deleteError.value = null;
  deleteNeedsReauth.value = false;
  const back = deleteReturnFocus;
  deleteReturnFocus = null;
  if (back?.isConnected) nextTick(() => back.focus({ preventScroll: true }));
}

function onDeleteModalKey(e: KeyboardEvent) {
  if (e.key === "Escape") {
    e.preventDefault();
    closeDeleteModal();
    return;
  }
  const panel = deletePanel.value;
  if (e.key !== "Tab" || !panel) return;
  const items = [
    ...panel.querySelectorAll<HTMLElement>(
      "input:not([disabled]):not([tabindex='-1']), button:not([disabled]), a[href]",
    ),
  ];
  if (!items.length) return;
  const first = items[0];
  const last = items[items.length - 1];
  if (e.shiftKey && document.activeElement === first) {
    e.preventDefault();
    last.focus();
  } else if (!e.shiftKey && document.activeElement === last) {
    e.preventDefault();
    first.focus();
  }
}

let deletePressedBackdrop = false;
function onDeleteBackdropDown(e: PointerEvent) {
  deletePressedBackdrop = e.target === e.currentTarget;
}
function onDeleteBackdropClick(e: MouseEvent) {
  const close = deletePressedBackdrop && e.target === e.currentTarget;
  deletePressedBackdrop = false;
  if (close) closeDeleteModal();
}

// ── Small presentational helpers ────────────────────────────────────────────────
function themeHint(th: (typeof THEMES)[number]): string {
  if (th.system) return t("themes.systemHint");
  return th.dark ? t("settings.appearance.baseDark") : t("settings.appearance.baseLight");
}
function statusPillClass(status: string): string {
  if (status === "completed") return "bg-success/10 text-success";
  if (status === "failed" || status === "timeout") return "bg-danger/10 text-danger";
  if (status === "cancelled") return "bg-surface text-muted";
  return "bg-accent/15 text-accent";
}

async function confirmDeleteAccount() {
  deleteBusy.value = true;
  deleteError.value = null;
  deleteNeedsReauth.value = false;
  reauthErrorKey.value = null;
  const current = deletePassword.value || undefined;
  try {
    await api.deleteAccount(current);
    await auth.logout();
    router.push("/login");
  } catch (e) {
    if (isReauthRequired(e)) deleteNeedsReauth.value = true;
    else deleteError.value = credentialErrorMessage(e, current !== undefined);
  } finally {
    deleteBusy.value = false;
  }
}

onMounted(() => {
  if (auth.user) {
    name.value = auth.user.name || "";
    avatarUrl.value = auth.user.avatar_url || "";
  }
  loadAuthConfig();
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
  <div ref="viewRoot" class="h-full overflow-y-auto bg-bg text-ink p-4 sm:p-8">
    <div class="max-w-5xl mx-auto space-y-6">
      <!-- Top Navigation & Header -->
      <!-- On phones: back and the title only; the gear tile and the email come back from sm. -->
      <div class="flex flex-col gap-3 border-b border-bd pb-4 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex min-w-0 items-center gap-3">
          <button
            type="button"
            class="press flex shrink-0 items-center gap-1.5 px-3 py-1.5 rounded-lg border border-bd text-muted hover:text-ink hover:bg-surface text-xs font-medium"
            @click="goBack"
          >
            <span aria-hidden="true">←</span>
            <span>{{ t("settings.back") }}</span>
          </button>
          <div class="hidden h-10 w-10 place-items-center rounded-xl bg-accent/15 text-accent border border-accent/25 shrink-0 sm:grid">
            <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="3" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
          </div>
          <div class="min-w-0">
            <h1 class="text-xl font-bold tracking-tight text-ink">{{ t("settings.title") }}</h1>
            <p class="text-xs text-muted mt-0.5 text-pretty">{{ t("settings.subtitle") }}</p>
          </div>
        </div>

        <div class="hidden min-w-0 items-center gap-2 text-xs sm:flex">
          <span class="truncate text-muted">{{ auth.user?.email }}</span>
          <span
            v-if="auth.user?.is_admin"
            class="rounded bg-accent/20 border border-accent/40 text-accent px-2 py-0.5 text-3xs font-semibold"
          >
            ADMIN
          </span>
        </div>
      </div>

      <!-- Settings Layout: Left Tabs + Right Content -->
      <div class="grid grid-cols-1 md:grid-cols-4 gap-6 items-start">
        <!-- Sidebar Tabs: the frame keeps its border and fill; only the row inside scrolls
             and fades. min-w-0: the row's full width never widens the grid column. -->
        <div ref="tabFrame" class="min-w-0 overflow-hidden rounded-xl border border-bd bg-surface/40 md:sticky md:top-0 md:z-10">
          <nav
            ref="tabNav"
            class="flex flex-row md:flex-col gap-1 p-1.5 overflow-x-auto scrollbar-none"
            :class="{ 'edge-fade-x': canScrollRight }"
            data-test="settings-tabs"
            @scroll.passive="updateTabOverflow"
          >
            <button
              type="button"
              class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
              :class="activeTab === 'profile' ? 'bg-accent/15 text-accent border border-accent/30 shadow-e1' : 'text-muted hover:text-ink hover:bg-surface'"
              :aria-current="activeTab === 'profile' ? 'page' : undefined"
              @click="selectTab('profile')"
            >
              <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
                <circle cx="12" cy="7" r="4" />
              </svg>
              <span>{{ t("settings.tabs.profile") }}</span>
            </button>
  
            <button
              type="button"
              class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
              :class="activeTab === 'research' ? 'bg-accent/15 text-accent border border-accent/30 shadow-e1' : 'text-muted hover:text-ink hover:bg-surface'"
              :aria-current="activeTab === 'research' ? 'page' : undefined"
              @click="selectTab('research')"
            >
              <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <path d="M4 21v-7m0-4V3m8 18v-9m0-4V3m8 18v-5m0-4V3M1 14h6m2-6h6m2 8h6" />
              </svg>
              <span>{{ t("settings.tabs.research") }}</span>
            </button>
  
            <button
              type="button"
              class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
              :class="activeTab === 'appearance' ? 'bg-accent/15 text-accent border border-accent/30 shadow-e1' : 'text-muted hover:text-ink hover:bg-surface'"
              :aria-current="activeTab === 'appearance' ? 'page' : undefined"
              @click="selectTab('appearance')"
            >
              <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10" />
                <path d="M12 2a10 10 0 0 1 0 20v-2a8 8 0 0 0 0-16V2z" />
              </svg>
              <span>{{ t("settings.tabs.appearance") }}</span>
            </button>
  
            <button
              type="button"
              class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
              :class="activeTab === 'analytics' ? 'bg-accent/15 text-accent border border-accent/30 shadow-e1' : 'text-muted hover:text-ink hover:bg-surface'"
              :aria-current="activeTab === 'analytics' ? 'page' : undefined"
              @click="selectTab('analytics')"
            >
              <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <line x1="18" y1="20" x2="18" y2="10" />
                <line x1="12" y1="20" x2="12" y2="4" />
                <line x1="6" y1="20" x2="6" y2="14" />
              </svg>
              <span>{{ t("settings.tabs.analytics") }}</span>
            </button>
  
            <button
              type="button"
              class="flex items-center gap-2.5 px-3 py-2 rounded-lg text-xs font-medium transition shrink-0 text-left"
              :class="activeTab === 'security' ? 'bg-accent/15 text-accent border border-accent/30 shadow-e1' : 'text-muted hover:text-ink hover:bg-surface'"
              :aria-current="activeTab === 'security' ? 'page' : undefined"
              @click="selectTab('security')"
            >
              <svg class="h-4 w-4 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
              </svg>
              <span>{{ t("settings.tabs.security") }}</span>
            </button>
          </nav>
        </div>

        <!-- Tab Content Area -->
        <main ref="tabPanel" class="md:col-span-3 space-y-6">
          <!-- ── TAB 1: PROFILE ───────────────────────────────────────────── -->
          <section v-if="activeTab === 'profile'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">{{ t("settings.profile.title") }}</h2>
              <p class="text-xs text-muted mt-1">{{ t("settings.profile.subtitle") }}</p>
            </div>

            <!-- Avatar selection -->
            <div class="space-y-3">
              <label class="text-xs font-medium text-ink block">{{ t("settings.profile.avatar") }}</label>
              <div class="flex items-center gap-4">
                <div class="h-16 w-16 rounded-2xl bg-surface border-2 border-bd flex items-center justify-center text-2xl shadow-inner shrink-0 overflow-hidden">
                  <img
                    v-if="isAvatarImage(avatarUrl)"
                    :src="avatarUrl"
                    alt=""
                    class="h-full w-full object-cover"
                  />
                  <span v-else>{{ avatarGlyph(avatarUrl, name, '👤') }}</span>
                </div>

                <div class="space-y-2 flex-1">
                  <div class="text-xs text-muted">{{ t("settings.profile.presetHint") }}</div>
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
                  :placeholder="t('settings.profile.avatarUrlPlaceholder')"
                  class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink placeholder-muted/60"
                />
              </div>
            </div>

            <!-- Name -->
            <div class="space-y-1.5">
              <label class="text-xs font-medium text-ink block">{{ t("settings.profile.name") }}</label>
              <input
                v-model="name"
                type="text"
                :placeholder="t('settings.profile.namePlaceholder')"
                class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink placeholder-muted/60"
              />
            </div>

            <!-- Email (readonly) -->
            <div class="space-y-1.5">
              <label class="text-xs font-medium text-ink block">{{ t("settings.profile.email") }}</label>
              <input
                :value="auth.user?.email"
                readonly
                disabled
                class="w-full rounded-lg border border-bd/60 bg-bg/30 px-3 py-2 text-xs text-muted cursor-not-allowed"
              />

              <!-- Email verification -->
              <div v-if="emailVerificationEnabled" class="space-y-2 pt-1">
                <div class="flex flex-wrap items-center gap-2">
                  <span
                    v-if="auth.user?.email_verified"
                    class="rounded border border-success/30 bg-success/10 px-2 py-0.5 text-2xs font-medium text-success"
                  >
                    ✓ {{ t("settings.profile.emailVerified") }}
                  </span>
                  <template v-else>
                    <span class="rounded border border-warning/30 bg-warning/10 px-2 py-0.5 text-2xs font-medium text-warning">
                      {{ t("settings.profile.emailNotVerified") }}
                    </span>
                    <button
                      type="button"
                      :disabled="verificationBusy"
                      class="px-3 py-1 rounded-lg border border-bd text-[11px] font-medium text-ink hover:bg-surface transition disabled:opacity-50"
                      @click="sendVerificationEmail"
                    >
                      {{ verificationBusy ? t("settings.profile.sendingVerification") : t("settings.profile.sendVerification") }}
                    </button>
                  </template>
                </div>
                <p v-if="!auth.user?.email_verified" class="text-[11px] text-muted">
                  {{ t("settings.profile.emailNotVerifiedHint") }}
                </p>
                <p v-if="verificationNotice === 'sent'" role="status" class="text-xs text-success">
                  {{ t("settings.profile.verificationSent", { email: auth.user?.email ?? "" }) }}
                </p>
                <p v-else-if="verificationNotice === 'already'" role="status" class="text-xs text-success">
                  {{ t("settings.profile.alreadyVerified") }}
                </p>
                <p v-if="verificationError" role="alert" class="text-xs text-danger">{{ verificationError }}</p>
              </div>
            </div>

            <!-- Alerts -->
            <div v-if="profileSuccess" role="status" class="rounded-lg bg-success/10 border border-success/30 p-3 text-xs text-success flex items-center gap-2">
              <span>✓</span>
              <span>{{ t("settings.profile.saved") }}</span>
            </div>
            <div v-if="profileError" role="alert" class="rounded-lg bg-danger/10 border border-danger/30 p-3 text-xs text-danger">
              {{ profileError }}
            </div>

            <div class="pt-2">
              <button
                type="button"
                :disabled="profileBusy"
                class="press rounded-lg bg-accent text-onAccent px-4 py-2 text-xs font-medium hover:bg-accent/90 disabled:opacity-50"
                @click="saveProfile"
              >
                {{ profileBusy ? t("settings.profile.saving") : t("settings.profile.save") }}
              </button>
            </div>
          </section>

          <!-- ── TAB 2: RESEARCH PREFERENCES ──────────────────────────────── -->
          <section v-if="activeTab === 'research'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">{{ t("settings.research.title") }}</h2>
              <p class="text-xs text-muted mt-1">{{ t("settings.research.subtitle") }}</p>
            </div>

            <!-- Default Depth -->
            <div class="space-y-2">
              <label class="text-xs font-medium text-ink block">{{ t("settings.research.depth") }}</label>
              <div class="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'easy' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="easy" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">{{ t("settings.research.depthEasy") }}</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">{{ t("settings.research.depthEasyHint") }}</div>
                  </div>
                </label>

                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'medium' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="medium" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">{{ t("settings.research.depthMedium") }}</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">{{ t("settings.research.depthMediumHint") }}</div>
                  </div>
                </label>

                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultDepth === 'hard' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultDepth" type="radio" value="hard" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink">{{ t("settings.research.depthHard") }}</div>
                    <div class="text-[11px] text-muted leading-tight mt-0.5">{{ t("settings.research.depthHardHint") }}</div>
                  </div>
                </label>
              </div>
            </div>

            <!-- Default Model -->
            <div class="space-y-2">
              <label class="text-xs font-medium text-ink block">{{ t("settings.research.model") }}</label>
              <div class="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
                <label
                  class="flex items-start gap-2.5 p-3 rounded-lg border cursor-pointer transition"
                  :class="defaultModel === 'deepseek-v4-pro' ? 'border-accent bg-accent/10' : 'border-bd bg-surface/40 hover:bg-surface'"
                >
                  <input v-model="defaultModel" type="radio" value="deepseek-v4-pro" class="mt-0.5 accent-accent" />
                  <div class="text-xs">
                    <div class="font-semibold text-ink flex items-center gap-1.5">
                      <span>V4 Pro</span>
                      <span class="text-3xs rounded bg-accent/15 text-accent px-1 py-px">{{ t("settings.research.recommended") }}</span>
                    </div>
                    <div class="text-[11px] text-muted leading-tight mt-1">{{ t("settings.research.proHint") }}</div>
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
                      <span class="text-3xs rounded bg-success/10 text-success px-1 py-px">{{ t("settings.research.economical") }}</span>
                    </div>
                    <div class="text-[11px] text-muted leading-tight mt-1">{{ t("settings.research.flashHint") }}</div>
                  </div>
                </label>
              </div>
            </div>

            <!-- Agent Toggles -->
            <div class="space-y-3 pt-2">
              <label class="text-xs font-medium text-ink block">{{ t("settings.research.agents") }}</label>

              <label class="flex items-center justify-between p-3 rounded-lg border border-bd bg-surface/40 cursor-pointer">
                <div>
                  <div class="text-xs font-semibold text-ink">{{ t("settings.research.planFirst") }}</div>
                  <div class="text-[11px] text-muted">{{ t("settings.research.planFirstHint") }}</div>
                </div>
                <input v-model="planFirst" type="checkbox" class="h-4 w-4 accent-accent rounded cursor-pointer" />
              </label>

              <label class="flex items-center justify-between p-3 rounded-lg border border-bd bg-surface/40 cursor-pointer">
                <div>
                  <div class="text-xs font-semibold text-ink">{{ t("settings.research.autoExpand") }}</div>
                  <div class="text-[11px] text-muted">{{ t("settings.research.autoExpandHint") }}</div>
                </div>
                <input v-model="autoExpandConsole" type="checkbox" class="h-4 w-4 accent-accent rounded cursor-pointer" />
              </label>
            </div>

            <!-- Alerts -->
            <div v-if="researchSuccess" role="status" class="rounded-lg bg-success/10 border border-success/30 p-3 text-xs text-success flex items-center gap-2">
              <span>✓</span>
              <span>{{ t("settings.research.saved") }}</span>
            </div>

            <div class="pt-2">
              <button
                type="button"
                class="press rounded-lg bg-accent text-onAccent px-4 py-2 text-xs font-medium hover:bg-accent/90"
                @click="saveResearchPreferences"
              >
                {{ t("settings.research.save") }}
              </button>
            </div>
          </section>

          <!-- ── TAB 3: APPEARANCE ────────────────────────────────────────── -->
          <section v-if="activeTab === 'appearance'" class="rounded-xl border border-bd bg-surface/50 p-6 space-y-6">
            <div>
              <h2 class="text-base font-semibold text-ink">{{ t("settings.appearance.title") }}</h2>
              <p class="text-xs text-muted mt-1">{{ t("settings.appearance.subtitle") }}</p>
            </div>

            <!-- Theme Picker -->
            <div class="space-y-2.5">
              <label class="text-xs font-medium text-ink block">{{ t("settings.appearance.theme") }}</label>
              <div class="grid grid-cols-2 sm:grid-cols-3 gap-2.5">
                <button
                  v-for="th in THEMES"
                  :key="th.id"
                  class="flex items-center gap-2.5 p-3 rounded-lg border text-left transition"
                  :class="ui.theme === th.id ? 'border-accent bg-accent/15 ring-2 ring-accent/20' : 'border-bd bg-surface/40 hover:bg-surface'"
                  @click="ui.setTheme(th.id)"
                >
                  <span class="h-4 w-4 rounded-full shrink-0 shadow-e1" :style="{ background: th.swatch }" />
                  <div class="min-w-0 flex-1">
                    <div class="text-xs font-semibold text-ink">{{ t(`themes.${th.id}`) }}</div>
                    <div class="text-3xs text-muted">{{ themeHint(th) }}</div>
                  </div>
                  <span v-if="ui.theme === th.id" class="text-accent text-xs font-bold">✓</span>
                </button>
              </div>
            </div>

            <!-- Language -->
            <div class="space-y-2.5">
              <label class="text-xs font-medium text-ink block">{{ t("settings.appearance.language") }}</label>
              <div class="grid grid-cols-3 gap-2.5">
                <button
                  v-for="lang in LANGUAGES"
                  :key="lang.value"
                  class="p-3 rounded-lg border text-xs font-semibold transition"
                  :class="ui.locale === lang.value ? 'border-accent bg-accent/15 text-accent ring-2 ring-accent/20' : 'border-bd bg-surface/40 text-ink hover:bg-surface'"
                  @click="ui.setLocale(lang.value)"
                >
                  {{ lang.label }}
                </button>
              </div>
            </div>

            <!-- Sidebar Width: only where the sidebar can be resized (lg and up) -->
            <div class="hidden space-y-2.5 pt-2 border-t border-bd lg:block">
              <div class="flex items-center justify-between">
                <div>
                  <div class="text-xs font-semibold text-ink">{{ t("settings.appearance.sidebarWidth") }}</div>
                  <div class="text-[11px] text-muted">{{ t("settings.appearance.sidebarWidthCurrent", { px: ui.sidebarWidth }) }}</div>
                </div>
                <button
                  type="button"
                  class="px-3 py-1.5 rounded-lg border border-bd text-xs text-muted hover:text-ink hover:bg-surface transition"
                  @click="ui.resetSidebarWidth()"
                >
                  {{ t("settings.appearance.sidebarReset", { px: SIDEBAR_DEFAULT }) }}
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
                  <span>{{ t("settings.analytics.title") }}</span>
                </h2>
                <p class="text-xs text-muted mt-1">{{ t("settings.analytics.subtitle") }}</p>
              </div>

              <div class="flex items-center gap-2.5">
                <!-- Live Real-Time Badge Toggle -->
                <button
                  type="button"
                  class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border text-xs font-medium transition cursor-pointer"
                  :class="autoRefresh ? 'bg-success/10 border-success/30 text-success hover:bg-success/15' : 'bg-surface border-bd text-muted hover:text-ink'"
                  @click="toggleAutoRefresh"
                  :title="autoRefresh ? t('settings.analytics.liveOnHint') : t('settings.analytics.liveOffHint')"
                >
                  <!-- The shared live signal: a slow breath, not a ping. -->
                  <span class="inline-flex h-2 w-2 rounded-full" :class="autoRefresh ? 'bg-success live-dot' : 'bg-muted'" aria-hidden="true"></span>
                  <span>{{ autoRefresh ? t("settings.analytics.live") : t("settings.analytics.paused") }}</span>
                </button>

                <!-- Last updated timestamp -->
                <span v-if="lastUpdated" class="text-2xs text-muted hidden sm:inline tabular-nums" :title="t('settings.analytics.lastUpdated', { time: lastUpdated })">
                  {{ lastUpdated }}
                </span>

                <!-- Manual refresh button -->
                <button
                  :disabled="statsLoading"
                  class="px-3 py-1.5 rounded-lg border border-bd text-xs text-muted hover:text-ink hover:bg-surface transition flex items-center gap-1.5 cursor-pointer disabled:opacity-50"
                  @click="loadTokenStats(false)"
                  :title="t('settings.analytics.refreshNow')"
                >
                  <span :class="{ 'animate-spin': statsLoading }">↻</span>
                  <span>{{ t("settings.analytics.refresh") }}</span>
                </button>
              </div>
            </div>

            <div v-if="statsLoading && !tokenStats" class="py-12 text-center text-xs text-muted">
              {{ t("settings.analytics.loading") }}
            </div>

            <div v-else-if="statsError" role="alert" class="rounded-lg bg-danger/10 border border-danger/30 p-3 text-xs text-danger">
              {{ statsError }}
            </div>

            <div v-else-if="tokenStats" class="space-y-6">
              <!-- KPI Cards Grid -->
              <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">{{ t("settings.analytics.reports") }}</div>
                  <div class="text-xl font-bold tabular-nums text-ink mt-1">{{ tokenStats.researches_count }}</div>
                  <div class="text-[10px] text-muted mt-0.5">{{ t("settings.analytics.llmCalls", tokenStats.calls_count) }}</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">{{ t("settings.analytics.totalTokens") }}</div>
                  <div class="text-xl font-bold tabular-nums text-accent mt-1">{{ tokenStats.total_tokens.toLocaleString() }}</div>
                  <div class="text-[10px] text-muted mt-0.5">{{ t("settings.analytics.inputPlusOutput") }}</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">{{ t("settings.analytics.promptTokens") }}</div>
                  <div class="text-xl font-bold tabular-nums text-ink mt-1">{{ tokenStats.prompt_tokens.toLocaleString() }}</div>
                  <div class="text-3xs text-success mt-0.5">{{ t("settings.analytics.withCaching") }}</div>
                </div>

                <div class="p-3.5 rounded-xl border border-bd bg-surface/60">
                  <div class="text-[11px] text-muted font-medium">{{ t("settings.analytics.cost") }}</div>
                  <div class="text-xl font-bold tabular-nums text-success mt-1">≈ ${{ tokenStats.estimated_cost_usd.toFixed(4) }}</div>
                  <div class="text-[10px] text-muted mt-0.5">{{ t("settings.analytics.deepseekRates") }}</div>
                </div>
              </div>

              <!-- Context Caching Info Banner -->
              <div class="rounded-xl border border-info/30 bg-info/10 p-4 text-xs text-info flex items-start gap-3">
                <span class="text-xl shrink-0" aria-hidden="true">⚡</span>
                <div class="space-y-1">
                  <div class="font-semibold text-ink">{{ t("settings.analytics.cachingTitle") }}</div>
                  <p class="text-info leading-relaxed text-2xs">
                    {{ t("settings.analytics.cachingBody") }}
                  </p>
                </div>
              </div>

              <!-- Breakdown by Model -->
              <div v-if="tokenStats.by_model.length" class="space-y-2.5">
                <h3 class="text-xs font-semibold text-ink">{{ t("settings.analytics.byModel") }}</h3>
                <div class="overflow-x-auto rounded-xl border border-bd">
                  <table class="w-full text-left text-xs">
                    <thead class="bg-surface/80 border-b border-bd text-muted font-medium">
                      <tr>
                        <th class="p-2.5">{{ t("settings.analytics.colModel") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colCalls") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colInput") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colOutput") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colTotal") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colCostUsd") }}</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-bd/60 bg-surface/30">
                      <tr v-for="m in tokenStats.by_model" :key="m.model" class="hover:bg-surface/60">
                        <td class="p-2.5 font-mono font-medium text-ink">{{ m.model }}</td>
                        <td class="p-2.5 text-right tabular-nums text-muted">{{ m.calls_count }}</td>
                        <td class="p-2.5 text-right tabular-nums text-muted">{{ m.prompt_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 text-right tabular-nums text-muted">{{ m.completion_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 text-right tabular-nums font-semibold text-ink">{{ m.total_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 text-right tabular-nums font-semibold text-success">≈ ${{ m.estimated_cost_usd.toFixed(4) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <!-- Recent Researches Table -->
              <div v-if="tokenStats.recent && tokenStats.recent.length" class="space-y-2.5">
                <h3 class="text-xs font-semibold text-ink">{{ t("settings.analytics.recent") }}</h3>
                <div class="overflow-x-auto rounded-xl border border-bd">
                  <table class="w-full text-left text-xs">
                    <thead class="bg-surface/80 border-b border-bd text-muted font-medium">
                      <tr>
                        <th class="p-2.5">{{ t("settings.analytics.colPrompt") }}</th>
                        <th class="p-2.5">{{ t("settings.analytics.colDepth") }}</th>
                        <th class="p-2.5">{{ t("settings.analytics.colStatus") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colTokens") }}</th>
                        <th class="p-2.5 text-right tabular-nums">{{ t("settings.analytics.colCost") }}</th>
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
                        <td class="p-2.5 text-muted uppercase text-3xs font-mono">{{ r.depth }}</td>
                        <td class="p-2.5">
                          <span
                            class="rounded px-1.5 py-0.5 text-3xs font-medium"
                            :class="statusPillClass(r.status)"
                          >
                            {{ statusLabel(r.status) }}
                          </span>
                        </td>
                        <td class="p-2.5 text-right tabular-nums text-muted">{{ r.total_tokens.toLocaleString() }}</td>
                        <td class="p-2.5 text-right tabular-nums font-semibold text-success">≈ ${{ r.estimated_cost_usd.toFixed(4) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>

          <!-- ── TAB 5: SECURITY & DATA ───────────────────────────────────── -->
          <section v-if="activeTab === 'security'" class="space-y-6">
            <GoogleReauthNotice
              v-if="reauthErrorKey"
              :return-to="REAUTH_RETURN_TO"
              :message="t(reauthErrorKey)"
              class="text-xs"
            />

            <!-- Password Change -->
            <div class="rounded-xl border border-bd bg-surface/50 p-6 space-y-4">
              <div>
                <h2 class="text-base font-semibold text-ink">{{ t("settings.security.passwordTitle") }}</h2>
                <p class="text-xs text-muted mt-1">{{ t("settings.security.passwordSubtitle") }}</p>
              </div>

              <!-- A real form: Enter saves, and password managers see the account and the fields. -->
              <form class="space-y-3 max-w-md" @submit.prevent="changePassword">
                <input type="email" autocomplete="username" :value="auth.user?.email ?? ''" readonly hidden tabindex="-1" />
                <div class="space-y-1">
                  <label :for="passwordFieldIds.current" class="text-xs font-medium text-ink block">{{ t("settings.security.currentPassword") }}</label>
                  <input
                    :id="passwordFieldIds.current"
                    v-model="currentPassword"
                    type="password"
                    autocomplete="current-password"
                    :placeholder="t('settings.security.currentPasswordPlaceholder')"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink"
                  />
                </div>

                <div class="space-y-1">
                  <label :for="passwordFieldIds.next" class="text-xs font-medium text-ink block">{{ t("settings.security.newPassword") }}</label>
                  <input
                    :id="passwordFieldIds.next"
                    v-model="newPassword"
                    type="password"
                    autocomplete="new-password"
                    :minlength="PASSWORD_MIN_LENGTH"
                    :aria-describedby="newPasswordHelpId"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink"
                  />
                  <!-- The rule is said once, under the field (no placeholder repeating it). -->
                  <PasswordRuleHint :id="newPasswordHelpId" :password="newPassword" :tried="passwordTried" />
                </div>

                <div class="space-y-1">
                  <label :for="passwordFieldIds.confirm" class="text-xs font-medium text-ink block">{{ t("settings.security.confirmPassword") }}</label>
                  <input
                    :id="passwordFieldIds.confirm"
                    v-model="confirmPassword"
                    type="password"
                    autocomplete="new-password"
                    :placeholder="t('settings.security.confirmPasswordPlaceholder')"
                    class="w-full rounded-lg border border-bd bg-bg/60 px-3 py-2 text-xs text-ink"
                  />
                </div>

                <div v-if="passwordSuccess" role="status" class="rounded-lg bg-success/10 border border-success/30 p-2.5 text-xs text-success flex items-center gap-2">
                  <span aria-hidden="true">✓</span>
                  <span>{{ t("settings.security.passwordChanged") }}</span>
                </div>
                <div v-if="passwordError" role="alert" class="rounded-lg bg-danger/10 border border-danger/30 p-2.5 text-xs text-danger">
                  {{ passwordError }}
                </div>
                <GoogleReauthNotice v-if="passwordNeedsReauth" :return-to="REAUTH_RETURN_TO" class="text-xs" />

                <button
                  type="submit"
                  :disabled="passwordBusy || !newPassword"
                  class="press rounded-lg bg-accent text-onAccent px-4 py-2 text-xs font-medium hover:bg-accent/90 disabled:opacity-50"
                >
                  {{ passwordBusy ? t("settings.security.updating") : t("settings.security.updatePassword") }}
                </button>
              </form>
            </div>

            <!-- Sessions -->
            <div class="rounded-xl border border-bd bg-surface/50 p-6 space-y-3">
              <div>
                <h2 class="text-base font-semibold text-ink">{{ t("settings.security.sessionsTitle") }}</h2>
                <p class="text-xs text-muted mt-1">{{ t("auth.logoutEverywhereHint") }}</p>
              </div>
              <button
                :disabled="logoutBusy"
                class="px-4 py-2 rounded-lg border border-bd text-xs font-medium text-ink hover:bg-surface transition disabled:opacity-50"
                @click="logoutEverywhere"
              >
                {{ t("auth.logoutEverywhere") }}
              </button>
            </div>

            <!-- Export History -->
            <div class="rounded-xl border border-bd bg-surface/50 p-6 space-y-3">
              <div>
                <h2 class="text-base font-semibold text-ink">{{ t("settings.security.exportTitle") }}</h2>
                <p class="text-xs text-muted mt-1">{{ t("settings.security.exportSubtitle") }}</p>
              </div>
              <button
                :disabled="exportBusy"
                class="px-4 py-2 rounded-lg border border-bd text-xs font-medium text-ink hover:bg-surface transition flex items-center gap-2"
                @click="exportHistoryJson"
              >
                <span>📦</span>
                <span>{{ exportBusy ? t("settings.security.exporting") : t("settings.security.exportButton") }}</span>
              </button>
              <p v-if="exportError" role="alert" class="text-xs text-danger">{{ exportError }}</p>
            </div>

            <!-- Danger Zone: Delete Account -->
            <div class="rounded-xl border border-danger/30 bg-danger/5 p-6 space-y-3">
              <div>
                <h2 class="text-base font-semibold text-danger">{{ t("settings.security.dangerTitle") }}</h2>
                <p class="text-xs text-muted mt-1">{{ t("settings.security.dangerSubtitle") }}</p>
              </div>

              <button
                type="button"
                class="press rounded-lg bg-danger/10 hover:bg-danger/15 border border-danger/40 text-danger px-4 py-2 text-xs font-medium"
                @click="openDeleteModal"
              >
                {{ t("settings.security.deleteAccount") }}
              </button>
            </div>
          </section>
        </main>
      </div>
    </div>

    <!-- Delete Confirmation Modal: the shared scrim and modal motion, a real form (Enter
         submits), and a backdrop that closes only on a press that starts and ends on it. -->
    <Transition name="modal">
      <div
        v-if="showDeleteModal"
        class="scrim fixed inset-0 z-50 flex items-center justify-center p-4"
        data-test="delete-backdrop"
        @pointerdown="onDeleteBackdropDown"
        @click="onDeleteBackdropClick"
        @keydown="onDeleteModalKey"
      >
        <div
          ref="deletePanel"
          role="dialog"
          aria-modal="true"
          :aria-labelledby="deleteTitleId"
          class="modal-panel max-w-md w-full rounded-2xl border border-danger/40 bg-surface p-6 shadow-e3"
          data-test="delete-panel"
        >
          <form class="space-y-4" @submit.prevent="confirmDeleteAccount">
            <input type="email" autocomplete="username" :value="auth.user?.email ?? ''" readonly hidden tabindex="-1" />
            <h3 :id="deleteTitleId" class="text-base font-bold text-danger flex items-center gap-2">
              <span aria-hidden="true">⚠️</span>
              <span>{{ t("settings.security.deleteTitle") }}</span>
            </h3>
            <p class="text-xs text-muted leading-relaxed text-pretty">
              {{ t("settings.security.deleteWarning") }}
            </p>

            <div class="space-y-1.5">
              <label :for="passwordFieldIds.delete" class="text-xs font-medium text-ink block">{{ t("settings.security.deletePasswordLabel") }}</label>
              <!-- Optional: a Google-only account confirms with an empty password and a new
                   Google sign-in, so the confirm button stays enabled. -->
              <input
                :id="passwordFieldIds.delete"
                ref="deletePasswordInput"
                v-model="deletePassword"
                type="password"
                autocomplete="current-password"
                autofocus
                :placeholder="t('settings.security.deletePasswordPlaceholder')"
                class="w-full rounded-lg border border-bd bg-bg/80 px-3 py-2 text-xs text-ink"
              />
            </div>

            <div v-if="deleteError" role="alert" class="text-xs text-danger">
              {{ deleteError }}
            </div>
            <GoogleReauthNotice
              v-if="deleteNeedsReauth"
              :return-to="REAUTH_RETURN_TO"
              :message="t('settings.security.deleteReauthRequired')"
              class="text-xs"
            />

            <div class="flex items-center justify-end gap-2.5 pt-2">
              <button
                type="button"
                class="press px-3.5 py-1.5 rounded-lg border border-bd text-xs text-ink hover:bg-surfaceHover"
                @click="closeDeleteModal"
              >
                {{ t("common.cancel") }}
              </button>
              <button
                type="submit"
                :disabled="deleteBusy"
                class="press px-3.5 py-1.5 rounded-lg bg-red-600 hover:bg-red-700 text-white text-xs font-medium disabled:opacity-50"
              >
                {{ deleteBusy ? t("settings.security.deleting") : t("settings.security.confirmDelete") }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </Transition>
  </div>
</template>
