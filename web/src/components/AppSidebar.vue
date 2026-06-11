<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import type { ResearchHistoryItem } from "@/lib/types";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";
import { useAuthStore } from "@/stores/auth";

const ui = useUiStore();
const auth = useAuthStore();

const displayName = computed(() => auth.user?.name || auth.user?.email || ui.userName);
const avatarUrl = computed(() => auth.user?.avatar_url || null);

async function logout() {
  await auth.logout();
  router.push({ name: "login" });
}
const store = useResearchStore();
const router = useRouter();
const route = useRoute();
const { t } = useI18n();

const collapsed = computed(() => ui.sidebarCollapsed);

const searchOpen = ref(false);
const searchQuery = ref("");
const searchInput = ref<HTMLInputElement | null>(null);
const editingId = ref<string | null>(null);
const editValue = ref("");

async function toggleSearch() {
  searchOpen.value = !searchOpen.value;
  if (searchOpen.value) {
    await nextTick();
    searchInput.value?.focus();
  } else {
    searchQuery.value = "";
  }
}

function rawTitle(item: ResearchHistoryItem): string {
  return (item.title?.trim() || item.prompt || "").replace(/\s+/g, " ");
}
function displayTitle(item: ResearchHistoryItem): string {
  const value = rawTitle(item);
  return value.length > 30 ? value.slice(0, 30) + "…" : value;
}

function threadKey(item: ResearchHistoryItem): string {
  return item.thread_id ?? item.id;
}

// Recents are grouped by conversation thread (one representative entry each).
const filteredHistory = computed(() => {
  const q = searchQuery.value.trim().toLowerCase();
  if (!q) return store.threads;
  return store.threads.filter((item) => rawTitle(item).toLowerCase().includes(q));
});

function startRename(item: ResearchHistoryItem) {
  editingId.value = item.id;
  editValue.value = rawTitle(item);
}
async function commitRename(id: string) {
  const value = editValue.value.trim();
  editingId.value = null;
  if (value) {
    try {
      await store.renameResearch(id, value);
    } catch {
      /* ignore */
    }
  }
}
async function onDelete(item: ResearchHistoryItem) {
  if (!window.confirm(t("sidebar.confirmDelete"))) return;
  try {
    await store.deleteResearch(item.id);
    if (currentThreadId.value === threadKey(item)) router.push("/");
  } catch {
    /* ignore */
  }
}

const nav = [
  { key: "researches", active: true },
];

const LOCALE_LABEL: Record<string, string> = { ru: "RU", en: "EN", es: "ES" };
function cycleLocale() {
  const order = ["ru", "en", "es"] as const;
  const next = order[(order.indexOf(ui.locale as "ru") + 1) % order.length];
  ui.setLocale(next);
}

function statusColor(status: string): string {
  if (status === "completed") return "bg-emerald-400";
  if (status === "failed") return "bg-red-400";
  if (status === "analyzing" || status === "processing") return "bg-accent";
  return "bg-muted";
}

function openThread(item: ResearchHistoryItem) {
  router.push({ name: "thread", params: { threadId: threadKey(item) } });
}

const currentThreadId = computed(() => (route.name === "thread" ? route.params.threadId : null));

// Local directive: autofocus the rename input when it mounts.
const vFocus = {
  mounted: (el: HTMLInputElement) => el.focus(),
};
</script>

<template>
  <!-- Collapsed icon-rail -->
  <aside
    v-if="collapsed"
    class="flex h-full w-16 flex-col items-center gap-2 border-r border-bd bg-rail py-3"
  >
    <button class="rail-btn" :title="$t('sidebar.expand')" @click="ui.toggleSidebar()">⌗</button>
    <button class="rail-btn" :title="$t('sidebar.newResearch')" @click="router.push('/')">+</button>
    <img
      v-if="avatarUrl"
      :src="avatarUrl"
      alt=""
      referrerpolicy="no-referrer"
      class="mt-auto h-9 w-9 rounded-full object-cover"
    />
    <div v-else class="mt-auto grid h-9 w-9 place-items-center rounded-full bg-surface text-sm">
      {{ displayName.charAt(0).toUpperCase() }}
    </div>
  </aside>

  <!-- Expanded sidebar -->
  <aside
    v-else
    class="flex h-full w-72 flex-col border-r border-bd bg-rail"
  >
    <!-- Brand -->
    <div class="flex items-center justify-between px-4 pt-4 pb-3">
      <span class="font-serif text-xl tracking-tight text-ink">{{ $t("sidebar.brand") }}</span>
      <div class="flex items-center gap-1 text-muted">
        <button class="icon-btn" :title="$t('sidebar.search')" @click="toggleSearch()">⌕</button>
        <button class="icon-btn text-[11px] font-medium" :title="LOCALE_LABEL[ui.locale]" @click="cycleLocale()">
          {{ LOCALE_LABEL[ui.locale] }}
        </button>
        <button
          class="icon-btn"
          :title="ui.theme === 'dark' ? $t('sidebar.themeLight') : $t('sidebar.themeDark')"
          @click="ui.toggleTheme()"
        >
          {{ ui.theme === "dark" ? "☀" : "☾" }}
        </button>
        <button class="icon-btn" :title="$t('sidebar.collapse')" @click="ui.toggleSidebar()">⌗</button>
      </div>
    </div>

    <!-- New research -->
    <div class="px-3">
      <button
        class="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm text-ink hover:bg-surface"
        @click="router.push('/')"
      >
        <span class="grid h-6 w-6 place-items-center rounded-full border border-bd text-base leading-none">+</span>
        {{ $t("sidebar.newResearch") }}
      </button>
    </div>

    <!-- Primary nav -->
    <nav class="mt-1 px-3">
      <button
        v-for="item in nav"
        :key="item.key"
        class="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm hover:bg-surface"
        :class="item.active ? 'bg-surface text-ink' : 'text-muted'"
      >
        {{ $t("nav." + item.key) }}
      </button>
    </nav>

    <!-- Recents -->
    <div class="mt-4 flex min-h-0 flex-1 flex-col">
      <div class="px-5 pb-1 text-xs uppercase tracking-wide text-muted">{{ $t("sidebar.recents") }}</div>

      <div v-if="searchOpen" class="px-3 pb-2">
        <input
          ref="searchInput"
          v-model="searchQuery"
          :placeholder="$t('sidebar.searchPlaceholder')"
          class="w-full rounded-lg border border-bd bg-surface/50 px-3 py-1.5 text-sm text-ink placeholder:text-muted focus:border-accent/40 focus:outline-none"
          @keydown.esc="toggleSearch()"
        />
      </div>

      <div class="min-h-0 flex-1 overflow-y-auto px-2">
        <div v-if="store.loadingHistory" class="space-y-2 px-3 py-2">
          <div v-for="i in 5" :key="i" class="h-4 animate-pulse rounded bg-surface" :style="{ width: 70 + ((i * 7) % 25) + '%' }" />
        </div>
        <p v-else-if="!filteredHistory.length" class="px-3 py-2 text-sm text-muted">
          {{ $t("sidebar.empty") }}
        </p>
        <div
          v-for="item in filteredHistory"
          :key="item.id"
          class="group flex items-center gap-1 rounded-lg pr-1 text-sm hover:bg-surface"
          :class="currentThreadId === threadKey(item) ? 'bg-surface text-ink' : 'text-muted'"
        >
          <input
            v-if="editingId === item.id"
            v-model="editValue"
            class="min-w-0 flex-1 rounded bg-transparent px-3 py-2 text-ink focus:outline-none"
            @keydown.enter="commitRename(item.id)"
            @keydown.esc="editingId = null"
            @blur="commitRename(item.id)"
            v-focus
          />
          <template v-else>
            <button
              class="flex min-w-0 flex-1 items-center gap-2 px-3 py-2 text-left"
              @click="openThread(item)"
            >
              <span class="h-1.5 w-1.5 shrink-0 rounded-full" :class="statusColor(item.status)" />
              <span class="truncate">{{ displayTitle(item) }}</span>
            </button>
            <button
              class="hidden shrink-0 rounded p-1 text-muted hover:text-ink group-hover:block"
              :title="$t('sidebar.rename')"
              @click.stop="startRename(item)"
            >
              ✎
            </button>
            <button
              class="hidden shrink-0 rounded p-1 text-muted hover:text-red-400 group-hover:block"
              :title="$t('sidebar.delete')"
              @click.stop="onDelete(item)"
            >
              ✕
            </button>
          </template>
        </div>
      </div>
    </div>

    <!-- User card -->
    <div class="mt-auto flex items-center gap-3 border-t border-bd px-4 py-3">
      <img
        v-if="avatarUrl"
        :src="avatarUrl"
        alt=""
        referrerpolicy="no-referrer"
        class="h-9 w-9 shrink-0 rounded-full object-cover"
      />
      <div v-else class="grid h-9 w-9 shrink-0 place-items-center rounded-full bg-surface text-sm">
        {{ displayName.charAt(0).toUpperCase() }}
      </div>
      <div class="min-w-0 flex-1">
        <div class="truncate text-sm text-ink">{{ displayName }}</div>
        <div class="text-xs text-muted">{{ $t("sidebar.plan") }}</div>
      </div>
      <button class="shrink-0 rounded p-1 text-muted hover:text-ink" :title="$t('auth.logout')" @click="logout">
        ⏻
      </button>
    </div>
  </aside>
</template>

<style scoped>
.rail-btn {
  display: grid;
  place-items: center;
  height: 2.25rem;
  width: 2.25rem;
  border-radius: 0.5rem;
  color: rgb(var(--c-muted));
}
.rail-btn:hover {
  background: rgb(var(--c-surface));
  color: rgb(var(--c-ink));
}
.icon-btn {
  display: grid;
  place-items: center;
  height: 1.75rem;
  width: 1.75rem;
  border-radius: 0.5rem;
}
.icon-btn:hover {
  background: rgb(var(--c-surface));
  color: rgb(var(--c-ink));
}
</style>
