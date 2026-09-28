<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import type { ResearchHistoryItem } from "@/lib/types";
import { apiErrorMessage } from "@/lib/api";
import { rubberband } from "@/lib/gesture";
import { prefersReducedMotion } from "@/lib/motion";
import { useDismiss } from "@/lib/useDismiss";
import { useResearchStore } from "@/stores/research";
import { useUiStore, THEMES, SIDEBAR_MAX, SIDEBAR_MIN, type ThemeId } from "@/stores/ui";
import { useAuthStore } from "@/stores/auth";
import { confirm } from "@/lib/confirm";
import { avatarGlyph, isAvatarImage } from "@/lib/avatar";
import SparkLogo from "@/components/SparkLogo.vue";

const ui = useUiStore();
const auth = useAuthStore();

const displayName = computed(() => {
  const user = auth.user;
  if (!user) return ui.userName || "";
  return user.name?.trim() || user.email?.split("@")[0] || ui.userName || "";
});
const avatarUrl = computed(() => auth.user?.avatar_url || undefined);

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
const themeMenuOpen = ref(false);
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

// Escape peels one layer: in the search or rename field it ends only that, marked handled
// so the mobile drawer around the sidebar stays open (App closes it on an unhandled
// Escape), and focus goes back where the field came from instead of onto the page.
const searchButton = ref<HTMLButtonElement | null>(null);
function onSearchEscape(e: KeyboardEvent) {
  e.preventDefault();
  toggleSearch();
  searchButton.value?.focus({ preventScroll: true });
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

// A rename or delete the server refused says so under its row for a few seconds, and the
// row keeps its old title (apple-design §16 feedback: an error next to what caused it).
const ROW_ERROR_MS = 4000;
const rowError = ref<{ id: string; message: string } | null>(null);
let rowErrorTimer: ReturnType<typeof setTimeout> | undefined;
function showRowError(id: string, e: unknown) {
  clearTimeout(rowErrorTimer);
  rowError.value = { id, message: apiErrorMessage(e, t) };
  rowErrorTimer = setTimeout(() => (rowError.value = null), ROW_ERROR_MS);
}

function startRename(item: ResearchHistoryItem) {
  editingId.value = item.id;
  editValue.value = rawTitle(item);
}
// A rename ended from the keyboard hands focus back to its row: the row's title, since
// its ✎ shows only on hover or focus within the row. Called after the edit has ended, so
// the tick it waits for is the one that draws the row's buttons again.
function focusRow(row: HTMLElement | null | undefined) {
  nextTick(() => row?.querySelector<HTMLElement>("button")?.focus({ preventScroll: true }));
}
function onRenameEnter(item: ResearchHistoryItem, e: KeyboardEvent) {
  const row = (e.currentTarget as HTMLElement).parentElement;
  commitRename(item); // ends the edit before its first await
  focusRow(row);
}
function onRenameEscape(e: KeyboardEvent) {
  e.preventDefault();
  const row = (e.currentTarget as HTMLElement).parentElement;
  editingId.value = null;
  focusRow(row);
}
async function commitRename(item: ResearchHistoryItem) {
  // Enter and Escape end the edit before the field's blur arrives: that blur is not a commit.
  if (editingId.value !== item.id) return;
  const value = editValue.value.trim();
  editingId.value = null;
  if (!value || value === rawTitle(item)) return;
  try {
    await store.renameResearch(item.id, value);
  } catch (e) {
    showRowError(item.id, e);
  }
}
async function onDelete(item: ResearchHistoryItem) {
  const ok = await confirm({
    title: t("sidebar.delete"),
    message: t("sidebar.confirmDeleteNamed", { title: displayTitle(item) }),
    confirmText: t("sidebar.delete"),
    cancelText: t("common.cancel"),
    danger: true,
  });
  if (!ok) return;
  try {
    await store.deleteResearch(item.id);
    if (currentThreadId.value === threadKey(item)) router.push("/");
  } catch (e) {
    showRowError(item.id, e);
  }
}

const LOCALE_LABEL: Record<string, string> = { ru: "RU", en: "EN", es: "ES" };
function cycleLocale() {
  const order = ["ru", "en", "es"] as const;
  const next = order[(order.indexOf(ui.locale as "ru") + 1) % order.length];
  ui.setLocale(next);
}

function statusColor(status: string): string {
  if (status === "completed") return "bg-success";
  if (status === "failed") return "bg-danger";
  // The one looping "live" signal (§14: slow, opacity only).
  if (status === "analyzing" || status === "processing") return "bg-accent live-dot";
  return "bg-muted";
}

function openThread(item: ResearchHistoryItem) {
  router.push({ name: "thread", params: { threadId: threadKey(item) } });
}

// ── Theme menu: grows from ◐ (origin-top-right), closes on an outside press or Escape ──
const themeRoot = ref<HTMLElement | null>(null);
const themeTrigger = ref<HTMLButtonElement | null>(null);
const themeMenu = ref<HTMLElement | null>(null);
useDismiss(themeRoot, themeMenuOpen, { trigger: themeTrigger });

function themeItems(): HTMLElement[] {
  return [...(themeMenu.value?.querySelectorAll<HTMLElement>("[role='menuitemradio']") ?? [])];
}
watch(themeMenuOpen, async (open) => {
  if (!open) return;
  await nextTick();
  const items = themeItems();
  (items.find((el) => el.getAttribute("aria-checked") === "true") ?? items[0])?.focus({ preventScroll: true });
});
function onThemeMenuKey(e: KeyboardEvent) {
  if (e.key === "Tab") {
    themeMenuOpen.value = false;
    return;
  }
  const items = themeItems();
  const at = items.indexOf(document.activeElement as HTMLElement);
  let next = -1;
  if (e.key === "ArrowRight" || e.key === "ArrowDown") next = (at + 1) % items.length;
  else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = (at - 1 + items.length) % items.length;
  else if (e.key === "Home") next = 0;
  else if (e.key === "End") next = items.length - 1;
  if (next < 0 || !items.length) return;
  e.preventDefault();
  items[next].focus();
}
// Close first, then switch: the menu isn't caught in the theme cross-fade.
function pickTheme(id: ThemeId) {
  themeMenuOpen.value = false;
  ui.setTheme(id);
  themeTrigger.value?.focus({ preventScroll: true });
}

// ── Resize: drag the right edge (§2 direct manipulation, §9 rubber-banding) ──
// The pointer is captured, so a fast drag out of the window keeps resizing and ends on
// release. Past the limits the edge resists instead of stopping dead, and settles back on
// release; dragged under COLLAPSE_BELOW it fades toward the rail (§8: the frames hint at
// the outcome) and collapses to it. The width is stored once, on release.
const aside = ref<HTMLElement | null>(null);
const resizing = ref(false);
const COLLAPSE_BELOW = 180;
const RUBBER_BAND_PX = 120;
const RUBBER_BAND_K = 0.55; // lib/gesture's default constant, named here for the inverse
const KEY_STEP_PX = 16;
const SETTLE_MS = 200;
let drag: { id: number; startX: number; startW: number; x: number } | null = null;
let dragFrame = 0;
let settleTimer: ReturnType<typeof setTimeout> | undefined;

function bandedWidth(raw: number): number {
  if (raw > SIDEBAR_MAX) return SIDEBAR_MAX + rubberband(raw - SIDEBAR_MAX, RUBBER_BAND_PX, RUBBER_BAND_K);
  if (raw < SIDEBAR_MIN) return SIDEBAR_MIN - rubberband(SIDEBAR_MIN - raw, RUBBER_BAND_PX, RUBBER_BAND_K);
  return raw;
}
// Its inverse: the drag width that shows as `shown`, so a drag that starts inside the
// band keeps the edge where it is (r = o·d·k / (d + k·o)  ⇔  o = r·d / (k·(d − r))).
function unbandedWidth(shown: number): number {
  const unband = (r: number) => {
    const d = RUBBER_BAND_PX;
    const band = Math.min(r, d - 1); // the band never reaches d
    return (band * d) / (RUBBER_BAND_K * (d - band));
  };
  if (shown > SIDEBAR_MAX) return SIDEBAR_MAX + unband(shown - SIDEBAR_MAX);
  if (shown < SIDEBAR_MIN) return SIDEBAR_MIN - unband(SIDEBAR_MIN - shown);
  return shown;
}
const rawWidth = (d: NonNullable<typeof drag>) => d.startW + (d.x - d.startX);

// One layout write per frame, however many pointermoves arrive in it.
function applyDrag() {
  dragFrame = 0;
  if (!drag) return;
  const raw = rawWidth(drag);
  ui.setSidebarWidth(bandedWidth(raw), { persist: false, clamp: false });
  if (aside.value) aside.value.style.opacity = raw < COLLAPSE_BELOW ? "0.6" : "";
}

function onResizeDown(e: PointerEvent) {
  if (drag) return;
  if (e.pointerType === "mouse" ? e.button !== 0 : !e.isPrimary) return;
  e.preventDefault(); // no text selection, no focus ring from a mouse press
  const handle = e.currentTarget as HTMLElement;
  try {
    handle.setPointerCapture?.(e.pointerId);
  } catch {
    // the pointer is already gone
  }
  clearTimeout(settleTimer);
  // Grabbed while it settles back from the rubber band: the drag continues from the edge
  // on screen, not from the limit it is heading to (§3: start from the presentation
  // value). Read before the transition is dropped, which would jump it to its end.
  let startW = ui.sidebarWidth;
  const el = aside.value;
  if (el?.style.transition) {
    const live = el.getBoundingClientRect().width;
    if (live > 0) {
      ui.setSidebarWidth(live, { persist: false, clamp: false });
      startW = unbandedWidth(ui.sidebarWidth); // still inside the band: undo its resistance
    }
  }
  if (el) el.style.transition = "";
  drag = { id: e.pointerId, startX: e.clientX, startW, x: e.clientX };
  resizing.value = true;
  document.documentElement.style.cursor = "col-resize";
  document.body.style.userSelect = "none";
}

function onResizeMove(e: PointerEvent) {
  if (!drag || e.pointerId !== drag.id) return;
  drag.x = e.clientX;
  if (!dragFrame) dragFrame = requestAnimationFrame(applyDrag);
}

// A settle from a rubber-banded overshoot back to the limit (never for a plain drag).
function settleWidth() {
  const el = aside.value;
  if (!el || prefersReducedMotion()) return;
  el.style.transition = `width ${SETTLE_MS}ms var(--ease-emph)`;
  clearTimeout(settleTimer);
  settleTimer = setTimeout(() => {
    el.style.transition = "";
  }, SETTLE_MS + 40);
}

function onResizeEnd(e: PointerEvent) {
  const d = drag;
  if (!d || e.pointerId !== d.id) return;
  if (e.type === "pointerup") d.x = e.clientX;
  drag = null;
  if (dragFrame) cancelAnimationFrame(dragFrame);
  dragFrame = 0;
  resizing.value = false;
  document.documentElement.style.cursor = "";
  document.body.style.userSelect = "";
  if (aside.value) aside.value.style.opacity = "";

  const raw = rawWidth(d);
  if (raw < COLLAPSE_BELOW) {
    ui.setSidebarWidth(d.startW); // the rail expands back to the width it had
    ui.toggleSidebar();
    return;
  }
  const clamped = Math.min(SIDEBAR_MAX, Math.max(SIDEBAR_MIN, raw));
  if (clamped !== raw) settleWidth();
  ui.setSidebarWidth(clamped);
}

function onResizeKey(e: KeyboardEvent) {
  const step = e.key === "ArrowLeft" ? -KEY_STEP_PX : e.key === "ArrowRight" ? KEY_STEP_PX : 0;
  if (!step) return;
  e.preventDefault();
  ui.setSidebarWidth(ui.sidebarWidth + step);
}

onBeforeUnmount(() => {
  clearTimeout(rowErrorTimer);
  clearTimeout(settleTimer);
  if (dragFrame) cancelAnimationFrame(dragFrame);
  if (drag) {
    document.documentElement.style.cursor = "";
    document.body.style.userSelect = "";
  }
});

const currentThreadId = computed(() => (route.name === "thread" ? route.params.threadId : null));

// Local directive: autofocus the rename input when it mounts.
const vFocus = {
  mounted: (el: HTMLInputElement) => el.focus(),
};

function openSettings() {
  router.push("/settings");
}
</script>

<template>
  <!-- Collapsed icon-rail -->
  <aside
    v-if="collapsed"
    class="flex h-full w-16 flex-col items-center gap-2 border-r border-bd bg-rail py-3"
  >
    <button type="button" class="rail-btn hit press" :title="$t('sidebar.expand')" :aria-label="$t('sidebar.expand')" @click="ui.toggleSidebar()">⌗</button>
    <button type="button" class="rail-btn hit press" :title="$t('sidebar.newResearch')" :aria-label="$t('sidebar.newResearch')" @click="router.push('/')">+</button>
    <button
      v-if="auth.user?.is_admin"
      type="button"
      class="rail-btn hit press"
      :class="route.path.startsWith('/admin') ? 'text-accent bg-surface' : ''"
      :title="$t('admin.title')"
      :aria-label="$t('admin.title')"
      @click="router.push('/admin')"
    >
      🛡️
    </button>
    <button
      type="button"
      class="rail-btn hit press mt-auto"
      :class="route.path === '/settings' ? 'text-accent bg-surface' : ''"
      :title="$t('sidebar.settings')"
      :aria-label="$t('sidebar.settings')"
      @click="openSettings"
    >
      <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="3" />
        <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
      </svg>
    </button>
    <button
      class="group relative cursor-pointer"
      :title="displayName"
      @click="openSettings"
    >
      <img
        v-if="isAvatarImage(avatarUrl)"
        :src="avatarUrl"
        alt=""
        referrerpolicy="no-referrer"
        class="h-9 w-9 rounded-full object-cover ring-1 ring-bd group-hover:ring-accent/50 transition-all"
      />
      <div v-else class="grid h-9 w-9 place-items-center rounded-full bg-surface text-sm font-medium ring-1 ring-bd group-hover:ring-accent/50 transition-all">
        {{ avatarGlyph(avatarUrl, displayName) }}
      </div>
    </button>
  </aside>

  <!-- Expanded sidebar (user-resizable) -->
  <aside
    v-else
    ref="aside"
    class="relative flex h-full max-w-[85vw] flex-col border-r border-bd bg-rail lg:max-w-none"
    :style="{ width: ui.sidebarWidth + 'px' }"
  >
    <!-- Resize handle: a 12px hit area around a 1px line; drag, double-click (default
         width) or ←/→ with the keyboard -->
    <div
      role="separator"
      aria-orientation="vertical"
      tabindex="0"
      :aria-label="$t('sidebar.resize')"
      :aria-valuenow="ui.sidebarWidth"
      :aria-valuemin="SIDEBAR_MIN"
      :aria-valuemax="SIDEBAR_MAX"
      class="group absolute inset-y-0 right-0 z-20 hidden w-3 translate-x-1/2 cursor-col-resize touch-none lg:block"
      @pointerdown="onResizeDown"
      @pointermove="onResizeMove"
      @pointerup="onResizeEnd"
      @pointercancel="onResizeEnd"
      @lostpointercapture="onResizeEnd"
      @dblclick="ui.resetSidebarWidth()"
      @keydown="onResizeKey"
    >
      <div
        class="mx-auto h-full w-px transition-colors group-hover:bg-accent/50 group-focus-visible:bg-accent"
        :class="resizing ? 'bg-accent' : 'bg-transparent'"
      />
    </div>

    <!-- Brand -->
    <div class="flex items-center justify-between px-4 pt-4 pb-3">
      <div class="flex items-center gap-2">
        <SparkLogo :size="26" />
        <span class="veris-wordmark text-xl font-semibold tracking-tight">{{ $t("sidebar.brand") }}</span>
      </div>
      <!-- Touch hit areas grow 8px up and down but only 2px sideways: half the gap, so
           no button covers its neighbour. -->
      <div class="flex items-center gap-1 text-muted">
        <button
          ref="searchButton"
          type="button"
          class="icon-btn hit press [@media(pointer:coarse)]:after:-inset-x-0.5"
          :title="$t('sidebar.search')"
          :aria-label="$t('sidebar.search')"
          :aria-expanded="searchOpen ? 'true' : 'false'"
          @click="toggleSearch()"
        >
          ⌕
        </button>
        <button
          type="button"
          class="icon-btn hit press text-2xs font-medium [@media(pointer:coarse)]:after:-inset-x-0.5"
          :title="LOCALE_LABEL[ui.locale]"
          @click="cycleLocale()"
        >
          {{ LOCALE_LABEL[ui.locale] }}
        </button>
        <div ref="themeRoot" class="relative">
          <button
            ref="themeTrigger"
            type="button"
            class="icon-btn hit press [@media(pointer:coarse)]:after:-inset-x-0.5"
            :title="$t('sidebar.theme')"
            :aria-label="$t('sidebar.theme')"
            aria-haspopup="menu"
            :aria-expanded="themeMenuOpen ? 'true' : 'false'"
            @click="themeMenuOpen = !themeMenuOpen"
          >
            ◐
          </button>
          <Transition name="pop">
            <div
              v-if="themeMenuOpen"
              ref="themeMenu"
              role="menu"
              :aria-label="$t('sidebar.theme')"
              class="material-popover absolute right-0 z-30 mt-1 flex origin-top-right gap-1.5 rounded-xl border border-bd p-2"
              @keydown="onThemeMenuKey"
            >
              <button
                v-for="th in THEMES"
                :key="th.id"
                type="button"
                role="menuitemradio"
                :aria-checked="ui.theme === th.id ? 'true' : 'false'"
                class="h-6 w-6 rounded-full border-2 transition-transform motion-safe:hover:scale-110"
                :class="ui.theme === th.id ? 'border-ink' : 'border-transparent'"
                :style="{ background: th.swatch }"
                :title="$t('themes.' + th.id)"
                :aria-label="$t('themes.' + th.id)"
                @click="pickTheme(th.id)"
              />
            </div>
          </Transition>
        </div>
        <button
          type="button"
          class="icon-btn hit press [@media(pointer:coarse)]:after:-inset-x-0.5"
          :title="$t('sidebar.collapse')"
          :aria-label="$t('sidebar.collapse')"
          @click="ui.toggleSidebar()"
        >
          ⌗
        </button>
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

    <!-- Recents -->
    <div class="mt-4 flex min-h-0 flex-1 flex-col">
      <div class="px-5 pb-1 text-xs uppercase tracking-wide text-muted">{{ $t("sidebar.recents") }}</div>

      <div v-if="searchOpen" class="px-3 pb-2">
        <input
          ref="searchInput"
          v-model="searchQuery"
          type="search"
          :placeholder="$t('sidebar.searchPlaceholder')"
          :aria-label="$t('sidebar.search')"
          class="w-full rounded-lg border border-bd bg-surface/50 px-3 py-1.5 text-sm text-ink placeholder:text-muted"
          @keydown.esc="onSearchEscape"
        />
      </div>

      <!-- touch-pan-y: a scroller is where the browser stops looking for touch-action, so
           without its own pan-y a sideways drag here would be taken by the browser and never
           reach the drawer's drag-to-close (it scrolls only vertically anyway). -->
      <div class="min-h-0 flex-1 touch-pan-y overflow-y-auto px-2">
        <!-- Skeleton only on a first load: a refresh keeps the list in place. -->
        <div v-if="store.loadingHistory && !store.history.length" class="space-y-2 px-3 py-2">
          <div v-for="i in 5" :key="i" class="h-4 animate-pulse rounded bg-surface" :style="{ width: 70 + ((i * 7) % 25) + '%' }" />
        </div>
        <div v-else-if="store.historyError && !store.history.length" class="px-3 py-2 text-sm">
          <p role="alert" class="text-danger">{{ store.historyError }}</p>
          <button type="button" class="press mt-1 text-accent hover:underline" @click="store.fetchHistory()">
            {{ $t("common.retry") }}
          </button>
        </div>
        <p v-else-if="!filteredHistory.length" class="px-3 py-2 text-sm text-muted">
          {{ searchQuery.trim() ? $t("sidebar.noMatches", { q: searchQuery.trim() }) : $t("sidebar.empty") }}
        </p>
        <template v-for="item in filteredHistory" :key="item.id">
          <div
            class="group flex items-center gap-1 rounded-lg pr-1 text-sm transition-colors hover:bg-surface"
            :class="currentThreadId === threadKey(item) ? 'bg-surface text-ink' : 'text-muted'"
          >
            <input
              v-if="editingId === item.id"
              v-model="editValue"
              :aria-label="$t('sidebar.rename')"
              class="min-w-0 flex-1 rounded border border-transparent bg-transparent px-3 py-2 text-ink focus:outline-none"
              @keydown.enter="onRenameEnter(item, $event)"
              @keydown.esc="onRenameEscape"
              @blur="commitRename(item)"
              v-focus
            />
            <template v-else>
              <button
                type="button"
                class="flex min-w-0 flex-1 items-center gap-2 px-3 py-2 text-left"
                :aria-current="currentThreadId === threadKey(item) ? 'page' : undefined"
                @click="openThread(item)"
              >
                <span class="h-1.5 w-1.5 shrink-0 rounded-full" :class="statusColor(item.status)" />
                <span class="truncate">{{ displayTitle(item) }}</span>
              </button>
              <!-- Shown on hover, on keyboard focus in the row, and always on touch screens
                   (36px there). No extra hit padding: it would cover the next row. -->
              <button
                type="button"
                class="press hidden h-7 w-7 shrink-0 place-items-center rounded text-muted hover:text-ink group-hover:grid group-focus-within:grid [@media(hover:none)]:grid [@media(pointer:coarse)]:h-9 [@media(pointer:coarse)]:w-9"
                :title="$t('sidebar.rename')"
                :aria-label="$t('sidebar.rename')"
                @click.stop="startRename(item)"
              >
                ✎
              </button>
              <button
                type="button"
                class="press hidden h-7 w-7 shrink-0 place-items-center rounded text-muted hover:text-danger group-hover:grid group-focus-within:grid [@media(hover:none)]:grid [@media(pointer:coarse)]:h-9 [@media(pointer:coarse)]:w-9"
                :title="$t('sidebar.delete')"
                :aria-label="$t('sidebar.delete')"
                @click.stop="onDelete(item)"
              >
                ✕
              </button>
            </template>
          </div>
          <p v-if="rowError?.id === item.id" role="alert" class="px-3 pb-1 text-2xs text-danger">{{ rowError.message }}</p>
        </template>
      </div>
    </div>

    <!-- Admin panel button -->
    <div v-if="auth.user?.is_admin" class="border-t border-bd px-3 py-2">
      <router-link
        to="/admin"
        class="flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm font-medium transition-colors"
        :class="route.path.startsWith('/admin') ? 'bg-accent/15 text-accent font-semibold' : 'text-muted hover:bg-surface hover:text-ink'"
      >
        <span class="text-base">🛡️</span>
        <span>{{ $t("admin.title") }}</span>
        <span
          class="ml-auto rounded bg-accent/20 px-1.5 py-px text-3xs font-semibold text-accent"
        >
          Admin
        </span>
      </router-link>
    </div>

    <!-- User card -->
    <div
      class="mt-auto flex items-center gap-3 border-t border-bd px-4 py-3 cursor-pointer hover:bg-surface/50 transition-colors group select-none"
      :class="route.path === '/settings' ? 'bg-surface/70' : ''"
      :title="$t('sidebar.settings')"
      @click="openSettings"
    >
      <img
        v-if="isAvatarImage(avatarUrl)"
        :src="avatarUrl"
        alt=""
        referrerpolicy="no-referrer"
        class="h-9 w-9 shrink-0 rounded-full object-cover ring-1 ring-bd group-hover:ring-accent/50 transition-all"
      />
      <div v-else class="grid h-9 w-9 shrink-0 place-items-center rounded-full bg-surface text-sm font-medium ring-1 ring-bd group-hover:ring-accent/50 transition-all">
        {{ avatarGlyph(avatarUrl, displayName) }}
      </div>
      <div class="min-w-0 flex-1">
        <div class="truncate text-sm font-medium text-ink group-hover:text-accent transition-colors">{{ displayName }}</div>
        <div class="truncate text-xs text-muted">{{ auth.user?.email }}</div>
      </div>
      <button
        class="shrink-0 rounded p-1.5 text-muted hover:text-ink hover:bg-surface transition-colors"
        :class="route.path === '/settings' ? 'text-accent bg-surface' : ''"
        :title="$t('sidebar.settings')"
        @click.stop="openSettings"
      >
        <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      </button>
      <!-- Logout revokes every session of the account, not only this browser's. -->
      <button
        class="shrink-0 rounded p-1.5 text-muted hover:text-ink hover:bg-surface transition-colors"
        :title="$t('auth.logoutEverywhere')"
        :aria-label="$t('auth.logoutEverywhere')"
        @click.stop="logout"
      >
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
.icon-btn {
  display: grid;
  place-items: center;
  height: 1.75rem;
  width: 1.75rem;
  border-radius: 0.5rem;
}
/* Hover only where a real hover exists: a tap must not leave a button lit. */
@media (hover: hover) and (pointer: fine) {
  .rail-btn:hover,
  .icon-btn:hover {
    background: rgb(var(--c-surface));
    color: rgb(var(--c-ink));
  }
}
</style>
