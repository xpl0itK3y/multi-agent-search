<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRoute } from "vue-router";
import AppSidebar from "@/components/AppSidebar.vue";
import ConfirmDialog from "@/components/ConfirmDialog.vue";
import SparkLogo from "@/components/SparkLogo.vue";
import { confirmState } from "@/lib/confirm";
import { useMediaQuery, useReducedMotion } from "@/lib/motion";
import { useDragDismiss } from "@/lib/useDragDismiss";
import { useAuthStore } from "@/stores/auth";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";

const store = useResearchStore();
const ui = useUiStore();
const auth = useAuthStore();
const route = useRoute();

onMounted(() => {
  store.fetchModels();
});

// The sidebar's history belongs to whoever is signed in: loaded once there is a user
// (not as a 401 on a signed-out page), again after a sign-in without a page load, and
// dropped on sign-out so the next account never sees it.
watch(
  () => auth.user?.id ?? null,
  (id, previous) => {
    if (!id) {
      store.history = [];
      store.historyError = null;
      return;
    }
    store.fetchHistory();
    if (previous !== undefined) store.fetchModels(); // signed in after the page loaded
  },
  { immediate: true },
);

// Sign-in screens, and a shared report opened by someone who isn't signed in, have no
// app shell. (/r/:token keeps its router meta: a signed-in reader sees it in the shell.)
const bare = computed(() => !!route.meta.bare || (route.name === "public-report" && !auth.user));

// Below lg the sidebar is an off-canvas drawer that comes from the left edge, where the
// ☰ that opens it sits (apple-design §7: it leaves the way it came).
const isLg = useMediaQuery("(min-width: 1024px)");
const reducedMotion = useReducedMotion();
const drawer = ref<HTMLElement | null>(null);
const scrim = ref<HTMLElement | null>(null);
const menuButton = ref<HTMLButtonElement | null>(null);
const drawerOpen = computed(() => ui.mobileOpen && !isLg.value);
// Closed below lg: out of the tab order and the accessibility tree.
const drawerHidden = computed(() => !ui.mobileOpen && !isLg.value);

// A finger drags the open drawer away 1:1; a flick closes it at the release velocity.
useDragDismiss({ panel: drawer, side: "left", enabled: () => drawerOpen.value, onDismiss: () => ui.closeMobile(), scrim });

// A touch is implicitly captured by the element under the finger. When useDragDismiss
// takes the capture for the drawer, that child's lostpointercapture bubbles up to the
// drawer, where useDragDismiss also listens for it and would take it for the end of its
// own capture, cancelling the drag it just claimed. Only the drawer's own event counts.
function onDrawerLostCapture(e: PointerEvent) {
  if (drawerOpen.value && e.target !== e.currentTarget) e.stopPropagation();
}

// Close the mobile drawer on any navigation.
watch(() => route.fullPath, () => ui.closeMobile());

// One view instance per page: a new path (another page, thread or research) mounts a new
// view and plays the view transition. A query-only change (Settings' and Admin's ?tab=)
// stays in the mounted view, which follows the query itself, so its focus, scroll and
// unsaved input survive a tab switch and nothing animates in again.
const viewKey = computed(() => route.path);

const FOCUSABLE = "a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])";

function firstFocusable(root: HTMLElement): HTMLElement | null {
  const all = [...root.querySelectorAll<HTMLElement>(FOCUSABLE)];
  // Skip controls that aren't rendered at this width (the lg-only resize handle).
  return all.find((el) => el.getClientRects().length > 0) ?? all[0] ?? null;
}

function onKeydown(e: KeyboardEvent) {
  // An open popover in the drawer (the theme menu) takes Escape first; a confirmation
  // handles its own keys.
  if (e.key !== "Escape" || !drawerOpen.value || e.defaultPrevented || confirmState.open) return;
  e.preventDefault();
  ui.closeMobile();
}

// Open: focus moves into the drawer. Closed: back to ☰, unless the reader already moved on.
watch(drawerOpen, async (open) => {
  if (typeof document === "undefined") return;
  if (open) {
    document.addEventListener("keydown", onKeydown);
    await nextTick();
    if (!drawerOpen.value || !drawer.value) return;
    firstFocusable(drawer.value)?.focus({ preventScroll: true });
  } else {
    document.removeEventListener("keydown", onKeydown);
    // Read before the drawer turns inert (this watcher runs before the DOM update).
    const active = document.activeElement;
    const focusWasInside = !active || active === document.body || !!drawer.value?.contains(active);
    if (!focusWasInside || isLg.value) return;
    await nextTick();
    menuButton.value?.focus({ preventScroll: true });
  }
});
onBeforeUnmount(() => {
  if (typeof document !== "undefined") document.removeEventListener("keydown", onKeydown);
});

// §11: only transform and opacity animate. The drawer's shadow never interpolates (a
// full-height blur repainted every frame): it is painted once on a pseudo-element behind
// the sidebar and only fades, so a closed drawer leaves no shadow at the screen's edge.
const DRAWER_SHADOW =
  "after:pointer-events-none after:absolute after:inset-0 after:-z-10 after:shadow-e3 after:transition-opacity after:ease-sheet lg:after:hidden";

const drawerClass = computed(() => {
  if (reducedMotion.value) {
    // §14: a short cross-fade instead of a slide; the fade hides the shadow too.
    return [
      "shadow-e3 transition-opacity duration-150 lg:opacity-100 lg:pointer-events-auto",
      ui.mobileOpen ? "opacity-100" : "pointer-events-none opacity-0",
    ];
  }
  // The sheet curve: a fast start that settles softly, a little quicker on the way out.
  return [
    "transition-transform ease-sheet",
    DRAWER_SHADOW,
    ui.mobileOpen
      ? "translate-x-0 duration-300 after:opacity-100 after:duration-300"
      : "-translate-x-full duration-[220ms] after:opacity-0 after:duration-[220ms]",
  ];
});
</script>

<template>
  <!-- Site-styled confirm modal (replaces window.confirm), available app-wide -->
  <ConfirmDialog />

  <!-- Both frames: 100vh, then 100dvh where supported. With the two height utilities side by
       side, the 100vh one came later in the built CSS and always won, so phones got the tallest
       viewport and a page's end sat under the browser toolbar. -->
  <!-- Sign-in screens (login, password reset, email verification) and a signed-out
       reader's shared report: no app shell -->
  <div v-if="bare" class="h-screen w-screen overflow-hidden bg-bg text-ink supports-[height:100dvh]:h-dvh">
    <router-view />
  </div>

  <div v-else class="flex h-screen w-screen overflow-hidden bg-bg text-ink supports-[height:100dvh]:h-dvh">
    <!-- Sidebar: static on lg+, off-canvas drawer on mobile -->
    <div
      id="app-drawer"
      ref="drawer"
      class="fixed inset-y-0 left-0 z-40 lg:static lg:z-auto lg:translate-x-0 lg:shadow-none"
      :class="drawerClass"
      :role="isLg ? undefined : 'dialog'"
      :aria-modal="drawerOpen ? 'true' : undefined"
      :aria-label="isLg ? undefined : $t('common.menu')"
      :aria-hidden="drawerHidden ? 'true' : undefined"
      :inert="drawerHidden ? true : undefined"
      @lostpointercapture.capture="onDrawerLostCapture"
    >
      <AppSidebar />
    </div>

    <!-- Mobile scrim: always mounted, so it fades in with the slide instead of popping -->
    <div
      ref="scrim"
      class="scrim fixed inset-0 z-30 transition-[opacity,visibility] duration-200 lg:hidden"
      :class="ui.mobileOpen ? 'opacity-100' : 'pointer-events-none invisible opacity-0'"
      aria-hidden="true"
      @click="ui.closeMobile()"
    />

    <!-- inert while the drawer is open below lg: the drawer is the modal task then -->
    <main class="relative flex min-h-0 flex-1 flex-col overflow-hidden" :inert="drawerOpen ? true : undefined">
      <!-- Mobile bar: opaque, since nothing scrolls beneath it; views start below it -->
      <header class="flex h-12 shrink-0 items-center gap-2 border-b border-bd/60 bg-bg px-1.5 lg:hidden">
        <button
          ref="menuButton"
          type="button"
          class="press grid h-11 w-11 place-items-center rounded-lg text-ink hover:bg-surface"
          :aria-label="$t('common.menu')"
          :aria-expanded="ui.mobileOpen ? 'true' : 'false'"
          aria-controls="app-drawer"
          @click="ui.toggleMobile()"
        >
          <span aria-hidden="true">☰</span>
        </button>
        <SparkLogo :size="22" />
        <span class="veris-wordmark text-base font-semibold tracking-tight">{{ $t("sidebar.brand") }}</span>
      </header>
      <div class="relative min-h-0 flex-1">
        <router-view v-slot="{ Component }">
          <transition name="view" mode="out-in">
            <component :is="Component" :key="viewKey" />
          </transition>
        </router-view>
      </div>
    </main>
  </div>
</template>
