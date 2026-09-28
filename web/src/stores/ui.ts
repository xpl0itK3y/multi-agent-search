import { defineStore } from "pinia";
import { computed, ref, watch } from "vue";
import { i18n, type Locale } from "@/i18n";
import { useMediaQuery } from "@/lib/motion";

export type ThemeId = "system" | "light" | "dark" | "midnight" | "emerald" | "rose" | "sand";
type ConcreteTheme = Exclude<ThemeId, "system">;

// id, whether it's a dark base (adds .dark for Tailwind variants), and a swatch (any CSS
// background). "system" follows the OS light/dark setting (light or dark).
export const THEMES: { id: ThemeId; dark: boolean; swatch: string; system?: boolean }[] = [
  { id: "system", dark: false, system: true, swatch: "linear-gradient(135deg,#f7f8fc 0 50%,#0e0f17 50% 100%)" },
  { id: "light", dark: false, swatch: "#5b54e8" },
  { id: "dark", dark: true, swatch: "#8b7cff" },
  { id: "midnight", dark: true, swatch: "#38a0ff" },
  { id: "emerald", dark: true, swatch: "#10c88c" },
  { id: "rose", dark: true, swatch: "#f46096" },
  { id: "sand", dark: false, swatch: "#d9923b" },
];

// User-draggable sidebar width, in px.
export const SIDEBAR_MIN = 220;
export const SIDEBAR_MAX = 480;
export const SIDEBAR_DEFAULT = 288;

const clampWidth = (px: number) => Math.min(SIDEBAR_MAX, Math.max(SIDEBAR_MIN, Math.round(px)));

function storageGet(key: string): string | null {
  try {
    return typeof localStorage !== "undefined" ? localStorage.getItem(key) : null;
  } catch {
    return null;
  }
}
function storageSet(key: string, value: string): void {
  try {
    if (typeof localStorage !== "undefined") localStorage.setItem(key, value);
  } catch {
    // storage full or blocked: the choice still applies for this visit
  }
}

// Two frames: the new colours have been painted before transitions come back on.
function afterTwoFrames(fn: () => void) {
  if (typeof requestAnimationFrame !== "function") return void setTimeout(fn, 0);
  requestAnimationFrame(() => requestAnimationFrame(fn));
}

export const useUiStore = defineStore("ui", () => {
  const sidebarCollapsed = ref(false);
  const userName = ref((import.meta.env.VITE_USER_NAME as string) || "");

  const storedWidth = Number(storageGet("sidebar.width"));
  const sidebarWidth = ref(storedWidth >= SIDEBAR_MIN && storedWidth <= SIDEBAR_MAX ? storedWidth : SIDEBAR_DEFAULT);

  /**
   * Set the sidebar width. A drag passes { persist: false } on every frame (no storage
   * write on the input path) and { clamp: false } for its rubber-banded overshoot, then
   * calls persistSidebarWidth() once on release.
   */
  function setSidebarWidth(px: number, { persist = true, clamp = true }: { persist?: boolean; clamp?: boolean } = {}) {
    sidebarWidth.value = clamp ? clampWidth(px) : Math.round(px);
    if (persist) persistSidebarWidth();
  }
  function persistSidebarWidth() {
    sidebarWidth.value = clampWidth(sidebarWidth.value);
    storageSet("sidebar.width", String(sidebarWidth.value));
  }
  function resetSidebarWidth() {
    setSidebarWidth(SIDEBAR_DEFAULT);
  }

  const locale = ref<Locale>(i18n.global.locale.value as Locale);
  // <html lang> drives screen readers, hyphenation and spell-check — keep it on
  // the active locale (index.html ships the default, "ru").
  function applyLang() {
    if (typeof document === "undefined") return;
    document.documentElement.lang = locale.value;
  }
  function setLocale(value: Locale) {
    locale.value = value;
    i18n.global.locale.value = value;
    storageSet("locale", value);
    applyLang();
  }
  applyLang();

  // New visitors follow the OS; a stored choice (including an explicit "dark") is kept.
  const stored = storageGet("theme") as ThemeId | null;
  const theme = ref<ThemeId>(THEMES.some((t) => t.id === stored) ? (stored as ThemeId) : "system");
  const systemDark = useMediaQuery("(prefers-color-scheme: dark)");
  const resolvedTheme = computed<ConcreteTheme>(() =>
    theme.value === "system" ? (systemDark.value ? "dark" : "light") : theme.value,
  );

  function applyTheme() {
    if (typeof document === "undefined") return;
    const t = THEMES.find((x) => x.id === resolvedTheme.value) ?? THEMES[1];
    document.documentElement.setAttribute("data-theme", t.id);
    document.documentElement.classList.toggle("dark", t.dark);
  }

  // One cross-fade of the whole page (§14: ease dark↔light, no abrupt brightness jump)
  // instead of each element easing its colours on its own schedule. It stays on under
  // reduced motion: a cross-fade is the non-vestibular form.
  function switchTheme() {
    if (typeof document === "undefined") return;
    const root = document.documentElement;
    root.classList.add("theme-switching");
    const done = () => root.classList.remove("theme-switching");
    const start = (document as Document & { startViewTransition?: (cb: () => void) => { finished: Promise<void> } })
      .startViewTransition;
    if (typeof start === "function") {
      try {
        start.call(document, applyTheme).finished.catch(() => {}).finally(done);
        return;
      } catch {
        // fall through to a plain switch
      }
    }
    applyTheme();
    afterTwoFrames(done);
  }

  function setTheme(id: ThemeId) {
    theme.value = id;
    storageSet("theme", id);
    switchTheme();
  }

  function toggleTheme() {
    // cycle to the next theme in the list (kept for the quick-toggle affordance)
    const i = THEMES.findIndex((t) => t.id === theme.value);
    setTheme(THEMES[(i + 1) % THEMES.length].id);
  }

  // The OS flipped light/dark while following it.
  watch(systemDark, () => {
    if (theme.value === "system") switchTheme();
  });

  applyTheme();

  function toggleSidebar() {
    sidebarCollapsed.value = !sidebarCollapsed.value;
  }

  const mobileOpen = ref(false);
  function toggleMobile() {
    mobileOpen.value = !mobileOpen.value;
  }
  function closeMobile() {
    mobileOpen.value = false;
  }

  return {
    sidebarCollapsed,
    sidebarWidth,
    setSidebarWidth,
    persistSidebarWidth,
    resetSidebarWidth,
    userName,
    theme,
    resolvedTheme,
    locale,
    mobileOpen,
    toggleSidebar,
    toggleTheme,
    setTheme,
    setLocale,
    toggleMobile,
    closeMobile,
  };
});
