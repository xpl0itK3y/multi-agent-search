import { defineStore } from "pinia";
import { ref } from "vue";
import { i18n, type Locale } from "@/i18n";

export type ThemeId = "light" | "dark" | "midnight" | "emerald" | "rose" | "sand";

// id, whether it's a dark base (adds .dark for Tailwind variants), and a swatch color.
export const THEMES: { id: ThemeId; dark: boolean; swatch: string }[] = [
  { id: "light", dark: false, swatch: "#5b54e8" },
  { id: "dark", dark: true, swatch: "#8b7cff" },
  { id: "midnight", dark: true, swatch: "#38a0ff" },
  { id: "emerald", dark: true, swatch: "#10c88c" },
  { id: "rose", dark: true, swatch: "#f46096" },
  { id: "sand", dark: false, swatch: "#d9923b" },
];

export const useUiStore = defineStore("ui", () => {
  const sidebarCollapsed = ref(false);
  const userName = ref((import.meta.env.VITE_USER_NAME as string) || "denis");

  const locale = ref<Locale>(i18n.global.locale.value as Locale);
  function setLocale(value: Locale) {
    locale.value = value;
    i18n.global.locale.value = value;
    if (typeof localStorage !== "undefined") localStorage.setItem("locale", value);
  }

  const stored = (typeof localStorage !== "undefined" ? localStorage.getItem("theme") : null) as ThemeId | null;
  const theme = ref<ThemeId>(THEMES.some((t) => t.id === stored) ? (stored as ThemeId) : "dark");

  function applyTheme() {
    if (typeof document === "undefined") return;
    const t = THEMES.find((x) => x.id === theme.value) ?? THEMES[1];
    document.documentElement.setAttribute("data-theme", t.id);
    document.documentElement.classList.toggle("dark", t.dark);
  }

  function setTheme(id: ThemeId) {
    theme.value = id;
    if (typeof localStorage !== "undefined") localStorage.setItem("theme", id);
    applyTheme();
  }

  function toggleTheme() {
    // cycle to the next theme in the list (kept for the quick-toggle affordance)
    const i = THEMES.findIndex((t) => t.id === theme.value);
    setTheme(THEMES[(i + 1) % THEMES.length].id);
  }

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
    userName,
    theme,
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
