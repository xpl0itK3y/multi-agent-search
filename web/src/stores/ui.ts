import { defineStore } from "pinia";
import { ref } from "vue";
import { i18n, type Locale } from "@/i18n";

type Theme = "dark" | "light";

export const useUiStore = defineStore("ui", () => {
  const sidebarCollapsed = ref(false);
  const userName = ref((import.meta.env.VITE_USER_NAME as string) || "denis");

  const locale = ref<Locale>(i18n.global.locale.value as Locale);
  function setLocale(value: Locale) {
    locale.value = value;
    i18n.global.locale.value = value;
    if (typeof localStorage !== "undefined") localStorage.setItem("locale", value);
  }

  const stored = (typeof localStorage !== "undefined" && localStorage.getItem("theme")) as Theme | null;
  const theme = ref<Theme>(stored === "light" || stored === "dark" ? stored : "dark");

  function applyTheme() {
    if (typeof document !== "undefined") {
      document.documentElement.classList.toggle("dark", theme.value === "dark");
    }
  }

  function toggleTheme() {
    theme.value = theme.value === "dark" ? "light" : "dark";
    if (typeof localStorage !== "undefined") localStorage.setItem("theme", theme.value);
    applyTheme();
  }

  applyTheme();

  function toggleSidebar() {
    sidebarCollapsed.value = !sidebarCollapsed.value;
  }

  return { sidebarCollapsed, userName, theme, locale, toggleSidebar, toggleTheme, setLocale };
});
