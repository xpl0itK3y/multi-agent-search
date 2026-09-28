// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { nextTick } from "vue";

import { i18n } from "@/i18n";
import { SIDEBAR_DEFAULT, SIDEBAR_MAX, useUiStore } from "./ui";

describe("ui store locale", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    i18n.global.locale.value = "es";
    document.documentElement.lang = "ru"; // what index.html ships
  });

  it("sets <html lang> from the active locale at startup and on every change", () => {
    const ui = useUiStore();
    expect(document.documentElement.lang).toBe("es");

    ui.setLocale("en");
    expect(document.documentElement.lang).toBe("en");
    expect(i18n.global.locale.value).toBe("en");
  });
});

// An OS colour scheme that can be flipped while the app runs.
function mockColorScheme(dark: boolean) {
  const state = { dark };
  const listeners = new Set<(e: MediaQueryListEvent) => void>();
  window.matchMedia = vi.fn((query: string) => ({
    get matches() {
      return query.includes("dark") ? state.dark : false;
    },
    media: query,
    onchange: null,
    addEventListener: (_: string, fn: (e: MediaQueryListEvent) => void) => listeners.add(fn),
    removeEventListener: (_: string, fn: (e: MediaQueryListEvent) => void) => listeners.delete(fn),
    addListener: vi.fn(),
    removeListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })) as unknown as typeof window.matchMedia;
  return {
    flip(next: boolean) {
      state.dark = next;
      for (const fn of listeners) fn({ matches: next } as MediaQueryListEvent);
    },
  };
}

const html = document.documentElement;

describe("ui store theme", () => {
  const originalMatchMedia = window.matchMedia;

  beforeEach(() => {
    setActivePinia(createPinia());
    localStorage.clear();
    html.removeAttribute("data-theme");
    html.className = "";
  });
  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    delete (document as { startViewTransition?: unknown }).startViewTransition;
    vi.useRealTimers();
  });

  it("follows the OS for a new visitor, live", async () => {
    const os = mockColorScheme(true);
    const ui = useUiStore();
    expect(ui.theme).toBe("system");
    expect(ui.resolvedTheme).toBe("dark");
    expect(html.getAttribute("data-theme")).toBe("dark");
    expect(html.classList.contains("dark")).toBe(true);

    os.flip(false);
    await nextTick();
    expect(html.getAttribute("data-theme")).toBe("light");
    expect(html.classList.contains("dark")).toBe(false);
  });

  it("keeps a stored choice, whatever the OS says", () => {
    mockColorScheme(true);
    localStorage.setItem("theme", "light");
    const ui = useUiStore();
    expect(ui.theme).toBe("light");
    expect(html.getAttribute("data-theme")).toBe("light");
  });

  it("switches at once without view transitions, then turns transitions back on", () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "requestAnimationFrame", "cancelAnimationFrame"] });
    const ui = useUiStore();
    ui.setTheme("midnight");
    expect(html.getAttribute("data-theme")).toBe("midnight");
    expect(html.classList.contains("dark")).toBe(true);
    expect(html.classList.contains("theme-switching")).toBe(true);
    expect(localStorage.getItem("theme")).toBe("midnight");

    vi.advanceTimersByTime(50);
    expect(html.classList.contains("theme-switching")).toBe(false);
  });

  it("cross-fades through a view transition where there is one", async () => {
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => (finish = resolve));
    const startViewTransition = vi.fn((update: () => void) => {
      update();
      return { finished };
    });
    (document as { startViewTransition?: unknown }).startViewTransition = startViewTransition;
    const ui = useUiStore();

    ui.setTheme("sand");
    expect(startViewTransition).toHaveBeenCalledTimes(1);
    expect(html.getAttribute("data-theme")).toBe("sand");
    expect(html.classList.contains("theme-switching")).toBe(true);

    finish();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(html.classList.contains("theme-switching")).toBe(false);
  });
});

describe("ui store sidebar width", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    localStorage.clear();
  });

  it("changes the width during a drag without writing storage, then persists once", () => {
    const ui = useUiStore();
    const setItem = vi.spyOn(Storage.prototype, "setItem");

    ui.setSidebarWidth(300, { persist: false });
    ui.setSidebarWidth(SIDEBAR_MAX + 30, { persist: false, clamp: false }); // rubber-banded
    expect(ui.sidebarWidth).toBe(SIDEBAR_MAX + 30);
    expect(setItem).not.toHaveBeenCalled();

    ui.persistSidebarWidth();
    expect(ui.sidebarWidth).toBe(SIDEBAR_MAX);
    expect(localStorage.getItem("sidebar.width")).toBe(String(SIDEBAR_MAX));
    setItem.mockRestore();
  });

  it("clamps and persists by default, and resets to the default width", () => {
    const ui = useUiStore();
    ui.setSidebarWidth(100);
    expect(ui.sidebarWidth).toBe(220);
    expect(localStorage.getItem("sidebar.width")).toBe("220");

    ui.resetSidebarWidth();
    expect(ui.sidebarWidth).toBe(SIDEBAR_DEFAULT);
    expect(localStorage.getItem("sidebar.width")).toBe(String(SIDEBAR_DEFAULT));
  });
});
