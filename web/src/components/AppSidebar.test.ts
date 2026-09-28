// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

const mocks = vi.hoisted(() => ({
  renameResearch: vi.fn(),
  deleteResearch: vi.fn(),
  listResearch: vi.fn(),
}));
// Real ApiError/apiErrorMessage; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});

import { i18n } from "@/i18n";
import { ApiError } from "@/lib/api";
import { answerConfirm, confirmState } from "@/lib/confirm";
import type { ResearchHistoryItem } from "@/lib/types";
import { useAuthStore } from "@/stores/auth";
import { useResearchStore } from "@/stores/research";
import { SIDEBAR_DEFAULT, SIDEBAR_MAX, SIDEBAR_MIN, useUiStore } from "@/stores/ui";
import AppSidebar from "./AppSidebar.vue";

const router = createRouter({
  history: createMemoryHistory(),
  routes: [{ path: "/", component: { template: "<div />" } }],
});

const t = (key: string, named?: Record<string, unknown>) => (named ? i18n.global.t(key, named) : i18n.global.t(key));

let mounted: VueWrapper | null = null;

function mountWithAvatar(avatarUrl: string | null, collapsed: boolean) {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "u1", email: "denis@example.com", name: "Denis", avatar_url: avatarUrl } as never;
  useUiStore().sidebarCollapsed = collapsed;
  mounted = mount(AppSidebar, { attachTo: document.body, global: { plugins: [pinia, router, i18n] } });
  return mounted;
}

const ITEMS: ResearchHistoryItem[] = [
  { id: "r1", prompt: "Compare vector databases for a small team", title: null, depth: "medium", status: "completed" },
  { id: "r2", prompt: "Robotaxi market", title: "Robotaxi market 2026", depth: "easy", status: "processing" },
];

function mountWithHistory(items = ITEMS) {
  const wrapper = mountWithAvatar(null, false);
  useResearchStore().history = items.map((i) => ({ ...i }));
  return wrapper;
}

const buttonByLabel = (w: VueWrapper, label: string) => w.findAll(`button[aria-label="${label}"]`);

afterEach(() => {
  answerConfirm(false);
  mounted?.unmount();
  mounted = null;
  vi.clearAllMocks();
  vi.useRealTimers();
  localStorage.clear();
});

describe("AppSidebar avatar", () => {
  beforeEach(async () => {
    await router.push("/");
  });

  for (const collapsed of [false, true]) {
    it(`renders an emoji preset as text, not as <img src> (collapsed=${collapsed})`, () => {
      const wrapper = mountWithAvatar("🤖", collapsed);

      expect(wrapper.find('img[src="🤖"]').exists()).toBe(false);
      expect(wrapper.text()).toContain("🤖");
    });

    it(`keeps rendering a real photo URL as an image (collapsed=${collapsed})`, () => {
      const wrapper = mountWithAvatar("https://lh3.googleusercontent.com/a/photo", collapsed);

      expect(wrapper.find('img[src="https://lh3.googleusercontent.com/a/photo"]').exists()).toBe(true);
    });
  }
});

describe("AppSidebar logout", () => {
  beforeEach(async () => {
    await router.push("/");
  });

  it("names the logout button for what it does: every device is signed out", () => {
    i18n.global.locale.value = "en";
    const wrapper = mountWithAvatar(null, false);

    const button = wrapper.find(`button[aria-label="${i18n.global.t("auth.logoutEverywhere")}"]`);
    expect(button.exists()).toBe(true);
    expect(button.attributes("title")).toBe("Log out on all devices");
  });

  it("shows the account's email under the name instead of a plan it doesn't have", () => {
    i18n.global.locale.value = "en";
    const wrapper = mountWithAvatar(null, false);

    expect(wrapper.text()).toContain("denis@example.com");
    expect(wrapper.text()).not.toContain(t("sidebar.plan"));
  });
});

describe("AppSidebar recents", () => {
  beforeEach(async () => {
    i18n.global.locale.value = "en";
    await router.push("/");
  });

  it("names the research in the delete confirmation, and deletes only on yes", async () => {
    mocks.deleteResearch.mockResolvedValue(undefined);
    const wrapper = mountWithHistory();
    await flushPromises();

    await buttonByLabel(wrapper, t("sidebar.delete"))[0].trigger("click");
    expect(confirmState.open).toBe(true);
    expect(confirmState.danger).toBe(true);
    expect(confirmState.message).toBe(t("sidebar.confirmDeleteNamed", { title: "Compare vector databases for a…" }));

    answerConfirm(false);
    await flushPromises();
    expect(mocks.deleteResearch).not.toHaveBeenCalled();

    await buttonByLabel(wrapper, t("sidebar.delete"))[0].trigger("click");
    answerConfirm(true);
    await flushPromises();
    expect(mocks.deleteResearch).toHaveBeenCalledWith("r1");
    expect(wrapper.text()).not.toContain("Compare vector databases");
  });

  it("says under the row when a rename fails, keeps the old title, and clears it after a while", async () => {
    vi.useFakeTimers();
    mocks.renameResearch.mockRejectedValue(new ApiError(500, "boom"));
    const wrapper = mountWithHistory();
    await flushPromises();

    await buttonByLabel(wrapper, t("sidebar.rename"))[1].trigger("click");
    const input = wrapper.find(`input[aria-label="${t("sidebar.rename")}"]`);
    await input.setValue("Renamed");
    await input.trigger("keydown", { key: "Enter" });
    await flushPromises();

    expect(mocks.renameResearch).toHaveBeenCalledTimes(1);
    const alert = wrapper.find("[role='alert']");
    expect(alert.text()).toBe(t("errors.api.server"));
    expect(wrapper.text()).toContain("Robotaxi market 2026");
    expect(wrapper.text()).not.toContain("Renamed");

    vi.advanceTimersByTime(4000);
    await flushPromises();
    expect(wrapper.find("[role='alert']").exists()).toBe(false);
  });

  it("Escape leaves a rename without saving it", async () => {
    const wrapper = mountWithHistory();
    await flushPromises();

    await buttonByLabel(wrapper, t("sidebar.rename"))[0].trigger("click");
    const input = wrapper.find(`input[aria-label="${t("sidebar.rename")}"]`);
    await input.setValue("Something else");
    await input.trigger("keydown", { key: "Escape" });
    await input.trigger("blur");
    await flushPromises();

    expect(mocks.renameResearch).not.toHaveBeenCalled();
  });

  it("says nothing matches a search, rather than that there is no research", async () => {
    const wrapper = mountWithHistory();
    await flushPromises();

    await buttonByLabel(wrapper, t("sidebar.search"))[0].trigger("click");
    await wrapper.find("input[type='search']").setValue("zeppelin");

    expect(wrapper.text()).toContain(t("sidebar.noMatches", { q: "zeppelin" }));
    expect(wrapper.text()).not.toContain(t("sidebar.empty"));
  });

  it("explains a failed history load and retries it", async () => {
    mocks.listResearch.mockRejectedValueOnce(new ApiError(500, "boom")).mockResolvedValueOnce(ITEMS);
    const wrapper = mountWithAvatar(null, false);
    await useResearchStore().fetchHistory();
    await flushPromises();

    expect(wrapper.find("[role='alert']").text()).toBe(t("errors.api.server"));
    expect(wrapper.text()).not.toContain(t("sidebar.empty"));

    await wrapper.findAll("button").find((b) => b.text() === t("common.retry"))!.trigger("click");
    await flushPromises();
    expect(wrapper.find("[role='alert']").exists()).toBe(false);
    expect(wrapper.text()).toContain("Robotaxi market 2026");
  });
});

describe("AppSidebar resize handle", () => {
  beforeEach(async () => {
    i18n.global.locale.value = "en";
    await router.push("/");
  });

  function pointer(type: string, clientX: number) {
    const e = new MouseEvent(type, { bubbles: true, cancelable: true, clientX, button: 0 });
    Object.defineProperties(e, { pointerId: { value: 1 }, pointerType: { value: "mouse" }, isPrimary: { value: true } });
    return e;
  }

  it("is a keyboard-operable separator", async () => {
    const wrapper = mountWithAvatar(null, false);
    const handle = wrapper.find("[role='separator']");
    const ui = useUiStore();

    expect(handle.attributes("aria-label")).toBe(t("sidebar.resize"));
    expect(handle.attributes("aria-valuemin")).toBe(String(SIDEBAR_MIN));
    expect(handle.attributes("aria-valuemax")).toBe(String(SIDEBAR_MAX));
    expect(handle.attributes("tabindex")).toBe("0");

    await handle.trigger("keydown", { key: "ArrowRight" });
    expect(ui.sidebarWidth).toBe(SIDEBAR_DEFAULT + 16);
    expect(handle.attributes("aria-valuenow")).toBe(String(SIDEBAR_DEFAULT + 16));
    expect(localStorage.getItem("sidebar.width")).toBe(String(SIDEBAR_DEFAULT + 16));

    await handle.trigger("dblclick");
    expect(ui.sidebarWidth).toBe(SIDEBAR_DEFAULT);
  });

  it("resists past the maximum while dragging, stores the width once, on release", async () => {
    vi.useFakeTimers();
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    const wrapper = mountWithAvatar(null, false);
    const handle = wrapper.find("[role='separator']").element;
    const ui = useUiStore();

    handle.dispatchEvent(pointer("pointerdown", 300));
    handle.dispatchEvent(pointer("pointermove", 300 + (SIDEBAR_MAX - SIDEBAR_DEFAULT) + 100));
    vi.advanceTimersToNextFrame();

    expect(ui.sidebarWidth).toBeGreaterThan(SIDEBAR_MAX);
    expect(ui.sidebarWidth).toBeLessThan(SIDEBAR_MAX + 100);
    expect(setItem.mock.calls.filter(([k]) => k === "sidebar.width")).toHaveLength(0);
    expect(document.documentElement.style.cursor).toBe("col-resize");

    handle.dispatchEvent(pointer("pointerup", 300 + (SIDEBAR_MAX - SIDEBAR_DEFAULT) + 100));
    expect(ui.sidebarWidth).toBe(SIDEBAR_MAX);
    expect(setItem.mock.calls.filter(([k]) => k === "sidebar.width")).toEqual([["sidebar.width", String(SIDEBAR_MAX)]]);
    expect(document.documentElement.style.cursor).toBe("");
    setItem.mockRestore();
  });

  it("collapses to the rail when dragged far enough, keeping the width it had", async () => {
    vi.useFakeTimers();
    const wrapper = mountWithAvatar(null, false);
    const handle = wrapper.find("[role='separator']").element;
    const ui = useUiStore();

    handle.dispatchEvent(pointer("pointerdown", 300));
    handle.dispatchEvent(pointer("pointermove", 300 - (SIDEBAR_DEFAULT - 150)));
    vi.advanceTimersToNextFrame();
    expect(ui.sidebarWidth).toBeGreaterThan(SIDEBAR_MIN - 60);
    handle.dispatchEvent(pointer("pointerup", 300 - (SIDEBAR_DEFAULT - 150)));

    expect(ui.sidebarCollapsed).toBe(true);
    expect(ui.sidebarWidth).toBe(SIDEBAR_DEFAULT);
  });
});

describe("AppSidebar theme menu", () => {
  beforeEach(async () => {
    i18n.global.locale.value = "en";
    await router.push("/");
  });

  it("opens from its trigger, closes on Escape with focus back on the trigger", async () => {
    const wrapper = mountWithAvatar(null, false);
    const trigger = wrapper.find(`button[aria-haspopup="menu"]`);

    await trigger.trigger("click");
    await flushPromises();
    expect(trigger.attributes("aria-expanded")).toBe("true");
    expect(wrapper.find("[role='menu']").exists()).toBe(true);
    expect(document.activeElement?.getAttribute("role")).toBe("menuitemradio");

    document.activeElement!.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    await flushPromises();
    expect(wrapper.find("[role='menu']").exists()).toBe(false);
    expect(document.activeElement).toBe(trigger.element);
  });

  it("closes, then switches the theme, when a swatch is picked", async () => {
    const wrapper = mountWithAvatar(null, false);
    await wrapper.find(`button[aria-haspopup="menu"]`).trigger("click");
    await flushPromises();

    await wrapper.find(`[role='menuitemradio'][aria-label="${t("themes.dark")}"]`).trigger("click");
    await flushPromises();
    expect(useUiStore().theme).toBe("dark");
    expect(wrapper.find("[role='menu']").exists()).toBe(false);
  });
});
