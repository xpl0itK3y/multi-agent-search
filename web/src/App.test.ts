// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, listModels: vi.fn(async () => []), listResearch: vi.fn(async () => []) } };
});

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import { useUiStore } from "@/stores/ui";
import App from "./App.vue";

const View = { template: "<div data-test='view'>view</div>" };

let wrapper: VueWrapper | null = null;

async function mountApp(url: string, { signedIn = true } = {}) {
  const pinia = createPinia();
  setActivePinia(pinia);
  if (signedIn) useAuthStore().user = { id: "u1", email: "denis@example.com", name: "Denis" } as never;
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", name: "home", component: View },
      { path: "/other", name: "other", component: View },
      { path: "/login", name: "login", component: View, meta: { bare: true } },
      { path: "/r/:token", name: "public-report", component: View, meta: { public: true } },
    ],
  });
  await router.push(url);
  wrapper = mount(App, { attachTo: document.body, global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return { wrapper, router, ui: useUiStore() };
}

const drawer = () => document.getElementById("app-drawer")!;
const menuButton = (w: VueWrapper) => w.find(`button[aria-label="${i18n.global.t("common.menu")}"]`);

describe("App shell", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
  });
  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
  });

  it("draws a shared report without the app shell for a signed-out reader only", async () => {
    const out = await mountApp("/r/tok", { signedIn: false });
    expect(out.wrapper.find("aside").exists()).toBe(false);
    expect(menuButton(out.wrapper).exists()).toBe(false);
    out.wrapper.unmount();

    const signedIn = await mountApp("/r/tok");
    expect(signedIn.wrapper.find("aside").exists()).toBe(true);
    expect(signedIn.wrapper.find("[data-test='view']").exists()).toBe(true);
  });

  it("keeps the closed drawer out of the tab order and the accessibility tree", async () => {
    const { wrapper } = await mountApp("/");
    expect(drawer().hasAttribute("inert")).toBe(true);
    expect(drawer().getAttribute("aria-hidden")).toBe("true");
    expect(menuButton(wrapper).attributes("aria-expanded")).toBe("false");
  });

  it("opens from ☰, takes focus, and makes the page behind it inert", async () => {
    const { wrapper, ui } = await mountApp("/");
    await menuButton(wrapper).trigger("click");
    await flushPromises();

    expect(ui.mobileOpen).toBe(true);
    expect(drawer().hasAttribute("inert")).toBe(false);
    expect(drawer().getAttribute("aria-hidden")).toBeNull();
    expect(drawer().getAttribute("role")).toBe("dialog");
    expect(menuButton(wrapper).attributes("aria-expanded")).toBe("true");
    expect(wrapper.find("main").attributes("inert")).toBeDefined();
    expect(drawer().contains(document.activeElement)).toBe(true);
  });

  it("closes on Escape and gives focus back to ☰", async () => {
    const { wrapper, ui } = await mountApp("/");
    await menuButton(wrapper).trigger("click");
    await flushPromises();

    document.activeElement!.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await flushPromises();

    expect(ui.mobileOpen).toBe(false);
    expect(drawer().hasAttribute("inert")).toBe(true);
    expect(wrapper.find("main").attributes("inert")).toBeUndefined();
    expect(document.activeElement).toBe(menuButton(wrapper).element);
  });

  it("keeps a child's lost implicit capture from cancelling the drawer's drag", async () => {
    const { wrapper } = await mountApp("/");
    await menuButton(wrapper).trigger("click");
    await flushPromises();
    const seen: EventTarget[] = [];
    drawer().addEventListener("lostpointercapture", (e) => seen.push(e.target!));

    // A touch starts on a row: the row holds the implicit capture until the drawer takes it.
    drawer().querySelector("button")!.dispatchEvent(new Event("lostpointercapture", { bubbles: true }));
    expect(seen).toEqual([]);

    drawer().dispatchEvent(new Event("lostpointercapture", { bubbles: true }));
    expect(seen).toEqual([drawer()]);
  });

  it("closes on a scrim tap and on navigation", async () => {
    const { wrapper, router, ui } = await mountApp("/");
    await menuButton(wrapper).trigger("click");
    await wrapper.find(".scrim").trigger("click");
    expect(ui.mobileOpen).toBe(false);

    await menuButton(wrapper).trigger("click");
    await router.push("/other");
    await flushPromises();
    expect(ui.mobileOpen).toBe(false);
  });
});

describe("App history loading", () => {
  afterEach(() => {
    wrapper?.unmount();
    wrapper = null;
  });

  it("loads the history only for a signed-in user, again after a sign-in, and drops it on sign-out", async () => {
    const { api } = await import("@/lib/api");
    const listResearch = vi.mocked(api.listResearch);
    listResearch.mockClear();

    await mountApp("/login", { signedIn: false });
    expect(listResearch).not.toHaveBeenCalled();

    const auth = useAuthStore();
    auth.user = { id: "u2", email: "anna@example.com" } as never;
    await flushPromises();
    expect(listResearch).toHaveBeenCalledTimes(1);

    const { useResearchStore } = await import("@/stores/research");
    useResearchStore().history = [{ id: "r1", prompt: "p", depth: "easy", status: "completed" }];
    auth.user = null;
    await flushPromises();
    expect(useResearchStore().history).toEqual([]);
  });
});
