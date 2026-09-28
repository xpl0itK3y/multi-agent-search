// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { defineComponent, onMounted, ref } from "vue";
import { createMemoryHistory, createRouter, useRoute, useRouter } from "vue-router";

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, listModels: vi.fn(async () => []), listResearch: vi.fn(async () => []) } };
});

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import { useUiStore } from "@/stores/ui";
import App from "./App.vue";

const View = { template: "<div data-test='view'>view</div>" };

// A view that keeps its open tab in the query, as Settings and Admin do; it counts its mounts.
const mounts = { tabbed: 0, thread: 0 };
const TabbedView = defineComponent({
  setup() {
    const route = useRoute();
    const router = useRouter();
    const draft = ref("");
    onMounted(() => mounts.tabbed++);
    const open = (tab: string) => router.replace({ query: { ...route.query, tab } });
    return { route, draft, open };
  },
  template: `<div>
    <input v-model="draft" data-test="draft" />
    <button type="button" data-test="tab-b" @click="open('b')">b</button>
    <p data-test="tab">{{ route.query.tab ?? "a" }}</p>
  </div>`,
});
const ThreadStub = defineComponent({
  setup: () => onMounted(() => mounts.thread++),
  template: "<div data-test='thread'>thread</div>",
});

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
      { path: "/tabbed", name: "tabbed", component: TabbedView },
      { path: "/thread/:threadId", name: "thread", component: ThreadStub },
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

  it("slides the drawer on transform only; its shadow fades on a pseudo-element, never animates", async () => {
    const { wrapper } = await mountApp("/");
    const classes = () => drawer().className.split(/\s+/);
    expect(drawer().className).not.toMatch(/box-shadow/);
    expect(classes()).toContain("transition-transform");
    expect(classes()).toContain("after:shadow-e3");
    expect(classes()).not.toContain("shadow-e3"); // on the drawer itself, it would slide in with it
    expect(classes()).toContain("after:opacity-0"); // closed: no shadow at the screen's edge

    await menuButton(wrapper).trigger("click");
    await flushPromises();
    expect(classes()).toContain("after:opacity-100");
    expect(classes()).toContain("after:shadow-e3");
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

  it("peels one layer per Escape: a rename or search in the drawer ends, the drawer stays", async () => {
    const { wrapper, ui } = await mountApp("/");
    const { useResearchStore } = await import("@/stores/research");
    useResearchStore().history = [{ id: "r1", prompt: "Vector databases", depth: "easy", status: "completed" }];
    await menuButton(wrapper).trigger("click");
    await flushPromises();
    const escape = (el: Element) => el.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    const t = (key: string) => i18n.global.t(key);

    await wrapper.find(`button[aria-label="${t("sidebar.rename")}"]`).trigger("click");
    escape(wrapper.find(`input[aria-label="${t("sidebar.rename")}"]`).element);
    await flushPromises();
    expect(wrapper.find(`input[aria-label="${t("sidebar.rename")}"]`).exists()).toBe(false);
    expect(ui.mobileOpen).toBe(true);
    expect(drawer().contains(document.activeElement)).toBe(true);

    await wrapper.find(`button[aria-label="${t("sidebar.search")}"]`).trigger("click");
    await flushPromises();
    escape(wrapper.find("input[type='search']").element);
    await flushPromises();
    expect(wrapper.find("input[type='search']").exists()).toBe(false);
    expect(ui.mobileOpen).toBe(true);

    // The next Escape is the drawer's.
    escape(document.activeElement!);
    await flushPromises();
    expect(ui.mobileOpen).toBe(false);
  });

  it("keeps the mounted view, its focus and its input across a query-only tab switch", async () => {
    mounts.tabbed = 0;
    const { wrapper, router } = await mountApp("/tabbed");
    await wrapper.find("[data-test='draft']").setValue("unsaved");
    const tab = wrapper.find<HTMLButtonElement>("[data-test='tab-b']");
    tab.element.focus();

    await tab.trigger("click");
    await flushPromises();

    expect(router.currentRoute.value.fullPath).toBe("/tabbed?tab=b");
    expect(wrapper.find("[data-test='tab']").text()).toBe("b");
    expect(mounts.tabbed).toBe(1);
    expect(tab.element.isConnected).toBe(true);
    expect(document.activeElement).toBe(tab.element);
    expect((wrapper.find("[data-test='draft']").element as HTMLInputElement).value).toBe("unsaved");
  });

  it("still mounts a new view for a new path, such as another thread", async () => {
    mounts.thread = 0;
    const { router } = await mountApp("/thread/a");
    await router.push("/thread/b");
    await flushPromises();

    expect(mounts.thread).toBe(2);
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
