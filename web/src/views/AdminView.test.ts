// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";
import { defineComponent, h } from "vue";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import type { AdminOverviewResponse } from "@/lib/types";

const adminApi = vi.hoisted(() => ({
  getOverview: vi.fn(),
  connectStream: vi.fn((_onData: (d: unknown) => void, _onError?: () => void) => () => {}),
}));

// Real ApiError/apiErrorMessage; only adminApi's network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  adminApi,
}));

import { ApiError } from "@/lib/api";
import AdminView from "./AdminView.vue";
import OverviewTab from "@/components/admin/OverviewTab.vue";

const t = (key: string) => i18n.global.t(key);

function snapshot(overall: string, failed = 0): AdminOverviewResponse {
  return {
    system_health: { overall, postgres: "ok" },
    active_researches_count: 0,
    pending_tasks_count: 0,
    failed_tasks_count: failed,
    workers: [],
    is_dev_mode: false,
  };
}

// The tabs are stubbed: these tests are about the view's header and tab viewport.
const stub = (name: string) => defineComponent({ name, render: () => h("div", { "data-test": name }) });
let overviewFromTab: AdminOverviewResponse | null = null;
const OverviewTabStub = defineComponent({
  name: "OverviewTab",
  emits: ["update"],
  mounted() {
    if (overviewFromTab) this.$emit("update", overviewFromTab);
  },
  render: () => h("div", { "data-test": "OverviewTab" }),
});

let currentRouter: ReturnType<typeof createRouter> | null = null;

async function mountAdmin(url = "/admin") {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "a1", email: "admin@example.com", name: "Admin", is_admin: true } as never;
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/admin", component: AdminView },
      { path: "/login", component: { render: () => null } },
    ],
  });
  await router.push(url);
  currentRouter = router;
  const wrapper = mount(AdminView, {
    global: {
      plugins: [pinia, router, i18n],
      stubs: {
        OverviewTab: OverviewTabStub,
        UsersTab: stub("UsersTab"),
        AnalyticsTab: stub("AnalyticsTab"),
        AgentsGraphTab: stub("AgentsGraphTab"),
        OperationsTab: stub("OperationsTab"),
      },
    },
  });
  await flushPromises();
  return wrapper;
}

async function openTab(wrapper: Awaited<ReturnType<typeof mountAdmin>>, key: string) {
  const tab = wrapper.findAll("button").find((b) => b.text() === t(`admin.tabs.${key}`));
  if (!tab) throw new Error(`no tab ${key}`);
  await tab.trigger("click");
  await flushPromises();
}

describe("AdminView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    overviewFromTab = null;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("reports a degraded system as degraded, not as operational", async () => {
    adminApi.getOverview.mockResolvedValue(snapshot("degraded"));
    const wrapper = await mountAdmin();
    await openTab(wrapper, "users");

    const status = wrapper.find('[data-test="admin-health"]');
    expect(status.text()).toBe(t("admin.systemDegraded"));
    expect(status.classes()).toContain("text-warning");
    expect(wrapper.text()).not.toContain(t("admin.allSystemsOperational"));
  });

  it("counts dead-letter tasks against a healthy database", async () => {
    adminApi.getOverview.mockResolvedValue(snapshot("healthy", 3));
    const wrapper = await mountAdmin();
    await openTab(wrapper, "users");

    expect(wrapper.find('[data-test="admin-health"]').text()).toBe(t("admin.systemDegraded"));
  });

  it("shows no status until the health is known", async () => {
    const wrapper = await mountAdmin();
    expect(wrapper.find('[data-test="OverviewTab"]').exists()).toBe(true);
    expect(wrapper.find('[data-test="admin-health"]').exists()).toBe(false);
  });

  it("follows the Overview tab's own snapshot instead of fetching a second one", async () => {
    overviewFromTab = snapshot("healthy");
    const wrapper = await mountAdmin();

    expect(adminApi.getOverview).not.toHaveBeenCalled();
    expect(wrapper.find('[data-test="admin-health"]').text()).toBe(t("admin.allSystemsOperational"));
  });

  it("opens the tab named in the address, and writes the chosen tab back to it", async () => {
    adminApi.getOverview.mockResolvedValue(snapshot("healthy"));
    const wrapper = await mountAdmin("/admin?tab=users");
    expect(wrapper.find('[data-test="UsersTab"]').exists()).toBe(true);
    const selected = wrapper.find('[role="tab"][aria-selected="true"]');
    expect(selected.text()).toBe(t("admin.tabs.users"));

    await openTab(wrapper, "operations");
    expect(currentRouter!.currentRoute.value.query.tab).toBe("operations");
    expect(wrapper.find('[data-test="OperationsTab"]').exists()).toBe(true);

    await openTab(wrapper, "overview");
    expect(currentRouter!.currentRoute.value.query.tab).toBeUndefined();
  });

  // jsdom has no layout, so the two CSS facts that keep the strip usable are pinned here.
  it("never lets the page squeeze the tab strip, and keeps the focus ring inside it", async () => {
    const wrapper = await mountAdmin();
    // The strip scrolls sideways: without shrink-0 its min-height is 0 in the flex column.
    expect(wrapper.find('[data-test="admin-tabbar"]').classes()).toContain("shrink-0");
    // The strip clips vertically too, so the ring is drawn inside each tab.
    for (const tab of wrapper.findAll('[role="tab"]')) {
      expect(tab.classes()).toContain("focus-visible:!outline-offset-[-2px]");
    }
  });

  it("opens the next tab at its start after scrolling far down one, and moves no further", async () => {
    adminApi.getOverview.mockResolvedValue(snapshot("healthy"));
    const wrapper = await mountAdmin("/admin?tab=users");
    const root = wrapper.element as HTMLElement;
    let top = 1500;
    Object.defineProperty(root, "scrollTop", { configurable: true, get: () => top, set: (v: number) => (top = v) });
    // The bar is stuck at the top (bottom 44px); the panel started 900px above the view.
    let panelTop = -900;
    const spy = vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      if (this.getAttribute("data-test") === "admin-tabbar") return { top: 0, bottom: 44 } as DOMRect;
      if (this.id === "admin-tabpanel") return { top: panelTop, bottom: panelTop + 400 } as DOMRect;
      return { top: 0, bottom: 0, left: 0, right: 0, width: 0 } as DOMRect;
    });
    try {
      await openTab(wrapper, "analytics");
      // Back up by 900 + 44 + the 24px gap: the panel now starts just under the bar.
      expect(top).toBe(1500 - (900 + 44 + 24));

      panelTop = 68; // already starts under the bar
      await openTab(wrapper, "operations");
      expect(top).toBe(1500 - (900 + 44 + 24));
    } finally {
      spy.mockRestore();
    }
  });

  describe("on a phone, where the strip scrolls sideways", () => {
    // jsdom has no layout: a 390px strip whose five 160px tabs run to 848px.
    const undo: (() => void)[] = [];
    const scrollBy = vi.fn();
    function define(proto: object, key: string, desc: PropertyDescriptor) {
      const own = Object.getOwnPropertyDescriptor(proto, key);
      Object.defineProperty(proto, key, { configurable: true, ...desc });
      undo.push(() => (own ? Object.defineProperty(proto, key, own) : delete (proto as Record<string, unknown>)[key]));
    }
    const isStrip = (el: Element) => el.getAttribute("role") === "tablist";
    beforeEach(() => {
      const rect = (left: number, width: number) => ({ left, right: left + width, width, top: 0, bottom: 40, height: 40 }) as DOMRect;
      define(Element.prototype, "getBoundingClientRect", {
        value(this: Element) {
          if (isStrip(this)) return rect(0, 390);
          const i = this.getAttribute("role") === "tab" ? [...this.parentElement!.children].indexOf(this) : -1;
          return i >= 0 ? rect(24 + i * 160, 160) : rect(0, 0);
        },
      });
      define(HTMLElement.prototype, "scrollWidth", { get(this: Element) { return isStrip(this) ? 848 : 0; } });
      define(HTMLElement.prototype, "clientWidth", { get(this: Element) { return isStrip(this) ? 390 : 0; } });
      define(HTMLElement.prototype, "scrollBy", { value: scrollBy });
    });
    afterEach(() => {
      while (undo.length) undo.pop()!();
      scrollBy.mockReset();
    });

    it("shows a fade while more tabs wait, and brings a tab opened by the address into view", async () => {
      adminApi.getOverview.mockResolvedValue(snapshot("healthy"));
      const wrapper = await mountAdmin("/admin?tab=operations");

      expect(wrapper.find('[role="tablist"]').classes()).toContain("edge-fade-x");
      // Operations ends at 824px: moved clear of the 28px fade, at once on arrival.
      expect(scrollBy).toHaveBeenCalledWith({ left: 824 - 390 + 28, behavior: "auto" });
    });

    it("follows Back and Forward to a tab off the strip's left edge", async () => {
      adminApi.getOverview.mockResolvedValue(snapshot("healthy"));
      await mountAdmin("/admin?tab=operations");
      scrollBy.mockClear();
      // As if the strip had been scrolled to its end: Users starts 300px left of it.
      define(Element.prototype, "getBoundingClientRect", {
        value(this: Element) {
          if (isStrip(this)) return { left: 0, right: 390, width: 390 } as DOMRect;
          return this.id === "admin-tab-users" ? ({ left: -300, right: -140, width: 160 } as DOMRect) : ({ left: 0, right: 0, width: 0 } as DOMRect);
        },
      });

      await currentRouter!.replace({ query: { tab: "users" } });
      await flushPromises();

      expect(scrollBy).toHaveBeenCalledWith({ left: -324, behavior: "smooth" });
    });
  });

  it("ignores an unknown tab in the address", async () => {
    const wrapper = await mountAdmin("/admin?tab=nope");
    expect(wrapper.find('[data-test="OverviewTab"]').exists()).toBe(true);
  });

  it("moves between tabs with the arrow keys", async () => {
    adminApi.getOverview.mockResolvedValue(snapshot("healthy"));
    const wrapper = await mountAdmin();
    const list = wrapper.find('[role="tablist"]');
    await list.trigger("keydown", { key: "ArrowRight" });
    await flushPromises();
    expect(wrapper.find('[data-test="UsersTab"]').exists()).toBe(true);
    await list.trigger("keydown", { key: "End" });
    await flushPromises();
    expect(wrapper.find('[data-test="OperationsTab"]').exists()).toBe(true);
    await list.trigger("keydown", { key: "ArrowRight" });
    await flushPromises();
    expect(wrapper.find('[data-test="OverviewTab"]').exists()).toBe(true);
    // Only the selected tab is in the Tab order.
    expect(wrapper.findAll('[role="tab"][tabindex="0"]')).toHaveLength(1);
  });

  it("keeps the other tabs usable when the overview request fails", async () => {
    adminApi.getOverview.mockRejectedValue(new ApiError(500, "boom"));
    const wrapper = await mountAdmin();
    await openTab(wrapper, "users");

    expect(adminApi.getOverview).toHaveBeenCalled();
    expect(wrapper.find('[data-test="UsersTab"]').exists()).toBe(true);
    expect(wrapper.find('[data-test="admin-health"]').exists()).toBe(false);
    expect(wrapper.text()).not.toContain("boom");
  });
});

// The Overview tab itself (mounted directly): its own loading, error and stream states.
describe("admin OverviewTab", () => {
  let streamHandlers: { onData: (d: AdminOverviewResponse) => void; onError: () => void } | null = null;

  beforeEach(() => {
    i18n.global.locale.value = "en";
    adminApi.connectStream.mockImplementation((onData, onError) => {
      streamHandlers = { onData: onData as (d: AdminOverviewResponse) => void, onError: onError ?? (() => {}) };
      return () => {};
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    streamHandlers = null;
  });

  async function mountOverview() {
    const wrapper = mount(OverviewTab, { props: { initialOverview: null }, global: { plugins: [i18n] } });
    await flushPromises();
    return wrapper;
  }

  it("shows a failed load inline with a retry, then the data", async () => {
    adminApi.getOverview.mockRejectedValueOnce(new ApiError(500, "boom"));
    const wrapper = await mountOverview();

    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.server"));
    expect(wrapper.text()).not.toContain(t("admin.overview.systemDegradedTitle"));

    adminApi.getOverview.mockResolvedValueOnce(snapshot("healthy"));
    await wrapper.findAll("button").find((b) => b.text() === t("common.retry"))!.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain(t("admin.overview.systemHealthy"));
    expect(wrapper.emitted("update")?.at(-1)).toEqual([snapshot("healthy")]);
  });

  it("says it is reconnecting when the live stream drops", async () => {
    adminApi.getOverview.mockResolvedValueOnce(snapshot("healthy"));
    const wrapper = await mountOverview();
    expect(wrapper.find('[data-test="overview-stream"]').text()).toBe(t("admin.overview.liveSse"));

    streamHandlers!.onError();
    await flushPromises();
    expect(wrapper.find('[data-test="overview-stream"]').text()).toBe(t("admin.overview.reconnecting"));

    streamHandlers!.onData(snapshot("degraded"));
    await flushPromises();
    expect(wrapper.find('[data-test="overview-stream"]').text()).toBe(t("admin.overview.liveSse"));
    expect(wrapper.text()).toContain(t("admin.overview.systemDegradedTitle"));
  });
});
