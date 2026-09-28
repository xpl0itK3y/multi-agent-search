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
