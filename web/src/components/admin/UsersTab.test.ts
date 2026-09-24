// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";

const adminApi = vi.hoisted(() => ({
  getTelemetrySummary: vi.fn(),
  getUsers: vi.fn(),
  getPrompts: vi.fn(),
  getUserEvents: vi.fn(),
  getUserDetail: vi.fn(),
  deleteUser: vi.fn(),
  exportUsersCsv: vi.fn(),
  exportPromptsCsv: vi.fn(),
}));

// Real ApiError/apiErrorMessage; only adminApi's network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  adminApi,
}));

import { ApiError } from "@/lib/api";
import UsersTab from "./UsersTab.vue";

const t = (key: string, params?: Record<string, unknown>) => i18n.global.t(key, params ?? {});

const user = {
  id: "u-2", email: "other@example.com", name: "Other", is_admin: false, created_at: "2026-09-01T00:00:00Z",
  is_online: false, researches_count: 1, total_tokens: 10, total_cost_usd: 0.01,
};

async function mountTab() {
  setActivePinia(createPinia());
  useAuthStore().user = { id: "admin-1", email: "admin@example.com", name: "Admin", is_admin: true } as never;
  const wrapper = mount(UsersTab, { global: { plugins: [i18n] } });
  await flushPromises();
  return wrapper;
}

function button(wrapper: Awaited<ReturnType<typeof mountTab>>, text: string) {
  const found = wrapper.findAll("button").find((b) => b.text().includes(text));
  if (!found) throw new Error(`no button "${text}"`);
  return found;
}

describe("admin UsersTab", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    adminApi.getTelemetrySummary.mockResolvedValue({ total_users: 2, total_researches: 1, total_tokens: 10, total_cost_usd: 0.01 });
    adminApi.getUsers.mockResolvedValue({ users: [user], total_users: 1, online_users: 0, page: 1, page_size: 15 });
    adminApi.getPrompts.mockResolvedValue({
      prompts: [{
        id: "p-1", prompt_type: "research", prompt: "Someone else's topic", research_id: "r-other",
        user_email: "other@example.com", total_tokens: 10, cost_usd: 0.01, created_at: "2026-09-02T00:00:00Z",
      }],
      total_count: 1, page: 1, page_size: 15,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("does not link to other users' research (owner-scoped routes would 404)", async () => {
    const wrapper = await mountTab();
    await button(wrapper, t("admin.users.tabPrompts")).trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("Someone else's topic");
    expect(wrapper.find('a[href^="/research/"]').exists()).toBe(false);
  });

  it("reads the live feed's total_count and shows load errors", async () => {
    adminApi.getUserEvents.mockResolvedValueOnce({ events: [], total_count: 7, page: 1, page_size: 40 });
    const wrapper = await mountTab();
    await button(wrapper, t("admin.users.tabLiveFeed")).trigger("click");
    await flushPromises();

    expect(adminApi.getUserEvents).toHaveBeenCalledWith(40, 0, undefined, undefined, undefined);
    expect(wrapper.text()).toContain(t("admin.users.totalStreamEvents", { count: 7 }));

    adminApi.getUserEvents.mockRejectedValueOnce(new ApiError(500, "boom"));
    await button(wrapper, t("admin.users.catSystem")).trigger("click");
    await flushPromises();

    expect(adminApi.getUserEvents).toHaveBeenLastCalledWith(40, 0, undefined, undefined, "system");
    expect(wrapper.text()).toContain(t("errors.api.server"));
  });

  it("shows CSV export and delete failures inline instead of alert()", async () => {
    const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
    vi.spyOn(window, "confirm").mockReturnValue(true);
    adminApi.exportUsersCsv.mockRejectedValue(new ApiError(403, "Admin privileges required"));
    adminApi.deleteUser.mockRejectedValue(new ApiError(429, "slow down"));
    const wrapper = await mountTab();

    await button(wrapper, t("admin.users.exportCsv")).trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("errors.api.forbidden"));

    await wrapper.find(`button[title="${t("admin.users.deleteUser")}"]`).trigger("click");
    await flushPromises();
    expect(adminApi.deleteUser).toHaveBeenCalledWith("u-2");
    expect(wrapper.text()).toContain(t("errors.api.rateLimited"));
    expect(wrapper.text()).not.toContain("slow down");
    expect(alertSpy).not.toHaveBeenCalled();
  });
});
