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

// The app's styled confirmation (ConfirmDialog), answered "yes" unless a test says otherwise.
const confirmMock = vi.hoisted(() => ({ confirm: vi.fn(), confirmState: { open: false } }));
vi.mock("@/lib/confirm", () => confirmMock);

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
  // The user drawer is teleported to <body>; render it in place so the wrapper sees it.
  const wrapper = mount(UsersTab, { global: { plugins: [i18n], stubs: { teleport: true } } });
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
    confirmMock.confirm.mockResolvedValue(true);
    confirmMock.confirmState.open = false;
    adminApi.getTelemetrySummary.mockResolvedValue({ total_users: 2, total_researches: 1, total_tokens: 10, total_cost_usd: 0.01 });
    adminApi.getUsers.mockResolvedValue({ users: [user], total_users: 1, online_users: 0, page: 1, page_size: 15 });
    adminApi.getUserDetail.mockResolvedValue({ user, sessions: [], researches: [], recent_events: [] });
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
    vi.useRealTimers();
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

  it("filters the live feed only by categories the server writes", async () => {
    // "prompt" (src/api/app.py prompt copies) plus CLIENT_TELEMETRY_CATEGORIES
    // (src/domain/models.py); the server matches the category exactly.
    const serverCategories = new Set(["prompt", "general", "system", "ui"]);
    adminApi.getUserEvents.mockResolvedValue({ events: [], total_count: 0, page: 1, page_size: 40 });
    const wrapper = await mountTab();
    await button(wrapper, t("admin.users.tabLiveFeed")).trigger("click");
    await flushPromises();

    for (const key of ["catUi", "catPrompts", "catSystem"]) {
      const chip = wrapper.findAll("button").find((el) => el.text() === t(`admin.users.${key}`));
      await chip!.trigger("click");
      await flushPromises();
    }

    const sent = adminApi.getUserEvents.mock.calls.map((call) => call[4]).filter((c) => c !== undefined);
    expect(sent).toEqual(["ui", "prompt", "system"]);
    expect(sent.filter((c) => !serverCategories.has(c))).toEqual([]);
  });

  it("shows CSV export and delete failures inline instead of alert()", async () => {
    const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
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

  it("asks in the styled dialog before deleting, then says it is done", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const nativeConfirm = vi.spyOn(window, "confirm");
    adminApi.deleteUser.mockResolvedValue({ status: "deleted", deleted_user_id: "u-2" });
    const wrapper = await mountTab();

    await wrapper.find(`button[title="${t("admin.users.deleteUser")}"]`).trigger("click");
    await flushPromises();

    expect(nativeConfirm).not.toHaveBeenCalled();
    expect(confirmMock.confirm).toHaveBeenCalledWith(
      expect.objectContaining({ danger: true, message: t("admin.users.deleteConfirm", { email: "other@example.com" }) }),
    );
    const notice = wrapper.find('[data-test="users-notice"]');
    expect(notice.attributes("role")).toBe("status");
    expect(notice.text()).toBe(`${t("admin.users.userDeleted")}: other@example.com`);

    vi.advanceTimersByTime(5000);
    await flushPromises();
    expect(wrapper.find('[data-test="users-notice"]').exists()).toBe(false);
  });

  it("deletes nothing when the confirmation is declined", async () => {
    confirmMock.confirm.mockResolvedValue(false);
    const wrapper = await mountTab();

    await wrapper.find(`button[title="${t("admin.users.deleteUser")}"]`).trigger("click");
    await flushPromises();
    expect(adminApi.deleteUser).not.toHaveBeenCalled();
  });

  it("keeps Delete out of the drawer header, in the profile's danger zone", async () => {
    adminApi.deleteUser.mockResolvedValue({ status: "deleted", deleted_user_id: "u-2" });
    const wrapper = await mountTab();

    await button(wrapper, t("admin.users.inspect")).trigger("click");
    await flushPromises();

    const drawer = wrapper.find('[data-test="user-drawer"]');
    expect(drawer.attributes("aria-hidden")).toBeUndefined();
    const zone = drawer.find('[data-test="danger-zone"]');
    expect(zone.text()).toContain(t("admin.users.dangerZone"));
    // The only delete control in the drawer is the one in the danger zone.
    const deletes = drawer.findAll("button").filter((b) => b.text().includes(t("admin.users.deleteUser")));
    expect(deletes).toHaveLength(1);
    expect(zone.element.contains(deletes[0].element)).toBe(true);

    await deletes[0].trigger("click");
    await flushPromises();
    expect(adminApi.deleteUser).toHaveBeenCalledWith("u-2");
    // The deleted user's sheet closes.
    expect(wrapper.find('[data-test="user-drawer"]').attributes("aria-hidden")).toBe("true");
  });

  it("holds the auto-refresh while the pointer is over the table or a user is open", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    const wrapper = await mountTab();
    const loads = () => adminApi.getUsers.mock.calls.length;
    const before = loads();

    const table = wrapper.find('[data-test="users-table"]');
    await table.trigger("pointerenter");
    vi.advanceTimersByTime(18000);
    await flushPromises();
    expect(loads()).toBe(before);

    await table.trigger("pointerleave");
    vi.advanceTimersByTime(6000);
    await flushPromises();
    expect(loads()).toBe(before + 1);

    await button(wrapper, t("admin.users.inspect")).trigger("click");
    await flushPromises();
    vi.advanceTimersByTime(12000);
    await flushPromises();
    expect(loads()).toBe(before + 1);
  });

  it("searches at once and lets only the newest query's answer fill the list", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const wrapper = await mountTab();
    const deferred = () => {
      let resolve!: (v: unknown) => void;
      const promise = new Promise((r) => (resolve = r));
      return { promise, resolve };
    };
    const older = deferred();
    const newer = deferred();
    const listOf = (email: string) => ({
      users: [{ ...user, id: email, email, name: email }], total_users: 1, online_users: 0, page: 1, page_size: 15,
    });

    const input = wrapper.find('input[type="text"]');
    adminApi.getUsers.mockReturnValueOnce(older.promise);
    await input.setValue("an");
    // Feedback on the keystroke, before any request goes out.
    expect(wrapper.find('[data-test="users-searching"]').exists()).toBe(true);
    vi.advanceTimersByTime(200);
    expect(adminApi.getUsers).toHaveBeenLastCalledWith(1, 15, "an", undefined, false, "activity");

    adminApi.getUsers.mockReturnValueOnce(newer.promise);
    await input.setValue("anna");
    vi.advanceTimersByTime(200);
    expect(adminApi.getUsers).toHaveBeenLastCalledWith(1, 15, "anna", undefined, false, "activity");

    newer.resolve(listOf("anna@example.com"));
    await flushPromises();
    older.resolve(listOf("an@example.com"));
    await flushPromises();

    expect(wrapper.text()).toContain("anna@example.com");
    expect(wrapper.text()).not.toContain("an@example.com");
    expect(wrapper.find('[data-test="users-searching"]').exists()).toBe(false);
  });

  it("holds the auto-refresh while a confirmation is pending", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    const wrapper = await mountTab();
    const before = adminApi.getUsers.mock.calls.length;

    confirmMock.confirmState.open = true;
    vi.advanceTimersByTime(12000);
    await flushPromises();
    expect(adminApi.getUsers.mock.calls.length).toBe(before);
    wrapper.unmount();
  });
});
