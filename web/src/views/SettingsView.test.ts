// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";

const mocks = vi.hoisted(() => ({
  getTokenStats: vi.fn(),
  setPassword: vi.fn(),
  deleteAccount: vi.fn(),
  logout: vi.fn(),
}));
const startGoogleSignIn = vi.hoisted(() => vi.fn());

// Real ApiError/apiErrorMessage; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});
vi.mock("@/lib/googleSignIn", () => ({ startGoogleSignIn }));

import { ApiError } from "@/lib/api";
import SettingsView from "./SettingsView.vue";

const t = (key: string) => i18n.global.t(key);

async function mountSettings(url = "/settings") {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "u1", email: "denis@example.com", name: "Denis" } as never;
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/settings", component: SettingsView },
      { path: "/login", component: { render: () => null } },
    ],
  });
  await router.push(url);
  const wrapper = mount(SettingsView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return wrapper;
}

async function openTab(wrapper: Awaited<ReturnType<typeof mountSettings>>, labelKey: string) {
  const tab = wrapper.findAll("nav button").find((b) => b.text() === t(labelKey));
  await tab!.trigger("click");
}

describe("SettingsView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    mocks.getTokenStats.mockResolvedValue({
      researches_count: 2, calls_count: 1, total_tokens: 10, prompt_tokens: 6, completion_tokens: 4,
      estimated_cost_usd: 0.01, by_model: [], recent: [],
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("shows 'current password is incorrect' for a 401 instead of a raw error", async () => {
    mocks.setPassword.mockRejectedValue(new ApiError(401, "Current password is incorrect"));
    const wrapper = await mountSettings();
    await openTab(wrapper, "settings.tabs.security");

    const inputs = wrapper.findAll('input[type="password"]');
    await inputs[0].setValue("wrong-old");
    await inputs[1].setValue("new-password");
    await inputs[2].setValue("new-password");
    await wrapper.findAll("button").find((b) => b.text() === t("settings.security.updatePassword"))!.trigger("click");
    await flushPromises();

    expect(mocks.setPassword).toHaveBeenCalledWith("new-password", "wrong-old");
    expect(wrapper.text()).toContain(t("settings.errors.wrongCurrentPassword"));
    expect(wrapper.text()).not.toContain("401");
  });

  it("offers a Google sign-in back to the security tab when the server wants one", async () => {
    mocks.setPassword.mockRejectedValue(new ApiError(403, "reauth_required: sign in with Google again"));
    const wrapper = await mountSettings();
    await openTab(wrapper, "settings.tabs.security");

    // No current password: a first password, or a reset on a Google-linked account.
    const inputs = wrapper.findAll('input[type="password"]');
    await inputs[1].setValue("new-password");
    await inputs[2].setValue("new-password");
    await wrapper.findAll("button").find((b) => b.text() === t("settings.security.updatePassword"))!.trigger("click");
    await flushPromises();

    expect(mocks.setPassword).toHaveBeenCalledWith("new-password", undefined);
    expect(wrapper.text()).toContain(t("errors.api.reauthRequired"));
    expect(wrapper.text()).not.toContain("reauth_required");
    expect(wrapper.text()).not.toContain(t("errors.api.forbidden"));

    await wrapper.findAll("button").find((b) => b.text() === t("auth.reauthGoogle"))!.trigger("click");
    expect(startGoogleSignIn).toHaveBeenCalledWith("/settings?tab=security");
  });

  it("keeps the generic forbidden text for other 403s", async () => {
    mocks.setPassword.mockRejectedValue(new ApiError(403, "Forbidden"));
    const wrapper = await mountSettings("/settings?tab=security");

    const inputs = wrapper.findAll('input[type="password"]');
    await inputs[1].setValue("new-password");
    await inputs[2].setValue("new-password");
    await wrapper.findAll("button").find((b) => b.text() === t("settings.security.updatePassword"))!.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain(t("errors.api.forbidden"));
    expect(wrapper.text()).not.toContain(t("auth.reauthGoogle"));
  });

  function buttonByText(wrapper: Awaited<ReturnType<typeof mountSettings>>, key: string) {
    const found = wrapper.findAll("button").find((b) => b.text() === t(key));
    if (!found) throw new Error(`no button "${key}"`);
    return found;
  }

  async function confirmDeletion(wrapper: Awaited<ReturnType<typeof mountSettings>>, password?: string) {
    await buttonByText(wrapper, "settings.security.deleteAccount").trigger("click");
    if (password !== undefined) await wrapper.findAll('input[type="password"]').at(-1)!.setValue(password);
    await buttonByText(wrapper, "settings.security.confirmDelete").trigger("click");
    await flushPromises();
  }

  it("asks a passwordless account to sign in with Google again before deleting it", async () => {
    mocks.deleteAccount.mockRejectedValue(
      new ApiError(403, "reauth_required: deleting the account needs a Google sign-in from the last 10 minutes"),
    );
    const wrapper = await mountSettings("/settings?tab=security");

    await confirmDeletion(wrapper);

    expect(mocks.deleteAccount).toHaveBeenCalledWith(undefined);
    expect(mocks.logout).not.toHaveBeenCalled();
    expect(wrapper.vm.$router.currentRoute.value.fullPath).toBe("/settings?tab=security");
    // The deletion's own text, not the set-password one, the generic 403 or the raw detail.
    expect(wrapper.text()).toContain(t("settings.security.deleteReauthRequired"));
    expect(wrapper.text()).not.toContain(t("errors.api.reauthRequired"));
    expect(wrapper.text()).not.toContain(t("errors.api.forbidden"));
    expect(wrapper.text()).not.toContain("reauth_required");

    await buttonByText(wrapper, "auth.reauthGoogle").trigger("click");
    expect(startGoogleSignIn).toHaveBeenCalledWith("/settings?tab=security");
  });

  it("drops the Google sign-in offer when the delete modal is closed", async () => {
    mocks.deleteAccount.mockRejectedValue(new ApiError(403, "reauth_required"));
    const wrapper = await mountSettings("/settings?tab=security");

    await confirmDeletion(wrapper);
    expect(wrapper.text()).toContain(t("settings.security.deleteReauthRequired"));

    await buttonByText(wrapper, "common.cancel").trigger("click");
    expect(wrapper.text()).not.toContain(t("settings.security.deleteTitle"));
    await buttonByText(wrapper, "settings.security.deleteAccount").trigger("click");
    expect(wrapper.text()).toContain(t("settings.security.deleteTitle"));
    expect(wrapper.text()).not.toContain(t("settings.security.deleteReauthRequired"));
    expect(wrapper.text()).not.toContain(t("auth.reauthGoogle"));
  });

  it("keeps the password and generic errors of a deletion without a Google sign-in offer", async () => {
    const wrapper = await mountSettings("/settings?tab=security");

    mocks.deleteAccount.mockRejectedValueOnce(new ApiError(401, "Current password is incorrect"));
    await confirmDeletion(wrapper, "wrong-password");
    expect(mocks.deleteAccount).toHaveBeenLastCalledWith("wrong-password");
    expect(wrapper.text()).toContain(t("settings.errors.wrongCurrentPassword"));
    expect(wrapper.text()).not.toContain(t("auth.reauthGoogle"));

    mocks.deleteAccount.mockRejectedValueOnce(new ApiError(403, "Forbidden"));
    await buttonByText(wrapper, "settings.security.confirmDelete").trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("errors.api.forbidden"));
    expect(wrapper.text()).not.toContain(t("settings.errors.wrongCurrentPassword"));
    expect(wrapper.text()).not.toContain(t("settings.security.deleteReauthRequired"));
    expect(wrapper.text()).not.toContain(t("auth.reauthGoogle"));
  });

  it("clears the Google sign-in offer when a retried deletion fails otherwise", async () => {
    const wrapper = await mountSettings("/settings?tab=security");

    mocks.deleteAccount.mockRejectedValueOnce(new ApiError(403, "reauth_required"));
    await confirmDeletion(wrapper);
    expect(wrapper.text()).toContain(t("settings.security.deleteReauthRequired"));

    mocks.deleteAccount.mockRejectedValueOnce(new ApiError(500, "boom"));
    await buttonByText(wrapper, "settings.security.confirmDelete").trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain(t("errors.api.server"));
    expect(wrapper.text()).not.toContain(t("settings.security.deleteReauthRequired"));
  });

  it("opens the tab named by ?tab= and ignores unknown ones", async () => {
    const security = await mountSettings("/settings?tab=security");
    expect(security.text()).toContain(t("settings.security.passwordTitle"));

    const unknown = await mountSettings("/settings?tab=nope");
    expect(unknown.text()).toContain(t("settings.profile.title"));
    expect(unknown.text()).not.toContain(t("settings.security.passwordTitle"));
  });

  it("says that logging out signs out every device, and logs out", async () => {
    mocks.logout.mockResolvedValue({ status: "ok" });
    const wrapper = await mountSettings("/settings?tab=security");

    expect(wrapper.text()).toContain(t("auth.logoutEverywhereHint"));
    await wrapper.findAll("button").find((b) => b.text() === t("auth.logoutEverywhere"))!.trigger("click");
    await flushPromises();

    expect(mocks.logout).toHaveBeenCalledOnce();
    expect(useAuthStore().user).toBeNull();
    expect(wrapper.vm.$router.currentRoute.value.fullPath).toBe("/login");
  });

  it("renders every tab without Russian text or raw keys in the English UI", async () => {
    const wrapper = await mountSettings();
    for (const tab of ["profile", "research", "appearance", "analytics", "security"]) {
      await openTab(wrapper, `settings.tabs.${tab}`);
      await flushPromises();
      // The language picker's endonym "Русский" is the only Cyrillic allowed.
      const text = wrapper.text().replace("Русский", "");
      expect(text, tab).not.toMatch(/[А-Яа-яЁё]/);
      expect(text, tab).not.toMatch(/settings\.\w+/);
    }
  });
});
