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
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: "/settings", component: SettingsView }] });
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

  it("opens the tab named by ?tab= and ignores unknown ones", async () => {
    const security = await mountSettings("/settings?tab=security");
    expect(security.text()).toContain(t("settings.security.passwordTitle"));

    const unknown = await mountSettings("/settings?tab=nope");
    expect(unknown.text()).toContain(t("settings.profile.title"));
    expect(unknown.text()).not.toContain(t("settings.security.passwordTitle"));
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
