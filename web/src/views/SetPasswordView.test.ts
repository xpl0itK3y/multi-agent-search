// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";

const setPassword = vi.hoisted(() => vi.fn());
const startGoogleSignIn = vi.hoisted(() => vi.fn());

// Real ApiError/apiErrorMessage/isReauthRequired; only the network call is stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, setPassword } };
});
vi.mock("@/lib/googleSignIn", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/googleSignIn")>()),
  startGoogleSignIn,
}));

import { ApiError } from "@/lib/api";
import SetPasswordView from "./SetPasswordView.vue";

const t = (key: string) => i18n.global.t(key);

async function mountView(url = "/set-password") {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "u1", email: "denis@example.com" } as never;
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", component: { render: () => null } },
      { path: "/set-password", component: SetPasswordView },
    ],
  });
  await router.push(url);
  const wrapper = mount(SetPasswordView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return { wrapper, router };
}

async function submit(wrapper: Awaited<ReturnType<typeof mountView>>["wrapper"]) {
  const inputs = wrapper.findAll('input[type="password"]');
  await inputs[0].setValue("first-password");
  await inputs[1].setValue("first-password");
  await wrapper.find("form").trigger("submit");
  await flushPromises();
}

describe("SetPasswordView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("sets the first password and goes home", async () => {
    setPassword.mockResolvedValue({ access_token: "t", token_type: "bearer", user: {} });
    const { wrapper, router } = await mountView();

    await submit(wrapper);

    expect(setPassword).toHaveBeenCalledWith("first-password");
    expect(router.currentRoute.value.fullPath).toBe("/");
  });

  it("asks for a new Google sign-in, back to this page, when the landing is no longer fresh", async () => {
    setPassword.mockRejectedValue(new ApiError(403, "reauth_required"));
    const { wrapper, router } = await mountView();

    await submit(wrapper);

    expect(router.currentRoute.value.fullPath).toBe("/set-password");
    expect(wrapper.text()).toContain(t("errors.api.reauthRequired"));
    expect(wrapper.text()).not.toContain("reauth_required");

    await wrapper.findAll("button").find((b) => b.text() === t("auth.reauthGoogle"))!.trigger("click");
    expect(startGoogleSignIn).toHaveBeenCalledWith("/set-password");
    // The notice's button must not submit the form again.
    expect(setPassword).toHaveBeenCalledTimes(1);
  });

  it("says why the Google sign-in it asked for did not finish, and offers it again", async () => {
    setPassword.mockRejectedValue(new ApiError(500, "boom"));
    const { wrapper } = await mountView("/set-password?reauth_error=oauth_conflict");

    expect(wrapper.text()).toContain(t("auth.reauthConflict"));
    expect(wrapper.text()).not.toContain("oauth_conflict");
    await wrapper.findAll("button").find((b) => b.text() === t("auth.reauthGoogle"))!.trigger("click");
    expect(startGoogleSignIn).toHaveBeenCalledWith("/set-password");
    expect(setPassword).not.toHaveBeenCalled();

    // A new attempt replaces the notice with its own outcome.
    await submit(wrapper);
    expect(wrapper.text()).not.toContain(t("auth.reauthConflict"));
    expect(wrapper.text()).toContain(t("errors.api.server"));
  });
});
