// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { holdLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";

const resetPassword = vi.hoisted(() => vi.fn());

// Real ApiError/apiErrorMessage/isResetTokenInvalid; only the network call is stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, resetPassword } };
});

import { ApiError } from "@/lib/api";
import ResetPasswordView from "./ResetPasswordView.vue";

const t = (key: string, params: Record<string, unknown> = {}) => i18n.global.t(key, params);
const DEAD_LINK = "reset_token_invalid: this password reset link is invalid or has expired";

// `hash`: the fragment the router took out of the link's address, if any.
async function mountView(hash?: string) {
  if (hash !== undefined) holdLinkToken("reset-password", hash);
  const pinia = createPinia();
  setActivePinia(pinia);
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/reset-password", component: ResetPasswordView },
      { path: "/forgot-password", component: { render: () => null } },
      { path: "/login", component: { render: () => null } },
    ],
  });
  await router.push("/reset-password");
  const wrapper = mount(ResetPasswordView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return wrapper;
}

type Wrapper = Awaited<ReturnType<typeof mountView>>;

async function submit(wrapper: Wrapper, password: string, confirm = password) {
  const inputs = wrapper.findAll('input[type="password"]');
  await inputs[0].setValue(password);
  await inputs[1].setValue(confirm);
  await wrapper.find("form").trigger("submit");
  await flushPromises();
}

function expectDeadLink(wrapper: Wrapper) {
  expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.resetTokenInvalid"));
  expect(wrapper.find('a[href="/forgot-password"]').text()).toBe(t("resetPassword.requestNew"));
  expect(wrapper.find("form").exists()).toBe(false);
  expect(wrapper.text()).not.toContain("reset_token_invalid");
}

describe("ResetPasswordView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    resetPassword.mockResolvedValue({ status: "ok" });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("sets the new password with the link's token, then sends the user to sign in", async () => {
    const wrapper = await mountView("#token=reset-tok");
    useAuthStore().user = { id: "u1", email: "denis@example.com" };

    await submit(wrapper, "new-password");

    expect(resetPassword).toHaveBeenCalledWith("reset-tok", "new-password");
    expect(wrapper.find('[role="status"]').text()).toBe(t("resetPassword.done"));
    expect(wrapper.find('a[href="/login"]').text()).toBe(t("resetPassword.toLogin"));
    expect(wrapper.find("form").exists()).toBe(false);
    // Every session was revoked: nobody stays signed in here, so /login opens.
    expect(useAuthStore().user).toBeNull();
  });

  it("asks for the server's minimum length and a matching confirmation first", async () => {
    const wrapper = await mountView("#token=reset-tok");

    await submit(wrapper, "12345");
    expect(wrapper.text()).toContain(t("resetPassword.tooShort", { min: 6 }));

    await submit(wrapper, "123456", "123457");
    expect(wrapper.text()).toContain(t("resetPassword.mismatch"));
    expect(resetPassword).not.toHaveBeenCalled();

    await submit(wrapper, "123456");
    expect(resetPassword).toHaveBeenCalledWith("reset-tok", "123456");
  });

  it("says a dead link is dead and offers a new one", async () => {
    resetPassword.mockRejectedValue(new ApiError(400, DEAD_LINK));
    const wrapper = await mountView("#token=used-tok");
    useAuthStore().user = { id: "u1", email: "denis@example.com" };

    await submit(wrapper, "new-password");

    expectDeadLink(wrapper);
    // A refused reset revoked nothing: the session stays.
    expect(useAuthStore().user).not.toBeNull();
  });

  it("treats a link without a token as dead, without asking the server", async () => {
    for (const hash of [undefined, "#", "#section"]) {
      const wrapper = await mountView(hash);

      expectDeadLink(wrapper);
      expect(resetPassword).not.toHaveBeenCalled();
    }
  });

  it("takes the link's token once: opened again, the page has none", async () => {
    await mountView("#token=reset-tok");
    const again = await mountView();

    expectDeadLink(again);
  });

  it("keeps the form, with the reason, for other refusals", async () => {
    resetPassword.mockRejectedValueOnce(new ApiError(429, "Too many attempts, please slow down"));
    const wrapper = await mountView("#token=reset-tok");

    await submit(wrapper, "new-password");
    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.rateLimited"));
    expect(wrapper.find("form").exists()).toBe(true);

    // The same token is tried again: it is still good.
    await submit(wrapper, "new-password");
    expect(resetPassword).toHaveBeenLastCalledWith("reset-tok", "new-password");
    expect(wrapper.find('[role="status"]').text()).toBe(t("resetPassword.done"));
  });
});
