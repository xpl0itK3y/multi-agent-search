// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { holdLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";

const mocks = vi.hoisted(() => ({ verifyEmail: vi.fn(), me: vi.fn() }));

// Real ApiError/apiErrorMessage/isVerificationTokenInvalid; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});

import { ApiError } from "@/lib/api";
import VerifyEmailView from "./VerifyEmailView.vue";

const t = (key: string) => i18n.global.t(key);
const USER = { id: "u1", email: "denis@example.com", email_verified: false };
const DEAD_LINK = "verification_token_invalid: this verification link is invalid or has expired";

// `hash`: the fragment the router took out of the link's address, if any.
async function mountView(hash?: string, { signedIn = false } = {}) {
  if (hash !== undefined) holdLinkToken("verify-email", hash);
  const pinia = createPinia();
  setActivePinia(pinia);
  if (signedIn) useAuthStore().user = { ...USER };
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/verify-email", component: VerifyEmailView },
      { path: "/", component: { render: () => null } },
      { path: "/login", component: { render: () => null } },
      { path: "/settings", component: { render: () => null } },
    ],
  });
  await router.push("/verify-email");
  const wrapper = mount(VerifyEmailView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return wrapper;
}

type Wrapper = Awaited<ReturnType<typeof mountView>>;

function expectDeadLink(wrapper: Wrapper, next: string, label: string) {
  expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.verificationTokenInvalid"));
  expect(wrapper.find(`a[href="${next}"]`).text()).toBe(t(label));
  expect(wrapper.text()).not.toContain("verification_token_invalid");
}

describe("VerifyEmailView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    mocks.verifyEmail.mockResolvedValue({ status: "verified" });
    mocks.me.mockResolvedValue({ ...USER, email_verified: true });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("confirms the address with the link's token, once, as soon as it opens", async () => {
    const wrapper = await mountView("#token=verify-tok");

    expect(mocks.verifyEmail).toHaveBeenCalledOnce();
    expect(mocks.verifyEmail).toHaveBeenCalledWith("verify-tok");
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verified"));
    // Signed out: nothing to refresh, and the way on is the sign-in page.
    expect(mocks.me).not.toHaveBeenCalled();
    expect(wrapper.find('a[href="/login"]').text()).toBe(t("verifyEmail.toLogin"));
  });

  it("says it is verifying while the request runs", async () => {
    mocks.verifyEmail.mockReturnValue(new Promise(() => {}));
    const wrapper = await mountView("#token=verify-tok");

    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verifying"));
  });

  it("refreshes a signed-in user, who then goes on into the app", async () => {
    const wrapper = await mountView("#token=verify-tok", { signedIn: true });

    expect(mocks.me).toHaveBeenCalledOnce();
    expect(useAuthStore().user?.email_verified).toBe(true);
    expect(wrapper.find('a[href="/"]').text()).toBe(t("verifyEmail.continue"));
  });

  it("says a dead link is dead: a signed-in user gets a new one from Settings", async () => {
    mocks.verifyEmail.mockRejectedValue(new ApiError(400, DEAD_LINK));

    expectDeadLink(await mountView("#token=used-tok", { signedIn: true }), "/settings", "verifyEmail.toSettings");
    expectDeadLink(await mountView("#token=used-tok"), "/login", "verifyEmail.toLogin");
    expect(mocks.me).not.toHaveBeenCalled();
  });

  it("treats a link without a token as dead, without asking the server", async () => {
    for (const hash of [undefined, "#", "#section"]) {
      expectDeadLink(await mountView(hash), "/login", "verifyEmail.toLogin");
    }
    expect(mocks.verifyEmail).not.toHaveBeenCalled();
  });

  it("takes the link's token once: opened again, the page posts nothing", async () => {
    await mountView("#token=verify-tok");
    const again = await mountView();

    expect(mocks.verifyEmail).toHaveBeenCalledOnce();
    expectDeadLink(again, "/login", "verifyEmail.toLogin");
  });

  it("offers to try the same link again after a refusal that is not about it", async () => {
    mocks.verifyEmail.mockRejectedValueOnce(new ApiError(429, "Too many attempts, please slow down"));
    const wrapper = await mountView("#token=verify-tok");

    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.rateLimited"));
    await wrapper.findAll("button").find((b) => b.text() === t("verifyEmail.retry"))!.trigger("click");
    await flushPromises();

    expect(mocks.verifyEmail).toHaveBeenCalledTimes(2);
    expect(mocks.verifyEmail).toHaveBeenLastCalledWith("verify-tok");
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verified"));
  });
});
