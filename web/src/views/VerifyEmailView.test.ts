// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { dropLinkToken, holdLinkToken, peekLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";

const mocks = vi.hoisted(() => ({ verifyEmail: vi.fn(), me: vi.fn(), logout: vi.fn() }));

// Real ApiError/apiErrorMessage and the detail tests; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});

import { ApiError } from "@/lib/api";
import VerifyEmailView from "./VerifyEmailView.vue";

const t = (key: string, params: Record<string, unknown> = {}) => i18n.global.t(key, params);
const USER = { id: "u1", email: "denis@example.com", email_verified: false };
const DEAD_LINK = "verification_token_invalid: this verification link is invalid or has expired";
const WRONG_ACCOUNT = "verification_wrong_account: sign in to the account this link was sent to";
const STORAGE_KEY = "auth.verify_email_link";

// A page left mounted would still take the next test's link (onLinkToken).
enableAutoUnmount(afterEach);

// `hash`: the fragment the link capture took out of the link's address, if any.
async function mountView(hash?: string, { signedIn = true, verified = false } = {}) {
  if (hash !== undefined) holdLinkToken("verify-email", hash);
  const pinia = createPinia();
  setActivePinia(pinia);
  if (signedIn) useAuthStore().user = { ...USER, email_verified: verified };
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/verify-email", component: VerifyEmailView },
      { path: "/", component: { render: () => null } },
      { path: "/login", name: "login", component: { render: () => null } },
      { path: "/settings", component: { render: () => null } },
    ],
  });
  await router.push("/verify-email");
  const wrapper = mount(VerifyEmailView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return Object.assign(wrapper, { router });
}

type Wrapper = Awaited<ReturnType<typeof mountView>>;

function button(wrapper: Wrapper, key: string) {
  const found = wrapper.findAll("button").find((b) => b.text() === t(key));
  expect(found, key).toBeDefined();
  return found!;
}

async function clickConfirm(wrapper: Wrapper) {
  await button(wrapper, "verifyEmail.confirm").trigger("click");
  await flushPromises();
}

const storedToken = () => JSON.parse(sessionStorage.getItem(STORAGE_KEY) ?? "null")?.token ?? null;

describe("VerifyEmailView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    sessionStorage.clear();
    dropLinkToken("verify-email");
    mocks.verifyEmail.mockResolvedValue({ status: "verified" });
    mocks.me.mockResolvedValue({ ...USER, email_verified: true });
    mocks.logout.mockResolvedValue({ status: "ok" });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("confirms nothing on opening: it names the signed-in account and waits for the button", async () => {
    const wrapper = await mountView("#token=verify-tok");

    expect(mocks.verifyEmail).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain(t("verifyEmail.signedInAs", { email: "denis@example.com" }));
    expect(wrapper.text()).toContain(t("verifyEmail.confirmPrompt"));
    button(wrapper, "verifyEmail.confirm");
    expect(peekLinkToken("verify-email")).toBe("verify-tok");
  });

  it("confirms with the link's token on the button, once, then lets go of the link", async () => {
    const wrapper = await mountView("#token=verify-tok");

    await clickConfirm(wrapper);

    expect(mocks.verifyEmail).toHaveBeenCalledOnce();
    expect(mocks.verifyEmail).toHaveBeenCalledWith("verify-tok");
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verified"));
    // The signed-in user sees the new status at once, and goes on into the app.
    expect(mocks.me).toHaveBeenCalledOnce();
    expect(useAuthStore().user?.email_verified).toBe(true);
    expect(wrapper.find('a[href="/"]').text()).toBe(t("verifyEmail.continue"));
    expect(peekLinkToken("verify-email")).toBeNull();
    expect(sessionStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("says it is verifying while the request runs, with no second button to press", async () => {
    mocks.verifyEmail.mockReturnValue(new Promise(() => {}));
    const wrapper = await mountView("#token=verify-tok");

    await clickConfirm(wrapper);

    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verifying"));
    expect(wrapper.findAll("button").filter((b) => !/^(ru|en|es)$/.test(b.text()))).toHaveLength(0);
  });

  it("says a dead link is dead, lets go of it and offers a new one from Settings", async () => {
    mocks.verifyEmail.mockRejectedValue(new ApiError(400, DEAD_LINK));
    const wrapper = await mountView("#token=used-tok");

    await clickConfirm(wrapper);

    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.verificationTokenInvalid"));
    expect(wrapper.find('a[href="/settings"]').text()).toBe(t("verifyEmail.toSettings"));
    expect(wrapper.text()).not.toContain("verification_token_invalid");
    expect(mocks.me).not.toHaveBeenCalled();
    expect(peekLinkToken("verify-email")).toBeNull();
    expect(sessionStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("another account's link: says so, keeps the link for the right one and offers to switch", async () => {
    mocks.verifyEmail.mockRejectedValue(new ApiError(403, WRONG_ACCOUNT));
    const wrapper = await mountView("#token=their-tok");

    await clickConfirm(wrapper);

    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.verificationWrongAccount"));
    expect(wrapper.text()).toContain(t("verifyEmail.signedInAs", { email: "denis@example.com" }));
    expect(wrapper.text()).not.toContain("verification_wrong_account");
    // The server left the link unused.
    expect(peekLinkToken("verify-email")).toBe("their-tok");
    expect(storedToken()).toBe("their-tok");

    await button(wrapper, "verifyEmail.switchAccount").trigger("click");
    await flushPromises();

    expect(mocks.logout).toHaveBeenCalledOnce();
    expect(useAuthStore().user).toBeNull();
    expect(wrapper.router.currentRoute.value.fullPath).toBe("/login?redirect=/verify-email");
    expect(peekLinkToken("verify-email")).toBe("their-tok");
  });

  it("offers to try the same link again after a refusal that is not about it", async () => {
    mocks.verifyEmail.mockRejectedValueOnce(new ApiError(429, "Too many attempts, please slow down"));
    const wrapper = await mountView("#token=verify-tok");

    await clickConfirm(wrapper);
    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.rateLimited"));
    expect(storedToken()).toBe("verify-tok");

    await button(wrapper, "verifyEmail.retry").trigger("click");
    await flushPromises();

    expect(mocks.verifyEmail).toHaveBeenCalledTimes(2);
    expect(mocks.verifyEmail).toHaveBeenLastCalledWith("verify-tok");
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.verified"));
  });

  it("without a link, asks to open the emailed one, with no verdict on it and no request", async () => {
    for (const hash of [undefined, "#", "#section"]) {
      const wrapper = await mountView(hash);

      expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.noLink"));
      expect(wrapper.find('[role="alert"]').exists()).toBe(false);
      expect(wrapper.find('a[href="/settings"]').text()).toBe(t("verifyEmail.toSettings"));
      wrapper.unmount();
    }
    expect(mocks.verifyEmail).not.toHaveBeenCalled();
  });

  it("without a link on a verified account (Back after it), says the address is verified", async () => {
    const wrapper = await mountView(undefined, { verified: true });

    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.alreadyVerified"));
    expect(wrapper.find('a[href="/"]').text()).toBe(t("verifyEmail.continue"));
  });

  it("takes a link opened again into the open page", async () => {
    const wrapper = await mountView();
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.noLink"));

    holdLinkToken("verify-email", "#token=again-tok");
    await flushPromises();
    await clickConfirm(wrapper);

    expect(mocks.verifyEmail).toHaveBeenCalledWith("again-tok");
  });

  it("a newer link replaces a refused one", async () => {
    mocks.verifyEmail.mockRejectedValueOnce(new ApiError(403, WRONG_ACCOUNT));
    const wrapper = await mountView("#token=their-tok");
    await clickConfirm(wrapper);

    holdLinkToken("verify-email", "#token=mine-tok");
    await flushPromises();
    await clickConfirm(wrapper);

    expect(mocks.verifyEmail).toHaveBeenLastCalledWith("mine-tok");
    expect(peekLinkToken("verify-email")).toBeNull();
  });

  it("signed out (a sign-out on its way to /login), sends to sign in and posts nothing", async () => {
    const wrapper = await mountView("#token=verify-tok", { signedIn: false });

    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.signInFirst"));
    const login = wrapper.router.resolve({ name: "login", query: { redirect: "/verify-email" } }).href;
    expect(wrapper.find(`a[href="${login}"]`).text()).toBe(t("verifyEmail.toLogin"));
    expect(mocks.verifyEmail).not.toHaveBeenCalled();
  });
});
