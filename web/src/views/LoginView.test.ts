// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";

const authConfig = vi.hoisted(() => vi.fn());
const startGoogleSignIn = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", () => ({
  api: { authConfig, googleLoginUrl: () => "/g" },
  apiErrorMessage: () => "error",
}));
vi.mock("@/lib/googleSignIn", () => ({ startGoogleSignIn }));

import LoginView from "./LoginView.vue";

async function mountAt(url: string) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/login", name: "login", component: LoginView },
      { path: "/forgot-password", component: { render: () => null } },
    ],
  });
  await router.push(url);
  const wrapper = mount(LoginView, { global: { plugins: [createPinia(), router, i18n] } });
  await flushPromises();
  return wrapper;
}

const t = (key: string) => i18n.global.t(key);

describe("LoginView", () => {
  beforeEach(() => {
    authConfig.mockResolvedValue({ google_oauth: true });
    startGoogleSignIn.mockClear();
  });

  it.each([
    ["oauth_conflict", "auth.oauthConflict"],
    ["oauth_failed", "auth.oauthFailed"],
  ])("renders the OAuth callback error ?error=%s as a localized message", async (code, key) => {
    const wrapper = await mountAt(`/login?error=${code}`);

    expect(wrapper.text()).toContain(t(key));
    expect(wrapper.text()).not.toContain(code);
  });

  it("ignores unknown error codes instead of echoing them", async () => {
    const wrapper = await mountAt("/login?error=<script>");

    expect(wrapper.find("p.text-red-400").exists()).toBe(false);
  });

  it("explains oauth_conflict as an account linked to another Google identity", () => {
    // A Google sign-in now links to an existing account with this email; the callback
    // refuses only an account that is linked to a different Google identity.
    for (const loc of ["ru", "en", "es"] as const) {
      expect(i18n.global.getLocaleMessage(loc).auth.oauthConflict, loc).toMatch(/Google.*Google/);
    }
  });

  it("tells a would-be admin how to get a password (no first-password-on-login)", async () => {
    const wrapper = await mountAt("/login?redirect=/admin");

    expect(wrapper.text()).toContain("scripts/create_admin.py");
    for (const loc of ["ru", "en", "es"] as const) {
      expect(i18n.global.getLocaleMessage(loc).admin.authPasswordHint).toContain("scripts/create_admin.py");
    }
  });

  it("asks the visitor of a verification link to sign in to the account it was sent to", async () => {
    const wrapper = await mountAt("/login?redirect=/verify-email");
    expect(wrapper.find('[role="status"]').text()).toBe(t("verifyEmail.signInFirst"));

    const plain = await mountAt("/login");
    expect(plain.text()).not.toContain(t("verifyEmail.signInFirst"));
  });

  it("comes back to the page that sent the user here after a Google sign-in too", async () => {
    const googleButton = (w: Awaited<ReturnType<typeof mountAt>>) =>
      w.findAll("button").find((b) => b.text() === t("auth.google"))!;

    await googleButton(await mountAt("/login?redirect=/verify-email")).trigger("click");
    expect(startGoogleSignIn).toHaveBeenLastCalledWith("/verify-email");

    // No page to come back to: Google's own landing, and no older return path either.
    await googleButton(await mountAt("/login")).trigger("click");
    expect(startGoogleSignIn).toHaveBeenLastCalledWith(undefined);
    expect(startGoogleSignIn).toHaveBeenCalledTimes(2);
  });

  it("links a forgotten password to the reset page when the server can send the link", async () => {
    authConfig.mockResolvedValue({ google_oauth: false, password_reset: true, email_verification: true });
    const wrapper = await mountAt("/login");

    expect(wrapper.find('a[href="/forgot-password"]').text()).toBe(t("auth.forgotPassword"));
    expect(wrapper.text()).not.toContain(t("auth.forgotPasswordAskAdmin"));
  });

  it("sends a forgotten password to the administrator when it cannot", async () => {
    authConfig.mockResolvedValue({ google_oauth: true, password_reset: false, email_verification: false });
    const wrapper = await mountAt("/login");

    expect(wrapper.text()).toContain(t("auth.forgotPasswordAskAdmin"));
    expect(wrapper.find('a[href="/forgot-password"]').exists()).toBe(false);
  });

  it("offers neither on the sign-up form, nor before the config is known", async () => {
    authConfig.mockResolvedValue({ google_oauth: false, password_reset: true, email_verification: true });
    const wrapper = await mountAt("/login");
    await wrapper.findAll("button").find((b) => b.text() === t("auth.toRegister"))!.trigger("click");
    expect(wrapper.find('a[href="/forgot-password"]').exists()).toBe(false);

    authConfig.mockReturnValue(new Promise(() => {}));
    const loading = await mountAt("/login");
    expect(loading.find('a[href="/forgot-password"]').exists()).toBe(false);
    expect(loading.text()).not.toContain(t("auth.forgotPasswordAskAdmin"));
  });
});
