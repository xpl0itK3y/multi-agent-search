// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";

const authConfig = vi.hoisted(() => vi.fn());
const startGoogleSignIn = vi.hoisted(() => vi.fn());
const register = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", () => ({
  api: { authConfig, register, googleLoginUrl: () => "/g" },
  apiErrorMessage: () => "error",
  PASSWORD_MIN_LENGTH: 6,
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
  const wrapper = mount(LoginView, { attachTo: document.body, global: { plugins: [createPinia(), router, i18n] } });
  mounted.push(wrapper);
  await flushPromises();
  return wrapper;
}

const t = (key: string) => i18n.global.t(key);
const mounted: { unmount(): void }[] = [];
const byText = (w: Awaited<ReturnType<typeof mountAt>>, key: string) =>
  w.findAll("button").find((b) => b.text() === t(key))!;

describe("LoginView", () => {
  beforeEach(() => {
    authConfig.mockResolvedValue({ google_oauth: true });
    startGoogleSignIn.mockClear();
    register.mockReset();
  });
  afterEach(() => {
    while (mounted.length) mounted.pop()!.unmount();
  });

  it("keeps the browser's bubbles out: novalidate, and an empty field is focused, not sent", async () => {
    const wrapper = await mountAt("/login");
    const form = wrapper.find("form");
    expect(form.attributes("novalidate")).toBeDefined();

    await form.trigger("submit");
    await flushPromises();
    expect(document.activeElement).toBe(wrapper.find('input[type="email"]').element);

    await wrapper.find('input[type="email"]').setValue("user@example.com");
    await form.trigger("submit");
    await flushPromises();
    expect(document.activeElement).toBe(wrapper.find('input[type="password"]').element);
    expect(register).not.toHaveBeenCalled();
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

    expect(wrapper.find("p.text-danger").exists()).toBe(false);
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

  it("states the password rule on sign-up and refuses a short one without asking the server", async () => {
    const wrapper = await mountAt("/login");
    await byText(wrapper, "auth.toRegister").trigger("click");

    const password = wrapper.find('input[type="password"]');
    const hint = wrapper.find(`#${password.attributes("aria-describedby")}`);
    expect(hint.text()).toBe(i18n.global.t("auth.passwordRule", { min: 6 }));
    expect(password.attributes("minlength")).toBe("6");

    await wrapper.find('input[type="email"]').setValue("new@example.com");
    await password.setValue("abc");
    await wrapper.find("form").trigger("submit");
    await flushPromises();

    expect(register).not.toHaveBeenCalled();
    expect(hint.classes()).toContain("text-danger");
    expect(document.activeElement).toBe(password.element);

    await password.setValue("abcdef");
    expect(hint.text()).toContain(t("auth.passwordOk"));
    expect(hint.classes()).toContain("text-success");
  });

  it("says what it is doing while it signs up", async () => {
    register.mockReturnValue(new Promise(() => {}));
    const wrapper = await mountAt("/login");
    await byText(wrapper, "auth.toRegister").trigger("click");
    await wrapper.find('input[type="email"]').setValue("new@example.com");
    await wrapper.find('input[type="password"]').setValue("long-enough");
    await wrapper.find("form").trigger("submit");
    await flushPromises();

    expect(register).toHaveBeenCalledWith("new@example.com", "long-enough");
    expect(wrapper.find('button[type="submit"]').text()).toBe(t("auth.registering"));
  });

  it("switching between sign-in and sign-up clears the other form's error", async () => {
    const wrapper = await mountAt("/login?error=oauth_failed");
    expect(wrapper.text()).toContain(t("auth.oauthFailed"));

    await byText(wrapper, "auth.toRegister").trigger("click");
    expect(wrapper.text()).not.toContain(t("auth.oauthFailed"));
    expect(wrapper.find("p.text-danger").exists()).toBe(false);
  });

  it("never states the sign-up rule on the sign-in form", async () => {
    const wrapper = await mountAt("/login");
    const password = wrapper.find('input[type="password"]');
    expect(password.attributes("aria-describedby")).toBeUndefined();
    expect(password.attributes("minlength")).toBeUndefined();
    expect(wrapper.text()).not.toContain(i18n.global.t("auth.passwordRule", { min: 6 }));
  });
});
