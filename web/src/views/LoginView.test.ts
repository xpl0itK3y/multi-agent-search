// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";

vi.mock("@/lib/api", () => ({
  api: { authConfig: vi.fn().mockResolvedValue({ google_oauth: true }), googleLoginUrl: () => "/g" },
  apiErrorMessage: () => "error",
}));

import LoginView from "./LoginView.vue";

async function mountAt(url: string) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/login", name: "login", component: LoginView }],
  });
  await router.push(url);
  const wrapper = mount(LoginView, { global: { plugins: [createPinia(), router, i18n] } });
  await flushPromises();
  return wrapper;
}

const t = (key: string) => i18n.global.t(key);

describe("LoginView", () => {
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

  it("tells a would-be admin how to get a password (no first-password-on-login)", async () => {
    const wrapper = await mountAt("/login?redirect=/admin");

    expect(wrapper.text()).toContain("scripts/create_admin.py");
    for (const loc of ["ru", "en", "es"] as const) {
      expect(i18n.global.getLocaleMessage(loc).admin.authPasswordHint).toContain("scripts/create_admin.py");
    }
  });
});
