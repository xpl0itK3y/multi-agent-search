// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";

const mocks = vi.hoisted(() => ({ authConfig: vi.fn(), forgotPassword: vi.fn() }));

// Real ApiError/apiErrorMessage; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});

import { ApiError } from "@/lib/api";
import ForgotPasswordView from "./ForgotPasswordView.vue";

const t = (key: string) => i18n.global.t(key);

async function mountView() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/forgot-password", component: ForgotPasswordView },
      { path: "/login", component: { render: () => null } },
    ],
  });
  await router.push("/forgot-password");
  const wrapper = mount(ForgotPasswordView, { global: { plugins: [createPinia(), router, i18n] } });
  await flushPromises();
  return wrapper;
}

type Wrapper = Awaited<ReturnType<typeof mountView>>;

async function requestLink(wrapper: Wrapper, email: string) {
  await wrapper.find('input[type="email"]').setValue(email);
  await wrapper.find("form").trigger("submit");
  await flushPromises();
}

describe("ForgotPasswordView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    mocks.authConfig.mockResolvedValue({ google_oauth: false, password_reset: true, email_verification: true });
    mocks.forgotPassword.mockResolvedValue({ status: "accepted" });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("answers any email with the same 'if an account exists' message", async () => {
    for (const email of ["known@example.com", "nobody@example.com"]) {
      const wrapper = await mountView();
      await requestLink(wrapper, `  ${email} `);

      expect(mocks.forgotPassword).toHaveBeenLastCalledWith(email);
      expect(wrapper.find('[role="status"]').text()).toBe(t("forgotPassword.sent"));
      expect(wrapper.find("form").exists()).toBe(false);
      expect(wrapper.text()).not.toContain(email);
    }
  });

  it("offers the form again, with the email kept, to send another link", async () => {
    const wrapper = await mountView();
    await requestLink(wrapper, "denis@example.com");

    const again = wrapper.findAll("button").find((b) => b.text() === t("forgotPassword.again"));
    await again!.trigger("click");

    expect((wrapper.find('input[type="email"]').element as HTMLInputElement).value).toBe("denis@example.com");
    expect(wrapper.text()).not.toContain(t("forgotPassword.sent"));
  });

  it("says to slow down on a 429 instead of claiming a link was sent", async () => {
    mocks.forgotPassword.mockRejectedValue(new ApiError(429, "Too many attempts, please slow down"));
    const wrapper = await mountView();
    await requestLink(wrapper, "denis@example.com");

    expect(wrapper.text()).toContain(t("errors.api.rateLimited"));
    expect(wrapper.text()).not.toContain(t("forgotPassword.sent"));
    expect(wrapper.text()).not.toContain("slow down");
    expect(wrapper.find("form").exists()).toBe(true);
  });

  it("sends nothing for an empty email", async () => {
    const wrapper = await mountView();
    await requestLink(wrapper, "   ");

    expect(mocks.forgotPassword).not.toHaveBeenCalled();
  });

  it("says a server that cannot send email has no reset by email, and asks for none", async () => {
    mocks.authConfig.mockResolvedValue({ google_oauth: true, password_reset: false, email_verification: false });
    const wrapper = await mountView();

    expect(wrapper.text()).toContain(t("forgotPassword.unavailable"));
    expect(wrapper.find("form").exists()).toBe(false);
  });

  it("keeps the form when the auth config cannot be read", async () => {
    mocks.authConfig.mockRejectedValue(new TypeError("fetch failed"));
    const wrapper = await mountView();

    expect(wrapper.find("form").exists()).toBe(true);
    expect(wrapper.text()).not.toContain(t("forgotPassword.unavailable"));
  });

  it("links back to the sign-in page", async () => {
    const wrapper = await mountView();

    expect(wrapper.find('a[href="/login"]').text()).toBe(t("forgotPassword.backToLogin"));
  });
});
