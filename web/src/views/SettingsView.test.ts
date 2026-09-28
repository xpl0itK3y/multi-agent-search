// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";

import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";

const mocks = vi.hoisted(() => ({
  getTokenStats: vi.fn(),
  setPassword: vi.fn(),
  deleteAccount: vi.fn(),
  logout: vi.fn(),
  authConfig: vi.fn(),
  requestEmailVerification: vi.fn(),
  me: vi.fn(),
}));
const startGoogleSignIn = vi.hoisted(() => vi.fn());

// Real ApiError/apiErrorMessage; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});
vi.mock("@/lib/googleSignIn", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/googleSignIn")>()),
  startGoogleSignIn,
}));

import { ApiError, PASSWORD_MIN_LENGTH } from "@/lib/api";
import SettingsView from "./SettingsView.vue";

const t = (key: string) => i18n.global.t(key);

// Attached: a submit button submits its form only in a connected document (as in a browser).
const mounted: VueWrapper[] = [];

async function mountSettings(url = "/settings", { emailVerified = false } = {}) {
  const pinia = createPinia();
  setActivePinia(pinia);
  useAuthStore().user = { id: "u1", email: "denis@example.com", name: "Denis", email_verified: emailVerified };
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", component: { render: () => null } },
      { path: "/settings", component: SettingsView },
      { path: "/login", component: { render: () => null } },
    ],
  });
  await router.push(url);
  const wrapper = mount(SettingsView, { attachTo: document.body, global: { plugins: [pinia, router, i18n] } });
  mounted.push(wrapper);
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
    mocks.authConfig.mockResolvedValue({ google_oauth: true, password_reset: true, email_verification: true });
  });

  afterEach(() => {
    while (mounted.length) mounted.pop()!.unmount();
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

  it.each([
    ["oauth_failed", "auth.reauthFailed"],
    ["oauth_conflict", "auth.reauthConflict"],
  ])("says why the Google sign-in it asked for did not finish (%s)", async (code, key) => {
    mocks.setPassword.mockResolvedValue({ access_token: "t", token_type: "bearer", user: {} });
    const wrapper = await mountSettings(`/settings?tab=security&reauth_error=${code}`);

    expect(wrapper.text()).toContain(t(key));
    expect(wrapper.text()).not.toContain(code);
    await buttonByText(wrapper, "auth.reauthGoogle").trigger("click");
    expect(startGoogleSignIn).toHaveBeenCalledWith("/settings?tab=security");

    // A new attempt replaces the notice with its own outcome.
    const inputs = wrapper.findAll('input[type="password"]');
    await inputs[1].setValue("new-password");
    await inputs[2].setValue("new-password");
    await buttonByText(wrapper, "settings.security.updatePassword").trigger("click");
    await flushPromises();
    expect(wrapper.text()).not.toContain(t(key));
  });

  it("shows no re-auth notice for an unknown reason", async () => {
    const wrapper = await mountSettings("/settings?tab=security&reauth_error=constructor");

    expect(wrapper.text()).not.toContain(t("auth.reauthGoogle"));
  });

  it("opens the tab named by ?tab= and ignores unknown ones", async () => {
    const security = await mountSettings("/settings?tab=security");
    expect(security.text()).toContain(t("settings.security.passwordTitle"));

    const unknown = await mountSettings("/settings?tab=nope");
    expect(unknown.text()).toContain(t("settings.profile.title"));
    expect(unknown.text()).not.toContain(t("settings.security.passwordTitle"));
  });

  it("beside the sticky tabs, opens the next tab at its start after scrolling far down one", async () => {
    const wrapper = await mountSettings("/settings?tab=analytics");
    const root = wrapper.element as HTMLElement;
    let top = 1200;
    Object.defineProperty(root, "scrollTop", { configurable: true, get: () => top, set: (v: number) => (top = v) });
    // md+: the tabs are stuck 32px down the view; the panel started 700px above it.
    let panelTop = -700;
    const frame = wrapper.find("[data-test='settings-tabs']").element.parentElement!;
    const panel = wrapper.find("main").element;
    const spy = vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      if (this === frame) return { top: 32, bottom: 300 } as DOMRect;
      if (this === panel) return { top: panelTop, bottom: panelTop + 500 } as DOMRect;
      return { top: 0, bottom: 0, left: 0, right: 0, width: 0 } as DOMRect;
    });
    try {
      await openTab(wrapper, "settings.tabs.profile");
      await flushPromises();
      expect(top).toBe(1200 - (700 + 32));

      panelTop = 32; // lined up already
      await openTab(wrapper, "settings.tabs.research");
      await flushPromises();
      expect(top).toBe(1200 - (700 + 32));
    } finally {
      spy.mockRestore();
    }
  });

  describe("on a phone, where the tabs are one sideways row", () => {
    // jsdom has no layout: a 390px row whose five 120px tabs run to 636px.
    const undo: (() => void)[] = [];
    const scrollBy = vi.fn();
    function define(proto: object, key: string, desc: PropertyDescriptor) {
      const own = Object.getOwnPropertyDescriptor(proto, key);
      Object.defineProperty(proto, key, { configurable: true, ...desc });
      undo.push(() => (own ? Object.defineProperty(proto, key, own) : delete (proto as Record<string, unknown>)[key]));
    }
    const isRow = (el: Element) => el.getAttribute("data-test") === "settings-tabs";
    beforeEach(() => {
      const rect = (left: number, width: number) => ({ left, right: left + width, width, top: 0, bottom: 36, height: 36 }) as DOMRect;
      define(Element.prototype, "getBoundingClientRect", {
        value(this: Element) {
          if (isRow(this)) return rect(0, 390);
          const row = this.parentElement;
          return row && isRow(row) ? rect(6 + [...row.children].indexOf(this) * 124, 120) : rect(0, 0);
        },
      });
      define(HTMLElement.prototype, "scrollWidth", { get(this: Element) { return isRow(this) ? 636 : 0; } });
      define(HTMLElement.prototype, "clientWidth", { get(this: Element) { return isRow(this) ? 390 : 0; } });
      define(HTMLElement.prototype, "scrollBy", { value: scrollBy });
    });
    afterEach(() => {
      while (undo.length) undo.pop()!();
      scrollBy.mockReset();
    });

    it("fades the row's end while more tabs wait, and brings the tab from the address into view", async () => {
      const wrapper = await mountSettings("/settings?tab=security");

      expect(wrapper.find("[data-test='settings-tabs']").classes()).toContain("edge-fade-x");
      // Security ends at 622px: moved clear of the 28px fade, at once on arrival.
      expect(scrollBy).toHaveBeenCalledWith({ left: 622 - 390 + 28, behavior: "auto" });
    });

    it("scrolls a tab picked at the row's edge fully into view", async () => {
      const wrapper = await mountSettings("/settings");
      scrollBy.mockClear();

      await openTab(wrapper, "settings.tabs.appearance"); // 254..374, under the fade
      await flushPromises();

      expect(scrollBy).toHaveBeenCalledWith({ left: 374 - 390 + 28, behavior: "smooth" });
    });
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

  describe("email verification", () => {
    const sendButton = (wrapper: Awaited<ReturnType<typeof mountSettings>>) =>
      wrapper.findAll("button").find((b) => b.text() === t("settings.profile.sendVerification"));

    it("shows an unverified address and sends a verification email", async () => {
      mocks.requestEmailVerification.mockResolvedValue({ status: "sent" });
      const wrapper = await mountSettings();

      expect(wrapper.text()).toContain(t("settings.profile.emailNotVerified"));
      await sendButton(wrapper)!.trigger("click");
      await flushPromises();

      expect(mocks.requestEmailVerification).toHaveBeenCalledOnce();
      expect(wrapper.text()).toContain(
        i18n.global.t("settings.profile.verificationSent", { email: "denis@example.com" }),
      );
    });

    it("shows a verified address, with nothing to send", async () => {
      const wrapper = await mountSettings("/settings", { emailVerified: true });

      expect(wrapper.text()).toContain(t("settings.profile.emailVerified"));
      expect(wrapper.text()).not.toContain(t("settings.profile.emailNotVerified"));
      expect(sendButton(wrapper)).toBeUndefined();
    });

    it("an address verified meanwhile: says so and shows the new status", async () => {
      mocks.requestEmailVerification.mockResolvedValue({ status: "already_verified" });
      mocks.me.mockResolvedValue({ id: "u1", email: "denis@example.com", name: "Denis", email_verified: true });
      const wrapper = await mountSettings();

      await sendButton(wrapper)!.trigger("click");
      await flushPromises();

      expect(mocks.me).toHaveBeenCalledOnce();
      expect(wrapper.text()).toContain(t("settings.profile.alreadyVerified"));
      expect(wrapper.text()).toContain(t("settings.profile.emailVerified"));
      expect(sendButton(wrapper)).toBeUndefined();
    });

    it("says to slow down when links were asked for too often", async () => {
      mocks.requestEmailVerification.mockRejectedValue(new ApiError(429, "Too many attempts, please slow down"));
      const wrapper = await mountSettings();

      await sendButton(wrapper)!.trigger("click");
      await flushPromises();

      expect(wrapper.text()).toContain(t("errors.api.rateLimited"));
      expect(wrapper.text()).not.toContain(i18n.global.t("settings.profile.verificationSent", { email: "denis@example.com" }));
      // Still unverified: the button stays for a later try.
      expect(sendButton(wrapper)).toBeDefined();
    });

    it("is not shown where the server cannot send email", async () => {
      mocks.authConfig.mockResolvedValue({ google_oauth: true, password_reset: false, email_verification: false });
      const wrapper = await mountSettings();

      expect(wrapper.text()).not.toContain(t("settings.profile.emailNotVerified"));
      expect(sendButton(wrapper)).toBeUndefined();
    });
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

  it("keeps the open tab in the address, so a reload or Back keeps it", async () => {
    const wrapper = await mountSettings("/settings?tab=security&reauth_error=oauth_failed");
    await openTab(wrapper, "settings.tabs.analytics");
    await flushPromises();

    expect(wrapper.vm.$router.currentRoute.value.query).toEqual({ tab: "analytics" });
    expect(wrapper.text()).toContain(t("settings.analytics.title"));

    await wrapper.vm.$router.replace({ query: { tab: "appearance" } });
    await flushPromises();
    expect(wrapper.text()).toContain(t("settings.appearance.title"));

    // The sidebar's plain /settings, while the view stays mounted: back to Profile.
    await wrapper.vm.$router.push("/settings");
    await flushPromises();
    expect(wrapper.text()).toContain(t("settings.profile.title"));
  });

  it("goes home from Back when Settings was opened on its own", async () => {
    const wrapper = await mountSettings();
    await wrapper.findAll("button").find((b) => b.text().includes(t("settings.back")))!.trigger("click");
    await flushPromises();

    expect(wrapper.vm.$router.currentRoute.value.fullPath).toBe("/");
  });

  it("checks the new password against the server's minimum and states it under the field", async () => {
    mocks.setPassword.mockResolvedValue({ access_token: "t", token_type: "bearer", user: {} });
    const wrapper = await mountSettings("/settings?tab=security");
    const inputs = wrapper.findAll('input[type="password"]');
    const hint = wrapper.find(`#${inputs[1].attributes("aria-describedby")}`);
    expect(hint.text()).toBe(i18n.global.t("auth.passwordRule", { min: PASSWORD_MIN_LENGTH }));
    // Said once: the field's placeholder doesn't repeat the rule under it.
    expect(inputs[1].attributes("placeholder")).toBeUndefined();

    const short = "x".repeat(PASSWORD_MIN_LENGTH - 1);
    await inputs[1].setValue(short);
    await inputs[2].setValue(short);
    await buttonByText(wrapper, "settings.security.updatePassword").trigger("click");
    await flushPromises();
    expect(mocks.setPassword).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain(i18n.global.t("settings.errors.passwordTooShort", { min: PASSWORD_MIN_LENGTH }));
    expect(hint.classes()).toContain("text-danger");

    const ok = "x".repeat(PASSWORD_MIN_LENGTH);
    await inputs[1].setValue(ok);
    await inputs[2].setValue(ok);
    await wrapper.find("form").trigger("submit");
    await flushPromises();
    expect(mocks.setPassword).toHaveBeenCalledWith(ok, undefined);
  });

  describe("delete dialog", () => {
    it("is a labelled modal dialog that takes focus and gives it back", async () => {
      const wrapper = await mountSettings("/settings?tab=security");
      const opener = buttonByText(wrapper, "settings.security.deleteAccount");
      (opener.element as HTMLElement).focus();
      await opener.trigger("click");
      await flushPromises();

      const dialog = wrapper.find("[role='dialog']");
      expect(dialog.attributes("aria-modal")).toBe("true");
      expect(document.getElementById(dialog.attributes("aria-labelledby")!)!.textContent).toContain(t("settings.security.deleteTitle"));
      expect(document.activeElement).toBe(wrapper.findAll('input[type="password"]').at(-1)!.element);

      document.activeElement!.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
      await flushPromises();
      expect(wrapper.find("[role='dialog']").exists()).toBe(false);
      expect(document.activeElement).toBe(opener.element);
    });

    it("submits with Enter (a form), even with an empty password", async () => {
      mocks.deleteAccount.mockRejectedValue(new ApiError(403, "reauth_required"));
      const wrapper = await mountSettings("/settings?tab=security");
      await buttonByText(wrapper, "settings.security.deleteAccount").trigger("click");

      await wrapper.find("[role='dialog'] form").trigger("submit");
      await flushPromises();
      expect(mocks.deleteAccount).toHaveBeenCalledWith(undefined);
    });

    it("closes on a backdrop press, but not when a drag started inside it", async () => {
      const wrapper = await mountSettings("/settings?tab=security");
      await buttonByText(wrapper, "settings.security.deleteAccount").trigger("click");

      await wrapper.find("[data-test='delete-panel']").trigger("pointerdown");
      await wrapper.find("[data-test='delete-backdrop']").trigger("click");
      expect(wrapper.find("[role='dialog']").exists()).toBe(true);

      await wrapper.find("[data-test='delete-backdrop']").trigger("pointerdown");
      await wrapper.find("[data-test='delete-backdrop']").trigger("click");
      expect(wrapper.find("[role='dialog']").exists()).toBe(false);
    });
  });

  it("describes the System theme as following the device, and the others by their base", async () => {
    const wrapper = await mountSettings("/settings?tab=appearance");
    expect(wrapper.text()).toContain(t("themes.systemHint"));
    expect(wrapper.text()).toContain(t("settings.appearance.baseDark"));
    expect(wrapper.text()).toContain(t("settings.appearance.baseLight"));
  });

  it("colours a failed or cancelled research differently from a finished one", async () => {
    mocks.getTokenStats.mockResolvedValue({
      researches_count: 3, calls_count: 1, total_tokens: 10, prompt_tokens: 6, completion_tokens: 4,
      estimated_cost_usd: 0.01, by_model: [],
      recent: [
        { id: "a", prompt: "done", depth: "easy", status: "completed", total_tokens: 1, estimated_cost_usd: 0 },
        { id: "b", prompt: "broke", depth: "easy", status: "failed", total_tokens: 1, estimated_cost_usd: 0 },
        { id: "c", prompt: "stopped", depth: "easy", status: "cancelled", total_tokens: 1, estimated_cost_usd: 0 },
      ],
    });
    const wrapper = await mountSettings("/settings?tab=analytics");
    const pill = (prompt: string) => wrapper.findAll("tr").find((r) => r.text().includes(prompt))!.find("span");

    expect(pill("done").classes()).toContain("text-success");
    expect(pill("broke").classes()).toContain("text-danger");
    expect(pill("stopped").classes()).toContain("text-muted");
  });
});
