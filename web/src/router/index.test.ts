// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";

// The routes' views are irrelevant here; stub them so a navigation resolves quickly.
const stubView = vi.hoisted(() => () => ({ default: { render: () => null } }));
vi.mock("@/views/LoginView.vue", stubView);
vi.mock("@/views/ForgotPasswordView.vue", stubView);
vi.mock("@/views/ResetPasswordView.vue", stubView);
vi.mock("@/views/SetPasswordView.vue", stubView);
vi.mock("@/views/HomeView.vue", stubView);
vi.mock("@/views/ResearchView.vue", stubView);
vi.mock("@/views/ThreadView.vue", stubView);
vi.mock("@/views/PublicReportView.vue", stubView);
vi.mock("@/views/AdminView.vue", stubView);
vi.mock("@/views/SettingsView.vue", stubView);

const RETURN_KEY = "auth.google_return_to";

// A fresh router (its page-load flag) and store per test, as on a real page load.
async function loadApp(signedIn: boolean) {
  vi.resetModules();
  const { createPinia, setActivePinia } = await import("pinia");
  setActivePinia(createPinia());
  const { useAuthStore } = await import("@/stores/auth");
  if (signedIn) useAuthStore().user = { id: "u1", email: "denis@example.com" };
  return (await import("@/router")).default;
}

describe("router: back from a Google sign-in", () => {
  beforeEach(() => {
    sessionStorage.clear();
    window.history.replaceState(null, "", "/");
  });

  it("continues from the callback's landing to the page that asked for the sign-in", async () => {
    sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path: "/settings?tab=security", at: Date.now() }));
    const router = await loadApp(true);

    await router.push("/");

    expect(router.currentRoute.value.fullPath).toBe("/settings?tab=security");
    expect(sessionStorage.getItem(RETURN_KEY)).toBeNull();
  });

  it("only the first navigation of a page load looks at it", async () => {
    const router = await loadApp(true);
    await router.push("/");
    sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path: "/settings", at: Date.now() }));

    await router.push("/research/r-1");

    expect(router.currentRoute.value.fullPath).toBe("/research/r-1");
  });

  it("brings a signed-in user back from a failed re-auth, with the reason, not to Home", async () => {
    sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path: "/settings?tab=security", at: Date.now() }));
    const router = await loadApp(true);

    await router.push("/login?error=oauth_conflict");

    expect(router.currentRoute.value.fullPath).toBe("/settings?tab=security&reauth_error=oauth_conflict");
    expect(sessionStorage.getItem(RETURN_KEY)).toBeNull();
  });

  it("keeps a signed-out user on the failed sign-in's login page", async () => {
    sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path: "/settings?tab=security", at: Date.now() }));
    const router = await loadApp(false);

    await router.push("/login?error=oauth_failed");

    expect(router.currentRoute.value.fullPath).toBe("/login?error=oauth_failed");
  });

  it("a signed-out landing still goes to /login, and the entry is dropped", async () => {
    sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path: "/settings", at: Date.now() }));
    const router = await loadApp(false);

    await router.push("/");

    expect(router.currentRoute.value.name).toBe("login");
    expect(sessionStorage.getItem(RETURN_KEY)).toBeNull();
  });
});

describe("router: signed-out account pages", () => {
  beforeEach(() => {
    sessionStorage.clear();
    window.history.replaceState(null, "", "/");
  });

  it.each(["/forgot-password", "/reset-password"])("opens %s without a session, outside the app shell", async (path) => {
    const router = await loadApp(false);

    await router.push(path);

    expect(router.currentRoute.value.fullPath).toBe(path);
    expect(router.currentRoute.value.meta).toMatchObject({ public: true, bare: true });
  });

  it.each(["/forgot-password", "/reset-password"])("keeps a signed-in user on %s", async (path) => {
    const router = await loadApp(true);

    await router.push(path);

    expect(router.currentRoute.value.fullPath).toBe(path);
  });

  it("draws the sign-in page outside the app shell too, and nothing else", async () => {
    const router = await loadApp(true);

    expect(router.resolve("/login").meta.bare).toBe(true);
    for (const path of ["/", "/settings", "/r/share-token", "/set-password"]) {
      expect(router.resolve(path).meta.bare, path).toBeUndefined();
    }
  });
});

describe("router: an emailed link's one-time token", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  // The page load of a link: the browser opened this address, the router starts there.
  async function openLink(address: string, signedIn = false) {
    window.history.replaceState(null, "", address);
    const router = await loadApp(signedIn);
    await router.push(router.options.history.location);
    const { takeLinkToken } = await import("@/lib/linkToken");
    return { router, takeLinkToken };
  }

  it.each([false, true])("takes it out of the address before the page renders (signed in: %s)", async (signedIn) => {
    const { router, takeLinkToken } = await openLink("/reset-password?lang=en#token=secret-tok", signedIn);

    expect(router.currentRoute.value.fullPath).toBe("/reset-password?lang=en");
    expect(window.location.pathname + window.location.search + window.location.hash).toBe("/reset-password?lang=en");
    // vue-router's own record of the entry, which it writes back on the next navigation.
    expect(JSON.stringify(window.history.state)).not.toContain("secret-tok");
    expect(takeLinkToken("reset-password")).toBe("secret-tok");
  });

  it("replaces the link's history entry instead of adding one", async () => {
    const { router } = await openLink("/reset-password#token=secret-tok");

    expect(window.history.state.replaced).toBe(true);
    expect(router.currentRoute.value.redirectedFrom?.hash).toBe("#token=secret-tok");
  });

  it("never brings it back when the user moves on", async () => {
    const { router } = await openLink("/reset-password#token=secret-tok");

    await router.push("/login");
    expect(JSON.stringify(window.history.state)).not.toContain("secret-tok");

    const back = new Promise((resolve) => window.addEventListener("popstate", resolve, { once: true }));
    window.history.back();
    await back;
    expect(window.location.hash).toBe("");
    expect(window.location.pathname).toBe("/reset-password");
  });

  it("drops any other fragment of such a page", async () => {
    const { router, takeLinkToken } = await openLink("/reset-password#section");

    expect(router.currentRoute.value.fullPath).toBe("/reset-password");
    expect(takeLinkToken("reset-password")).toBeNull();
  });

  it("leaves the fragments of other pages alone", async () => {
    const { router, takeLinkToken } = await openLink("/forgot-password#token=not-a-link");

    expect(router.currentRoute.value.fullPath).toBe("/forgot-password#token=not-a-link");
    expect(takeLinkToken("forgot-password")).toBeNull();
  });
});
