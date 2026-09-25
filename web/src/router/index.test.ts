// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";

// The routes' views are irrelevant here; stub them so a navigation resolves quickly.
const stubView = vi.hoisted(() => () => ({ default: { render: () => null } }));
vi.mock("@/views/LoginView.vue", stubView);
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
