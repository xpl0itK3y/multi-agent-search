// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const USER = { id: "u1", email: "denis@example.com", email_verified: false };

// The whole app, booted the way the browser does it: index.html's #app and main.ts.
// Every request records the address as it was when it went out. Nobody is signed in
// until a password sign-in.
function stubServer() {
  const calls: { url: string; address: string; body?: string; headers?: Record<string, string> }[] = [];
  let signedIn = false;
  let verified = false;
  const json = (body: unknown) =>
    new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
    const address = window.location.pathname + window.location.search + window.location.hash;
    calls.push({ url, address, body: init?.body as string | undefined, headers: init?.headers as Record<string, string> });
    const user = { ...USER, email_verified: verified };
    if (url.endsWith("/v1/auth/me")) return signedIn ? json(user) : new Response("", { status: 401 });
    if (url.endsWith("/v1/auth/login")) {
      signedIn = true;
      return json({ access_token: "access", token_type: "bearer", user });
    }
    if (url.endsWith("/v1/auth/password/reset")) return json({ status: "ok" });
    if (url.endsWith("/v1/auth/email/verify")) {
      verified = true;
      return json({ status: "verified" });
    }
    return json(url.endsWith("/v1/auth/config") ? {} : []);
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}

const address = () => window.location.pathname + window.location.search + window.location.hash;

// The window listeners a boot added (the link capture's, the router's): a later boot in
// this same window must not have an earlier app react to its navigations.
let listeners: Parameters<typeof window.addEventListener>[] = [];

async function boot(address: string) {
  window.history.replaceState(null, "", address);
  document.body.innerHTML = '<div id="app"></div>';
  const calls = stubServer();
  const add = window.addEventListener.bind(window);
  vi.spyOn(window, "addEventListener").mockImplementation((...args: Parameters<typeof add>) => {
    listeners.push(args);
    add(...args);
  });
  await import("./main");
  const router = (await import("./router")).default;
  await vi.waitFor(() => expect(calls.map((c) => c.url)).toContain("/v1/models"), { timeout: 5000 });
  await router.isReady();
  return { calls, router };
}

beforeEach(() => {
  vi.resetModules();
  sessionStorage.clear();
  localStorage.clear();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const args of listeners) window.removeEventListener(...(args as [string, EventListener]));
  listeners = [];
  document.body.innerHTML = "";
});

describe("main: an emailed link's token on a page load", () => {
  it.each([
    ["/reset-password?lang=en#token=boot-tok", "/reset-password?lang=en", "/reset-password?lang=en"],
    // Signed out: it signs in first (see below).
    ["/verify-email#token=boot-tok", "/verify-email", "/login?redirect=/verify-email"],
  ])("leaves the address of %s before the first request", async (link, clean, page) => {
    const { calls, router } = await boot(link);

    // /v1/auth/me first, then App's startup requests: none while the token was there.
    expect(calls[0]).toMatchObject({ url: "/v1/auth/me", address: clean });
    expect(calls.length).toBeGreaterThan(1);
    for (const call of calls) expect(call.address, call.url).not.toContain("boot-tok");
    expect(window.location.hash).toBe("");
    expect(JSON.stringify(window.history.state)).not.toContain("boot-tok");
    // The router only ever saw the clean address.
    expect(router.currentRoute.value.fullPath).toBe(page);
    expect(JSON.stringify({ ...router.currentRoute.value, matched: undefined })).not.toContain("boot-tok");
  });

  it("hands the token to the page all the same", async () => {
    await boot("/reset-password#token=boot-tok");

    // The reset form, which only a page holding a token shows.
    await vi.waitFor(() => expect(document.querySelectorAll('input[type="password"]')).toHaveLength(2));
  });

  it("leaves the address of any other page alone", async () => {
    const { calls } = await boot("/forgot-password#token=not-a-link");

    expect(calls[0].address).toBe("/forgot-password#token=not-a-link");
    expect(window.location.hash).toBe("#token=not-a-link");
  });
});

describe("main: a link opened again into the open page", () => {
  it("gives a reset page reloaded without its token the new link's form", async () => {
    const { calls, router } = await boot("/reset-password");
    await vi.waitFor(() => expect(document.querySelector('[role="status"]')).not.toBeNull());
    expect(document.querySelector("form")).toBeNull();

    // The same-document navigation of a link opened into this tab.
    window.location.hash = "#token=again-tok";
    await vi.waitFor(() => expect(document.querySelectorAll('input[type="password"]')).toHaveLength(2));

    expect(window.location.hash).toBe("");
    expect(router.currentRoute.value.redirectedFrom).toBeUndefined();
    for (const input of document.querySelectorAll<HTMLInputElement>('input[type="password"]')) {
      input.value = "new-password";
      input.dispatchEvent(new Event("input"));
    }
    document.querySelector("form")!.dispatchEvent(new Event("submit"));
    await vi.waitFor(() => expect(calls.map((c) => c.url)).toContain("/v1/auth/password/reset"));
    expect(JSON.parse(calls.find((c) => c.url === "/v1/auth/password/reset")!.body!)).toEqual({
      token: "again-tok",
      password: "new-password",
    });
  });
});

describe("main: a verification link opened signed out", () => {
  it("signs in first, then confirms on the button only, with that session", async () => {
    const { calls, router } = await boot("/verify-email#token=boot-vt");
    await vi.waitFor(() => expect(document.querySelector('input[type="email"]')).not.toBeNull());
    expect(address()).toBe("/login?redirect=/verify-email");

    const email = document.querySelector<HTMLInputElement>('input[type="email"]')!;
    const password = document.querySelector<HTMLInputElement>('input[type="password"]')!;
    email.value = "denis@example.com";
    email.dispatchEvent(new Event("input"));
    password.value = "the-password";
    password.dispatchEvent(new Event("input"));
    document.querySelector("form")!.dispatchEvent(new Event("submit"));

    // Back on the link's page, which names the account and waits.
    await vi.waitFor(() => expect(router.currentRoute.value.fullPath).toBe("/verify-email"));
    await vi.waitFor(() => expect(document.body.textContent).toContain("denis@example.com"));
    expect(calls.map((c) => c.url)).not.toContain("/v1/auth/email/verify");

    const { i18n } = await import("@/i18n");
    const label = i18n.global.t("verifyEmail.confirm");
    [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === label)!.click();
    await vi.waitFor(() => expect(calls.map((c) => c.url)).toContain("/v1/auth/email/verify"));

    const verify = calls.filter((c) => c.url === "/v1/auth/email/verify");
    expect(verify).toHaveLength(1);
    expect(JSON.parse(verify[0].body!)).toEqual({ token: "boot-vt" });
    expect(verify[0].headers).toMatchObject({ Authorization: "Bearer access" });
    // Used: this tab lets go of it.
    await vi.waitFor(() => expect(sessionStorage.getItem("auth.verify_email_link")).toBeNull());
    for (const call of calls) expect(call.address, call.url).not.toContain("boot-vt");
  });
});
