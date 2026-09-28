// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// The whole app, booted the way the browser does it: index.html's #app and main.ts.
// Every request records the address as it was when it went out.
function stubServer() {
  const calls: { url: string; address: string; body?: string }[] = [];
  const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
    const address = window.location.pathname + window.location.search + window.location.hash;
    calls.push({ url, address, body: init?.body as string | undefined });
    // Signed out: the link pages are the ones a signed-out visitor opens.
    if (url.endsWith("/v1/auth/me")) return new Response("", { status: 401 });
    const body = url.endsWith("/v1/auth/password/reset") ? { status: "ok" } : [];
    return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}

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
    ["/reset-password?lang=en#token=boot-tok", "/reset-password?lang=en", "reset-password"],
    ["/verify-email#token=boot-tok", "/verify-email", "verify-email"],
  ])("leaves the address of %s before the first request", async (link, clean, page) => {
    const { calls, router } = await boot(link);

    // /v1/auth/me first, then App's startup requests: none while the token was there.
    expect(calls[0].url).toBe("/v1/auth/me");
    expect(calls.length).toBeGreaterThan(1);
    for (const call of calls) expect(call.address, call.url).toBe(clean);
    expect(window.location.hash).toBe("");
    expect(JSON.stringify(window.history.state)).not.toContain("boot-tok");
    // The router only ever saw the clean address.
    expect(router.currentRoute.value.fullPath).toBe(clean);
    expect(router.currentRoute.value.redirectedFrom).toBeUndefined();
    expect(router.currentRoute.value.name).toBe(page);
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
