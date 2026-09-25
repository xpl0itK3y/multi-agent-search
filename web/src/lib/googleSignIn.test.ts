import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const RETURN_KEY = "auth.google_return_to";
const MINUTE = 60 * 1000;

// api.ts reads localStorage at import time; the helper also needs sessionStorage and
// window.location/addEventListener. Stub them before each dynamic import.
function stubBrowser(token: string | null = "password-session-token") {
  const session = new Map<string, string>();
  const sessionStorage = {
    getItem: vi.fn((k: string) => session.get(k) ?? null),
    setItem: vi.fn((k: string, v: string) => void session.set(k, v)),
    removeItem: vi.fn((k: string) => void session.delete(k)),
  };
  const localRemove = vi.fn();
  const assign = vi.fn();
  const listeners: { type: string; fn: (e: unknown) => void; opts: unknown }[] = [];
  vi.stubGlobal("localStorage", { getItem: vi.fn(() => token), setItem: vi.fn(), removeItem: localRemove });
  vi.stubGlobal("sessionStorage", sessionStorage);
  vi.stubGlobal("document", { cookie: "" });
  vi.stubGlobal("window", {
    location: { pathname: "/settings", search: "", assign },
    addEventListener: vi.fn((type: string, fn: (e: unknown) => void, opts: unknown) => listeners.push({ type, fn, opts })),
  });
  return { session, sessionStorage, localRemove, assign, listeners };
}

function landing(name: string, fullPath: string, meta: Record<string, unknown> = {}) {
  const query = Object.fromEntries(new URL(fullPath, "http://landing.invalid").searchParams);
  return { name, fullPath, meta, query };
}

describe("safeReturnPath", () => {
  beforeEach(() => {
    vi.resetModules();
    stubBrowser();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("keeps paths on this site", async () => {
    const { safeReturnPath } = await import("./googleSignIn");
    for (const path of ["/", "/settings?tab=security", "/research/abc#sources", "/set-password"]) {
      expect(safeReturnPath(path), path).toBe(path);
    }
  });

  it("rejects anything a browser could resolve to another site", async () => {
    const { safeReturnPath } = await import("./googleSignIn");
    for (const value of [
      "https://evil.example/settings",
      "//evil.example/settings",
      "/\\evil.example",
      "/settings\\..\\x",
      "/\t/evil.example",
      "/\n/evil.example",
      "javascript:alert(1)",
      "settings",
      "",
      "/" + "a".repeat(600),
      null,
      undefined,
      42,
    ]) {
      expect(safeReturnPath(value), String(value)).toBeNull();
    }
  });
});

describe("startGoogleSignIn", () => {
  beforeEach(() => vi.resetModules());
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("remembers the page, drops the bearer token and goes to Google", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const { session, localRemove, assign } = stubBrowser();
    const { startGoogleSignIn } = await import("./googleSignIn");

    startGoogleSignIn("/settings?tab=security");

    expect(JSON.parse(session.get(RETURN_KEY)!)).toEqual({ path: "/settings?tab=security", at: 1_000_000 });
    // A bearer from a password sign-in would shadow the fresh Google cookie session.
    expect(localRemove).toHaveBeenCalledWith("access_token");
    expect(assign).toHaveBeenCalledWith("/v1/auth/google/login");
  });

  it("stores no unsafe return path, and drops an older one", async () => {
    const { session, assign } = stubBrowser();
    session.set(RETURN_KEY, JSON.stringify({ path: "/old", at: Date.now() }));
    const { startGoogleSignIn } = await import("./googleSignIn");

    startGoogleSignIn("//evil.example/settings");

    expect(session.has(RETURN_KEY)).toBe(false);
    expect(assign).toHaveBeenCalledWith("/v1/auth/google/login");
  });

  it("forgets the return path when Back restores the page from the bfcache", async () => {
    const { session, listeners } = stubBrowser();
    const { startGoogleSignIn } = await import("./googleSignIn");
    startGoogleSignIn("/settings?tab=security");

    const pageshow = listeners.find((l) => l.type === "pageshow")!;
    expect(pageshow.opts).toEqual({ once: true });
    pageshow.fn({ persisted: false });
    expect(session.has(RETURN_KEY)).toBe(true);
    pageshow.fn({ persisted: true });
    expect(session.has(RETURN_KEY)).toBe(false);
  });

  it("still signs in when storage is blocked", async () => {
    const { sessionStorage, assign } = stubBrowser();
    sessionStorage.setItem.mockImplementation(() => {
      throw new Error("SecurityError");
    });
    const { startGoogleSignIn } = await import("./googleSignIn");

    expect(() => startGoogleSignIn("/settings")).not.toThrow();
    expect(assign).toHaveBeenCalledWith("/v1/auth/google/login");
  });
});

describe("googleReturnRedirect", () => {
  const NOW = 50 * MINUTE;

  beforeEach(() => vi.resetModules());
  afterEach(() => vi.unstubAllGlobals());

  async function withStored(value: unknown) {
    const env = stubBrowser();
    env.session.set(RETURN_KEY, typeof value === "string" ? value : JSON.stringify(value));
    const mod = await import("./googleSignIn");
    return { ...env, ...mod };
  }

  it("sends the callback's landing on to the stored page, once", async () => {
    const { googleReturnRedirect, session } = await withStored({ path: "/settings?tab=security", at: NOW - 2 * MINUTE });

    expect(googleReturnRedirect(landing("home", "/"), true, NOW)).toBe("/settings?tab=security");
    expect(session.has(RETURN_KEY)).toBe(false);
    expect(googleReturnRedirect(landing("home", "/"), true, NOW)).toBeNull();
  });

  it("is a no-op without a stored page", async () => {
    stubBrowser();
    const { googleReturnRedirect } = await import("./googleSignIn");
    expect(googleReturnRedirect(landing("home", "/"), true, NOW)).toBeNull();
  });

  it.each([
    ["expired", { path: "/settings", at: NOW - 11 * MINUTE }, landing("home", "/"), true],
    ["from the future", { path: "/settings", at: NOW + MINUTE }, landing("home", "/"), true],
    ["not signed in", { path: "/settings", at: NOW }, landing("home", "/"), false],
    ["failed sign-in, signed out", { path: "/settings", at: NOW }, landing("login", "/login?error=oauth_failed"), false],
    ["plain login page", { path: "/settings", at: NOW }, landing("login", "/login"), true],
    ["new account onboarding", { path: "/settings", at: NOW }, landing("set-password", "/set-password"), true],
    ["public page", { path: "/settings", at: NOW }, landing("public-report", "/r/t", { public: true }), true],
    ["already there", { path: "/settings?tab=security", at: NOW }, landing("settings", "/settings?tab=security"), true],
    ["tampered path", { path: "//evil.example", at: NOW }, landing("home", "/"), true],
    ["no timestamp", { path: "/settings" }, landing("home", "/"), true],
    ["not JSON", "{oops", landing("home", "/"), true],
    ["JSON null", "null", landing("home", "/"), true],
  ])("keeps the landing page (%s) and drops the entry", async (_case, stored, to, signedIn) => {
    const { googleReturnRedirect, session } = await withStored(stored);

    expect(googleReturnRedirect(to, signedIn, NOW)).toBeNull();
    expect(session.has(RETURN_KEY)).toBe(false);
  });

  it("returns null when storage is blocked", async () => {
    const { sessionStorage } = stubBrowser();
    sessionStorage.getItem.mockImplementation(() => {
      throw new Error("SecurityError");
    });
    const { googleReturnRedirect } = await import("./googleSignIn");
    expect(googleReturnRedirect(landing("home", "/"), true, NOW)).toBeNull();
  });

  // A cancelled or refused re-auth lands on /login?error=<code> while the old session is
  // still valid: back to the asking page, which is told why (not silently to Home).
  it.each([
    ["/settings?tab=security", "/login?error=oauth_failed", "/settings?tab=security&reauth_error=oauth_failed"],
    ["/settings?tab=security", "/login?error=oauth_conflict", "/settings?tab=security&reauth_error=oauth_conflict"],
    ["/set-password", "/login?error=oauth_failed", "/set-password?reauth_error=oauth_failed"],
    ["/research/abc#sources", "/login?error=oauth_conflict", "/research/abc?reauth_error=oauth_conflict#sources"],
    // An unknown code still reads as a failed sign-in, never as text of its own.
    ["/set-password", "/login?error=%3Cb%3Eboom", "/set-password?reauth_error=oauth_failed"],
  ])("sends a failed re-auth from %s back there with its reason (%s)", async (path, to, expected) => {
    const { googleReturnRedirect, session } = await withStored({ path, at: NOW - MINUTE });

    expect(googleReturnRedirect(landing("login", to), true, NOW)).toBe(expected);
    expect(session.has(RETURN_KEY)).toBe(false);
  });
});

describe("googleReauthErrorKey", () => {
  beforeEach(() => {
    vi.resetModules();
    stubBrowser();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("names the explanation of a known code only", async () => {
    const { googleReauthErrorKey } = await import("./googleSignIn");
    expect(googleReauthErrorKey("oauth_failed")).toBe("auth.reauthFailed");
    expect(googleReauthErrorKey("oauth_conflict")).toBe("auth.reauthConflict");
    for (const code of [undefined, null, "", "boom", "constructor", "__proto__", ["oauth_failed"]]) {
      expect(googleReauthErrorKey(code), String(code)).toBeNull();
    }
  });
});
