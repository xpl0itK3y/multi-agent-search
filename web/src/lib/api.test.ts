import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// api.ts touches localStorage / document.cookie at import time and
// window.location on 401 recovery — stub the globals before each dynamic import.
function stubEnv(token: string | null, pathname: string, search = "") {
  const assign = vi.fn();
  const removeItem = vi.fn();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn(() => token),
    setItem: vi.fn(),
    removeItem,
  });
  vi.stubGlobal("document", { cookie: "" });
  vi.stubGlobal("window", { location: { pathname, search, assign } });
  return { assign, removeItem };
}

describe("api request error handling", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("throws a typed ApiError with the JSON detail from the body", async () => {
    const { assign } = stubEnv(null, "/research/abc");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "research not found" }), { status: 404 }),
      ),
    );

    const { api, ApiError } = await import("./api");
    const err: unknown = await api.getReport("abc").catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect((err as InstanceType<typeof ApiError>).status).toBe(404);
    expect((err as InstanceType<typeof ApiError>).detail).toBe("research not found");
    expect((err as Error).message).toBe("404 research not found");
    expect(assign).not.toHaveBeenCalled();
  });

  it("falls back to statusText when the error body has no detail", async () => {
    stubEnv(null, "/research/abc");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("", { status: 500, statusText: "Internal Server Error" })),
    );

    const { api, ApiError } = await import("./api");
    const err: unknown = await api.getReport("abc").catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect((err as InstanceType<typeof ApiError>).detail).toBe("Internal Server Error");
  });

  it("on 401 with a stored token: clears it and redirects to /login with a redirect param", async () => {
    const { assign, removeItem } = stubEnv("stale-token", "/research/abc", "?tab=sources");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "token expired" }), { status: 401 }),
      ),
    );

    const { api } = await import("./api");
    await expect(api.getReport("abc")).rejects.toMatchObject({ status: 401 });

    expect(removeItem).toHaveBeenCalledWith("access_token");
    expect(assign).toHaveBeenCalledOnce();
    expect(assign).toHaveBeenCalledWith("/login?redirect=%2Fresearch%2Fabc%3Ftab%3Dsources");
  });

  it("on 401 without a stored token: no redirect (unauthenticated page)", async () => {
    const { assign, removeItem } = stubEnv(null, "/research/abc");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));

    const { api } = await import("./api");
    await expect(api.getReport("abc")).rejects.toMatchObject({ status: 401 });

    expect(assign).not.toHaveBeenCalled();
    expect(removeItem).not.toHaveBeenCalled();
  });

  it("on 401 on public routes (/r/…, /login): no redirect loop", async () => {
    for (const pathname of ["/r/share-token", "/login"]) {
      const { assign } = stubEnv("stale-token", pathname);
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));

      const { api } = await import("./api");
      await expect(api.getPublicReport("t")).rejects.toMatchObject({ status: 401 });
      expect(assign).not.toHaveBeenCalled();

      vi.unstubAllGlobals();
    }
  });
});

describe("account endpoints", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("deleteAccount sends the password together with confirm=true", async () => {
    stubEnv("token", "/settings");
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ status: "deleted" }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const { api } = await import("./api");
    await api.deleteAccount("secret123");

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/v1/auth/account");
    expect(init.method).toBe("DELETE");
    expect(JSON.parse(init.body as string)).toEqual({ current_password: "secret123", confirm: true });
  });

  it("a wrong current password (401) keeps the session instead of redirecting", async () => {
    const { assign, removeItem } = stubEnv("valid-token", "/settings");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(async () =>
        new Response(JSON.stringify({ detail: "Current password is incorrect" }), { status: 401 }),
      ),
    );

    const { api } = await import("./api");
    await expect(api.setPassword("new-password", "wrong")).rejects.toMatchObject({ status: 401 });
    await expect(api.deleteAccount("wrong")).rejects.toMatchObject({ status: 401 });

    expect(removeItem).not.toHaveBeenCalled();
    expect(assign).not.toHaveBeenCalled();
  });

  it("without a current password a 401 is still an expired session", async () => {
    const { assign, removeItem } = stubEnv("stale-token", "/settings");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: "Not authenticated" }), { status: 401 })),
    );

    const { api } = await import("./api");
    await expect(api.setPassword("new-password")).rejects.toMatchObject({ status: 401 });

    expect(removeItem).toHaveBeenCalledWith("access_token");
    expect(assign).toHaveBeenCalledWith("/login?redirect=%2Fsettings");
  });
});

describe("admin API contract", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("getUserEvents sends limit/offset/category and reads total_count", async () => {
    stubEnv("token", "/admin");
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ events: [], total_count: 7, page: 1, page_size: 40 }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const { adminApi } = await import("./api");
    const resp = await adminApi.getUserEvents(40, 80, "u-1", undefined, "research");

    const url = new URL(fetchMock.mock.calls[0][0] as string, "http://host");
    expect(url.pathname).toBe("/v1/admin/users/events");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      limit: "40",
      offset: "80",
      user_id: "u-1",
      category: "research",
    });
    expect(resp.total_count).toBe(7);
  });
});

describe("apiErrorMessage", () => {
  it("maps frequent statuses to errors.api.* keys", async () => {
    stubEnv(null, "/");
    const { ApiError, apiErrorMessage } = await import("./api");
    const t = (key: string) => `[${key}]`;

    expect(apiErrorMessage(new ApiError(401, "nope"), t)).toBe("[errors.api.unauthorized]");
    expect(apiErrorMessage(new ApiError(429, "slow down"), t)).toBe("[errors.api.rateLimited]");
    expect(apiErrorMessage(new ApiError(500, "boom"), t)).toBe("[errors.api.server]");
  });

  it("falls back to the server detail, network and generic texts", async () => {
    stubEnv(null, "/");
    const { ApiError, apiErrorMessage } = await import("./api");
    const t = (key: string) => `[${key}]`;

    expect(apiErrorMessage(new ApiError(418, "teapot"), t)).toBe("teapot");
    expect(apiErrorMessage(new ApiError(418, ""), t)).toBe("[errors.api.unexpected]");
    expect(apiErrorMessage(new TypeError("fetch failed"), t)).toBe("[errors.api.network]");
    expect(apiErrorMessage(new Error("custom"), t)).toBe("custom");
  });
});
