import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { i18n, LOCALES } from "@/i18n";

// api.ts touches localStorage / document.cookie at import time and
// window.location on 401 recovery — stub the globals before each dynamic import.
function stubEnv(token: string | null, pathname: string, search = "", cookie = "") {
  const assign = vi.fn();
  const removeItem = vi.fn();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn(() => token),
    setItem: vi.fn(),
    removeItem,
  });
  vi.stubGlobal("document", { cookie });
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

describe("file downloads", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("report export and admin CSVs send the bearer token and read the filename", async () => {
    stubEnv("access", "/admin");
    const fetchMock = vi.fn().mockImplementation(async () =>
      new Response("a,b\n", {
        status: 200,
        headers: { "Content-Disposition": "attachment; filename=token_usage.csv" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const { api, adminApi } = await import("./api");
    const report = await api.exportReport("r-1", new URLSearchParams({ format: "pdf" }));
    const csv = await adminApi.exportTokensCsv();

    const calls = fetchMock.mock.calls as [string, RequestInit][];
    expect(calls.map(([url]) => url)).toEqual(["/v1/research/r-1/export?format=pdf", "/v1/admin/tokens/export"]);
    for (const [, init] of calls) {
      expect(init).toMatchObject({ credentials: "include", headers: { Authorization: "Bearer access" } });
    }
    expect(report.filename).toBe("token_usage.csv");
    expect(await csv.blob.text()).toBe("a,b\n");
  });

  it("downloads echo the csrf cookie so admin CSV exports work with a cookie session", async () => {
    // A Google sign-in leaves no bearer token: the session is the cookie alone.
    stubEnv(null, "/admin", "", "theme=dark; csrf_token=c%2Bsrf%3D; x=1");
    const fetchMock = vi.fn().mockImplementation(async () => new Response("a,b\n", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const { api, adminApi } = await import("./api");
    await adminApi.exportUsersCsv();
    await adminApi.exportPromptsCsv();
    await adminApi.exportTokensCsv();
    await api.exportReport("r-1", new URLSearchParams({ format: "md" }));
    await api.getReport("r-1").catch(() => undefined);

    const calls = fetchMock.mock.calls as [string, RequestInit][];
    for (const [url, init] of calls.slice(0, 4)) {
      expect(init.headers, url).toEqual({ "X-CSRF-Token": "c+srf=" });
    }
    // Other GETs stay without it: only the downloads opt in.
    expect(calls[4][1].headers).not.toHaveProperty("X-CSRF-Token");
  });

  it("downloads send the bearer and the csrf token when both exist", async () => {
    stubEnv("access", "/admin", "", "csrf_token=tok");
    const fetchMock = vi.fn().mockImplementation(async () => new Response("", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const { adminApi } = await import("./api");
    await adminApi.exportUsersCsv();

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init.headers).toEqual({ Authorization: "Bearer access", "X-CSRF-Token": "tok" });
  });

  it("a 401 on a download goes through the shared session recovery", async () => {
    const { assign, removeItem } = stubEnv("stale-token", "/admin");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));

    const { adminApi, ApiError } = await import("./api");
    const err: unknown = await adminApi.exportUsersCsv().catch((e) => e);

    expect(err).toBeInstanceOf(ApiError);
    expect(removeItem).toHaveBeenCalledWith("access_token");
    expect(assign).toHaveBeenCalledWith("/login?redirect=%2Fadmin");
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

  it("names the known 409 reasons instead of one generic conflict text", async () => {
    stubEnv(null, "/");
    const { ApiError, apiErrorMessage } = await import("./api");
    const t = (key: string) => `[${key}]`;

    // The server's ConflictError texts (src/services): they carry no error code.
    const cases: [string, string][] = [
      ["A research is already in progress. Please wait for it to finish before starting another.", "researchInProgress"],
      ["Research capacity is currently full or the research state changed. Please retry.", "capacityFull"],
      ["Email already registered", "emailTaken"],
      ["An account with this email already exists", "emailTaken"],
      ["Report is not ready yet", "reportNotReady"],
      ["Only dead-letter finalize jobs can be requeued", "notDeadLetter"],
      ["Only dead-letter search jobs can be requeued", "notDeadLetter"],
      ["Research state changed. Please retry.", "conflict"],
      ["<b>some other internal text</b>", "conflict"],
    ];
    for (const [detail, key] of cases) {
      expect(apiErrorMessage(new ApiError(409, detail), t), detail).toBe(`[errors.api.${key}]`);
      for (const { value } of LOCALES) expect(i18n.global.te(`errors.api.${key}`, value), `${value}: ${key}`).toBe(true);
    }
    // The detail refines only its own status.
    expect(apiErrorMessage(new ApiError(404, "Report is not ready yet"), t)).toBe("[errors.api.notFound]");
  });

  it("explains a refused sign-up with an administrator's email", async () => {
    stubEnv(null, "/");
    const { ApiError, apiErrorMessage } = await import("./api");
    const t = (key: string) => `[${key}]`;
    const detail =
      "This email is reserved for an administrator: sign in with Google, or ask the operator to provision it with scripts/create_admin.py";

    expect(apiErrorMessage(new ApiError(403, detail), t)).toBe("[errors.api.adminEmailReserved]");
    for (const { value } of LOCALES) expect(i18n.global.te("errors.api.adminEmailReserved", value)).toBe(true);
    expect(apiErrorMessage(new ApiError(403, "Admin privileges required"), t)).toBe("[errors.api.forbidden]");
  });
});
