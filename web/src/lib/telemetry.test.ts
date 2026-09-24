// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type Telemetry = typeof import("./telemetry");
let loaded: Telemetry | null = null;

// telemetry.ts keeps module state (active user, listeners): import a fresh copy per test.
async function loadTelemetry(): Promise<Telemetry> {
  vi.resetModules();
  loaded = await import("./telemetry");
  return loaded;
}

function sentEvents(fetchMock: ReturnType<typeof vi.fn>) {
  return fetchMock.mock.calls.map(([url, init]) => ({
    url: url as string,
    headers: (init as RequestInit).headers as Record<string, string>,
    body: JSON.parse((init as RequestInit).body as string),
  }));
}

describe("telemetry client", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock = vi.fn().mockResolvedValue(new Response("{}"));
    vi.stubGlobal("fetch", fetchMock);
    vi.stubEnv("VITE_API_BASE", "https://api.example.test");
    sessionStorage.clear();
    localStorage.setItem("access_token", "tok-123");
    document.cookie = "csrf_token=csrf-abc";
  });

  afterEach(() => {
    // Silence this test's copy: its document listeners outlive the module reset.
    loaded?.stopTelemetry();
    loaded = null;
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    localStorage.clear();
  });

  it("sends nothing while signed out", async () => {
    const { trackEvent } = await loadTelemetry();

    trackEvent("tab_focus");
    await vi.advanceTimersByTimeAsync(60_000);

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("announces the session with device info through BASE and the auth headers", async () => {
    const { startTelemetry } = await loadTelemetry();

    startTelemetry("u1");

    const [start] = sentEvents(fetchMock);
    expect(start.url).toBe("https://api.example.test/v1/telemetry/event");
    expect(start.headers).toMatchObject({
      Authorization: "Bearer tok-123",
      "X-CSRF-Token": "csrf-abc",
    });
    expect(start.body).toMatchObject({ event_name: "session_start", event_category: "system" });
    expect(start.body.device_info).toMatchObject({ device_type: expect.any(String) });
  });

  it("heartbeats while signed in and goes quiet after sign-out", async () => {
    const { startTelemetry, stopTelemetry } = await loadTelemetry();
    startTelemetry("u1");

    await vi.advanceTimersByTimeAsync(30_000);
    expect(sentEvents(fetchMock).map((e) => e.body.event_name)).toEqual(["session_start", "heartbeat"]);

    stopTelemetry();
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.advanceTimersByTimeAsync(90_000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("mints a new session id on sign-in and on sign-out", async () => {
    const { startTelemetry, stopTelemetry } = await loadTelemetry();
    sessionStorage.setItem("telemetry_session_id", "sess_previous_user");

    startTelemetry("u2", { newSession: true });
    const signedIn = sentEvents(fetchMock)[0].body.session_id;
    stopTelemetry();

    expect(signedIn).not.toBe("sess_previous_user");
    expect(sessionStorage.getItem("telemetry_session_id")).not.toBe(signedIn);
  });

  it("resumes a restored session under its existing id and announces it once", async () => {
    const { startTelemetry } = await loadTelemetry();
    sessionStorage.setItem("telemetry_session_id", "sess_same_tab");

    startTelemetry("u1");
    startTelemetry("u1");

    const events = sentEvents(fetchMock);
    expect(events).toHaveLength(1);
    expect(events[0].body.session_id).toBe("sess_same_tab");
  });
});
