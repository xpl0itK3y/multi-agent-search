// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// stream.ts localizes its errors through vue-i18n, which needs a real DOM at
// import time — so set the CSRF cookie on jsdom's document instead of stubbing it.
function setCsrfCookie(value: string | null) {
  document.cookie =
    value === null ? "csrf_token=; expires=Thu, 01 Jan 1970 00:00:00 GMT" : `csrf_token=${value}`;
}

describe("streamChatAnswer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubGlobal("localStorage", {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });
    setCsrfCookie("csrf%20value");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("sends bearer and CSRF credentials with the streaming POST", async () => {
    const { setAuthToken } = await import("./api");
    setAuthToken("test-access-token");

    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        'event: done\ndata: {"answer":"ok","sources":[{"source_id":"S4","url":"https://example.com"}]}\n\n',
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const { streamChatAnswer } = await import("./stream");
    const onDone = vi.fn();
    await streamChatAnswer("research-id", "question", { onDone });

    expect(fetchMock).toHaveBeenCalledOnce();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init).toMatchObject({
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer test-access-token",
        "X-CSRF-Token": "csrf value",
      },
    });
    expect(onDone).toHaveBeenCalledWith("ok", [
      { source_id: "S4", url: "https://example.com" },
    ]);
  });

  it("fires onError exactly once when the body ends without a terminal event", async () => {
    stubEnv();
    const fetchMock = vi.fn().mockResolvedValue(
      // A delta but no `done` — the stream is truncated mid-answer.
      new Response('event: delta\ndata: {"answer":"partial"}\n\n', { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const { streamChatAnswer } = await import("./stream");
    const onDelta = vi.fn();
    const onDone = vi.fn();
    const onError = vi.fn();
    await streamChatAnswer("research-id", "question", { onDelta, onDone, onError });

    expect(onDelta).toHaveBeenCalledWith("partial");
    expect(onDone).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0]).toEqual(expect.any(String));
  });

  it("fires onError once when reading the body throws mid-stream", async () => {
    stubEnv();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('event: delta\ndata: {"answer":"par"}\n\n'));
        controller.error(new Error("aborted"));
      },
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body, { status: 200 })));

    const { streamChatAnswer } = await import("./stream");
    const { i18n } = await import("@/i18n");
    const onError = vi.fn();
    const onDone = vi.fn();
    await streamChatAnswer("research-id", "question", { onError, onDone });

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith(i18n.global.t("errors.stream.incomplete"));
    expect(onDone).not.toHaveBeenCalled();
  });

  it("never surfaces raw server or HTTP error text", async () => {
    stubEnv();
    const { streamChatAnswer } = await import("./stream");
    const { i18n } = await import("@/i18n");
    const t = (key: string) => i18n.global.t(key);

    // stream_error carrying an internal exception string → generic chat text.
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response('event: stream_error\ndata: {"detail":"KeyError: \'report\'"}\n\n', { status: 200 }),
      ),
    );
    const onStreamError = vi.fn();
    await streamChatAnswer("research-id", "question", { onError: onStreamError });
    expect(onStreamError).toHaveBeenCalledWith(t("errors.stream.chatFailed"));

    // A non-2xx response goes through the errors.api.* mapping.
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: "slow down" }), { status: 429 })),
    );
    const onHttpError = vi.fn();
    await streamChatAnswer("research-id", "question", { onError: onHttpError });
    expect(onHttpError).toHaveBeenCalledWith(t("errors.api.rateLimited"));
  });
});

// Minimal EventSource double: captures listeners so tests can emit events and
// drive the connection-level error path.
class FakeEventSource {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  readyState = FakeEventSource.CONNECTING;
  onerror: (() => void) | null = null;
  private listeners = new Map<string, Array<(e: { data: string }) => void>>();

  constructor(public url: string) {}

  addEventListener(type: string, fn: (e: { data: string }) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn]);
  }

  emit(type: string, data: unknown): void {
    for (const fn of this.listeners.get(type) ?? []) fn({ data: JSON.stringify(data) });
  }

  close(): void {
    this.readyState = FakeEventSource.CLOSED;
  }
}

describe("openResearchStream", () => {
  let es: FakeEventSource;

  function stubEnv() {
    vi.resetModules();
    vi.stubGlobal("localStorage", {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });
    setCsrfCookie(null);
    const cls = class extends FakeEventSource {
      constructor(url: string) {
        super(url);
        es = this;
      }
    };
    vi.stubGlobal("EventSource", cls);
    return cls;
  }

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("fires onDone exactly once; later terminal paths cannot double-fire", async () => {
    stubEnv();
    const { openResearchStream } = await import("./stream");
    const onDone = vi.fn();
    const onError = vi.fn();
    openResearchStream("research-id", { onDone, onError });

    es.emit("done", { status: "completed" });
    expect(onDone).toHaveBeenCalledTimes(1);
    expect(onDone).toHaveBeenCalledWith("completed");

    // Duplicate done event and the CLOSED error path must both stay silent.
    es.emit("done", { status: "completed" });
    es.onerror?.();
    expect(onDone).toHaveBeenCalledOnce();
    expect(onError).not.toHaveBeenCalled();
  });

  it("fires onError once when the connection dies before done (CLOSED)", async () => {
    stubEnv();
    const { openResearchStream } = await import("./stream");
    const onDone = vi.fn();
    const onError = vi.fn();
    openResearchStream("research-id", { onDone, onError });

    es.readyState = FakeEventSource.CLOSED;
    es.onerror?.();
    es.onerror?.();
    expect(onError).toHaveBeenCalledOnce();
    expect(onDone).not.toHaveBeenCalled();
  });

  it("keeps stream_error non-terminal — a later done still fires onDone", async () => {
    stubEnv();
    const { openResearchStream } = await import("./stream");
    const { i18n } = await import("@/i18n");
    const onDone = vi.fn();
    const onError = vi.fn();
    openResearchStream("research-id", { onDone, onError });

    es.emit("stream_error", { detail: "worker hiccup" });
    expect(onError).toHaveBeenCalledTimes(1);
    // Internal detail text is never shown raw.
    expect(onError).toHaveBeenCalledWith(i18n.global.t("errors.stream.failed"));

    // The connection recovered and completed — the UI must reach the done state.
    es.emit("done", { status: "completed" });
    expect(onDone).toHaveBeenCalledOnce();
  });

  it("maps server stream codes and connection loss to localized messages", async () => {
    stubEnv();
    const { openResearchStream } = await import("./stream");
    const { i18n } = await import("@/i18n");
    const onError = vi.fn();
    openResearchStream("research-id", { onError });

    es.emit("stream_error", { detail: "stream_timeout" });
    es.readyState = FakeEventSource.CLOSED;
    es.onerror?.();

    expect(onError.mock.calls).toEqual([
      [i18n.global.t("errors.stream.timeout")],
      [i18n.global.t("errors.stream.connectionLost")],
    ]);
    expect(onError.mock.calls.flat()).not.toContain("stream_timeout");
  });

  it("parses and forwards trace_step events with full agent metadata", async () => {
    stubEnv();
    const { openResearchStream } = await import("./stream");
    const onTrace = vi.fn();
    openResearchStream("research-id", { onTrace });

    es.emit("trace_step", {
      step: "search",
      agent: "SearchAgent",
      phase: "search",
      action: "query",
      detail: "Searching web",
      sources: [{ domain: "example.com", title: "Example" }],
      metrics: { count: 5 },
    });

    expect(onTrace).toHaveBeenCalledOnce();
    const entry = onTrace.mock.calls[0][0];
    expect(entry.step).toBe("search");
    expect(entry.agent).toBe("SearchAgent");
    expect(entry.phase).toBe("search");
    expect(entry.action).toBe("query");
    expect(entry.detail).toBe("Searching web");
    expect(entry.sources).toHaveLength(1);
    expect(entry.metrics).toEqual({ count: 5 });
  });
});

function stubEnv() {
  vi.stubGlobal("localStorage", {
    getItem: vi.fn(() => null),
    setItem: vi.fn(),
    removeItem: vi.fn(),
  });
  setCsrfCookie("csrf%20value");
}
