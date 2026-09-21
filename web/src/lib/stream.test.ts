import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("streamChatAnswer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.stubGlobal("localStorage", {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });
    vi.stubGlobal("document", { cookie: "csrf_token=csrf%20value" });
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
    const onError = vi.fn();
    const onDone = vi.fn();
    await streamChatAnswer("research-id", "question", { onError, onDone });

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith("aborted");
    expect(onDone).not.toHaveBeenCalled();
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
    vi.stubGlobal("document", { cookie: "" });
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
    const onDone = vi.fn();
    const onError = vi.fn();
    openResearchStream("research-id", { onDone, onError });

    es.emit("stream_error", { detail: "worker hiccup" });
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledWith("worker hiccup");

    // The connection recovered and completed — the UI must reach the done state.
    es.emit("done", { status: "completed" });
    expect(onDone).toHaveBeenCalledOnce();
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
  vi.stubGlobal("document", { cookie: "csrf_token=csrf%20value" });
}
