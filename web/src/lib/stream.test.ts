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
      new Response('event: done\ndata: {"answer":"ok"}\n\n', { status: 200 }),
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
    expect(onDone).toHaveBeenCalledWith("ok");
  });
});
