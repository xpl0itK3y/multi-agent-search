// SSE client for the live research stream (F1).
// In dev the Vite proxy serves /v1 from the same origin, so EventSource works directly.

import { authHeaders } from "./api";
import type { SourcePreview } from "./types";

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

export interface TraceSource {
  domain: string;
  title?: string;
}

export interface StreamHandlers {
  onStatus?: (status: string) => void;
  onTrace?: (step: string, detail: string, sources?: TraceSource[]) => void;
  onReasoning?: (reasoning: string) => void;
  onReport?: (report: string, final: boolean) => void;
  onDone?: (status: string) => void;
  onError?: (message: string) => void;
}

export function openResearchStream(id: string, h: StreamHandlers): () => void {
  // The UI treats onDone / a fatal onError as "finished". Exactly one of them
  // may fire per connection — the flag guarantees no double-firing (e.g. a
  // "done" event followed by the CLOSED error path).
  let terminated = false;
  const done = (status: string) => {
    if (terminated) return;
    terminated = true;
    h.onDone?.(status);
  };
  const fail = (message: string) => {
    if (terminated) return;
    terminated = true;
    h.onError?.(message);
  };

  const es = new EventSource(`${BASE}/v1/research/${id}/events`, {
    withCredentials: true,
  });

  const parse = (e: Event) => JSON.parse((e as MessageEvent).data);

  es.addEventListener("status_change", (e) => h.onStatus?.(parse(e).status));
  es.addEventListener("trace_step", (e) => {
    const d = parse(e);
    h.onTrace?.(d.step ?? "", d.detail ?? "", d.sources ?? []);
  });
  es.addEventListener("reasoning_delta", (e) => h.onReasoning?.(parse(e).reasoning ?? ""));
  es.addEventListener("report", (e) => {
    const d = parse(e);
    h.onReport?.(d.report ?? "", Boolean(d.final));
  });
  // The server's named error is not terminal for this connection — it keeps
  // listening (the connection may still recover and deliver "done").
  es.addEventListener("stream_error", (e) => {
    if (!terminated) h.onError?.(parse(e).detail ?? "stream error");
  });
  es.addEventListener("done", (e) => {
    done(parse(e).status ?? "completed");
    es.close();
  });
  // Connection-level failures (not the server's named "stream_error").
  // readyState CONNECTING is a transient drop — EventSource retries on its own.
  es.onerror = () => {
    if (es.readyState === EventSource.CLOSED) fail("Соединение со стримом разорвано");
  };

  return () => {
    // Manual close is a deliberate unsubscribe, not a terminal event.
    terminated = true;
    es.close();
  };
}

export interface ChatStreamHandlers {
  onDelta?: (answer: string) => void;
  onSearching?: () => void;
  onDone?: (answer: string, sources: SourcePreview[]) => void;
  onError?: (message: string) => void;
}

// Chat answers stream over a POST (question in body), which EventSource can't do —
// so we parse the SSE stream manually over a fetch ReadableStream.
export async function streamChatAnswer(
  id: string,
  question: string,
  h: ChatStreamHandlers,
): Promise<void> {
  // Either onDone or onError fires exactly once — whichever path ends the
  // stream — so the caller's "streaming" state can never hang.
  let terminated = false;
  const done = (answer: string, sources: SourcePreview[]) => {
    if (terminated) return;
    terminated = true;
    h.onDone?.(answer, sources);
  };
  const fail = (message: string) => {
    if (terminated) return;
    terminated = true;
    h.onError?.(message);
  };

  let res: Response;
  try {
    res = await fetch(`${BASE}/v1/research/${id}/messages/stream`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders("POST"),
      },
      credentials: "include",
      body: JSON.stringify({ question }),
    });
  } catch (e) {
    fail((e as Error).message);
    return;
  }
  if (!res.ok || !res.body) {
    fail(`${res.status} ${await res.text().catch(() => res.statusText)}`);
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { value, done: readableDone } = await reader.read();
      if (readableDone) break;
      buffer += decoder.decode(value, { stream: true });

      let sep: number;
      while ((sep = buffer.indexOf("\n\n")) !== -1) {
        const rawEvent = buffer.slice(0, sep);
        buffer = buffer.slice(sep + 2);

        let event = "message";
        let data = "";
        for (const line of rawEvent.split("\n")) {
          if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) data += line.slice(5).trim();
          // lines starting with ":" are keep-alive comments — ignore
        }
        if (!data) continue;

        const parsed = JSON.parse(data);
        if (event === "delta") h.onDelta?.(parsed.answer ?? "");
        else if (event === "searching") h.onSearching?.();
        else if (event === "done") {
          done(parsed.answer ?? "", parsed.sources ?? []);
          return;
        } else if (event === "stream_error") {
          fail(parsed.detail ?? "stream error");
          return;
        }
      }
    }
    // The body ended without a terminal `done`/`stream_error` event (server
    // crash, proxy timeout, truncated response) — fire a terminal error
    // ourselves so the UI never stays stuck in "streaming".
    fail("Соединение прервано до завершения ответа");
  } catch (e) {
    fail((e as Error).message);
  }
}
