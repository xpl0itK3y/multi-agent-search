// SSE client for the live research stream (F1).
// In dev the Vite proxy serves /v1 from the same origin, so EventSource works directly.

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

export interface StreamHandlers {
  onStatus?: (status: string) => void;
  onTrace?: (step: string, detail: string) => void;
  onReasoning?: (reasoning: string) => void;
  onReport?: (report: string, final: boolean) => void;
  onDone?: (status: string) => void;
  onError?: (message: string) => void;
}

export function openResearchStream(id: string, h: StreamHandlers): () => void {
  const es = new EventSource(`${BASE}/v1/research/${id}/events`, {
    withCredentials: true,
  });

  const parse = (e: Event) => JSON.parse((e as MessageEvent).data);

  es.addEventListener("status_change", (e) => h.onStatus?.(parse(e).status));
  es.addEventListener("trace_step", (e) => {
    const d = parse(e);
    h.onTrace?.(d.step ?? "", d.detail ?? "");
  });
  es.addEventListener("reasoning_delta", (e) => h.onReasoning?.(parse(e).reasoning ?? ""));
  es.addEventListener("report", (e) => {
    const d = parse(e);
    h.onReport?.(d.report ?? "", Boolean(d.final));
  });
  es.addEventListener("stream_error", (e) => h.onError?.(parse(e).detail ?? "stream error"));
  es.addEventListener("done", (e) => {
    h.onDone?.(parse(e).status ?? "completed");
    es.close();
  });
  // Connection-level failures (not the server's named "stream_error").
  es.onerror = () => {
    if (es.readyState === EventSource.CLOSED) h.onError?.("Соединение со стримом разорвано");
  };

  return () => es.close();
}

export interface ChatStreamHandlers {
  onDelta?: (answer: string) => void;
  onDone?: (answer: string) => void;
  onError?: (message: string) => void;
}

// Chat answers stream over a POST (question in body), which EventSource can't do —
// so we parse the SSE stream manually over a fetch ReadableStream.
export async function streamChatAnswer(
  id: string,
  question: string,
  h: ChatStreamHandlers,
): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`${BASE}/v1/research/${id}/messages/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ question }),
    });
  } catch (e) {
    h.onError?.((e as Error).message);
    return;
  }
  if (!res.ok || !res.body) {
    h.onError?.(`${res.status} ${await res.text().catch(() => res.statusText)}`);
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
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
      else if (event === "done") {
        h.onDone?.(parsed.answer ?? "");
        return;
      } else if (event === "stream_error") {
        h.onError?.(parsed.detail ?? "stream error");
        return;
      }
    }
  }
}
