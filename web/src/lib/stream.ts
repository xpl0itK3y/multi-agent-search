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
