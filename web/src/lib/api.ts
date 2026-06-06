import type {
  Conflict,
  CreateResearchResponse,
  Depth,
  ModelOption,
  PlanItem,
  ResearchGraph,
  ResearchHistoryItem,
  ResearchPlan,
  ResearchReport,
  ResearchStatusSummary,
  SourcePreview,
} from "./types";

// Empty base => same origin => Vite dev-proxy forwards /v1 to the backend.
// Set VITE_API_BASE (e.g. http://localhost:8000) for a non-proxied build.
const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    ...init,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${detail}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  listModels: () => request<ModelOption[]>("/v1/models"),

  listResearch: (limit = 30) =>
    request<ResearchHistoryItem[]>(`/v1/research?limit=${limit}`),

  createResearch: (body: { prompt: string; depth: Depth; model?: string; plan_first?: boolean }) =>
    request<CreateResearchResponse>("/v1/research", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getPlan: (id: string) => request<ResearchPlan>(`/v1/research/${id}/plan`),

  updatePlan: (id: string, items: PlanItem[]) =>
    request<ResearchPlan>(`/v1/research/${id}/plan`, {
      method: "PUT",
      body: JSON.stringify({ items }),
    }),

  approvePlan: (id: string) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/plan/approve`, { method: "POST" }),

  getReport: (id: string) =>
    request<ResearchReport>(`/v1/research/${id}/report`),

  getStatus: (id: string) =>
    request<ResearchStatusSummary>(`/v1/research/${id}/status`),

  getSources: (id: string) =>
    request<SourcePreview[]>(`/v1/research/${id}/sources`),

  getConflicts: (id: string) =>
    request<Conflict[]>(`/v1/research/${id}/conflicts`),

  getGraph: (id: string) =>
    request<ResearchGraph>(`/v1/research/${id}/graph`),

  deleteResearch: (id: string) =>
    request<void>(`/v1/research/${id}`, { method: "DELETE" }),
};
