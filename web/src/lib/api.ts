import type {
  AuthSession,
  AuthUser,
  ChatMessage,
  Clarification,
  ComparisonTable,
  Conflict,
  CreateResearchResponse,
  Depth,
  ModelOption,
  CitationAudit,
  PlanItem,
  RedTeamReport,
  ResearchDiff,
  ResearchGraph,
  ResearchHistoryItem,
  ResearchPlan,
  ResearchReport,
  ResearchStatusSummary,
  SourcePreview,
  VerificationReport,
} from "./types";

// Empty base => same origin => Vite dev-proxy forwards /v1 to the backend.
// Set VITE_API_BASE (e.g. http://localhost:8000) for a non-proxied build.
const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

// JWT bearer token — persisted so the session survives reloads (the httpOnly
// cookie is a parallel fallback, e.g. for SSE streams).
const TOKEN_KEY = "access_token";
let authToken: string | null = localStorage.getItem(TOKEN_KEY);
export function setAuthToken(token: string | null): void {
  authToken = token;
  if (token) localStorage.setItem(TOKEN_KEY, token);
  else localStorage.removeItem(TOKEN_KEY);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((init?.headers as Record<string, string>) ?? {}),
  };
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
  const res = await fetch(`${BASE}${path}`, {
    credentials: "include",
    ...init,
    headers,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${detail}`);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export const api = {
  me: () => request<AuthUser>("/v1/auth/me"),

  register: async (email: string, password: string) => {
    const res = await request<AuthSession>("/v1/auth/register", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    setAuthToken(res.access_token);
    return res;
  },

  login: async (email: string, password: string) => {
    const res = await request<AuthSession>("/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    setAuthToken(res.access_token);
    return res;
  },

  logout: async () => {
    try {
      return await request<{ status: string }>("/v1/auth/logout", { method: "POST" });
    } finally {
      setAuthToken(null);
    }
  },

  authConfig: () => request<{ google_oauth: boolean }>("/v1/auth/config"),

  setPassword: (password: string) =>
    request<{ status: string }>("/v1/auth/set-password", {
      method: "POST",
      body: JSON.stringify({ password }),
    }),

  // Full-page navigation to start the Google OAuth redirect flow.
  googleLoginUrl: () => `${BASE}/v1/auth/google/login`,

  listModels: () => request<ModelOption[]>("/v1/models"),

  listResearch: (limit = 30) =>
    request<ResearchHistoryItem[]>(`/v1/research?limit=${limit}`),

  createResearch: (body: { prompt: string; depth: Depth; model?: string; plan_first?: boolean; thread_id?: string }) =>
    request<CreateResearchResponse>("/v1/research", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  getThread: (threadId: string) =>
    request<ResearchHistoryItem[]>(`/v1/threads/${threadId}`),

  getClarifications: (id: string) =>
    request<Clarification>(`/v1/research/${id}/clarifications`),

  submitClarify: (id: string, answers: string[]) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/clarify`, {
      method: "POST",
      body: JSON.stringify({ answers }),
    }),

  getPlan: (id: string) => request<ResearchPlan>(`/v1/research/${id}/plan`),

  updatePlan: (id: string, items: PlanItem[]) =>
    request<ResearchPlan>(`/v1/research/${id}/plan`, {
      method: "PUT",
      body: JSON.stringify({ items }),
    }),

  approvePlan: (id: string) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/plan/approve`, { method: "POST" }),

  getMessages: (id: string) => request<ChatMessage[]>(`/v1/research/${id}/messages`),

  askResearch: (id: string, question: string) =>
    request<ChatMessage>(`/v1/research/${id}/messages`, {
      method: "POST",
      body: JSON.stringify({ question }),
    }),

  getReport: (id: string) =>
    request<ResearchReport>(`/v1/research/${id}/report`),

  getStatus: (id: string) =>
    request<ResearchStatusSummary>(`/v1/research/${id}/status`),

  getSources: (id: string) =>
    request<SourcePreview[]>(`/v1/research/${id}/sources`),

  getConflicts: (id: string) =>
    request<Conflict[]>(`/v1/research/${id}/conflicts`),

  getVerification: (id: string) =>
    request<VerificationReport>(`/v1/research/${id}/verification`),

  getRedTeam: (id: string) =>
    request<RedTeamReport>(`/v1/research/${id}/red-team`),

  getCitations: (id: string) =>
    request<CitationAudit>(`/v1/research/${id}/citations`),

  getDiff: (id: string) =>
    request<ResearchDiff>(`/v1/research/${id}/diff`),

  getComparison: (id: string) =>
    request<ComparisonTable>(`/v1/research/${id}/comparison`),

  refreshResearch: (id: string) =>
    request<CreateResearchResponse>(`/v1/research/${id}/refresh`, { method: "POST" }),

  getGraph: (id: string) =>
    request<ResearchGraph>(`/v1/research/${id}/graph`),

  deleteResearch: (id: string) =>
    request<void>(`/v1/research/${id}`, { method: "DELETE" }),

  renameResearch: (id: string, title: string) =>
    request<{ id: string }>(`/v1/research/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ title }),
    }),
};
