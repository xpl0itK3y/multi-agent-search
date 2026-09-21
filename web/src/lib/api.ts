import type {
  AdminAuditLogItem,
  AdminDryRunResult,
  AdminEventLogResponse,
  AdminOverviewResponse,
  AdminPromptsResponse,
  AdminTelemetrySummaryResponse,
  AdminTokenAnalyticsResponse,
  AdminUserDetailResponse,
  AdminUserListResponse,
  AgentMetadataItem,
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
  SourceIndependence,
  SourceReputation,
  SourceIntegrity,
  CrossLanguageReport,
  StanceBalance,
  ShareInfo,
  PublicReport,
  NumericCheck,
  ConfidenceReport,
  PlanItem,
  RedTeamReport,
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

function readCookie(name: string): string | null {
  const escaped = name.replace(/([.*+?^${}()|[\]\\])/g, "\\$1");
  const match = document.cookie.match(new RegExp("(?:^|; )" + escaped + "=([^;]*)"));
  return match ? decodeURIComponent(match[1]) : null;
}

export function authHeaders(method = "GET"): Record<string, string> {
  const headers: Record<string, string> = {};
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;

  // Double-submit CSRF token for cookie-authenticated mutations (e.g. after Google OAuth,
  // where there is no Bearer token). Safe methods don't need it.
  if (!["GET", "HEAD", "OPTIONS"].includes(method.toUpperCase())) {
    const csrf = readCookie("csrf_token");
    if (csrf) headers["X-CSRF-Token"] = csrf;
  }
  return headers;
}

// Typed error for every non-2xx API response. `detail` is the server-provided
// reason (FastAPI's `detail` field when present), `status` the HTTP code.
export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(`${status} ${detail}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

// FastAPI errors are `{"detail": ...}` — prefer that, then any JSON string
// body, then a truncated non-JSON body, and finally the status text.
async function errorDetail(res: Response): Promise<string> {
  const raw = (await res.text().catch(() => "")).trim();
  if (raw) {
    try {
      const body: unknown = JSON.parse(raw);
      if (typeof body === "string" && body.trim()) return body.trim();
      if (body && typeof body === "object") {
        const detail = (body as { detail?: unknown }).detail;
        if (typeof detail === "string" && detail.trim()) return detail.trim();
        if (detail != null) return JSON.stringify(detail);
      }
    } catch {
      // Not JSON (e.g. an HTML error page) — show a slice of the raw body.
      return raw.length > 200 ? `${raw.slice(0, 200)}…` : raw;
    }
  }
  return res.statusText || `HTTP ${res.status}`;
}

export async function apiErrorFromResponse(res: Response): Promise<ApiError> {
  return new ApiError(res.status, await errorDetail(res));
}

// Routes that legitimately make unauthenticated calls — the public share page,
// and /login itself (redirecting there from a failed sign-in would loop).
function isPublicPath(pathname: string): boolean {
  return pathname === "/login" || pathname.startsWith("/r/");
}

// A stored bearer token the server just rejected is stale — drop it and bounce
// to /login (with a `redirect` back param) so the user can re-authenticate.
function recoverFromExpiredSession(hadToken: boolean): void {
  if (!hadToken) return;
  const { pathname, search } = window.location;
  if (isPublicPath(pathname)) return;
  setAuthToken(null);
  window.location.assign(`/login?redirect=${encodeURIComponent(pathname + search)}`);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const method = init?.method ?? "GET";
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((init?.headers as Record<string, string>) ?? {}),
    ...authHeaders(method),
  };
  const hadToken = authToken !== null;
  const res = await fetch(`${BASE}${path}`, {
    credentials: "include",
    ...init,
    headers,
  });
  if (!res.ok) {
    if (res.status === 401) recoverFromExpiredSession(hadToken);
    throw await apiErrorFromResponse(res);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

// Frequent HTTP statuses → `errors.api.*` i18n keys (see web/src/i18n/index.ts).
const API_STATUS_KEYS: Record<number, string> = {
  401: "unauthorized",
  403: "forbidden",
  404: "notFound",
  409: "conflict",
  422: "validation",
  429: "rateLimited",
  500: "server",
};

// Localized, user-facing message for errors thrown by `api`/`fetch`. Mapped
// statuses get a translated text; anything else falls back to the server
// detail, and network-level failures (fetch's TypeError) to a network text.
export function apiErrorMessage(err: unknown, t: (key: string) => string): string {
  if (err instanceof ApiError) {
    const key = API_STATUS_KEYS[err.status];
    if (key) return t(`errors.api.${key}`);
    return err.detail || t("errors.api.unexpected");
  }
  if (err instanceof TypeError) return t("errors.api.network");
  if (err instanceof Error && err.message) return err.message;
  return t("errors.api.unexpected");
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

  setPassword: async (password: string, currentPassword?: string) => {
    const res = await request<AuthSession>("/v1/auth/set-password", {
      method: "POST",
      body: JSON.stringify({ password, current_password: currentPassword }),
    });
    setAuthToken(res.access_token);
    return res;
  },

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

  getSourceIndependence: (id: string) =>
    request<SourceIndependence>(`/v1/research/${id}/source-independence`),

  getSourceReputation: (id: string) =>
    request<SourceReputation>(`/v1/research/${id}/source-reputation`),

  getSourceIntegrity: (id: string) =>
    request<SourceIntegrity>(`/v1/research/${id}/source-integrity`),

  getCrossLanguage: (id: string) =>
    request<CrossLanguageReport>(`/v1/research/${id}/cross-language`),

  getStance: (id: string) =>
    request<StanceBalance>(`/v1/research/${id}/stance`),

  getShare: (id: string) =>
    request<ShareInfo>(`/v1/research/${id}/share`),
  createShare: (id: string) =>
    request<ShareInfo>(`/v1/research/${id}/share`, { method: "POST" }),
  revokeShare: (id: string) =>
    request<ShareInfo>(`/v1/research/${id}/share`, { method: "DELETE" }),
  getPublicReport: (token: string) =>
    request<PublicReport>(`/v1/public/research/${encodeURIComponent(token)}`),

  getConfidence: (id: string) =>
    request<ConfidenceReport>(`/v1/research/${id}/confidence`),

  getNumericCheck: (id: string) =>
    request<NumericCheck>(`/v1/research/${id}/numeric-check`),

  getComparison: (id: string) =>
    request<ComparisonTable>(`/v1/research/${id}/comparison`),

  getGraph: (id: string) =>
    request<ResearchGraph>(`/v1/research/${id}/graph`),

  deleteResearch: (id: string) =>
    request<void>(`/v1/research/${id}`, { method: "DELETE" }),

  cancelResearch: (id: string) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/cancel`, { method: "POST" }),

  renameResearch: (id: string, title: string) =>
    request<{ id: string }>(`/v1/research/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ title }),
    }),
};

export const adminApi = {
  getUsers: (
    page: number = 1,
    pageSize: number = 20,
    search?: string,
    role?: string,
    onlineOnly: boolean = false,
    sortBy: string = "activity"
  ) => {
    let url = `/v1/admin/users?page=${page}&page_size=${pageSize}&online_only=${onlineOnly}&sort_by=${sortBy}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    if (role) url += `&role=${encodeURIComponent(role)}`;
    return request<AdminUserListResponse>(url);
  },

  getUserDetail: (userId: string) =>
    request<AdminUserDetailResponse>(`/v1/admin/users/${userId}`),

  getTelemetrySummary: () =>
    request<AdminTelemetrySummaryResponse>("/v1/admin/users/analytics/summary"),

  getUserEvents: (
    page: number = 1,
    pageSize: number = 50,
    userId?: string,
    eventName?: string,
    eventCategory?: string
  ) => {
    let url = `/v1/admin/users/events?page=${page}&page_size=${pageSize}`;
    if (userId) url += `&user_id=${encodeURIComponent(userId)}`;
    if (eventName) url += `&event_name=${encodeURIComponent(eventName)}`;
    if (eventCategory) url += `&event_category=${encodeURIComponent(eventCategory)}`;
    return request<AdminEventLogResponse>(url);
  },

  exportUsersCsvUrl: () => `${BASE}/v1/admin/users/export`,

  getPrompts: (
    page: number = 1,
    pageSize: number = 25,
    search?: string,
    userId?: string,
    promptType?: string
  ) => {
    let url = `/v1/admin/prompts?page=${page}&page_size=${pageSize}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    if (userId) url += `&user_id=${encodeURIComponent(userId)}`;
    if (promptType && promptType !== "all") url += `&prompt_type=${encodeURIComponent(promptType)}`;
    return request<AdminPromptsResponse>(url);
  },

  exportPromptsCsvUrl: () => `${BASE}/v1/admin/prompts/export`,

  deleteUser: (userId: string) =>
    request<{ status: string; deleted_user_id: string }>(`/v1/admin/users/${userId}`, {
      method: "DELETE",
    }),

  getOverview: () => request<AdminOverviewResponse>("/v1/admin/overview"),

  getTokens: (page: number = 1, pageSize: number = 20) =>
    request<AdminTokenAnalyticsResponse>(`/v1/admin/tokens?page=${page}&page_size=${pageSize}`),

  exportTokensCsvUrl: () => `${BASE}/v1/admin/tokens/export`,

  getAgents: () => request<AgentMetadataItem[]>("/v1/admin/agents"),

  getAuditLogs: (limit: number = 50, offset: number = 0, action?: string) => {
    let url = `/v1/admin/audit?limit=${limit}&offset=${offset}`;
    if (action) url += `&action=${encodeURIComponent(action)}`;
    return request<AdminAuditLogItem[]>(url);
  },

  previewOperation: (action: string, params: Record<string, any> = {}) =>
    request<AdminDryRunResult>("/v1/admin/operations/preview", {
      method: "POST",
      body: JSON.stringify({ action, params }),
    }),

  executeOperation: (action: string, params: Record<string, any> = {}) =>
    request<AdminDryRunResult>("/v1/admin/operations/execute", {
      method: "POST",
      body: JSON.stringify({ action, params }),
    }),

  connectStream: (
    onOverview: (data: AdminOverviewResponse) => void,
    onError?: (err: any) => void
  ) => {
    const url = `${BASE}/v1/admin/stream`;
    const es = new EventSource(url, { withCredentials: true });
    es.addEventListener("overview", (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data);
        onOverview(data);
      } catch (err) {
        if (onError) onError(err);
      }
    });
    es.onerror = (e) => {
      if (onError) onError(e);
    };
    return () => es.close();
  },
};

export const telemetryApi = {
  recordEvent: (payload: {
    session_id?: string;
    event_name: string;
    event_category?: string;
    details?: Record<string, any>;
    device_info?: Record<string, any>;
  }) =>
    request<{ ok: boolean; event_id: string }>("/v1/telemetry/event", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};

