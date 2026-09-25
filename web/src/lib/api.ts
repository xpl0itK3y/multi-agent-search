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
  UserTokenStats,
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

// Double-submit CSRF token (the readable csrf cookie echoed in a header) for
// cookie-authenticated requests, e.g. after Google OAuth, where there is no Bearer token.
function csrfHeaders(): Record<string, string> {
  const csrf = readCookie("csrf_token");
  return csrf ? { "X-CSRF-Token": csrf } : {};
}

export function authHeaders(method = "GET"): Record<string, string> {
  const headers: Record<string, string> = {};
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;

  // Mutations need the CSRF token; safe methods don't (file downloads add it themselves).
  if (!["GET", "HEAD", "OPTIONS"].includes(method.toUpperCase())) {
    Object.assign(headers, csrfHeaders());
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

// Whether this page signed a user in: set once /v1/auth/me, a login or a registration
// succeeds, cleared by logout. A Google sign-in's session is the httpOnly cookie alone
// (no bearer token), so a 401 cannot be told from a stale session by the token only.
let sessionActive = false;

// Whether the server still accepts this page's session: a 401 from /v1/auth/me. Any
// other answer, or no answer (offline), is no verdict on the session.
async function sessionRejected(): Promise<boolean> {
  try {
    const res = await fetch(`${BASE}/v1/auth/me`, { credentials: "include", headers: authHeaders("GET") });
    return res.status === 401;
  } catch {
    return false;
  }
}

// A session the server just rejected is stale (expired, or revoked: a logout signs out
// every device): drop the stored bearer and bounce to /login (with a `redirect` back
// param) so the user can re-authenticate. Nothing to recover before a sign-in (the
// page-load /v1/auth/me probe) or on public pages. A cookie-only session is confirmed
// dead with /v1/auth/me first: JS cannot drop that cookie, so a 401 that is not about
// the session would otherwise bounce the user to /login and straight back, forever.
async function recoverFromExpiredSession(hadToken: boolean, hadSession: boolean): Promise<void> {
  if (!hadToken && !hadSession) return;
  const { pathname, search } = window.location;
  if (isPublicPath(pathname)) return;
  if (!hadToken && !(await sessionRejected())) return;
  sessionActive = false;
  setAuthToken(null);
  window.location.assign(`/login?redirect=${encodeURIComponent(pathname + search)}`);
}

interface RequestOptions {
  // false: a 401 from this call means "wrong credential" (a mistyped current
  // password), not a stale session — keep the token and let the caller show it.
  sessionRecovery?: boolean;
}

async function request<T>(
  path: string,
  init?: RequestInit,
  { sessionRecovery = true }: RequestOptions = {},
): Promise<T> {
  const method = init?.method ?? "GET";
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((init?.headers as Record<string, string>) ?? {}),
    ...authHeaders(method),
  };
  const hadToken = authToken !== null;
  const hadSession = sessionActive;
  const res = await fetch(`${BASE}${path}`, {
    credentials: "include",
    ...init,
    headers,
  });
  if (!res.ok) {
    if (res.status === 401 && sessionRecovery) await recoverFromExpiredSession(hadToken, hadSession);
    throw await apiErrorFromResponse(res);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export interface ApiFile {
  blob: Blob;
  filename: string | null; // from Content-Disposition, when the server names it
}

function attachmentFilename(disposition: string | null): string | null {
  const cd = disposition || "";
  const m = cd.match(/filename\*=UTF-8''([^;]+)/) || cd.match(/filename="?([^";]+)"?/);
  return m ? decodeURIComponent(m[1]) : null;
}

// File downloads (report exports, admin CSVs) go through the same credentials,
// bearer header and 401 recovery as request(); only the body differs. They always
// carry the CSRF token too, although they are GETs: the admin CSV exports have side
// effects (an audit row, the shared admin rate budget), so the server checks the token
// on them like on a mutation whenever the session is the cookie (a Google sign-in).
async function fetchFile(path: string): Promise<ApiFile> {
  const hadToken = authToken !== null;
  const hadSession = sessionActive;
  const res = await fetch(`${BASE}${path}`, {
    credentials: "include",
    headers: { ...authHeaders("GET"), ...csrfHeaders() },
  });
  if (!res.ok) {
    if (res.status === 401) await recoverFromExpiredSession(hadToken, hadSession);
    throw await apiErrorFromResponse(res);
  }
  return { blob: await res.blob(), filename: attachmentFilename(res.headers.get("Content-Disposition")) };
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

// Known server details of a mapped status → a more specific `errors.api.*` key. The
// server sends no error codes, so these match the English error texts raised in
// src/services; any other detail gets the status's generic text, never the raw one.
const API_DETAIL_KEYS: Record<number, [RegExp, string][]> = {
  403: [
    // Sign-up with an ADMIN_EMAILS address (auth_mixin.register_user).
    [/^This email is reserved for an administrator\b/, "adminEmailReserved"],
    // An action that needs a fresh Google sign-in (see isReauthRequired). The text
    // speaks of passwords; the account deletion shows its own (SettingsView).
    [/^reauth_required/, "reauthRequired"],
  ],
  409: [
    [/^A research is already in progress\b/, "researchInProgress"],
    [/^Research capacity is currently full\b/, "capacityFull"],
    [/^(?:Email already registered|An account with this email already exists)\b/, "emailTaken"],
    [/^Report is not ready yet\b/, "reportNotReady"],
    [/^Only dead-letter \w+ jobs can be requeued\b/, "notDeadLetter"],
    // An admin requeue of a dead-letter finalize job (job_queue_mixin): the research is
    // no longer FAILED, a newer job replaced this one, or the job changed concurrently.
    [/^Only the finalize job of a failed research can be requeued\b/, "finalizeResearchNotFailed"],
    [/^A newer finalize job has superseded this one\b/, "finalizeJobSuperseded"],
    [/^Finalize job state changed\b/, "finalizeJobChanged"],
  ],
};

// Refused until the user signs in with Google again: POST /v1/auth/set-password for
// the first password of a Google-only account, or a reset without the current password
// on a Google-linked one, and DELETE /v1/auth/account for a passwordless account. They
// need a session from a Google sign-in of the last few minutes.
export function isReauthRequired(err: unknown): boolean {
  return err instanceof ApiError && err.status === 403 && err.detail.startsWith("reauth_required");
}

// Localized, user-facing message for errors thrown by `api`/`fetch`. Mapped
// statuses get a translated text (a specific one for known details); anything
// else falls back to the server detail, and network-level failures (fetch's
// TypeError) to a network text.
export function apiErrorMessage(err: unknown, t: (key: string) => string): string {
  if (err instanceof ApiError) {
    const specific = API_DETAIL_KEYS[err.status]?.find(([pattern]) => pattern.test(err.detail));
    if (specific) return t(`errors.api.${specific[1]}`);
    const key = API_STATUS_KEYS[err.status];
    if (key) return t(`errors.api.${key}`);
    return err.detail || t("errors.api.unexpected");
  }
  if (err instanceof TypeError) return t("errors.api.network");
  if (err instanceof Error && err.message) return err.message;
  return t("errors.api.unexpected");
}

export const api = {
  me: async () => {
    const user = await request<AuthUser>("/v1/auth/me");
    sessionActive = true;
    return user;
  },

  register: async (email: string, password: string) => {
    const res = await request<AuthSession>("/v1/auth/register", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    setAuthToken(res.access_token);
    sessionActive = true;
    return res;
  },

  login: async (email: string, password: string) => {
    const res = await request<AuthSession>("/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    setAuthToken(res.access_token);
    sessionActive = true;
    return res;
  },

  // A 401 here only says the session had already ended: the caller signs out anyway.
  logout: async () => {
    try {
      return await request<{ status: string }>("/v1/auth/logout", { method: "POST" }, { sessionRecovery: false });
    } finally {
      setAuthToken(null);
      sessionActive = false;
    }
  },

  authConfig: () => request<{ google_oauth: boolean }>("/v1/auth/config"),

  updateProfile: async (payload: { name?: string; avatar_url?: string }) => {
    return await request<AuthUser>("/v1/auth/profile", {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
  },

  getTokenStats: () => request<UserTokenStats>("/v1/auth/token-stats"),

  // The server requires an explicit confirm flag (DATA-LIFECYCLE); callers invoke
  // this only after the user confirmed in the Settings delete modal. With a
  // password sent, a 401 means that password was wrong: no session recovery.
  deleteAccount: async (currentPassword?: string) => {
    return await request<{ status: string }>(
      "/v1/auth/account",
      {
        method: "DELETE",
        body: JSON.stringify({ current_password: currentPassword, confirm: true }),
      },
      { sessionRecovery: currentPassword === undefined },
    );
  },

  setPassword: async (password: string, currentPassword?: string) => {
    const res = await request<AuthSession>(
      "/v1/auth/set-password",
      {
        method: "POST",
        body: JSON.stringify({ password, current_password: currentPassword }),
      },
      { sessionRecovery: currentPassword === undefined },
    );
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

  exportReport: (id: string, params: URLSearchParams) =>
    fetchFile(`/v1/research/${id}/export?${params.toString()}`),

  deleteResearch: (id: string) =>
    request<void>(`/v1/research/${id}`, { method: "DELETE" }),

  cancelResearch: (id: string) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/cancel`, { method: "POST" }),

  retryResearch: (id: string) =>
    request<{ id: string; status: string }>(`/v1/research/${id}/retry`, { method: "POST" }),

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

  // Offset-paginated (the route takes limit/offset/category, not page/page_size).
  getUserEvents: (
    limit: number = 50,
    offset: number = 0,
    userId?: string,
    eventName?: string,
    category?: string
  ) => {
    let url = `/v1/admin/users/events?limit=${limit}&offset=${offset}`;
    if (userId) url += `&user_id=${encodeURIComponent(userId)}`;
    if (eventName) url += `&event_name=${encodeURIComponent(eventName)}`;
    if (category) url += `&category=${encodeURIComponent(category)}`;
    return request<AdminEventLogResponse>(url);
  },

  exportUsersCsv: () => fetchFile("/v1/admin/users/export"),

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

  exportPromptsCsv: () => fetchFile("/v1/admin/prompts/export"),

  deleteUser: (userId: string) =>
    request<{ status: string; deleted_user_id: string }>(`/v1/admin/users/${userId}`, {
      method: "DELETE",
    }),

  getOverview: () => request<AdminOverviewResponse>("/v1/admin/overview"),

  getTokens: (page: number = 1, pageSize: number = 20) =>
    request<AdminTokenAnalyticsResponse>(`/v1/admin/tokens?page=${page}&page_size=${pageSize}`),

  exportTokensCsv: () => fetchFile("/v1/admin/tokens/export"),

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
