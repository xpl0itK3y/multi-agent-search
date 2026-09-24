export type Depth = "easy" | "medium" | "hard";

export interface AuthUser {
  id: string;
  email: string;
  name?: string | null;
  avatar_url?: string | null;
  is_admin?: boolean;
}

export interface AuthSession {
  access_token: string;
  token_type: string;
  user: AuthUser;
}

export interface UserTokenModelBreakdown {
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  estimated_cost_usd: number;
  calls_count: number;
}

export interface UserRecentResearchTokenUsage {
  id: string;
  prompt: string;
  depth: string;
  status: string;
  total_tokens: number;
  estimated_cost_usd: number;
  created_at: string | null;
}

export interface UserTokenStats {
  total_tokens: number;
  prompt_tokens: number;
  completion_tokens: number;
  estimated_cost_usd: number;
  calls_count: number;
  researches_count: number;
  by_model: UserTokenModelBreakdown[];
  recent: UserRecentResearchTokenUsage[];
}

export interface ModelOption {
  id: string;
  label: string;
  description: string;
  tier: "pro" | "flash";
  reasoning: boolean;
  default: boolean;
}

export interface ResearchHistoryItem {
  id: string;
  prompt: string;
  title?: string | null;
  thread_id?: string | null;
  depth: Depth;
  status: string;
  created_at?: string;
  updated_at?: string;
  has_final_report?: boolean;
}

export interface CreateResearchResponse {
  research_id: string;
  status: string;
  message: string;
  thread_id?: string | null;
}

export interface ResearchReport {
  research_id: string;
  status: string;
  final_report: string | null;
}

export interface ResearchStatusSummary {
  id: string;
  prompt: string;
  depth: Depth;
  status: string;
  task_count: number;
  completed_tasks: number;
  collected_sources: number;
  finalize_ready: boolean;
  has_final_report: boolean;
  llm_token_usage?: Record<string, number>;
  queue_position?: number | null;
}

export interface SourcePreview {
  url: string;
  source_id?: string | null;
  title?: string | null;
  domain?: string | null;
  source_quality?: string | null;
  extraction_status?: string | null;
  snippet?: string | null;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  sources?: SourcePreview[];
}

export interface Clarification {
  research_id: string;
  status: string;
  questions: string[];
  answers: string[];
}

export interface PlanItem {
  id: string;
  description: string;
  queries: string[];
}

export interface ResearchPlan {
  research_id: string;
  status: string;
  items: PlanItem[];
}

export interface Conflict {
  topic: string;
  reason?: string | null;
  source_ids: string[];
  sentences: string[];
}

export interface ConfidenceFinding {
  statement: string;
  support_level: "strong" | "medium" | "weak";
  source_count: number;
  source_ids: string[];
}

export interface PlanCoverageItem {
  question: string;
  covered: boolean;
  match_ratio: number;
}

export interface VerificationReport {
  research_id: string;
  findings: ConfidenceFinding[];
  plan_coverage: PlanCoverageItem[];
  uncovered_questions: string[];
  coverage_ratio: number;
  claim_verification: {
    uncited_lines: number;
    unsupported_lines: number;
    insufficient_evidence_lines: number;
    downgraded_lines: number;
    verification_notes: string[];
  };
}

export interface RedTeamFinding {
  claim: string;
  verdict: "refuted" | "contested" | "qualified" | "holds";
  challenge: string;
  source_urls: string[];
}

export interface RedTeamReport {
  research_id: string;
  findings: RedTeamFinding[];
  challenged: number;
  held: number;
}

export interface CitationGround {
  source_id: string;
  url: string;
  title: string;
  quote: string;
  supported: boolean;
}

export interface CitationAudit {
  research_id: string;
  total: number;
  supported: number;
  integrity: number;
  unverified?: number;
  unsupported_claims: string[];
  grounding: CitationGround[];
}

export interface OriginCluster {
  label: string;
  kind: "unique" | "single-domain" | "syndicated";
  size: number;
  domains: string[];
  source_ids: string[];
}

export interface SourceIndependence {
  research_id: string;
  total_sources: number;
  independent_origins: number;
  independence_score: number;
  dominant_origin_share: number;
  clusters: OriginCluster[];
  echo_warnings: string[];
}

export interface ShareInfo {
  shared: boolean;
  token: string;
}

export interface PublicReport {
  prompt: string;
  final_report: string;
  depth: string;
  model: string;
  created_at: string;
  sources: SourcePreview[];
  citations: CitationAudit;
  confidence: ConfidenceReport;
  source_independence: SourceIndependence;
  source_reputation: SourceReputation;
  numeric_check: NumericCheck;
  stance: StanceBalance;
  red_team: RedTeamReport;
  source_integrity: SourceIntegrity;
  cross_language: CrossLanguageReport;
}

export interface StanceSource {
  source_id: string;
  stance: "supports" | "opposes" | "neutral";
}

export interface StanceBalance {
  research_id: string;
  applicable: boolean;
  proposition: string;
  supports: number;
  opposes: number;
  neutral: number;
  dominant_side: string;
  skew: number;
  sources: StanceSource[];
}

export interface LanguageCount {
  lang: string;
  count: number;
}

export interface CrossLanguageFinding {
  lang: string;
  finding: string;
}

export interface CrossLanguageReport {
  research_id: string;
  query_language: string;
  languages: LanguageCount[];
  target_languages: string[];
  foreign_source_count: number;
  monolingual: boolean;
  unique_findings: CrossLanguageFinding[];
}

export interface IntegrityFlag {
  source_id: string;
  doi: string;
  kind: "retraction" | "concern";
  detail: string;
}

export interface SourceIntegrity {
  research_id: string;
  checked_dois: number;
  retracted_count: number;
  flagged: IntegrityFlag[];
}

export interface ReputationFlag {
  source_id: string;
  domain: string;
  category: "satire" | "fabricated" | "conspiracy" | "state_media";
  reason: string;
}

export interface SourceReputation {
  research_id: string;
  total_sources: number;
  flagged_count: number;
  categories: string[];
  flagged: ReputationFlag[];
}

export interface ConfidenceComponent {
  key: string;
  score: number;
  weight: number;
  detail: string;
}

export interface ConfidenceClaim {
  statement: string;
  band: "solid" | "contested" | "speculative";
  support_level: string;
  source_ids: string[];
  note: string;
}

export interface ConfidenceReport {
  research_id: string;
  overall: number;
  grade: "high" | "medium" | "low";
  total_claims: number;
  solid: number;
  contested: number;
  speculative: number;
  components: ConfidenceComponent[];
  claims: ConfidenceClaim[];
}

export interface NumericClaim {
  value: string;
  subject: string;
  source_id: string;
  sentence: string;
}

export interface NumericContradiction {
  subject: string;
  values: string[];
  sentences: string[];
}

export interface NumericCheck {
  research_id: string;
  total: number;
  supported: number;
  integrity: number;
  unsupported: NumericClaim[];
  contradictions: NumericContradiction[];
}

export interface ComparisonCell {
  option: string;
  value: string;
  source_ids: string[];
}

export interface ComparisonRow {
  criterion: string;
  cells: ComparisonCell[];
}

export interface ComparisonTable {
  research_id: string;
  options: string[];
  rows: ComparisonRow[];
  recommendation: string;
}

export interface GraphTrailEntry {
  step?: string;
  detail?: string;
  timestamp?: string;
  agent?: string;
  phase?: string;
  action?: string;
  metrics?: Record<string, any>;
  sources?: any[];
}

export interface ResearchGraph {
  research_id: string;
  status: string;
  graph_state: Record<string, unknown>;
  graph_trail: GraphTrailEntry[];
}

export interface AdminAuditLogItem {
  id: string;
  actor_email: string;
  action: string;
  target_type: string;
  target_id?: string | null;
  details: Record<string, any>;
  ip_address?: string | null;
  created_at: string;
}

export interface AdminWorkerFleetItem {
  worker_name: string;
  status: string;
  processed_jobs: number;
  last_error?: string | null;
  last_seen_at: string;
  is_alive: boolean;
  extraction_metrics: Record<string, any>;
  graph_metrics: Record<string, any>;
  maintenance_summary: Record<string, any>;
}

export interface AdminOverviewResponse {
  system_health: Record<string, any>;
  active_researches_count: number;
  pending_tasks_count: number;
  failed_tasks_count: number;
  workers: AdminWorkerFleetItem[];
  is_dev_mode: boolean;
}

export interface AdminTokenModelBreakdown {
  model: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  estimated_cost_usd: number;
  calls_count: number;
}

export interface AdminTokenDepthBreakdown {
  depth: string;
  total_tokens: number;
  estimated_cost_usd: number;
  researches_count: number;
}

export interface AdminTokenResearchUsageItem {
  research_id: string;
  prompt: string;
  depth: string;
  status: string;
  total_tokens: number;
  estimated_cost_usd: number;
  created_at: string;
}

export interface AdminTokenAnalyticsResponse {
  total_prompt_tokens: number;
  total_completion_tokens: number;
  total_tokens: number;
  total_cost_usd: number;
  by_model: AdminTokenModelBreakdown[];
  by_depth: AdminTokenDepthBreakdown[];
  researches: AdminTokenResearchUsageItem[];
  total_researches: number;
  page: number;
  page_size: number;
}

export interface AdminDryRunResult {
  action: string;
  dry_run: boolean;
  affected_count: number;
  sample_affected_ids: string[];
  summary: string;
}

export interface AgentMetadataItem {
  id: string;
  name: string;
  stage: "planning" | "search" | "synthesis" | "delivery";
  role: string;
  trigger: string;
  llm_model?: string | null;
  inputs: string[];
  outputs: string[];
  source_file: string;
  line_number: number;
  description: string;
  dependencies: string[];
  system_prompt?: string | null;
  temperature?: number | null;
  max_tokens?: number | null;
  context_window?: string | null;
  response_format?: string | null;
  tools?: string[];
  timeout_seconds?: number | null;
  retry_policy?: string | null;
  cache_ttl?: string | null;
  example_input?: Record<string, any> | null;
  example_output?: Record<string, any> | null;
}

export interface AdminUserListItem {
  id: string;
  email: string;
  name: string;
  avatar_url?: string | null;
  is_admin: boolean;
  created_at: string;
  last_seen_at?: string | null;
  is_online: boolean;
  last_ip?: string | null;
  last_device?: string | null;
  last_browser?: string | null;
  last_os?: string | null;
  researches_count: number;
  total_tokens: number;
  total_cost_usd: number;
}

export interface AdminUserListResponse {
  users: AdminUserListItem[];
  total_users: number;
  online_users: number;
  page: number;
  page_size: number;
}

export interface AdminUserSessionItem {
  id: string;
  session_id: string;
  ip_address?: string | null;
  user_agent?: string | null;
  device_type: string;
  browser?: string | null;
  os?: string | null;
  screen_res?: string | null;
  viewport?: string | null;
  language?: string | null;
  timezone?: string | null;
  country?: string | null;
  city?: string | null;
  started_at: string;
  last_active_at: string;
}

export interface AdminUserResearchItem {
  id: string;
  prompt: string;
  depth: string;
  status: string;
  total_tokens: number;
  cost_usd: number;
  created_at: string;
}

export interface AdminUserDetailResponse {
  user: AdminUserListItem;
  sessions: AdminUserSessionItem[];
  researches?: AdminUserResearchItem[];
  recent_researches?: AdminUserResearchItem[];
  recent_events: Array<{
    id: string;
    event_name: string;
    event_category: string;
    details: Record<string, any>;
    ip_address?: string | null;
    created_at: string;
  }>;
}

export interface AdminTelemetrySummaryResponse {
  total_users: number;
  online_users_now?: number;
  online_now?: number;
  dau_today?: number;
  dau?: number;
  wau_7d?: number;
  wau?: number;
  mau_30d?: number;
  mau?: number;
  total_researches: number;
  total_tokens: number;
  total_cost_usd: number;
  os_breakdown?: Record<string, number>;
  browser_breakdown?: Record<string, number>;
  device_breakdown?: Record<string, number>;
  depth_distribution?: Record<string, number>;
  by_os?: Array<{ name: string; count: number }>;
  by_browser?: Array<{ name: string; count: number }>;
  by_device?: Array<{ name: string; count: number }>;
  by_country?: Array<{ name: string; count: number }>;
  popular_depths?: Array<{ depth: string; count: number }>;
  popular_models?: Array<{ model: string; count: number }>;
  avg_prompt_len?: number;
}

export interface AdminEventLogItem {
  id: string;
  user_id?: string | null;
  session_id?: string | null;
  event_name: string;
  event_category: string;
  details: Record<string, any>;
  ip_address?: string | null;
  user_agent?: string | null;
  created_at: string;
}

export interface AdminEventLogResponse {
  events: AdminEventLogItem[];
  total_count: number;
  page: number;
  page_size: number;
}

export interface AdminPromptItem {
  id: string;
  prompt_type: "research" | "chat" | string;
  prompt: string;
  research_id: string;
  user_id?: string | null;
  user_email?: string | null;
  user_name?: string | null;
  depth?: string | null;
  status?: string | null;
  total_tokens: number;
  cost_usd: number;
  created_at: string;
}

export interface AdminPromptsResponse {
  prompts: AdminPromptItem[];
  total_count: number;
  page: number;
  page_size: number;
}

