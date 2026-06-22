export type Depth = "easy" | "medium" | "hard";

export interface AuthUser {
  id: string;
  email: string;
  name?: string | null;
  avatar_url?: string | null;
}

export interface AuthSession {
  access_token: string;
  token_type: string;
  user: AuthUser;
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
  title?: string | null;
  domain?: string | null;
  source_quality?: string | null;
  extraction_status?: string | null;
  snippet?: string | null;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
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

export interface ResearchWatch {
  research_id: string;
  enabled: boolean;
  interval_seconds: number;
  next_run_at: string;
  last_run_at: string;
  last_change_at: string;
  acknowledged_at: string;
  runs: number;
  has_unseen_change: boolean;
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

export interface DiffClaim {
  statement: string;
  old_level: string;
  new_level: string;
}

export interface ResearchDiff {
  research_id: string;
  compared_to: string;
  compared_at: string;
  new_claims: string[];
  dropped_claims: string[];
  shifted_claims: DiffClaim[];
  new_sources: number;
  new_domains: string[];
}

export interface GraphTrailEntry {
  step?: string;
  detail?: string;
  timestamp?: string;
}

export interface ResearchGraph {
  research_id: string;
  status: string;
  graph_state: Record<string, unknown>;
  graph_trail: GraphTrailEntry[];
}
