export type Depth = "easy" | "medium" | "hard";

export interface AuthUser {
  id: string;
  email: string;
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
