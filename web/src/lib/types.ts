export type Depth = "easy" | "medium" | "hard";

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
