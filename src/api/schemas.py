from pydantic import BaseModel, Field, model_validator
from enum import Enum
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

class SearchDepth(str, Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class ResearchStatus(str, Enum):
    PROCESSING = "processing"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"


class FinalizeJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class SearchJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

class OptimizeRequest(BaseModel):
    prompt: str = Field(..., description="The original user prompt to optimize", min_length=1)

class OptimizeResponse(BaseModel):
    optimized_prompt: str
    status: str = "success"

class DecomposeRequest(BaseModel):
    prompt: str = Field(..., description="The complex query to decompose")
    depth: SearchDepth = Field(default=SearchDepth.EASY, description="Depth of the search (easy, medium, hard)")

class SearchTaskMetrics(BaseModel):
    candidate_count: int = 0
    extraction_attempts: int = 0
    extraction_success_count: int = 0
    extraction_failure_count: int = 0
    selected_source_count: int = 0
    avg_content_chars: float = 0.0

class SearchTask(BaseModel):
    id: str
    research_id: Optional[str] = None
    description: str
    queries: List[str]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    result: Optional[List[Dict[str, Any]]] = None
    logs: List[str] = Field(default_factory=list)
    search_metrics: SearchTaskMetrics = Field(default_factory=SearchTaskMetrics)


class SearchSourcePreview(BaseModel):
    url: str
    title: Optional[str] = None
    domain: Optional[str] = None
    source_quality: Optional[str] = None
    extraction_status: Optional[str] = None
    snippet: Optional[str] = None


class SearchTaskSummary(BaseModel):
    id: str
    research_id: Optional[str] = None
    description: str
    queries: List[str]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime
    updated_at: datetime
    result_count: int = 0
    log_count: int = 0
    recent_logs: List[str] = Field(default_factory=list)
    source_preview: List[SearchSourcePreview] = Field(default_factory=list)
    search_metrics: SearchTaskMetrics = Field(default_factory=SearchTaskMetrics)
    latest_search_job: Optional["SearchTaskJob"] = None


class SourceCriticSummary(BaseModel):
    total_sources: int = 0
    high_confidence_sources: int = 0
    medium_confidence_sources: int = 0
    low_confidence_sources: int = 0
    primary_sources: int = 0
    editorial_sources: int = 0
    community_sources: int = 0
    speculative_sources: int = 0
    flagged_sources: int = 0
    dominant_domains: List[str] = Field(default_factory=list)


class EvidenceCoverageSummary(BaseModel):
    evidence_group_count: int = 0
    multi_source_group_count: int = 0
    weak_group_count: int = 0
    avg_sources_per_group: float = 0.0


class ClaimVerificationSummary(BaseModel):
    uncited_lines: int = 0
    unsupported_lines: int = 0
    downgraded_lines: int = 0
    verification_notes: List[str] = Field(default_factory=list)


class ReplanRecommendation(BaseModel):
    reason: str
    suggested_queries: List[str] = Field(default_factory=list)


class GraphExecutionSummary(BaseModel):
    branching_active: bool = False
    follow_up_task_count: int = 0
    replan_task_count: int = 0
    tie_break_task_count: int = 0
    follow_up_query_count: int = 0
    follow_up_queries: List[str] = Field(default_factory=list)


class TaskUpdate(BaseModel):
    status: Optional[TaskStatus] = None
    result: Optional[List[Dict[str, Any]]] = None
    log: Optional[str] = None
    search_metrics: Optional[SearchTaskMetrics] = None

class DecomposeResponse(BaseModel):
    tasks: List[SearchTask]
    depth: SearchDepth

class ResearchRequest(BaseModel):
    prompt: str = Field(..., description="The goal or topic of the research")
    depth: SearchDepth = Field(default=SearchDepth.EASY)

class ResearchRecord(BaseModel):
    id: str
    prompt: str
    depth: SearchDepth
    status: ResearchStatus = ResearchStatus.PROCESSING
    task_ids: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    final_report: Optional[str] = None
    graph_state: Dict[str, Any] = Field(default_factory=dict)
    graph_trail: List[Dict[str, Any]] = Field(default_factory=list)


class ResearchSummary(BaseModel):
    id: str
    prompt: str
    depth: SearchDepth
    status: ResearchStatus = ResearchStatus.PROCESSING
    task_ids: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    has_final_report: bool = False
    task_count: int = 0
    completed_tasks: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    failed_tasks: int = 0
    collected_sources: int = 0
    avg_sources_per_task: float = 0.0
    total_candidates: int = 0
    total_extraction_attempts: int = 0
    total_extraction_success_count: int = 0
    total_extraction_failure_count: int = 0
    total_selected_source_count: int = 0
    finalize_ready: bool = False
    source_critic_summary: SourceCriticSummary = Field(default_factory=SourceCriticSummary)
    evidence_coverage_summary: EvidenceCoverageSummary = Field(default_factory=EvidenceCoverageSummary)
    claim_verification_summary: ClaimVerificationSummary = Field(default_factory=ClaimVerificationSummary)
    replan_recommendations: List[ReplanRecommendation] = Field(default_factory=list)
    graph_execution_summary: GraphExecutionSummary = Field(default_factory=GraphExecutionSummary)
    latest_finalize_job: Optional["ResearchFinalizeJob"] = None
    tasks: List[SearchTaskSummary] = Field(default_factory=list)


class ResearchReportResponse(BaseModel):
    research_id: str
    status: ResearchStatus
    final_report: Optional[str] = None


class ResearchGraphResponse(BaseModel):
    research_id: str
    status: ResearchStatus
    graph_state: Dict[str, Any] = Field(default_factory=dict)
    graph_trail: List[Dict[str, Any]] = Field(default_factory=list)

class ResearchResponse(BaseModel):
    research_id: str
    status: str
    message: str


class ResearchFinalizeJob(BaseModel):
    id: str
    research_id: str
    status: FinalizeJobStatus = FinalizeJobStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SearchTaskJob(BaseModel):
    id: str
    task_id: str
    depth: SearchDepth
    status: SearchJobStatus = SearchJobStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ResearchFinalizeResponse(BaseModel):
    research: ResearchRecord
    finalize_job_id: Optional[str] = None


class ExtractionMetrics(BaseModel):
    attempts: int = 0
    success_count: int = 0
    empty_count: int = 0
    failure_count: int = 0
    downloaded_bytes: int = 0
    content_chars: int = 0
    total_download_ms: float = 0.0
    total_extract_ms: float = 0.0
    total_post_process_ms: float = 0.0
    total_total_ms: float = 0.0
    success_rate_percent: float = 0.0
    avg_download_ms: float = 0.0
    avg_extract_ms: float = 0.0
    avg_post_process_ms: float = 0.0
    avg_total_ms: float = 0.0

    @model_validator(mode="after")
    def populate_derived_fields(self):
        attempts = max(self.attempts, 0)
        if attempts > 0:
            self.success_rate_percent = round((self.success_count / attempts) * 100, 1)
            self.avg_download_ms = round(self.total_download_ms / attempts, 2)
            self.avg_extract_ms = round(self.total_extract_ms / attempts, 2)
            self.avg_post_process_ms = round(self.total_post_process_ms / attempts, 2)
            self.avg_total_ms = round(self.total_total_ms / attempts, 2)
        else:
            self.success_rate_percent = 0.0
            self.avg_download_ms = 0.0
            self.avg_extract_ms = 0.0
            self.avg_post_process_ms = 0.0
            self.avg_total_ms = 0.0
        return self


class GraphStepMetrics(BaseModel):
    run_count: int = 0
    failure_count: int = 0
    total_ms: float = 0.0
    avg_ms: float = 0.0

    @model_validator(mode="after")
    def _derive_averages(self) -> "GraphStepMetrics":
        runs = max(self.run_count, 0)
        self.avg_ms = round(self.total_ms / runs, 2) if runs > 0 else 0.0
        return self


def _default_graph_steps() -> Dict[str, GraphStepMetrics]:
    return {
        "collect_context": GraphStepMetrics(),
        "replan": GraphStepMetrics(),
        "analyze": GraphStepMetrics(),
        "verify": GraphStepMetrics(),
        "tie_break": GraphStepMetrics(),
    }


class GraphMetrics(BaseModel):

    resume_count: int = 0
    replan_pass_count: int = 0
    tie_break_pass_count: int = 0
    analyze_pass_count: int = 0
    completed_run_count: int = 0
    steps: Dict[str, GraphStepMetrics] = Field(default_factory=_default_graph_steps)

    @model_validator(mode="after")
    def _normalize_steps(self) -> "GraphMetrics":
        merged_steps = _default_graph_steps()
        for step_name, payload in (self.steps or {}).items():
            if step_name not in merged_steps:
                continue
            merged_steps[step_name] = GraphStepMetrics.model_validate(payload)
        self.steps = merged_steps
        return self


class GraphAlert(BaseModel):
    code: str
    severity: str = "warning"
    step: Optional[str] = None
    current_value: float = 0.0
    threshold: float = 0.0
    hint: Optional[str] = None


class GraphAlertHistoryEntry(BaseModel):
    timestamp: datetime
    code: str
    severity: str = "warning"
    step: Optional[str] = None
    current_value: float = 0.0
    threshold: float = 0.0
    research_id: Optional[str] = None
    worker_name: Optional[str] = None


class GraphAlertTrend(BaseModel):
    worsening_steps: List[str] = Field(default_factory=list)
    improving_steps: List[str] = Field(default_factory=list)
    repeated_alerts: Dict[str, int] = Field(default_factory=dict)
    top_research_ids: List[str] = Field(default_factory=list)
    top_worker_names: List[str] = Field(default_factory=list)
    recent_alerts: List[GraphAlertHistoryEntry] = Field(default_factory=list)


class WorkerHeartbeat(BaseModel):
    worker_name: str
    processed_jobs: int = 0
    status: str = "idle"
    last_error: Optional[str] = None
    extraction_metrics: ExtractionMetrics = Field(default_factory=ExtractionMetrics)
    graph_metrics: GraphMetrics = Field(default_factory=GraphMetrics)
    graph_alerts: List[GraphAlert] = Field(default_factory=list)
    graph_alert_trend: GraphAlertTrend = Field(default_factory=GraphAlertTrend)
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QueueMetrics(BaseModel):
    pending_search_jobs: int = 0
    running_search_jobs: int = 0
    dead_letter_search_jobs: int = 0
    pending_finalize_jobs: int = 0
    running_finalize_jobs: int = 0
    dead_letter_finalize_jobs: int = 0
    extraction_metrics: ExtractionMetrics = Field(default_factory=ExtractionMetrics)
    graph_metrics: GraphMetrics = Field(default_factory=GraphMetrics)
    graph_alerts: List[GraphAlert] = Field(default_factory=list)
    graph_alert_trend: GraphAlertTrend = Field(default_factory=GraphAlertTrend)


class JobRecoveryResponse(BaseModel):
    recovered_job_ids: List[str] = Field(default_factory=list)
    recovered_count: int = 0


class JobCleanupResponse(BaseModel):
    deleted_job_ids: List[str] = Field(default_factory=list)
    deleted_count: int = 0


class QueueMaintenanceResponse(BaseModel):
    recovered_search_job_ids: List[str] = Field(default_factory=list)
    recovered_finalize_job_ids: List[str] = Field(default_factory=list)
    deleted_search_job_ids: List[str] = Field(default_factory=list)
    deleted_finalize_job_ids: List[str] = Field(default_factory=list)
    compacted_graph_event_worker_names: List[str] = Field(default_factory=list)
    compacted_graph_trail_research_ids: List[str] = Field(default_factory=list)
    recovered_count: int = 0
    deleted_count: int = 0
    compacted_count: int = 0
    total_count: int = 0


SearchTaskSummary.model_rebuild()
ResearchSummary.model_rebuild()
