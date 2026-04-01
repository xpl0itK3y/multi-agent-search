from pydantic import BaseModel, Field
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
    latest_search_job: Optional["SearchTaskJob"] = None

class TaskUpdate(BaseModel):
    status: Optional[TaskStatus] = None
    result: Optional[List[Dict[str, Any]]] = None
    log: Optional[str] = None

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
    finalize_ready: bool = False
    latest_finalize_job: Optional["ResearchFinalizeJob"] = None
    tasks: List[SearchTaskSummary] = Field(default_factory=list)


class ResearchReportResponse(BaseModel):
    research_id: str
    status: ResearchStatus
    final_report: Optional[str] = None

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


class WorkerHeartbeat(BaseModel):
    worker_name: str
    processed_jobs: int = 0
    status: str = "idle"
    last_error: Optional[str] = None
    extraction_metrics: ExtractionMetrics = Field(default_factory=ExtractionMetrics)
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QueueMetrics(BaseModel):
    pending_search_jobs: int = 0
    running_search_jobs: int = 0
    dead_letter_search_jobs: int = 0
    pending_finalize_jobs: int = 0
    running_finalize_jobs: int = 0
    dead_letter_finalize_jobs: int = 0


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
    recovered_count: int = 0
    deleted_count: int = 0
    total_count: int = 0


SearchTaskSummary.model_rebuild()
ResearchSummary.model_rebuild()
