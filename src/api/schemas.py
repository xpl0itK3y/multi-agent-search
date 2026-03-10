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


class SearchJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

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

class ResearchResponse(BaseModel):
    research_id: str
    status: str
    message: str


class ResearchFinalizeJob(BaseModel):
    id: str
    research_id: str
    status: FinalizeJobStatus = FinalizeJobStatus.PENDING
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SearchTaskJob(BaseModel):
    id: str
    task_id: str
    depth: SearchDepth
    status: SearchJobStatus = SearchJobStatus.PENDING
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ResearchFinalizeResponse(BaseModel):
    research: ResearchRecord
    finalize_job_id: Optional[str] = None
