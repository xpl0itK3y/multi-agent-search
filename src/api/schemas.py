from pydantic import BaseModel, Field
from enum import Enum
from typing import List

class SearchDepth(str, Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class OptimizeRequest(BaseModel):
    prompt: str = Field(..., description="The raw prompt to be optimized", min_length=1)

class OptimizeResponse(BaseModel):
    optimized_prompt: str = Field(..., description="The improved and structured prompt")
    status: str = "success"

class DecomposeRequest(BaseModel):
    prompt: str = Field(..., description="The complex prompt to be broken into search tasks")
    depth: SearchDepth = Field(SearchDepth.EASY, description="Search depth (number of tasks)")

class SearchTask(BaseModel):
    id: str = Field(..., description="Unique ID for the task")
    description: str = Field(..., description="Description of the search task for a bot")
    queries: List[str] = Field(..., description="Specific search queries for this task")
    status: TaskStatus = Field(TaskStatus.PENDING, description="Current status of the task")

class DecomposeResponse(BaseModel):
    tasks: List[SearchTask] = Field(..., description="List of search tasks for bots")
    depth: SearchDepth
    status: str = "success"
