from fastapi import BackgroundTasks, HTTPException

from src.agents.analyzer import AnalyzerAgent
from src.agents.optimizer import PromptOptimizerAgent
from src.agents.orchestrator import OrchestratorAgent
from src.agents.search import SearchAgent
from src.api.schemas import (
    DecomposeResponse,
    FinalizeJobStatus,
    ResearchRecord,
    ResearchRequest,
    ResearchResponse,
    ResearchStatus,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    SearchDepth,
    SearchTask,
    TaskUpdate,
    TaskStatus,
)
from src.repositories.protocols import TaskStore


class ResearchService:
    def __init__(
        self,
        task_store: TaskStore,
        optimizer: PromptOptimizerAgent | None = None,
        orchestrator: OrchestratorAgent | None = None,
        analyzer: AnalyzerAgent | None = None,
    ):
        self.task_store = task_store
        self.optimizer = optimizer
        self.orchestrator = orchestrator
        self.analyzer = analyzer

    def require_agent(self, agent, agent_name: str):
        if agent is None:
            raise HTTPException(
                status_code=503,
                detail=f"{agent_name} is unavailable. Check service configuration.",
            )
        return agent

    def optimize_prompt(self, prompt: str) -> str:
        optimizer = self.require_agent(self.optimizer, "Prompt optimizer")
        return optimizer.run(prompt)

    def list_tasks(self) -> list[SearchTask]:
        return self.task_store.get_all_tasks()

    def get_task(self, task_id: str) -> SearchTask | None:
        return self.task_store.get_task(task_id)

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        return self.task_store.update_task(task_id, update)

    def decompose_prompt(
        self,
        prompt: str,
        depth: SearchDepth,
        background_tasks: BackgroundTasks,
    ) -> DecomposeResponse:
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        tasks_raw = orchestrator.run_decompose(prompt, depth)

        registered_tasks = []
        for task_dict in tasks_raw:
            task = self.task_store.add_task(task_dict)
            registered_tasks.append(task)
            if task.status == TaskStatus.PENDING and task.queries:
                self.task_store.add_search_task_job(task.id, depth.value)

        return DecomposeResponse(tasks=registered_tasks, depth=depth)

    def start_research(
        self,
        request: ResearchRequest,
        background_tasks: BackgroundTasks,
    ) -> ResearchResponse:
        orchestrator = self.require_agent(self.orchestrator, "Orchestrator")
        tasks_raw = orchestrator.run_decompose(request.prompt, request.depth)

        task_ids = []
        registered_tasks = []
        research = self.task_store.add_research(request, task_ids=[])

        for task_dict in tasks_raw:
            task_dict["research_id"] = research.id
            task = self.task_store.add_task(task_dict)
            registered_tasks.append(task)
            task_ids.append(task.id)

        self.task_store.set_research_task_ids(research.id, task_ids)

        for task in registered_tasks:
            if task.status == TaskStatus.PENDING and task.queries:
                self.task_store.add_search_task_job(task.id, request.depth.value)

        return ResearchResponse(
            research_id=research.id,
            status="success",
            message=f"Research started with {len(registered_tasks)} tasks.",
        )

    def get_research_status(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        return research

    def _get_research_for_finalization(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research

        tasks = self.task_store.get_tasks_by_research(research_id)
        all_done = all(t.status in [TaskStatus.COMPLETED, TaskStatus.FAILED] for t in tasks)
        any_failed = any(t.status == TaskStatus.FAILED for t in tasks)

        if not tasks:
            raise HTTPException(status_code=409, detail="Research has no tasks to finalize")

        if not all_done:
            raise HTTPException(status_code=409, detail="Research tasks are still in progress")

        if any_failed and all(t.status == TaskStatus.FAILED for t in tasks):
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.FAILED,
                "All tasks failed.",
            )
            return self.task_store.get_research(research_id)

        return research

    def complete_research_finalization(self, research_id: str) -> ResearchRecord:
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")

        tasks = self.task_store.get_tasks_by_research(research_id)
        analyzer = self.require_agent(self.analyzer, "Analyzer")

        try:
            report = analyzer.run_analysis(research.prompt, tasks)
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.COMPLETED,
                report,
            )
        except Exception as exc:
            self.task_store.update_research_status(
                research_id,
                ResearchStatus.FAILED,
                f"Analysis failed: {str(exc)}",
            )

        return self.task_store.get_research(research_id)

    def enqueue_research_finalization(self, research_id: str) -> tuple[ResearchRecord, ResearchFinalizeJob | None]:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research, None

        self.require_agent(self.analyzer, "Analyzer")
        self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
        job = self.task_store.add_research_finalize_job(research_id)
        return self.task_store.get_research(research_id), job

    def queue_research_finalization(
        self,
        research_id: str,
        background_tasks: BackgroundTasks,
    ) -> ResearchRecord:
        research, job = self.enqueue_research_finalization(research_id)
        if job is not None:
            background_tasks.add_task(self.process_finalize_job, job.id)
        return research

    def process_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        job = self.task_store.get_research_finalize_job(job_id)
        if job is None:
            return None

        self.task_store.update_research_finalize_job(job_id, FinalizeJobStatus.RUNNING)
        try:
            self.complete_research_finalization(job.research_id)
            return self.task_store.update_research_finalize_job(
                job_id,
                FinalizeJobStatus.COMPLETED,
            )
        except Exception as exc:
            return self.task_store.update_research_finalize_job(
                job_id,
                FinalizeJobStatus.FAILED,
                str(exc),
            )

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        return self.task_store.get_research_finalize_job(job_id)

    def get_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        return self.task_store.get_search_task_job(job_id)

    def finalize_research(self, research_id: str) -> ResearchRecord:
        research = self._get_research_for_finalization(research_id)
        if research.status in [ResearchStatus.ANALYZING, ResearchStatus.COMPLETED, ResearchStatus.FAILED]:
            return research

        self.task_store.update_research_status(research_id, ResearchStatus.ANALYZING)
        return self.complete_research_finalization(research_id)

    def run_search_task(self, task_id: str, depth: SearchDepth):
        source_limit_map = {
            SearchDepth.EASY: 5,
            SearchDepth.MEDIUM: 12,
            SearchDepth.HARD: 20,
        }
        limit = source_limit_map.get(depth, 5)
        agent = SearchAgent(task_store=self.task_store, max_sources=limit)
        agent.run_task(task_id)

    def process_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        job = self.task_store.get_search_task_job(job_id)
        if job is None:
            return None

        task = self.task_store.get_task(job.task_id)
        if task is None:
            return self.task_store.update_search_task_job(
                job_id,
                SearchJobStatus.FAILED,
                "Task not found",
            )

        self.task_store.update_search_task_job(job_id, SearchJobStatus.RUNNING)
        try:
            self.run_search_task(task.id, job.depth)
            return self.task_store.update_search_task_job(
                job_id,
                SearchJobStatus.COMPLETED,
            )
        except Exception as exc:
            return self.task_store.update_search_task_job(
                job_id,
                SearchJobStatus.FAILED,
                str(exc),
            )
