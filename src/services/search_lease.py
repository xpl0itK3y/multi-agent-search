"""A search runner's view of the task store: its task writes land only under its job's lease.

SearchAgent writes its task many times during a run (RUNNING, log lines, the results, then
COMPLETED or FAILED). A runner that stale recovery or a requeue took the job from must not
keep doing that next to the runner that has it now, so every task write goes through
update_task_under_search_lease, which checks the lease in the same transaction. The first
refused write raises SearchJobLeaseLost and ends the stale run.

The lease travels in a context variable rather than as an argument: run_search_task keeps its
(task_id, depth) signature, which tests and inline callers replace or call directly.
"""

from contextlib import contextmanager
from contextvars import ContextVar

from src.domain import SearchJobLeaseLost, SearchTask, TaskUpdate
from src.repositories.protocols import TaskStore

_current_lease: ContextVar[tuple[str, int] | None] = ContextVar("search_job_lease", default=None)


@contextmanager
def holding_search_lease(job_id: str, lease_epoch: int | None):
    """While inside, search task writes in this context are fenced by (job_id, lease_epoch);
    None (an unclaimed job) leaves them unfenced."""
    token = _current_lease.set((job_id, lease_epoch) if lease_epoch is not None else None)
    try:
        yield
    finally:
        _current_lease.reset(token)


def store_for_search(store: TaskStore):
    """The store a search run in this context writes through."""
    lease = _current_lease.get()
    return LeasedTaskStore(store, *lease) if lease is not None else store


class LeasedTaskStore:
    def __init__(self, store: TaskStore, job_id: str, lease_epoch: int) -> None:
        self._store = store
        self._job_id = job_id
        self._lease_epoch = lease_epoch

    def update_task(self, task_id: str, update: TaskUpdate, user_id: str | None = None) -> SearchTask:
        # user_id scoping is for API reads; a runner writes its own job's task.
        task = self._store.update_task_under_search_lease(task_id, update, self._job_id, self._lease_epoch)
        if task is None:
            raise SearchJobLeaseLost(self._job_id)
        return task

    def __getattr__(self, name: str):
        # Reads and every other write (progress events, the search cache) pass through.
        return getattr(self._store, name)
