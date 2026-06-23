"""Watch / monitoring concern of ResearchService (standing watches that re-run on a schedule
and flag when the answer materially changes), extracted as a mixin (AUD-030). Composed into
ResearchService; run_due_watches uses self.broker (sweep lock) + self.task_store and helpers
(self._parse_iso) that remain on ResearchService, via composition.
"""
import logging
from datetime import datetime, timedelta, timezone

from src.domain.errors import NotFoundError

from src.config import settings
from src.domain import *  # noqa: F401,F403

logger = logging.getLogger(__name__)


class WatchMixin:
    def set_research_watch(self, research_id: str, enabled: bool, interval_seconds: int | None) -> ResearchWatch:
        """Enable/disable a standing watch. When enabled, the worker sweep re-runs this
        question every interval and flags a change if the answer materially shifts."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        state = dict(research.graph_state or {})
        watch = dict(state.get("watch") or {})
        if enabled:
            interval = interval_seconds or watch.get("interval_seconds") or settings.watch_default_interval_seconds
            interval = max(settings.watch_min_interval_seconds, int(interval))
            now = datetime.now(timezone.utc)
            watch.update(
                enabled=True,
                interval_seconds=interval,
                next_run_at=(now + timedelta(seconds=interval)).isoformat(),
            )
            watch.setdefault("runs", 0)
        else:
            watch["enabled"] = False
            watch["next_run_at"] = ""
        state["watch"] = watch
        self.task_store.update_research_graph_state(research_id, state)
        return self._watch_view(research_id, watch)

    def get_research_watch(self, research_id: str) -> ResearchWatch:
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        return self._watch_view(research_id, (research.graph_state or {}).get("watch") or {})

    def acknowledge_research_watch(self, research_id: str) -> ResearchWatch:
        """Dismiss the 'answer changed' badge (mark the latest change as seen)."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise NotFoundError("Research not found")
        state = dict(research.graph_state or {})
        watch = dict(state.get("watch") or {})
        watch["acknowledged_at"] = watch.get("last_change_at") or datetime.now(timezone.utc).isoformat()
        state["watch"] = watch
        self.task_store.update_research_graph_state(research_id, state)
        return self._watch_view(research_id, watch)

    def _watch_view(self, research_id: str, watch: dict) -> ResearchWatch:
        last_change = watch.get("last_change_at") or ""
        ack = watch.get("acknowledged_at") or ""
        return ResearchWatch(
            research_id=research_id,
            enabled=bool(watch.get("enabled")),
            interval_seconds=int(watch.get("interval_seconds") or 0),
            next_run_at=watch.get("next_run_at") or "",
            last_run_at=watch.get("last_run_at") or "",
            last_change_at=last_change,
            acknowledged_at=ack,
            runs=int(watch.get("runs") or 0),
            has_unseen_change=bool(last_change) and last_change > ack,
        )

    def run_due_watches(self) -> int:
        """Fire a re-run for every watch whose next_run_at has passed. Single-flight across
        workers via a short Redis lock so a watch never double-fires; called each maintenance
        tick. The watch migrates to the new run (the thread head)."""
        if not settings.watch_enabled:
            return 0
        if self.broker and not self.broker.try_acquire_lock("watch_sweep", settings.watch_sweep_lock_ttl_seconds):
            return 0  # another worker owns this window
        try:
            watch_ids = self.task_store.list_active_watch_research_ids()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("watch_list_failed error=%s", exc)
            return 0
        now = datetime.now(timezone.utc)
        fired = 0
        for rid in watch_ids:
            try:
                research = self.task_store.get_research(rid)
                watch = ((research.graph_state if research else None) or {}).get("watch") or {}
                if not watch.get("enabled"):
                    continue
                due_at = self._parse_iso(watch.get("next_run_at"))
                if due_at is None or due_at > now:
                    continue
                if research.status not in (ResearchStatus.COMPLETED, ResearchStatus.FAILED):
                    continue  # previous run still in flight — retry next sweep
                fired += self._trigger_watch(research, dict(watch))
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("watch_trigger_failed research_id=%s error=%s", rid, exc)
        return fired

    def _trigger_watch(self, head, watch: dict) -> int:
        """Re-run the watched question and move the watch onto the new run."""
        head_id = head.id
        now = datetime.now(timezone.utc)
        interval = max(
            settings.watch_min_interval_seconds,
            int(watch.get("interval_seconds") or settings.watch_default_interval_seconds),
        )
        # Disable the watch on the old head FIRST, so a crash mid-trigger can't double-fire it.
        old_state = dict((self.task_store.get_research(head_id).graph_state) or {})
        old_watch = dict(old_state.get("watch") or {})
        old_watch["enabled"] = False
        old_watch["next_run_at"] = ""
        old_state["watch"] = old_watch
        self.task_store.update_research_graph_state(head_id, old_state)

        _resp, new_id, request = self.refresh_research(head_id, user_id=getattr(head, "user_id", None))
        import threading as _threading
        _threading.Thread(
            target=self.decompose_and_enqueue, args=(new_id, request),
            daemon=True, name=f"watch-{new_id[:8]}",
        ).start()

        new_state = dict((self.task_store.get_research(new_id).graph_state) or {})
        new_state["watch"] = {
            "enabled": True,
            "interval_seconds": interval,
            "next_run_at": (now + timedelta(seconds=interval)).isoformat(),
            "last_run_at": now.isoformat(),
            "last_change_at": watch.get("last_change_at") or "",
            "acknowledged_at": watch.get("acknowledged_at") or "",
            "runs": int(watch.get("runs") or 0) + 1,
        }
        self.task_store.update_research_graph_state(new_id, new_state)
        logger.info("watch_fired head_id=%s new_id=%s", head_id, new_id)
        return 1

